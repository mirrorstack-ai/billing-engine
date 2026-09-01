package main

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// GrantRow is one answer about what the read-only ops role may actually do.
//
// Catalog answers only — role names and privilege booleans. Nothing here reads
// a billing table, which is the entire point: this action has to work when the
// grants are MISSING, because that is exactly when someone needs it.
type GrantRow struct {
	Check  string `json:"check"`
	Actual string `json:"actual"`
	Want   string `json:"want"`
	OK     bool   `json:"ok"`
}

// runGrants reports whether billing_ro holds the privileges the ops path needs.
//
// 🔴 THE DIAGNOSTIC THAT WORKS WHEN NOTHING ELSE DOES.
//
// Migrations 058 (the read-only grant set) and 064 (the REVOKE that keeps the
// INV-014 evidence outbox away from that role) are both gated on the role
// existing, and the role is created by infra's db-bootstrap — which ran AFTER
// they were applied. Both therefore took their ELSE branches, raised a NOTICE,
// exited 0, and were recorded as APPLIED having granted and revoked nothing.
//
// The failure that produces is worse than a missing role. billing_ro exists and
// holds rds_iam, so this function CONNECTS, and then every other action fails on
// its first SELECT with permission denied. A working credential with an empty
// grant set is much harder to read than an identity that is not there.
//
// So this action asks the CATALOG instead of the data. has_table_privilege and
// pg_roles do not require SELECT on the tables being asked about, which means
// this returns a real answer from exactly the broken state the other actions
// cannot survive — and it is the pre-flight for migration 068, which repairs it.
//
// Every answer is a boolean about a privilege. No account, no amount, no row of
// any billing table is read, so the result is safe to return and safe to log.
func runGrants(ctx context.Context, tx pgx.Tx) ([]GrantRow, error) {
	const role = "billing_ro"

	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, role).Scan(&exists); err != nil {
		return nil, fmt.Errorf("read pg_roles: %w", err)
	}
	rows := []GrantRow{{
		Check:  "role exists",
		Actual: boolWord(exists),
		Want:   "yes",
		OK:     exists,
	}}
	if !exists {
		// Nothing else is answerable, and saying so is the answer. Migration
		// 068 will refuse rather than skip, so this is the state that stops a
		// deploy rather than passing silently through it.
		return rows, nil
	}

	// Can it log in at all? A role with no LOGIN is a privilege bundle, not an
	// identity, and the ops function connects AS this role.
	var canLogin bool
	if err := tx.QueryRow(ctx,
		`SELECT rolcanlogin FROM pg_roles WHERE rolname = $1`, role).Scan(&canLogin); err != nil {
		return nil, fmt.Errorf("read rolcanlogin: %w", err)
	}
	rows = append(rows, GrantRow{
		Check: "can log in", Actual: boolWord(canLogin), Want: "yes", OK: canLogin,
	})

	// The read set. ms_billing.invoices stands for the schema: 058 grants ON
	// ALL TABLES, so if this one is missing the whole grant did not happen.
	for _, table := range []string{"invoices", "charge_intents"} {
		oid, err := tableOID(ctx, tx, "ms_billing", table)
		if err != nil {
			return nil, err
		}
		var ok bool
		if err := tx.QueryRow(ctx,
			`SELECT has_table_privilege($1, $2::oid, 'SELECT')`, role, oid).Scan(&ok); err != nil {
			return nil, fmt.Errorf("has_table_privilege(%s, SELECT): %w", table, err)
		}
		rows = append(rows, GrantRow{
			Check: "can SELECT ms_billing." + table, Actual: boolWord(ok), Want: "yes", OK: ok,
		})
	}

	// 🔴 The evidence outbox must NOT be readable. 058's ALTER DEFAULT
	// PRIVILEGES makes every table in the schema readable, so this is only
	// false if 064's REVOKE actually took — and re-issuing the grants without
	// the revoke is precisely how it comes back.
	evidenceOID, err := tableOID(ctx, tx, "ms_billing", "evidence_records")
	if err != nil {
		return nil, err
	}
	var evidence bool
	if err := tx.QueryRow(ctx,
		`SELECT has_table_privilege($1, $2::oid, 'SELECT')`, role, evidenceOID).Scan(&evidence); err != nil {
		return nil, fmt.Errorf("has_table_privilege(evidence_records, SELECT): %w", err)
	}
	rows = append(rows, GrantRow{
		Check: "can SELECT ms_billing.evidence_records", Actual: boolWord(evidence),
		Want: "no", OK: !evidence,
	})

	// Read-only means read-only. A role that can write is not a safer way to
	// read, and a blanket GRANT is how the write privilege comes back.
	invoicesOID, err := tableOID(ctx, tx, "ms_billing", "invoices")
	if err != nil {
		return nil, err
	}
	for _, priv := range []string{"INSERT", "UPDATE", "DELETE"} {
		var ok bool
		if err := tx.QueryRow(ctx,
			`SELECT has_table_privilege($1, $2::oid, $3)`, role, invoicesOID, priv).Scan(&ok); err != nil {
			return nil, fmt.Errorf("has_table_privilege(invoices, %s): %w", priv, err)
		}
		rows = append(rows, GrantRow{
			Check: "can " + priv + " ms_billing.invoices", Actual: boolWord(ok), Want: "no", OK: !ok,
		})
	}

	// Sequence USAGE is deliberately not granted: a read-only role that can
	// advance a sequence is not read-only.
	// pg_class rather than information_schema.sequences: the latter is a view
	// that filters on what the CURRENT user can see, so with no schema usage it
	// returns zero rows and the check passes for the wrong reason.
	// MATERIALIZED is load-bearing: without it the planner may evaluate
	// has_sequence_privilege before the relkind filter and call it on a toast
	// index, which errors rather than returning false. The fence makes the
	// filter happen first.
	var seqs int
	if err := tx.QueryRow(ctx, `
		WITH seqs AS MATERIALIZED (
			SELECT c.oid
			  FROM pg_class c
			  JOIN pg_namespace n ON n.oid = c.relnamespace
			 WHERE n.nspname = 'ms_billing' AND c.relkind = 'S'
		)
		SELECT count(*) FROM seqs WHERE has_sequence_privilege($1, oid, 'USAGE')`,
		role).Scan(&seqs); err != nil {
		return nil, fmt.Errorf("sequence privileges: %w", err)
	}
	rows = append(rows, GrantRow{
		Check: "sequences it may advance", Actual: fmt.Sprintf("%d", seqs), Want: "0", OK: seqs == 0,
	})

	return rows, nil
}

// tableOID resolves a table to its OID from the system catalogs.
//
// 🔴 BY OID, NOT BY NAME, AND THAT IS THE WHOLE TRICK.
//
// has_table_privilege('billing_ro', 'ms_billing.invoices', 'SELECT') has to
// RESOLVE that name, and resolving a qualified name requires USAGE on the
// schema. In the broken state this diagnostic exists for, billing_ro has no
// schema USAGE — so the name form fails with "permission denied for schema
// ms_billing" and the tool that is supposed to explain the problem dies of it.
//
// pg_class and pg_namespace are world-readable system catalogs. Looking the OID
// up there needs no privilege on the schema at all, and has_table_privilege's
// OID form does no name resolution. So this answers from exactly the state that
// defeats the obvious implementation.
//
// Found by the test that strips every privilege and asks for an answer anyway.
func tableOID(ctx context.Context, tx pgx.Tx, schema, table string) (uint32, error) {
	var oid uint32
	err := tx.QueryRow(ctx, `
		SELECT c.oid
		  FROM pg_class c
		  JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = $1 AND c.relname = $2`, schema, table).Scan(&oid)
	if err != nil {
		return 0, fmt.Errorf("resolve %s.%s: %w", schema, table, err)
	}
	return oid, nil
}

func boolWord(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}
