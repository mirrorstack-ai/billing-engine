// Package store is the durable owner of sealed intent state.
//
// It is deliberately the only package that writes migration 054's
// tables. docs/DESIGN.md's counter-example is a decision spread across
// seven call sites; the same failure applies to writes, and a second
// writer is how an invariant enforced in one place stops being an
// invariant.
//
// Two things it does NOT do, both on purpose:
//
// It holds no provider client. A store that could reach Stripe would be
// a place where reading state and changing money meet, which is the
// shape docs/SECURITY.md §2 records as the capability gap.
//
// It makes no decisions. Whether an intent may execute is
// internal/intent/predicate's single question, over state assembled by
// a caller. This package answers "what is stored", never "what is
// allowed" — so a bug here can lose or corrupt a record, but it cannot
// authorise a charge.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mirrorstack-ai/billing-engine/internal/intent"
	"github.com/mirrorstack-ai/billing-engine/internal/intent/evidence"
)

// Store reads and writes sealed intent state.
type Store struct {
	pool *pgxpool.Pool
}

// New returns a Store over the given pool.
func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Errors callers distinguish.
var (
	ErrNotFound       = errors.New("store: no such intent")
	ErrAlreadyClaimed = errors.New("store: this intent is already claimed")
	ErrStateChanged   = errors.New("store: the intent was not in the expected state")
)

// SaveIntent writes a sealed intent, its lines and its source facts in
// one transaction.
//
// All three or none. A total with no lines behind it is a number nobody
// can check, and lines with no intent are orphans — either way the
// charge stops being answerable, which is the property the whole
// document exists to have.
//
// Re-saving the same intent is a no-op rather than an error. The digest
// is the identity of the content, so a duplicate save is the same
// document arriving twice, and a retrying caller should not have to
// tell the difference.
func (s *Store) SaveIntent(ctx context.Context, sealed intent.ChargeIntent) error {
	return s.saveIntent(ctx, sealed, nil, evidence.Event{})
}

// SaveIntentWithEvidence stores a sealed intent AND its INV-014 record, in one
// transaction.
//
// This is the path a proposer takes. Sealing an intent is the first of
// docs/DESIGN.md:388's eight events, and it is the only one reachable in
// production today — so it is the one that decides whether the outbox is a
// table with writers or a table with a comment about writers.
//
// Committing them together is the point: an intent that exists without its
// evidence record is a document the customer has no independent trace of, and
// no later reconciler can distinguish "never recorded" from "recorded and
// withheld".
func (s *Store) SaveIntentWithEvidence(
	ctx context.Context,
	sealed intent.ChargeIntent,
	rec *evidence.Recorder,
	e evidence.Event,
) error {
	if rec == nil {
		return ErrNoRecorder
	}
	return s.saveIntent(ctx, sealed, rec, e)
}

func (s *Store) saveIntent(
	ctx context.Context,
	sealed intent.ChargeIntent,
	rec *evidence.Recorder,
	e evidence.Event,
) error {
	if !sealed.Sealed() {
		return errors.New("store: refusing to save an unsealed intent")
	}

	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		return insertIntentTx(ctx, tx, sealed, rec, e)
	})
}

// insertIntentTx is the whole of one intent's persistence, on a caller's
// transaction: the row, its lines, its source facts and its evidence record.
//
// Split out of saveIntent so that a GROUP of intents and the grouping itself
// can commit together. A group written in a second transaction is a group that
// a crash can lose, and a lost grouping is not a missing convenience — the
// executor's PendingExecutionGrouped returns ungrouped intents as groups of
// ONE, so two boundary halves that lost their grouping are collected as two
// separate invoices, with two roundings. That is precisely the divergence the
// two-intent boundary exists to avoid, and it would appear as a few cents, not
// as an error.
func insertIntentTx(
	ctx context.Context,
	tx pgx.Tx,
	sealed intent.ChargeIntent,
	rec *evidence.Recorder,
	e evidence.Event,
) error {
	if !sealed.Sealed() {
		return errors.New("store: refusing to save an unsealed intent")
	}

	notBefore, notAfter := sealed.ExecutionWindow()
	tax := sealed.Tax()
	payer := sealed.Payer()

	{
		tag, err := tx.Exec(ctx, `
			INSERT INTO ms_billing.charge_intents
			  (digest, payer_kind, payer_id, currency, kind, price_book_revision,
			   terms_revision, notice_policy, tax_jurisdiction, tax_rule_revision,
			   tax_amount_micros, tax_verification, subtotal_micros, total_micros,
			   wallet_allocation_micros, provider_remainder_micros,
			   selected_rail, routing_policy_revision,
			   authorization_id, execute_not_before, execute_not_after,
			   supersedes_digest, collects_digest)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,
			        NULLIF($22, ''), NULLIF($23, ''))
			ON CONFLICT (digest) DO NOTHING`,
			sealed.Digest(), payer.Kind, payer.ID, sealed.Currency(), string(sealed.Kind()),
			sealed.PriceBookRevision(), sealed.TermsRevision(), sealed.NoticePolicy(),
			tax.Jurisdiction, tax.RuleRevision, tax.AmountMicros, string(tax.Verification),
			sealed.SubtotalMicros(), sealed.TotalMicros(),
			sealed.WalletAllocationMicros(), sealed.ProviderRemainderMicros(),
			sealed.SelectedRail(), sealed.RoutingPolicyRevision(),
			sealed.AuthorizationID(),
			notBefore, notAfter, sealed.Supersedes(), sealed.Collects(),
		)
		if err != nil {
			return fmt.Errorf("insert intent: %w", err)
		}
		if tag.RowsAffected() == 0 {
			// Already stored. The digest covers every sealed field, so
			// the existing row is this document — there is nothing to
			// reconcile and nothing to overwrite.
			//
			// The evidence record is still attempted, and its own
			// uniqueness constraint makes that idempotent too. A first
			// attempt that stored the intent and then failed before its
			// record would otherwise leave the intent permanently
			// without one, since every retry would return here.
			if rec != nil {
				return appendEvidence(ctx, tx, rec, e)
			}
			return nil
		}

		for i, line := range sealed.Lines() {
			if _, err := tx.Exec(ctx, `
				INSERT INTO ms_billing.charge_intent_lines
				  (intent_digest, line_index, meter, module, module_version,
				   quantity, unit_price_micros, amount_micros)
				VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
				sealed.Digest(), i, line.Meter, line.Module, line.ModuleVersion,
				line.Quantity, line.UnitPriceMicros, line.AmountMicros(),
			); err != nil {
				return fmt.Errorf("insert line %d: %w", i, err)
			}
		}

		for _, key := range sealed.SourceFactKeys() {
			if _, err := tx.Exec(ctx, `
				INSERT INTO ms_billing.charge_intent_source_facts
				  (intent_digest, idempotency_key)
				VALUES ($1,$2)`, sealed.Digest(), key,
			); err != nil {
				return fmt.Errorf("insert source fact %q: %w", key, err)
			}
		}

		if rec != nil {
			return appendEvidence(ctx, tx, rec, e)
		}
		return nil
	}
}

// SaveIntentGroupWithEvidence stores several intents AND the grouping that
// makes them one collection, in a single transaction.
//
// This is what a boundary proposes through. The period boundary seals two
// intents — the closed period's usage arrears and the next period's
// subscription — and they must reach the executor as a group or not at all:
// PendingExecutionGrouped treats an ungrouped intent as a group of one, so a
// half-written boundary is collected as two invoices with two roundings, which
// is a silent few cents of divergence from what the legacy path took.
//
// The whole set commits or none of it does. Nothing partial is a state the
// executor can see.
func (s *Store) SaveIntentGroupWithEvidence(
	ctx context.Context,
	groupID string,
	sealed []intent.ChargeIntent,
	rec *evidence.Recorder,
	events []evidence.Event,
) error {
	if rec == nil {
		return ErrNoRecorder
	}
	if groupID == "" {
		return errors.New("store: refusing to group intents under an empty id")
	}
	if len(sealed) != len(events) {
		return fmt.Errorf("store: %d intents but %d evidence events", len(sealed), len(events))
	}
	if len(sealed) == 0 {
		return nil
	}

	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		for i, in := range sealed {
			if err := insertIntentTx(ctx, tx, in, rec, events[i]); err != nil {
				return fmt.Errorf("group member %d (%s): %w", i, in.Digest(), err)
			}
			if _, err := tx.Exec(ctx, `
				INSERT INTO ms_billing.intent_groups (intent_digest, group_id)
				VALUES ($1, $2)
				ON CONFLICT (intent_digest) DO NOTHING`, in.Digest(), groupID); err != nil {
				return fmt.Errorf("group intent %s: %w", in.Digest(), err)
			}
		}
		return nil
	})
}

// LoadIntent reads an intent back and verifies it against its digest.
//
// The verification is intent.Rehydrate's, not this package's: every
// stored field is fed through the same canonical encoding and the
// result compared with the stored digest. A row edited in place, a
// restored backup, or a column rewritten by a migration in passing all
// fail to load rather than being charged against.
func (s *Store) LoadIntent(ctx context.Context, digest string) (intent.ChargeIntent, error) {
	var (
		stored     intent.Stored
		kind       string
		supersedes *string
		collects   *string
	)
	stored.Digest = digest

	// Read back as a plain string and convert, so an unknown value from the
	// database becomes an unsealable class rather than being trusted. The
	// column has a CHECK, but the Go side is the authority on the vocabulary
	// and re-Seal is what actually enforces it.
	var taxVerification string

	err := s.pool.QueryRow(ctx, `
		SELECT payer_kind, payer_id, currency, kind, price_book_revision, terms_revision,
		       notice_policy, tax_jurisdiction, tax_rule_revision, tax_amount_micros,
		       tax_verification, subtotal_micros, total_micros,
		       wallet_allocation_micros, provider_remainder_micros,
		       selected_rail, routing_policy_revision, authorization_id,
		       execute_not_before, execute_not_after, supersedes_digest,
		       collects_digest
		  FROM ms_billing.charge_intents
		 WHERE digest = $1`, digest,
	).Scan(
		&stored.Payer.Kind, &stored.Payer.ID, &stored.Currency, &kind,
		&stored.PriceBookRevision, &stored.TermsRevision, &stored.NoticePolicy,
		&stored.Tax.Jurisdiction, &stored.Tax.RuleRevision, &stored.Tax.AmountMicros,
		&taxVerification, &stored.SubtotalMicros, &stored.TotalMicros,
		&stored.WalletAllocationMicros, &stored.ProviderRemainderMicros,
		&stored.SelectedRail, &stored.RoutingPolicyRevision, &stored.AuthorizationID,
		&stored.ExecuteNotBefore, &stored.ExecuteNotAfter, &supersedes, &collects,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return intent.ChargeIntent{}, fmt.Errorf("%w: %s", ErrNotFound, digest)
	}
	if err != nil {
		return intent.ChargeIntent{}, fmt.Errorf("load intent: %w", err)
	}
	stored.Kind = intent.ChargeKind(kind)
	if supersedes != nil {
		stored.Supersedes = *supersedes
	}
	// collects is inside the digest, so a receivable that loses this link
	// rehydrates as a different document and refuses to load. It had no
	// column at all until migration 063.
	if collects != nil {
		stored.Collects = *collects
	}
	// A stored row always carries a determination; the column is NOT
	// NULL. Resolved is set here rather than stored, because storing it
	// would allow a row asserting "unresolved but priced".
	stored.Tax.Resolved = true
	// The class is stored, NOT defaulted. Defaulting it here would let a row
	// that never stated one reload as though it had — the substitution the
	// column exists to prevent. A value the Go side does not recognise fails
	// the re-Seal below, which is the correct direction to fail in.
	stored.Tax.Verification = intent.TaxVerificationClass(taxVerification)

	rows, err := s.pool.Query(ctx, `
		SELECT meter, module, module_version, quantity, unit_price_micros
		  FROM ms_billing.charge_intent_lines
		 WHERE intent_digest = $1
		 ORDER BY line_index`, digest)
	if err != nil {
		return intent.ChargeIntent{}, fmt.Errorf("load lines: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var l intent.Line
		if err := rows.Scan(&l.Meter, &l.Module, &l.ModuleVersion,
			&l.Quantity, &l.UnitPriceMicros); err != nil {
			return intent.ChargeIntent{}, fmt.Errorf("scan line: %w", err)
		}
		stored.Lines = append(stored.Lines, l)
	}
	if err := rows.Err(); err != nil {
		return intent.ChargeIntent{}, fmt.Errorf("read lines: %w", err)
	}

	factRows, err := s.pool.Query(ctx, `
		SELECT idempotency_key
		  FROM ms_billing.charge_intent_source_facts
		 WHERE intent_digest = $1
		 ORDER BY idempotency_key`, digest)
	if err != nil {
		return intent.ChargeIntent{}, fmt.Errorf("load source facts: %w", err)
	}
	defer factRows.Close()
	for factRows.Next() {
		var key string
		if err := factRows.Scan(&key); err != nil {
			return intent.ChargeIntent{}, fmt.Errorf("scan source fact: %w", err)
		}
		stored.SourceFactKeys = append(stored.SourceFactKeys, key)
	}
	if err := factRows.Err(); err != nil {
		return intent.ChargeIntent{}, fmt.Errorf("read source facts: %w", err)
	}

	return intent.Rehydrate(stored)
}

// ClaimSettlement takes the exclusive right to settle an intent.
//
// INV-008: one intent settles at most once, across all providers. The
// claim is a row whose primary key is the digest, so exclusivity is the
// database's answer rather than a check-then-act this code could lose.
// Two executors racing produce one winner and one ErrAlreadyClaimed,
// with no window between them.
//
// A claim is never released here. docs/DESIGN.md §4: missing or
// ambiguous prior evidence RETAINS the claim, because releasing it is
// exactly what would let a second attempt begin against an outcome
// nobody has established.
func (s *Store) ClaimSettlement(ctx context.Context, digest, claimedBy string) error {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO ms_billing.intent_settlement_claims (intent_digest, claimed_by)
		VALUES ($1, $2)
		ON CONFLICT (intent_digest) DO NOTHING`, digest, claimedBy)
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23503" {
			return fmt.Errorf("%w: %s", ErrNotFound, digest)
		}
		return fmt.Errorf("claim settlement: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", ErrAlreadyClaimed, digest)
	}
	return nil
}

// RecordOutcome closes a claim with a terminal answer.
//
// Only a claim with no outcome may be given one. A second call is
// refused rather than overwriting, because an intent that settled and
// then "settled differently" is a record nobody can reason about.
func (s *Store) RecordOutcome(ctx context.Context, digest, outcome string, at time.Time) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE ms_billing.intent_settlement_claims
		   SET outcome = $2, outcome_at = $3
		 WHERE intent_digest = $1 AND outcome IS NULL`, digest, outcome, at)
	if err != nil {
		return fmt.Errorf("record outcome: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s already has an outcome, or is unclaimed", ErrStateChanged, digest)
	}
	return nil
}

// AdvanceState moves an intent from one lifecycle state to another.
//
// The expected current state is part of the WHERE clause, so a
// transition made against stale knowledge fails instead of silently
// overwriting a state something else set.
//
// The trigger on the table permits state and state_changed_at to change
// and freezes every other column, including ones added after it was
// written. That last part became true only in migration 063: 054's
// version compared a hardcoded 17-column tuple, so the five sealed
// columns migrations 060-062 added were editable in place while this
// comment claimed otherwise.
func (s *Store) AdvanceState(ctx context.Context, digest, from, to string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE ms_billing.charge_intents
		   SET state = $3, state_changed_at = now()
		 WHERE digest = $1 AND state = $2`, digest, from, to)
	if err != nil {
		return fmt.Errorf("advance state: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s is not in state %q", ErrStateChanged, digest, from)
	}
	return nil
}

// State reads an intent's current lifecycle state.
func (s *Store) State(ctx context.Context, digest string) (string, error) {
	var state string
	err := s.pool.QueryRow(ctx,
		`SELECT state FROM ms_billing.charge_intents WHERE digest = $1`, digest).Scan(&state)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", fmt.Errorf("%w: %s", ErrNotFound, digest)
	}
	if err != nil {
		return "", fmt.Errorf("read state: %w", err)
	}
	return state, nil
}
