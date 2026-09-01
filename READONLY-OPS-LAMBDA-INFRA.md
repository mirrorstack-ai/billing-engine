# The read-only ops Lambda — the infra half

The billing-engine half is being built. This is the `mirrorstack-infra` half,
written up so the decision is one word rather than a research task.

## The good news: no DSN, no password, nothing to hand out

Production Lambdas already reach Aurora with **no secret at all**:

```
DATABASE_URL=postgres://billing_svc@<host>:5432/mirrorstack?sslmode=require
DB_AUTH=rds-iam
```

The only credential is an IAM `rds-db:connect` grant on the dbuser ARN. So
"give Claude a production DSN" was never the right shape of the question — there
is no DSN to give.

## The precedent is already deployed

`mirrorstack-migration-048-verifier` is a manually-invoked, read-only,
direct-to-Aurora Lambda:

| property | where |
|---|---|
| construct | `mirrorstack-infra/constructs/db_bootstrap.go:141` |
| wired | `mirrorstack-infra/stacks/data.go:107` |
| every query inside `BeginTx{AccessMode: pgx.ReadOnly}` | its handler |
| asserts `transaction_read_only='on'` before doing anything | its handler |
| statement / lock / idle timeouts set | its handler |
| holds **no** Secrets Manager grant | its IAM |
| invokable only by the prod deploy role, via a per-function resource policy | its IAM |
| triggered by | `.github/workflows/verify-migration-048.yml` |

Copying this shape is the whole infra change. It is not novel work.

## ✅ THE DECISION IS MADE, AND BOTH HALVES ARE BUILT — updated 2026-09-01

**Option ROLE was chosen, and it was already implemented on both sides.** What
follows below is kept because the reasoning still explains WHY, but the "one
real decision" framing is spent. Read this section first.

**The role exists.** It is not driven by `config.DbServices` — that would have
been the wrong mechanism, because every entry there mints a
`{username,password}` secret and an RDS Proxy auth registration
(`constructs/db_bootstrap.go:70-79`), while `billing_ro` uses `DB_AUTH=rds-iam`
with no password and connects DIRECT to Aurora, bypassing the proxy
(`constructs/intent_shadow_ops.go:98`). A `DbServices` entry would have created
a password secret nothing reads and a proxy registration for a connection that
does not use the proxy.

Instead `assets/db-bootstrap/main.go` has a separate password-less path with
`billing_ro` hardcoded — `ensureReadOnlyRole` runs `CREATE ROLE billing_ro WITH
LOGIN` (no password) then `GRANT rds_iam TO billing_ro`. Its own comment states
the division this document arrived at independently: *"infra owns identities,
migrations own privileges"*, and it names 058 as the grant owner. It landed in
infra #225 and is present in the deployed pin.

**The privileges did NOT exist, and that is the actual defect.** Migration 058
grants the read set and 064 revokes the evidence outbox, but both are gated on
`IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'billing_ro')`, and the role
was created AFTER they were applied. So both took their ELSE branches, raised a
NOTICE, exited 0, and were recorded as APPLIED having done nothing. A migration
is applied once, so creating the role later does not fix it.

🔴 **The failure this produces is worse than a missing role.** `billing_ro`
exists and `rds_iam` is granted, so the ops Lambda CONNECTS successfully and
then fails on every SELECT with permission denied. A working credential with an
empty grant set is a considerably more confusing thing to debug than an identity
that does not exist.

**Migration 068 repairs it** — re-issuing 058's grants AND 064's revoke, in that
order, and raising rather than skipping if the role is absent. The order is
load-bearing: `GRANT SELECT ON ALL TABLES` covers `evidence_records` because it
already exists, so re-issuing the grants alone would have exposed the INV-014
evidence outbox. Fixing one half without the other would have been worse than
leaving both broken.

**Both defences are now in place, not one.** This document framed ROLE and
TRANSACTION-ONLY as alternatives. The shipped design has both: a genuinely
read-only identity, AND `withReadOnlyTx` (`cmd/intent-shadow/readonly.go`),
which asks the server `current_setting('transaction_read_only')` and aborts on
anything but `on` — *"a flag we set and never verify is a belief, not a
guarantee"*. The transaction guard's weakness was always that it is a habit
rather than a wall; backed by a role that holds no write privilege at all, it
stops being the only thing standing between a diagnostic and the ledger.

---

## The one real decision (historical — see above)

**No read-only database role exists.** `billing_svc` holds
SELECT/INSERT/UPDATE/DELETE on every table in `ms_billing`, plus
`ALTER DEFAULT PRIVILEGES`. No migration in either repo runs `CREATE ROLE` —
roles are minted by the db-bootstrap Lambda from `config.DbServices`.

> ⚠️ Both sentences above were true when written and the first is now wrong: the
> role is created by db-bootstrap's own `ensureReadOnlyRole`, not from
> `config.DbServices`. The second remains true — no migration creates a role,
> which is exactly why 058 could skip.

### Option ROLE — a genuinely read-only identity

* new migration `055_billing_readonly_role` — `CREATE ROLE billing_ro`,
  `GRANT USAGE ON SCHEMA ms_billing`, `GRANT SELECT ON ALL TABLES`, and
  `ALTER DEFAULT PRIVILEGES ... GRANT SELECT` so future tables are covered
* `config.DbServices` gains the role so db-bootstrap mints it
* new `rds-db:connect` grant on the `billing_ro` dbuser ARN, for the ops
  function only
* **Cost:** a production DDL migration, and the migration is the thing that must
  be right — a `GRANT SELECT ON ALL TABLES` without the `ALTER DEFAULT
  PRIVILEGES` half silently fails to cover every table created afterwards.
* **Benefit:** the function *cannot* write, as a property of the credential.
  A bug, a bad query, or a future edit cannot change that.

### Option TRANSACTION-ONLY — `billing_svc` inside a read-only transaction

* no migration, no new role, no DDL against production
* the function connects as `billing_svc` and opens
  `BeginTx{AccessMode: pgx.ReadOnly}`, then asserts `transaction_read_only='on'`
  and refuses to continue if it is not — exactly what the 048 verifier does
* **Cost:** read-only is enforced by *the code*, not by the credential. The
  identity retains full DML; a future edit that opens a second connection, or
  forgets the transaction, can write. The guard is a habit, not a wall.
* **Benefit:** ships now, touches no production schema, and is already proven in
  this codebase.

## Recommendation

**TRANSACTION-ONLY first, ROLE second.** The 048 verifier has been running this
way against production, so it is the option with evidence behind it, and it
needs no production DDL to answer the questions currently blocking the legacy
drop. Then land ROLE as a follow-up, because "cannot write" should eventually be
a property of the credential rather than of a code path someone has to keep
getting right.

Taking ROLE first inverts that: it puts a production DDL migration on the
critical path of a diagnostic that is trying to tell you whether production is
safe to change.

## The risk that is NOT about writes

The shadow report prints per-account UUIDs and money figures to stdout. Under
Lambda, stdout is CloudWatch — one-week retention, readable by anyone with logs
access and **without** `lambda:InvokeFunction`. So the log is a wider audience
than the invoke permission implies.

The billing-engine change under way returns the detailed report as the function
*result* and puts only aggregate counters in the log. That has to hold whichever
option above you pick — it is independent of how the connection authenticates.
