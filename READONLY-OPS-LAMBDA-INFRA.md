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

## The one real decision

**No read-only database role exists.** `billing_svc` holds
SELECT/INSERT/UPDATE/DELETE on every table in `ms_billing`, plus
`ALTER DEFAULT PRIVILEGES`. No migration in either repo runs `CREATE ROLE` —
roles are minted by the db-bootstrap Lambda from `config.DbServices`.

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
