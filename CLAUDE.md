# `billing-engine` — agent guide

The public-source billing authority for MirrorStack. Public source, not a public
endpoint: `api-platform` calls it over private RPC, and
[`README.md#runtime-and-trust-boundary`](README.md#runtime-and-trust-boundary)
owns that boundary with its citations.

> 🔴 **`docs/DESIGN.md` is a specification, not production.** `main` still
> carries direct Stripe paths outside the intent, notice, and authorization
> boundary. Never present a target document as a current guarantee. Overstating
> the code is a defect of the same class as a code bug, and
> [`SECURITY.md`](SECURITY.md) treats it that way.
>
> 🔴 **Automatic merge and promotion are paused.** Work on a branch, keep manual
> billing and security review, and never enable collection because CI went green.
>
> Defects are enumerated in one place,
> [`SECURITY.md#known-current-gaps`](SECURITY.md#known-current-gaps), and nothing
> starts a second list. This repo is v1; re-derive anything taken from the v0
> attempt at `mirrorstack-ai/billing-engine-old`.

## Layout

Seven binaries. Six are built and uploaded per commit
(`.github/workflows/publish.yml:35-41`). `pm-default-backfill` is not, and is run
by hand.

```text
cmd/account-api/                  internal RPC Lambda; local HTTP :8091   (published)
cmd/account-webhook/              Stripe HTTPS receiver, checks signature (published)
cmd/account-webhook-eventbridge/  Stripe partner EventBridge consumer     (published)
cmd/billing-cycle/                scheduled per-period usage charge driver (published)
cmd/infra-egress-sync/            pulls CDN egress totals from Cloudflare (published)
cmd/infra-ssr-compute-sync/       pulls SSR compute totals from CloudWatch (published)
cmd/pm-default-backfill/          one-shot Stripe default-PM repair       (by hand)
internal/{account,billingperiod,shared}/   195 Go files
migrations/billing/               ms_billing schema, 001..052 up/down (040 unused)
```

## Schema source of truth

`migrations/billing/` is authoritative. The `ms_billing` schema is documented
under `db/ms_billing/` in
[`mirrorstack-ai/mirrorstack-docs`](https://github.com/mirrorstack-ai/mirrorstack-docs).
If `tables.md` there disagrees with a migration, the migration wins and the doc
is the bug.

## Two secrets, not one

Both gate local HTTP from `internal/shared/auth/internal_secret.go`, and both
fail closed: an unset secret returns 503 (`:59`) rather than opening the route.

- `X-MS-Internal-Secret` (`:80`) gates every control-plane RPC route
  (`cmd/account-api/main.go:652`). `/__health` is the one route outside it (`:647`).
- `X-MS-Meter-Secret` (`:81`) gates `RecordUsage` alone
  (`cmd/account-api/main.go:775-778`), so the meter credential rotates on its own.

Production differs, and the difference is a filed gap: the dispatch role can
invoke the whole account-api Lambda. Never describe the metering seam as
dedicated.

## Hard rules

- **Every table this repo owns lives in `ms_billing.*`. Never write outside it.**
  `owner_user_id` and `owner_org_id` are soft FKs
  (`migrations/billing/001_init.up.sql:25-26`); nothing reads `ms_account`
  (`internal/account/db/queries/cycle.sql:375-376`).
- **`api-platform` must never touch Stripe.** Every Stripe API call belongs in
  this repo, behind `internal/shared/stripe/client.go`.
- **`account-api` must never become customer-reachable.** Its only
  unauthenticated route is the static health body at
  `cmd/account-api/main.go:647`. Provider ingress stays in the webhook binaries.
- **`api-platform` initiates account RPCs.** The engine must not call the browser
  or push into the customer-facing account API. State is pulled by authenticated
  read, or delivered by `internal/account/standing/notifier.go`.
- **Do not widen the infrastructure charge path.** It is already a customer
  charge kind, against [INV-010](docs/DESIGN.md#inv-010), and
  [`SECURITY.md#known-current-gaps`](SECURITY.md#known-current-gaps) holds the
  detail. Another caller makes the release blocker worse.
- **Do not call the deployment intent-only** until the `Capabilities` action
  (unbuilt) reports `legacyMoneyPaths: 0` and legacy provider credentials are
  revoked. The weakest reachable legacy path defines the real guarantee.

## Where the rules actually live

Link, never restate. [`docs/DESIGN.md`](docs/DESIGN.md) owns every invariant:
[§3 INV-001..INV-014](docs/DESIGN.md#3--what-must-be-true-before-any-money-moves),
[the execution predicate](docs/DESIGN.md#executechargeintent),
[§5 ports and permits](docs/DESIGN.md#5--paying-and-what-happens-when-the-answer-never-comes),
[§9 the credential enclave](docs/DESIGN.md#9--where-the-provider-credential-lives),
[§11 the readiness gate](docs/DESIGN.md#11--getting-from-here-to-there).
[`SECURITY.md#adversary-model`](SECURITY.md#adversary-model) owns adversaries,
assumptions, and limits. [`docs/VERIFICATION.md`](docs/VERIFICATION.md) owns
evidence levels and the charge bundle, and
[its §5](docs/VERIFICATION.md#5--what-ci-enforces-against-this-tree) owns the
checks CI should grow. [`README.md`](README.md) owns the five money flows.

## Commit identity

Commit as **Sheng Kun Chang <nothingchang@mirrorstack.ai>**, or the local
`sheng-kun-chang@mirrorstack.ai`. Never as `mirrorstack-ops[bot]` — override it:

```bash
git config --local user.name "Sheng Kun Chang"
git config --local user.email "nothingchang@mirrorstack.ai"
```

## When you edit this repo

1. **Branch off `main`** — `git checkout -b <type>/<slug>`, type being `feat`,
   `fix`, `chore`, `docs`, or `refactor`.
2. **Make the change.** Touching the schema means a matching
   `mirrorstack-docs/db/ms_billing/` update in the same PR cycle.
3. **Commit prefix** `feat:` / `fix:` / `chore:` / `docs:` / `refactor:`.
   Co-author tail: `Co-Authored-By: Claude Opus 5 (1M context) <noreply@anthropic.com>`.
4. **Open a PR against `main`.** Never push directly to `main`, and never
   auto-merge. Add **`Closes #N`** when a tracking issue exists.

After a merge to `main`, a successful terminal `Publish` run triggers
`.github/workflows/notify-core-pointer.yml`, which opens a pointer PR in
`mirrorstack-ai/mirrorstack-core-v2`. Do not hand-edit the core gitlink.

## Cross-repo coordination

A schema change spans two repos, then the parent, in one cycle.

1. `billing-engine/` — write the migration, open a PR.
2. `mirrorstack-docs/` — update `db/ms_billing/{README,tables,migrations}.md`.
3. `MirrorStack-AI-V2/` — bump the submodule pointer once both child PRs merge.

## Do not put here

- Stripe fixtures or mocks holding real keys. Those stay in `.env.local`.
- Frontend or `web-*` UI code — `web-account/` or `web-applications/` owns it.
- Schema docs for shipped state. Those graduate to `mirrorstack-docs/`.
- A second copy of any rule DESIGN, SECURITY, or README already owns.

## Quickstart

```bash
make db         # boot Postgres 17 on localhost:5432
make db-init    # apply migrations/billing/
make lint       # go vet ./...
make build      # go build ./...
make test       # unit tests, no external calls
```

`make test-integration` needs a reachable Docker daemon, and not the `make db`
instance. Each test boots its own ephemeral Postgres 17 container and skips when
Docker is unreachable (`internal/shared/testutil/db.go:34-58`).

Lambda-capable binaries switch on `AWS_LAMBDA_FUNCTION_NAME` and otherwise run
local HTTP (`internal/shared/config/config.go:40`).

- `cd cmd/account-api && go run .` — `:8091`, `ACCOUNT_API_PORT` overrides
  (`cmd/account-api/main.go:860`).
- `make dev-webhook` — `:8092`, `ACCOUNT_WEBHOOK_PORT` overrides
  (`cmd/account-webhook/main.go:51`). Pair with
  `stripe listen --forward-to localhost:8092/webhook`.
- `make dev-cycle`, `make dev-egress-sync`, `make dev-ssr-compute-sync` — one-shot
  runs of the three scheduled workers.
