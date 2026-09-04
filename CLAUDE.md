# `billing-engine` — agent guide

The public-source billing authority for MirrorStack. Public source, not a public
endpoint: `api-platform` calls it over private RPC, and
[`README.md#runtime-and-trust-boundary`](README.md#runtime-and-trust-boundary)
owns that boundary with its citations.

> 🔴 **`docs/DESIGN.md` is a specification, not production.** Every collecting
> leg now only proposes a sealed `ChargeIntent`; `cmd/intent-executor` is the
> only collector and it refuses to start
> (`capabilities.LegacyMoneyPaths` is **3**, not 0). What remains outside the
> intent boundary is three paths, and none of them is a collector: two finish a
> charge a legacy run already put in front of the provider, and the third is a
> scan false positive on billing's own `PayInvoice`. They drain rather than being
> deleted — `internal/account/capabilities/capabilities.go` says how, and
> `scripts/legacy-drop-preconditions.sql` measures when they are done.
> Never present a target document as a current guarantee. Overstating the code is
> a defect of the same class as a code bug, and [`SECURITY.md`](SECURITY.md)
> treats it that way.
>
> 🔴 **Collection stays off until the owner enables it — never because CI went
> green.** Work on a branch; every PR gets a manual billing and security review by
> the merging reviewer before it merges. Green checks are a precondition, not the
> review (owner rule 2026-08-17, reaffirmed 2026-09-04).
>
> Defects are enumerated in one place,
> [`SECURITY.md#known-current-gaps`](SECURITY.md#known-current-gaps), and nothing
> starts a second list. This repo is v1; re-derive anything taken from the v0
> attempt at `mirrorstack-ai/billing-engine-old`.

## Layout

Ten binaries. Eight are built and uploaded per commit — the `for pair in` list in
`.github/workflows/publish.yml`. `pm-default-backfill` and `signing-keygen` are
not, and are run by hand.

```text
cmd/account-api/                  internal RPC Lambda; local HTTP :8091    (published)
cmd/account-webhook/              HTTP ingress for PSPs that cannot reach
                                  EventBridge; dispatch table empty        (published)
cmd/account-webhook-eventbridge/  Stripe partner EventBridge consumer;
                                  the ONLY path Stripe events arrive on    (published)
cmd/billing-cycle/                scheduled per-period usage charge driver (published)
cmd/infra-egress-sync/            pulls CDN egress totals from Cloudflare  (published)
cmd/infra-ssr-compute-sync/       pulls SSR compute totals from CloudWatch (published)
cmd/intent-executor/              the only collector on the intent rail;
                                  refuses to start, see the banner above   (published)
cmd/intent-shadow/                read-only shadow rater, no money path    (published)
cmd/pm-default-backfill/          one-shot Stripe default-PM repair        (by hand)
cmd/signing-keygen/               mints one signing key; PRINTS THE SEED   (by hand)
internal/{account,architecture,billingperiod,intent,
          meteringlock,provider,shared}/           324 Go files
migrations/billing/               ms_billing schema, 001..069 up/down
                                  (040 and 053 unused)
```

## Schema source of truth

`migrations/billing/` is authoritative. The `ms_billing` schema is documented
under `db/ms_billing/` in
[`mirrorstack-ai/mirrorstack-docs`](https://github.com/mirrorstack-ai/mirrorstack-docs).
If `tables.md` there disagrees with a migration, the migration wins and the doc
is the bug.

## Two secrets, not one

`internal/shared/auth` exposes two middlewares, `InternalSecret` and
`MeterSecret`, over one constant-time `secretGuard`. Both fail closed: an unset
secret returns 503 rather than opening the route.

- `InternalSecret` gates every control-plane RPC route on the local HTTP
  transport. The static health probe is the one route outside it.
- `MeterSecret` gates `RecordUsage` alone, on its own credential, so the
  high-volume metering seam rotates independently of the Stripe-touching RPCs.

**This split exists only on the local HTTP path.** Production invokes
`account-api` through Lambda, where IAM gates the call and the dispatch role
reaches the whole action dispatcher — so the metering credential is not a
narrower capability in production. That is a filed gap in
[`SECURITY.md#known-current-gaps`](SECURITY.md#known-current-gaps). Never
describe the metering seam as dedicated without saying which transport you mean.

## Hard rules

- **Every table this repo owns lives in `ms_billing.*`. Never write outside it.**
  `owner_user_id` and `owner_org_id` are soft FKs
  (`migrations/billing/001_init.up.sql:24-26`); nothing reads `ms_account`
  (`AccountCollectionFields` in `internal/account/db/queries/cycle.sql`).
- **`api-platform` must never touch Stripe.** Every Stripe API call belongs in
  this repo, behind `internal/shared/stripe/client.go`.
- **`account-api` must never become customer-reachable.** Its only
  unauthenticated route is the static health body (`health` in
  `cmd/account-api/main.go`). Provider ingress stays in the webhook binaries.
- **`api-platform` initiates account RPCs.** The engine must not call the browser
  or push into the customer-facing account API. State is pulled by authenticated
  read, or delivered by `internal/account/standing/notifier.go`.
- **Do not widen the infrastructure charge path.** Infrastructure cost already
  reaches the customer bill as a marked-up usage line — `infraMarkupNum` /
  `infraMarkupDen` in `internal/account/cycle/types.go`, snapshotted onto each
  aggregate — which is what [INV-010](docs/DESIGN.md#inv-010) says it must not
  be. (It is not a charge *kind*: the closed catalog in
  `internal/intent/catalog.go` has seven and none is infrastructure.)
  [`SECURITY.md#known-current-gaps`](SECURITY.md#known-current-gaps) holds the
  detail. Another caller makes the release blocker worse.
- **Do not call the deployment intent-only** until `Capabilities` reports
  `legacyMoneyPaths: 0` and legacy provider credentials are revoked. The action
  is built and served, at `/v1/billing.Capabilities` and through the Lambda
  dispatcher; today it reports **3**. The weakest reachable legacy path defines
  the real guarantee.
- **Never lower `LegacyMoneyPaths` by editing the scanner.** The constant is
  pinned against an AST scan of the tree (`internal/architecture`), and it falls
  only when a money path is deleted. Changing the count any other way is the
  one edit that makes every other claim in these documents untrue.

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
checks CI should grow. [`README.md`](README.md) owns the outside reader's
entry point: what can be checked against a clone, and what cannot.

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
4. **Open a PR against `main`.** Never push directly to `main`; merge only after
   the manual billing and security review above — green CI alone is not a merge.
   Add **`Closes #N`** when a tracking issue exists.

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
- 🔴 **Root-level plan, decision, status, migration-wave or session files.** No
  `*-PLAN.md`, `DECISION-*.md`, `*-GAP.md` or working note at the repository
  root, ever — and no second defect list anywhere, for the reason above.

  Two things make this a hard rule rather than tidiness. **This repository is
  public**: a working file carries production counts, owner quotes, unlaunched
  pricing, credential blast-radius maps and open legal questions, none of which
  anyone chose to publish, and none of which appears in `README.md`'s
  documentation map. And **`git rm` does not undo it**: the content stays
  world-readable at every commit and in every merged PR diff, in every fork and
  clone, permanently. Removing such a file is a rule going forward, not
  remediation.

  Write the plan outside the repository. When a decision inside it turns out to
  be durable, fold that one paragraph into `docs/DESIGN.md`, `SECURITY.md` or
  `docs/VERIFICATION.md`, which are the documents that own rules — and cite the
  symbol that makes it checkable.

## Quickstart

```bash
make db         # boot Postgres 17 on localhost:5432
make db-init    # apply migrations/billing/
make lint       # go vet ./...
make build      # go build ./...
make test       # unit tests, no external calls
```

`make test-integration` needs a reachable Docker daemon, and not the `make db`
instance. Each test boots its own ephemeral Postgres 17 container and **skips**
when Docker is unreachable (`NewTestDB` in `internal/shared/testutil/db.go`) — a
skipped package still reports `ok`, so set `REQUIRE_DOCKER=1` wherever the green
is load-bearing.

Lambda-capable binaries switch on `AWS_LAMBDA_FUNCTION_NAME` and otherwise run
local HTTP (`config.IsLambda`).

- `cd cmd/account-api && go run .` — `:8091`, `ACCOUNT_API_PORT` overrides.
- `make dev-webhook` — `:8092`, `ACCOUNT_WEBHOOK_PORT` overrides. **There is
  nothing to forward to it.** Stripe now arrives only on the EventBridge partner
  bus, consumed by `cmd/account-webhook-eventbridge`, which has no local HTTP
  mode — so `stripe listen` no longer replicates anything, and this binary's
  dispatch table is empty until a non-Stripe PSP is wired.
- `make dev-cycle`, `make dev-egress-sync`, `make dev-ssr-compute-sync` — one-shot
  runs of the three scheduled workers.
