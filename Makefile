.PHONY: db db-init db-reset test test-integration lint build dev-webhook dev-cycle dev-egress-sync dev-ssr-compute-sync

# Start infrastructure (Postgres)
db:
	docker compose up -d postgres
	@echo "Postgres: localhost:5432"

# Initialize DB schemas + migrations (run once after first `make db`)
db-init:
	@sleep 2
	psql -h localhost -U mirrorstack -d mirrorstack -f scripts/init-db.sql

# Reset DB (drop + recreate)
db-reset:
	docker compose down -v
	docker compose up -d postgres
	@sleep 2
	psql -h localhost -U mirrorstack -d mirrorstack -f scripts/init-db.sql

# Tests (unit only — no external deps)
test:
	go test ./...

# Integration tests (require Docker — testcontainers-go boots Postgres as needed).
test-integration:
	REQUIRE_DOCKER=1 go test -tags=integration -race -count=1 ./...

# Lint
lint:
	go vet ./...

# Build
build:
	go build ./...

# Run the local-HTTP webhook receiver. It is a deliberately empty ingress:
# Stripe now arrives only on the EventBridge partner bus (consumed by
# cmd/account-webhook-eventbridge), so this binary verifies nothing, holds no
# provider credential, touches no database, and answers 501. It is kept so a
# payment provider that cannot publish to EventBridge has a URL to register
# against. `stripe listen` will not exercise anything here.
dev-webhook:
	cd cmd/account-webhook && go run .

# Run the billing charge cycle once locally (the usage/arrears leg). Derives
# the just-closed UTC calendar-month window, seals a ChargeIntent for every
# account with unbilled usage in it, then exits. It proposes; it does not
# collect. Requires DATABASE_URL + STRIPE_SECRET_KEY (use a restricted
# rk_test_* key from .env.local). Prod runs the same binary on a schedule.
dev-cycle:
	cd cmd/billing-cycle && go run .

# Run the CDN-egress puller once locally (the platform-infra egress metering
# chokepoint). Sweeps the last few CLOSED hour windows,
# queries the Cloudflare Analytics Engine "cdn_egress" dataset, and records each
# (app, module) egress total via RecordInfraUsage (idempotent on a deterministic
# event_id), then exits. Requires DATABASE_URL + CF_ANALYTICS_API_TOKEN (a
# READ-ONLY CF API token) + CF_ACCOUNT_ID. Prod runs the same binary on an
# EventBridge schedule.
dev-egress-sync:
	cd cmd/infra-egress-sync && go run .

# Run the SSR-compute puller once locally (app-hosting SSR metering).
# Enumerates the ms-apphost-*
# Lambda fleet via lambda:ListFunctions, pulls Duration/Invocations sums from
# cloudwatch:GetMetricData over the last few CLOSED hour windows, and records
# both infra.compute.ssr.gb_seconds / infra.compute.ssr.request.count via
# RecordInfraUsage (idempotent on a deterministic event_id), then exits.
# Requires DATABASE_URL (+ optional DB_AUTH) — AWS auth resolves through the
# ambient SDK credential chain (no separate secret, unlike the CF puller).
# Prod runs the same binary on an EventBridge schedule (created disabled
# pending design doc §3 Decision B / §7 Open Question 1b).
dev-ssr-compute-sync:
	cd cmd/infra-ssr-compute-sync && go run .
