.PHONY: db db-init db-reset test test-integration lint build dev-webhook dev-cycle

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
	go test -tags=integration -race -count=1 ./...

# Lint
lint:
	go vet ./...

# Build
build:
	go build ./...

# Run the local-HTTP webhook receiver. Requires STRIPE_WEBHOOK_SECRET
# and DATABASE_URL set in the environment (load .env.local first).
# Pair with `stripe listen --forward-to localhost:8092/webhook` to
# receive real test-mode events from your Stripe sandbox.
dev-webhook:
	cd cmd/account-webhook && go run .

# Run the billing charge cycle once locally (the USAGE/arrears leg, Milestone D
# PR #6). Derives the just-closed UTC calendar-month window, charges every
# account with unbilled usage in it via Stripe, then exits. Requires
# DATABASE_URL + STRIPE_SECRET_KEY (use a restricted rk_test_* key from
# .env.local). Prod runs the same binary on an EventBridge schedule.
dev-cycle:
	cd cmd/billing-cycle && go run .
