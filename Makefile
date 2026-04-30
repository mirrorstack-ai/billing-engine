.PHONY: dev dev-account-api dev-account-webhook db db-init db-reset test test-integration lint build

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

# Run account-api Lambda locally
dev-account-api:
	STRIPE_SECRET_KEY=$${STRIPE_SECRET_KEY:-sk_test_placeholder} \
	go run ./cmd/account-api

# Run account-webhook Lambda locally (use `stripe listen --forward-to ...`)
dev-account-webhook:
	STRIPE_SECRET_KEY=$${STRIPE_SECRET_KEY:-sk_test_placeholder} \
	STRIPE_WEBHOOK_SECRET=$${STRIPE_WEBHOOK_SECRET:-whsec_placeholder} \
	go run ./cmd/account-webhook

dev:
	@echo "Run in separate terminals:"
	@echo "  make dev-account-api       (port 8090)"
	@echo "  make dev-account-webhook   (port 8091)"

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
