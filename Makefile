.PHONY: db db-init db-reset test test-integration lint build

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
