.PHONY: db test test-integration lint build

# Start infrastructure (Postgres)
db:
	docker compose up -d postgres
	@echo "Postgres: localhost:5432"

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
