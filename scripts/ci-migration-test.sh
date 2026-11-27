#!/usr/bin/env bash
# Spin up a fresh Postgres, apply every migration in sequence, then run the
# full test suite against it. A failing migration stops the run before the
# rest of the suite executes. Intended to run on every pull request.
set -euo pipefail

cd "$(dirname "$0")/.."

COMPOSE_FILE=docker-compose.test.yml
PROJECT=plimsoll-migration-test
PORT="${TEST_DB_PORT:-54330}"
export TEST_DB_PORT="$PORT"

cleanup() {
	docker compose -p "$PROJECT" -f "$COMPOSE_FILE" down -v --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT

# Always start from an empty database.
cleanup
docker compose -p "$PROJECT" -f "$COMPOSE_FILE" up -d --wait

URL="postgres://plimsoll:plimsoll@localhost:${PORT}/plimsoll_test?sslmode=disable"
export TEST_DATABASE_URL="$URL"
export DATABASE_URL="$URL"

echo "==> Applying all migrations on a fresh database"
go test -count=1 -v ./internal/store -run 'TestMigrationsApplyStepByStepOnFreshDatabase|TestFreshAndExistingDatabasesEndAtTheSameSchema|TestLoadMigrations'

echo "==> Running the full test suite"
go test -count=1 -race ./...
