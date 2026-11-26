# Contributing to Plimsoll indexer

## Picking up an issue

1. Find an open issue (`good first issue` for a first pull request).
2. Comment to be assigned and wait for assignment.
3. Ask in the issue if anything is unclear.

## Local setup

```bash
go run ./cmd/devdb &
cp .env.example .env && set -a && . ./.env && set +a
go run ./cmd/plimsoll-indexer
```

Use `.env.local` for machine-specific overrides; it is gitignored and loaded
automatically by the indexer. Set `GO_ENV=production` to ignore it (as
deployments do).

## End-to-end test

`internal/e2e` runs the standalone network preset and the event ingester
against a local Stellar network (`stellar/quickstart --local` in Docker). It is
skipped unless `PLIMSOLL_E2E=1`, so `go test ./...` and CI never need Docker:

```bash
PLIMSOLL_E2E=1 go test ./internal/e2e/ -v -timeout 10m
```

It needs Docker and port 8000 free, and removes its container afterwards.
`PLIMSOLL_E2E_IMAGE` overrides the image. The Plimsoll contracts are not
deployed to that network, so it covers connectivity, the preset defaults and
the ingest loop, not decoding of contract events.

## Weekly testnet integration

The contracts live on testnet independently of this repository, so
`.github/workflows/weekly-testnet.yml` runs every Sunday at 06:00 UTC against
the live deployments. It reads the addresses from `deployments/testnet.json`
(the same manifest the contracts repository publishes), runs the offline suite,
the `TestTestnet…` cases in `internal/e2e`, and the live RPC read in
`internal/worker`. A failure opens or appends to a GitHub issue, and posts to
`TESTNET_ALERT_WEBHOOK_URL` when that repository secret is set.

Run the live part yourself with:

```bash
PLIMSOLL_TESTNET=1 go test ./internal/e2e/ -run TestTestnet -v -timeout 10m
PLIMSOLL_LIVE=1 go test ./internal/worker/ -run TestLive -v
```

`PLIMSOLL_TESTNET_DEPLOYMENT` points the suite at another manifest, for example
a fork's or a redeploy's. `TestTestnetDeploymentManifest` needs no network and
runs in the normal suite, so a malformed address fails the pull request that
introduced it.

## Pull request checklist

- [ ] One change per pull request, linked to its issue.
- [ ] `gofmt -l .` prints nothing; `go vet ./...`, `go test ./...`, and `golangci-lint run` pass.
- [ ] New behaviour has tests. HTTP and Horizon interactions are tested with `httptest`.
- [ ] Schema changes are a new migration in `internal/store/migrations/` (see below), with a test against embedded Postgres.
- [ ] Conventional Commits: `feat(api): …`, `fix(poster): …`.

## Schema migrations

The schema lives in numbered files in `internal/store/migrations/`. On start,
`store.Open` applies every file the database has not recorded yet, in order,
inside one transaction, and records each in the `schema_migrations` table. The
transaction holds a Postgres advisory lock, so instances starting together
apply each migration once and the rest wait for it.

To change the schema:

1. Add the next file, `NNNN_short_description.sql`: four digits, one more than
   the highest existing version, lower case and underscores. A gap, a repeated
   number or any other name fails `go test` and the indexer's start.
2. Write plain SQL. A migration can change a column type, backfill data or drop
   an index; it no longer has to be idempotent, because it runs exactly once.
   It runs in a transaction, so `CREATE INDEX CONCURRENTLY` and other
   statements that refuse one cannot be used.
3. Never edit a migration that has been merged: databases that already ran it
   will not run it again. Fix it with a new migration.
4. Test it in `internal/store` against embedded Postgres. The tests start one
   server for the package (the binaries download once, to
   `~/.embedded-postgres-go`) and give each test its own database through
   `newDatabase(t)` or `openStore(t)`. `go test -short ./...` skips them.

`0001_initial.sql` is the schema from before migrations. Its statements are
`IF NOT EXISTS`, so a database created by an older indexer adopts it without
changes.

## Standards

- Wrap errors with context: `fmt.Errorf("load account: %w", err)`. Never drop an error silently.
- Log with `log/slog`, structured key/value pairs, no string concatenation.
- Amounts are `*big.Int` from Horizon to Postgres (`NUMERIC(40,0)`) to JSON (strings). No floats.
- The canonical breakdown encoding is part of the protocol. Changing `horizon.Breakdown` changes every future hash and needs an issue first.
- Secrets come from the environment only.

## AI-assisted contributions

Allowed if you understand and have tested every line. Untested generated code is closed.
