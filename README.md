<p align="center">
  <img src="https://raw.githubusercontent.com/plimsoll-protocol/plimsoll-contracts/main/docs/banner.svg" alt="Plimsoll" width="100%" />
</p>

<p align="center">
  <a href="https://github.com/plimsoll-protocol/plimsoll-indexer/actions/workflows/ci.yml"><img src="https://github.com/plimsoll-protocol/plimsoll-indexer/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
  <img src="https://img.shields.io/badge/go-1.27-00ADD8" alt="Go" />
  <img src="https://img.shields.io/badge/postgres-16%2B-336791" alt="Postgres" />
  <a href="https://plimsoll-protocol.gitbook.io/plimsoll-protocol-docs/"><img src="https://img.shields.io/badge/docs-gitbook-0f1d2b" alt="Docs" /></a>
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue" alt="Apache-2.0" />
</p>

# Plimsoll indexer

The off-chain half of [Plimsoll](https://github.com/plimsoll-protocol/plimsoll-contracts).
One Go binary with five jobs and an HTTP API:

| Job | What it does |
| --- | --- |
| **Supply poster** | For every listed asset, sums Horizon's per-bucket holdings (trustlines in every auth state, claimable balances, liquidity pools, contract balances) and calls `post_supply` on the coverage ledger with the total and the SHA-256 of the breakdown. Posts when supply changes, or every `REPOST_AFTER` so freshness checks keep passing. Whether a post is due is read from the coverage ledger itself, so a stalled event ingest cannot make it repost on every tick. |
| **Event ingester** | Reads `asset_listed`, `supply_posted`, `reserve_posted`, `reporter_set`, `reporter_metadata_updated` and `reporter_revoked` from Soroban RPC into Postgres, keeping history past RPC's ~7-day retention. |
| **Reporter watcher** | Reads `reporter_set` and `reporter_revoked` from the registry contract on its own cursor every `REPORTER_INTERVAL` (5s, about one ledger), so a new registration appears at `/v1/reporters` immediately instead of waiting for the next ingest or reconcile pass. |
| **State reconciler** | Reads each listed asset's `get_supply` and `get_report` back from the coverage ledger and reporter profile data from the reporter registry, storing anything newer than ingestion has. RPC serves only about seven days of events, so a longer ingestion gap cannot be repaired from events; the contracts always hold the latest figure. Recovered rows are marked `"source": "chain"` and carry no `tx_hash`. Reading is simulation, so it needs no key and runs even on an instance that cannot sign. |
| **stellar.toml sync** | Resolves each issuer's `home_domain`, reads its SEP-1 `stellar.toml`, and stores the matching `[[CURRENCIES]]` entry, including `attestation_of_reserve`. Successful fetches are SHA-256 tracked per issuer; changes are logged and can be delivered to registered webhooks. The linked attestation document is downloaded under the same size, time and SSRF limits as reserve documents, hashed, and kept as version history; the asset response exposes `toml.attestation_hash`, `toml.attestation_first_seen_at` and `toml.attestation_changed_at`. |
| **Coverage alerts** | Posts a signed alert to each registered integrator URL when an asset's coverage falls below the registration's `min_bps`, selecting reports of at least `min_tier`. One alert per crossing, re-armed when coverage recovers; failed deliveries are retried with exponential backoff, at most 5 attempts in total. |
| **Event ingester** | Reads `asset_listed`, `supply_posted`, `reserve_posted`, `reporter_set` and `reporter_revoked` from Soroban RPC into Postgres, keeping history past RPC's ~7-day retention. |
| **State reconciler** | Reads each listed asset's `get_supply` and `get_report` back from the coverage ledger and stores anything newer than ingestion has. RPC serves only about seven days of events, so a longer ingestion gap cannot be repaired from events; the contracts always hold the latest figure. Recovered rows are marked `"source": "chain"` and carry no `tx_hash`. Reading is simulation, so it needs no key and runs even on an instance that cannot sign. |
| **stellar.toml sync** | Resolves each issuer's `home_domain`, reads its SEP-1 `stellar.toml`, and stores the matching `[[CURRENCIES]]` entry, including `attestation_of_reserve`. |
| **Retention** | Nightly, deletes supply snapshots, reserve reports and unreferenced breakdowns older than `RETENTION_DAYS`, then vacuums the affected tables. `0` keeps everything forever. |
| **stellar.toml sync** | Resolves each issuer's `home_domain`, reads its SEP-1 `stellar.toml`, and stores the matching `[[CURRENCIES]]` entry, including `attestation_of_reserve`. Successful fetches are SHA-256 tracked per issuer; changes are logged and can be delivered to registered webhooks. |

**Live API:** https://plimsoll-indexer.onrender.com/v1/assets  
**Docs:** https://plimsoll-protocol.gitbook.io/plimsoll-protocol-docs/

| Repo | What it is |
| --- | --- |
| [plimsoll-contracts](https://github.com/plimsoll-protocol/plimsoll-contracts) | Rust/Soroban contracts |
| [plimsoll-app](https://github.com/plimsoll-protocol/plimsoll-app) | Web app and TypeScript SDK |
| **plimsoll-indexer** (this repo) | This service |

## Maintainers

| Maintainer | GitHub | Contact |
| --- | --- | --- |
| dunnidev | [@dunnidev](https://github.com/dunnidev) | [GitHub Discussions](https://github.com/plimsoll-protocol/plimsoll-indexer/discussions) |

## Verifiable by design

The poster is trusted to count, so its counting is checkable. The breakdown it
hashed is served byte-for-byte at `/v1/breakdowns/{hash}`; `sha256` of that
body equals the `breakdown_hash` in the on-chain `supply_posted` event, and each
bucket can be recomputed from Horizon's `/assets` endpoint.

### How the snapshot ledger is determined

The `ledger` field in a breakdown is part of the hashed JSON, so it has to name
the ledger the balances were actually read at: anyone recomputing the breakdown
at that ledger must get the same totals. The indexer never derives it from a
separate call.

- The balances come from Horizon's `/assets`. Horizon stamps that response with
a `Latest-Ledger` header, and that header is the ledger the response body
reflects, so it becomes the breakdown's ledger.
- If the header is absent (an older Horizon, or a proxy that strips it), the
indexer brackets the read with two root queries. If `history_latest_ledger` did
not move across the read, the read is attributable to that ledger; if it did
move, the read is retried and, when it will not settle, abandoned rather than
labelled with a guess.
- A Soroban RPC usually trails Horizon by a ledger or two, so before signing the
poster waits briefly for the RPC's latest ledger to reach the one the balances
were read at. It never rewrites the snapshot's ledger to the RPC head; if the
RPC does not catch up, nothing is posted that tick.
- A zero snapshot for an asset with no trustlines has no balances to read, so it
takes the RPC's latest ledger explicitly.

### Verifying supply snapshots

Verify an asset's latest supply snapshot against live Horizon balances without needing a local database:

```bash
go run ./cmd/plimsoll-indexer verify --sac <SAC_ADDRESS> [--indexer http://localhost:8080] [--horizon <HORIZON_URL>] [--rpc <RPC_URL>] [--ledger-id <CONTRACT_ID>]
```

The command fetches the asset's snapshot breakdown from the indexer, verifies the snapshot on-chain via Soroban RPC simulation (`get_supply`), checks its SHA-256 hash integrity, recomputes balances directly from Horizon, prints a formatted diff table, and exits `0` on match or `1` on mismatch. Network discovery is fetched automatically from `/v1/network`.

### Raw events and replay

Ingestion keeps every event the two contracts emit in `raw_events` (id,
contract, ledger, close time, transaction hash, topic and value XDR), written
in the same transaction as the decoded rows, including events the decoder does
not track yet. RPC only serves about seven days of events, so this is what lets
a decoder change apply to older history:

```bash
DATABASE_URL=postgres://... go run ./cmd/plimsoll-indexer replay [-batch 500]
```

`replay` decodes every raw event again, in chain order, with the binary's
decoder and applies the typed rows. Rows that already exist are left as they
are, so it is safe on a live database, and on an empty one that holds only
`raw_events` it rebuilds `assets`, `supply_snapshots`, `reserve_reports` and
`reporters`. The ingest cursor is not touched.

### Replaying a ledger range

Re-ingest a range from the RPC without deleting anything. Rows are keyed by
event id, so the command is idempotent and leaves the ingest cursor where it
was; run it beside a live indexer. This is what recovers events the indexer
missed or dropped before a fix:

```bash
go run ./cmd/plimsoll-indexer replay -from 5034380 -to 5035000 [-dry-run]
```

`-dry-run` logs what the range contains without writing to the database, and
needs no database connection at all. Progress is logged every 1 000 ledgers.
One command serves both modes: `replay` with no `-from`/`-to` rebuilds from
`raw_events`, and `replay -from N -to M` re-reads that range from the RPC.
### Backfilling archived events

Soroban RPC retains only a limited window of events. To recover events outside
that window, install Stellar Core v23 or later and run the bounded archive
backfill against the same database as the live indexer:

```bash
go run ./cmd/plimsoll-indexer backfill --from 5000000 --to 5100000
```

The command uses the configured network passphrase, the public testnet or
mainnet history archives, and `stellar-core` from `PATH`. Override the binary
with `STELLAR_CORE_PATH` or `--stellar-core`. Custom networks need one or more
HTTPS `--archive-url` values. `--timeout` defaults to 24 hours. Backfill writes
to its own progress cursor and uses the same event IDs as live ingestion, so
overlapping ranges are safe to rerun without duplicating events.

A 100,000-ledger range typically takes roughly 30 minutes to two hours on a
4-vCPU, 8-GB host; archive bandwidth, ledger complexity, and disk speed can
change this substantially. The bounded Captive Core replay is memory intensive
(allow at least 3 GB for Core); run a 1,000-ledger sample first and use its
progress logs to estimate the full range.

## API

| Method and path | Returns |
| --- | --- |
| `GET /healthz` | `{status, ingested_to_ledger, last_event_ledger, last_event_at, last_event_age_seconds}` |
| `GET /v1/network` | Contract ids, RPC and Horizon URLs |
| `GET /v1/assets?limit=50&cursor=<opaque>&min_tier=1` | Paginated assets (default limit 50, maximum 200) with latest supply, latest report per tier, coverage and stellar.toml data. Responses include `next_cursor` while another page exists. |
| `GET /v1/assets/{sac}?min_tier=1` | One asset, same shape |
| `HEAD /v1/assets/{sac}` | Status only, no response body: `200` if the asset is listed, `404` if not |
| `GET /v1/assets/{sac}/supply?limit=100` | Supply snapshot history. Each row carries `source`: `event` for a figure decoded from a contract event, with the `tx_hash` that produced it, or `chain` for one recovered from contract state after an ingestion gap, where `tx_hash` is empty. |
| `GET /v1/assets/{sac}/reports?limit=100` | Reserve report history |
| `GET /v1/assets/{sac}/coverage/now` | The `bps`, `tier`, `as_of` and `reporter` of the live report `coverage()` selects, without the asset document. `404` when no live report exists |
| `GET /v1/assets/{sac}/coverage?from=&to=&interval=1h&min_tier=1` | Coverage time series. Each point uses the latest supply snapshot and the best report posted at or before it, selected and rounded by the same rules as `coverage()` on-chain. `from` and `to` are RFC 3339 timestamps (default: the last 24 hours); `interval` runs from `5m` to `7d`; a request returns at most 1,000 points. |
| `GET /v1/reporters` | Active registered reporters |
| `GET /v1/changelog` | Breaking API changes per released version, newest first |
| `GET /v1/breakdowns/{hash}` | Exact bytes of a supply breakdown that backs an on-chain snapshot |
| `POST /v1/webhooks`, `GET /v1/webhooks`, `DELETE /v1/webhooks/{id}` | Manage authenticated issuer TOML change subscriptions. One API key may register up to 10 endpoints; every event fans out to all of them, and `GET /v1/webhooks` reports delivery state per endpoint. |
| `POST /v1/coverage-webhooks` | Register a coverage threshold (admin token required) |
| `DELETE /v1/coverage-webhooks/{id}` | Remove a registration and its delivery log (admin token required) |
| `GET /v1/breakdowns/{hash}` | Exact bytes of a supply breakdown |
| `GET /v1/events` | `text/event-stream` of `asset_listed`, `supply_posted` and `reserve_posted`, for clients that would otherwise poll |
| `POST /graphql` | Optional GraphQL endpoint. Off unless `ENABLE_GRAPHQL=true`; introspection is disabled when `ENVIRONMENT=production`. |
| `POST /v1/webhooks`, `GET /v1/webhooks`, `DELETE /v1/webhooks/{id}` | Manage authenticated issuer TOML change subscriptions |
| `GET /v1/webhooks/{id}/dlq`, `POST /v1/webhooks/{id}/dlq/{delivery_id}/retry` | Inspect and retry failed webhook deliveries |

Every `4xx` and `5xx` response has the same body:
`{"code":"asset_not_listed","message":"…"}`, with an optional `details` object.
`code` is machine-readable and stable, so clients should match on it instead of
on `message`. See `ErrorResponse` in
[`internal/api/openapi.yaml`](internal/api/openapi.yaml).

Webhook registration and signature verification are covered in the
[webhook guide](docs/webhooks.md).

Breaking API changes (a removed field, a renamed endpoint, a newly required
parameter) are recorded by hand in
[internal/changelog/CHANGELOG.yaml](internal/changelog/CHANGELOG.yaml) and served
at `GET /v1/changelog`, newest version first. The indexer refuses to start if
that file is malformed.
### Prometheus metrics

Set `METRICS_ENABLED=true` to expose `GET /metrics`; it is not registered when
disabled. The endpoint is unauthenticated, so restrict access at the network
or reverse-proxy layer when needed.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `plimsoll_ingested_ledger` | Gauge | — | Latest ledger reached by live event ingestion |
| `plimsoll_ingest_lag_ledgers` | Gauge | — | Gap between latest known and ingested ledger |
| `plimsoll_supply_posts_attempted_total` | Counter | — | Supply transactions submitted |
| `plimsoll_supply_posts_succeeded_total` | Counter | — | Supply transactions successfully submitted |
| `plimsoll_supply_posts_failed_total` | Counter | — | Supply transactions that failed |
| `plimsoll_supply_post_last_age_seconds` | Gauge | `asset_sac` | Seconds since each asset's last successful supply post |
| `plimsoll_http_requests_total` | Counter | `method`, `path`, `status` | HTTP request totals by route and status |
| `plimsoll_http_request_duration_seconds` | Histogram | `method`, `path` | HTTP request latency in seconds |
`/v1/assets/{sac}`, `/v1/network` and `/v1/breakdowns/{hash}` return an `ETag`;
send it back as `If-None-Match` and the API answers `304 Not Modified` with no
body while the representation is unchanged.
### OpenAPI spec

[internal/api/openapi.yaml](internal/api/openapi.yaml) is generated, not
maintained by hand: the endpoints come from the route table in
[internal/api/routes.go](internal/api/routes.go), which is also what the server
registers, and the schemas come from
[internal/api/openapi.components.yaml](internal/api/openapi.components.yaml).
After changing a route or a schema, regenerate the golden file:

```bash
make generate
```

CI runs `make generate` and fails when the committed file differs, and
`go test ./internal/api` checks the same thing plus that every served route is
documented. Edit the route table or the components file and regenerate; never
edit `openapi.yaml` directly.

Coverage in the API uses the same selection and rounding rules as the contract
(`internal/coverage`), so the API and `coverage()` on-chain agree.

### Coverage alerts

Set `WEBHOOK_ADMIN_TOKEN` and `WEBHOOK_SIGNING_SECRET` to enable coverage
threshold alerts. Register one with:

```bash
curl -X POST localhost:8080/v1/coverage-webhooks \
  -H "Authorization: Bearer $WEBHOOK_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"url":"https://integrator.example/plimsoll","sac":"C...","min_bps":9500,"min_tier":2}'
```

When coverage crosses below `min_bps`, the indexer POSTs JSON to `url` with
`Content-Type: application/json` and

```
X-Plimsoll-Signature: sha256=<lower-case hex HMAC-SHA256 of the raw body keyed with WEBHOOK_SIGNING_SECRET>
X-Plimsoll-Delivery: <delivery id>
```

Verify the signature over the exact bytes received before trusting the body.
The payload carries `event` (`coverage_dropped`), `webhook_id`, `sac`,
`min_bps`, `min_tier`, `bps`, `tier`, `supply`, `reserves`,
`supply_posted_at`, `report_as_of` and `sent_at`; amounts are decimal strings
and timestamps are UTC. Deliveries are logged in `coverage_webhook_deliveries` and
retried with exponential backoff (30s, 1m, 2m, 4m) until they succeed or five
attempts have been made.
### Live events

`GET /v1/events` streams ingested events as server-sent events, so a dashboard
can update when a report lands instead of refetching every asset on a timer:

```bash
curl -N localhost:8080/v1/events
```

```
id: 0023421401557905408-0000000000
event: supply_posted
data: {"amount":"9999000000000","ledger":5453219,"posted_at":"2026-10-29T16:02:20Z",...}

: keepalive

```

Each event's `data` is the same JSON the corresponding REST resource serves
(`Supply`, `Report`, or the listed fields of `Asset`). Events are fanned out
from the ingest transaction as soon as it commits, so they arrive within one
`INGEST_INTERVAL`; a `: keepalive` comment every 15 s holds idle connections
open. At most 128 subscribers are served at once (the rest get `503`); a
subscriber that stops reading is disconnected rather than allowed to delay
ingestion.

Resume with the event id: send the `id` of the last event received as
`Last-Event-ID` on reconnect — EventSource does this automatically — and the
indexer replays what was missed from Postgres before continuing with live
events:

```bash
curl -N -H 'Last-Event-ID: 0023421401557905408-0000000000' localhost:8080/v1/events
```

The replay comes from storage, not a memory buffer, so it still works after a
restart and past RPC's ~7-day event retention.
### GraphQL

Set `ENABLE_GRAPHQL=true` to mount `/graphql`. The schema covers assets,
supply snapshots, reports and reporters, so a client can ask for exactly the
fields it needs instead of receiving the full REST shape:

```bash
curl localhost:8080/graphql \
  -H 'Content-Type: application/json' \
  -d '{"query":"{ assets { sac code supply { amount ledger } coverage { bps tier } } }"}'
```

`assets(minTier: 3)` filters coverage by tier, and `coverage(minTier: ...)` on
an individual asset overrides it. Introspection is available in development
but disabled when `ENVIRONMENT=production`.

### Reindexing from a ledger

If an ingest wrote bad or incomplete history, discard it from a chosen ledger
and replay from RPC:

```bash
go run ./cmd/plimsoll-indexer reindex --from-ledger 5034380
go run ./cmd/plimsoll-indexer reindex --from-ledger 5034380 --dry-run
```

Reindex deletes asset listings, supply snapshots and reserve reports at or
after the ledger, resets the ingest cursor, and replays from RPC, logging
progress every 1 000 ledgers. `--dry-run` prints the row counts without
deleting anything. Reporters are current state rather than an event log, so
the replay updates them idempotently instead of deleting them.

### Retention

`RETENTION_DAYS` defaults to `365`; `0` keeps history forever. The nightly
job deletes older snapshots, reports and breakdowns that no surviving snapshot
still references, then vacuums the affected tables. It logs the row counts it
removed.

## Quick start

Go 1.27+. No Postgres or Docker needed locally:

```bash
git clone https://github.com/plimsoll-protocol/plimsoll-indexer
cd plimsoll-indexer
go run ./cmd/devdb &          # embedded Postgres on :54329
cp .env.example .env          # defaults point at the testnet deployment
set -a; . ./.env; set +a
go run ./cmd/plimsoll-indexer
curl localhost:8080/v1/assets
```

To override any value locally, put it in `.env.local` in the repository root.
The indexer loads that file automatically, after the shell has exported `.env`,
and skips it entirely when `GO_ENV=production`.

Leave `SUPPLY_POSTER_SECRET` empty to run read-only. To post supply, use the
secret of an account registered in the reporter registry with the
`SupplyPoster` role.

```bash
go test ./...
```

### Linting

CI runs two complementary Go static analyzers on every pull request:

- **`golangci-lint`** (configured in [.golangci.yml](.golangci.yml)): enforces `errcheck`, `govet`, `ineffassign`, `unused`, `gosec`, `bodyclose`, and `staticcheck` on production packages.
- **`staticcheck`** (standalone `staticcheck ./...`): runs the full `SA`/`S`/`ST` check suite across all packages including `_test.go` files, catching Go-specific bugs, deprecated APIs, and style issues that `.golangci.yml` excludes from test files.

Run both locally before pushing:

```bash
golangci-lint run
staticcheck ./...
```

Only suppress a finding when it is a verified false positive or intentional legacy/regression test coverage (never to hide real bugs), and always include a short justification comment:

- `golangci-lint`: `//nolint:<linter> // reason` (or a scoped rule in `.golangci.yml`).
- `staticcheck`: `//lint:ignore <CHECK> <reason>` on the line above the finding.

## Configuration

Every variable is documented in [.env.example](.env.example). Required:
`DATABASE_URL`, `COVERAGE_LEDGER_ID`, `REPORTER_REGISTRY_ID`. A `.env.local`
file in the working directory is applied over the environment (see
[Quick start](#quick-start)); set `GO_ENV=production` to disable it.

The poster secret and the database password can come from a secrets backend
instead of the environment. Set `SECRETS_PROVIDER=ssm` (AWS Systems Manager
Parameter Store) or `gcp` (GCP Secret Manager) and name the parameter or secret
in `SUPPLY_POSTER_SECRET_NAME` and `DATABASE_PASSWORD_SECRET_NAME`; both are
fetched once at startup and never logged. `env`, the default, reads
`SUPPLY_POSTER_SECRET` and the password in `DATABASE_URL`, and either credential
falls back to its environment variable when its `*_SECRET_NAME` is empty.

`NETWORK` selects a preset (`testnet`, the default, `mainnet` or `standalone`)
that supplies defaults for `STELLAR_RPC_URL`, `HORIZON_URL` and
`STELLAR_NETWORK_PASSPHRASE`. Setting any of those three overrides the preset.
`DECIMAL_CHECK_ENABLED` (default `true`) warns when a new supply snapshot
differs from the previous on-chain one by more than six orders of magnitude,
which usually means an asset's decimals were applied wrongly. The snapshot is
still posted; set it to `false` to silence the warning.
`mainnet` has no default RPC (SDF runs no public Soroban RPC for it), so
`STELLAR_RPC_URL` is required there. `standalone` points at a local
[stellar/quickstart](https://github.com/stellar/quickstart) on `localhost:8000`.

Running it in production? See the [operations runbook](docs/runbook.md).

## Docker

A pre-built multi-arch image is published to the GitHub Container Registry on
every release tag:

```bash
docker pull ghcr.io/plimsoll-protocol/plimsoll-indexer:latest
```

Run the full local stack (Postgres + indexer, testnet defaults) with Compose:

```bash
docker compose up
curl localhost:8080/healthz
```

To build the image locally instead, replace the `image:` line in
`docker-compose.yml` with `build: .` and run `docker compose up --build`.

The image is multi-stage (`golang:1.27-bookworm` → `gcr.io/distroless/static-debian12`)
and is about 31 MB; most of that is the AWS and GCP SDKs behind
`SECRETS_PROVIDER`. Leave `SUPPLY_POSTER_SECRET` empty in `docker-compose.yml`
for a read-only run.

The binary includes a `healthcheck` subcommand used by the Docker
`HEALTHCHECK` instruction and by the Compose file. It requests `/healthz` on
`localhost` (honouring `PORT`) and exits 0 on success or 1 on failure:

```bash
plimsoll-indexer healthcheck
```

## Deploying

[Deployment guide](docs/deploy.md) covers Docker, required configuration,
Postgres provisioning, Render and Railway setup, and safe secret management.
The [Render blueprint](render.yaml) provisions a web service and Postgres
database and can be launched with the one-click button in the guide.

`SUPPLY_POSTER_SECRET` is never committed: set it in the hosting provider's
settings, or point `SECRETS_PROVIDER` at AWS SSM or GCP Secret Manager (see
[.env.example](.env.example)).

[.github/workflows/keepalive.yml](.github/workflows/keepalive.yml) watches the
deployment from outside: it wakes the service, fails if nothing has been
ingested recently, and fails if any asset's supply snapshot has gone stale.
The ingestion check exists because a scan position alone is not evidence of
progress — ingestion once stopped for nine days while `/healthz` kept
answering `ok` with a current ledger.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Security: [SECURITY.md](SECURITY.md).

## Contributors

<a href="https://github.com/plimsoll-protocol/plimsoll-indexer/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=plimsoll-protocol/plimsoll-indexer" alt="Contributors" />
</a>

## License

[Apache-2.0](LICENSE)
