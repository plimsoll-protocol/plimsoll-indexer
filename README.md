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
| **Event ingester** | Reads `asset_listed`, `supply_posted`, `reserve_posted`, `reporter_set` and `reporter_revoked` from Soroban RPC into Postgres, keeping history past RPC's ~7-day retention. |
| **State reconciler** | Reads each listed asset's `get_supply` and `get_report` back from the coverage ledger and stores anything newer than ingestion has. RPC serves only about seven days of events, so a longer ingestion gap cannot be repaired from events; the contracts always hold the latest figure. Recovered rows are marked `"source": "chain"` and carry no `tx_hash`. Reading is simulation, so it needs no key and runs even on an instance that cannot sign. |
| **stellar.toml sync** | Resolves each issuer's `home_domain`, reads its SEP-1 `stellar.toml`, and stores the matching `[[CURRENCIES]]` entry, including `attestation_of_reserve`. Successful fetches are SHA-256 tracked per issuer; changes are logged and can be delivered to registered webhooks. The linked attestation document is downloaded under the same size, time and SSRF limits as reserve documents, hashed, and kept as version history; the asset response exposes `toml.attestation_hash` and `toml.attestation_changed_at`. |
| **Coverage alerts** | Posts a signed alert to each registered integrator URL when an asset's coverage falls below the registration's `min_bps`, selecting reports of at least `min_tier`. One alert per crossing, re-armed when coverage recovers; failed deliveries are retried with exponential backoff, at most 5 attempts in total. |

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

### Verifying supply snapshots

Verify an asset's latest supply snapshot against live Horizon balances without needing a local database:

```bash
go run ./cmd/plimsoll-indexer verify --sac <SAC_ADDRESS> [--indexer http://localhost:8080] [--horizon <HORIZON_URL>] [--rpc <RPC_URL>] [--ledger-id <CONTRACT_ID>]
```

The command fetches the asset's snapshot breakdown from the indexer, verifies the snapshot on-chain via Soroban RPC simulation (`get_supply`), checks its SHA-256 hash integrity, recomputes balances directly from Horizon, prints a formatted diff table, and exits `0` on match or `1` on mismatch. Network discovery is fetched automatically from `/v1/network`.

## API

| Method and path | Returns |
| --- | --- |
| `GET /healthz` | `{status, ingested_to_ledger, last_event_ledger, last_event_at, last_event_age_seconds}` |
| `GET /v1/network` | Contract ids, RPC and Horizon URLs |
| `GET /v1/assets?min_tier=1` | Every asset with latest supply, latest report per tier, coverage and stellar.toml data |
| `GET /v1/assets/{sac}?min_tier=1` | One asset, same shape |
| `GET /v1/assets/{sac}/supply?limit=100` | Supply snapshot history. Each row carries `source`: `event` for a figure decoded from a contract event, with the `tx_hash` that produced it, or `chain` for one recovered from contract state after an ingestion gap, where `tx_hash` is empty. |
| `GET /v1/assets/{sac}/reports?limit=100` | Reserve report history |
| `GET /v1/reporters` | Active registered reporters |
| `GET /v1/breakdowns/{hash}` | Exact bytes of a supply breakdown |
| `POST /v1/webhooks`, `GET /v1/webhooks`, `DELETE /v1/webhooks/{id}` | Manage authenticated issuer TOML change subscriptions |
| `POST /v1/coverage-webhooks` | Register a coverage threshold (admin token required) |
| `DELETE /v1/coverage-webhooks/{id}` | Remove a registration and its delivery log (admin token required) |

Every `4xx` and `5xx` response has the same body:
`{"code":"asset_not_listed","message":"…"}`, with an optional `details` object.
`code` is machine-readable and stable, so clients should match on it instead of
on `message`. See `ErrorResponse` in
[`internal/api/openapi.yaml`](internal/api/openapi.yaml).

Webhook registration and signature verification are covered in the
[webhook guide](docs/webhooks.md).

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

Leave `SUPPLY_POSTER_SECRET` empty to run read-only. To post supply, use the
secret of an account registered in the reporter registry with the
`SupplyPoster` role.

```bash
go test ./...
```

## Configuration

Every variable is documented in [.env.example](.env.example). Required:
`DATABASE_URL`, `COVERAGE_LEDGER_ID`, `REPORTER_REGISTRY_ID`.

`NETWORK` selects a preset (`testnet`, the default, `mainnet` or `standalone`)
that supplies defaults for `STELLAR_RPC_URL`, `HORIZON_URL` and
`STELLAR_NETWORK_PASSPHRASE`. Setting any of those three overrides the preset.
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
and stays well under 30 MB. Leave `SUPPLY_POSTER_SECRET` empty in
`docker-compose.yml` for a read-only run.

The binary includes a `healthcheck` subcommand used by the Docker
`HEALTHCHECK` instruction and by the Compose file. It requests `/healthz` on
`localhost` (honouring `PORT`) and exits 0 on success or 1 on failure:

```bash
plimsoll-indexer healthcheck
```

## Deploying

[render.yaml](render.yaml) defines a Render web service plus a Postgres
database in the same region, connected over the internal URL. Set
`SUPPLY_POSTER_SECRET` in the Render dashboard; it is never committed.

On Render the indexer requests its own `/healthz` every 10 minutes
(`KEEP_AWAKE_URL`, derived from `RENDER_EXTERNAL_URL`), so the free tier does
not put it to sleep and supply posting keeps running. One always-on service
uses about 744 of the free tier's 750 monthly instance hours.

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
