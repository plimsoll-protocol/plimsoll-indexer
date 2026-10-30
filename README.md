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
One Go binary with four jobs and an HTTP API:

| Job | What it does |
| --- | --- |
| **Supply poster** | For every listed asset, sums Horizon's per-bucket holdings (trustlines in every auth state, claimable balances, liquidity pools, contract balances) and calls `post_supply` on the coverage ledger with the total and the SHA-256 of the breakdown. Posts when supply changes, or every `REPOST_AFTER` so freshness checks keep passing. Whether a post is due is read from the coverage ledger itself, so a stalled event ingest cannot make it repost on every tick. |
| **Event ingester** | Reads `asset_listed`, `supply_posted`, `reserve_posted`, `reporter_set` and `reporter_revoked` from Soroban RPC into Postgres, keeping history past RPC's ~7-day retention. |
| **State reconciler** | Reads each listed asset's `get_supply` and `get_report` back from the coverage ledger and stores anything newer than ingestion has. RPC serves only about seven days of events, so a longer ingestion gap cannot be repaired from events; the contracts always hold the latest figure. Recovered rows are marked `"source": "chain"` and carry no `tx_hash`. Needs `SUPPLY_POSTER_SECRET`, since reading contract state means simulating from an account. |
| **stellar.toml sync** | Resolves each issuer's `home_domain`, reads its SEP-1 `stellar.toml`, and stores the matching `[[CURRENCIES]]` entry, including `attestation_of_reserve`. |

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

Coverage in the API uses the same selection and rounding rules as the contract
(`internal/coverage`), so the API and `coverage()` on-chain agree.

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

## Docker

Prefer containers? Build a distroless image and run with Compose (Postgres +
indexer, testnet defaults):

```bash
docker compose up --build
curl localhost:8080/healthz
```

The image is multi-stage (`golang:1.27-bookworm` → `gcr.io/distroless/static-debian12`)
and stays well under 30 MB. Leave `SUPPLY_POSTER_SECRET` empty in
`docker-compose.yml` for a read-only run.

## Deploying

[render.yaml](render.yaml) defines a Render web service plus a Postgres
database in the same region, connected over the internal URL. Set
`SUPPLY_POSTER_SECRET` in the Render dashboard; it is never committed.

On Render the indexer requests its own `/healthz` every 10 minutes
(`KEEP_AWAKE_URL`, derived from `RENDER_EXTERNAL_URL`), so the free tier does
not put it to sleep and supply posting keeps running. One always-on service
uses about 744 of the free tier's 750 monthly instance hours.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Security: [SECURITY.md](SECURITY.md).

## Contributors

<a href="https://github.com/plimsoll-protocol/plimsoll-indexer/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=plimsoll-protocol/plimsoll-indexer" alt="Contributors" />
</a>

## License

[Apache-2.0](LICENSE)
