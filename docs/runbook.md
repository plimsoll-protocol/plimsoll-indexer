# Operations runbook

How to run, watch and recover the Plimsoll indexer. Everything here is read
from the code in this repository; variable names and defaults come from
`internal/config/config.go` and `.env.example`.

## What is running

One process (`cmd/plimsoll-indexer`) runs these jobs and an HTTP server on
`PORT` (default `8080`):

| Job | Log field `job` | Interval | Needs a key |
| --- | --- | --- | --- |
| Event ingester | `ingest` | `INGEST_INTERVAL` (10s) | no |
| State reconciler | `reconcile` | `RECONCILE_INTERVAL` (15m) | no |
| stellar.toml sync | `toml` | checked every minute, each asset refreshed every `TOML_INTERVAL` (6h) | no |
| Supply poster | `poster` | `POST_INTERVAL` (15m), reposts after `REPOST_AFTER` (6h) | yes, `SUPPLY_POSTER_SECRET` |
| Keep-awake | `keepawake` | `KEEP_AWAKE_INTERVAL` (10m) | no |

Ingest and reconcile run once before the server starts listening. A job that
returns an error is logged as `job failed` with `err` and retried on its next
tick; it does not stop the process. The process exits non-zero only if the
database cannot be opened, the poster secret is invalid, or the HTTP server
fails.

## Configuration

Required: `DATABASE_URL`, `COVERAGE_LEDGER_ID`, `REPORTER_REGISTRY_ID`. Missing
values are reported together at startup.

| Variable | Default | Notes |
| --- | --- | --- |
| `NETWORK` | `testnet` | Preset: `testnet`, `mainnet` or `standalone`. Sets the next three defaults. |
| `STELLAR_RPC_URL` | preset | Required for `mainnet` (no public SDF RPC). |
| `HORIZON_URL` | preset | |
| `STELLAR_NETWORK_PASSPHRASE` | preset | Must match the network, or signed posts are rejected. |
| `HORIZON_CHECK_URL` | empty | Second, independently operated Horizon. Empty disables the cross-check. |
| `START_LEDGER` | `0` | First ledger to ingest. `0` or a ledger in the future means "latest"; older than RPC retention is clamped. |
| `SUPPLY_POSTER_SECRET` | empty | Empty runs read-only. Never log or share it. |
| `WEBHOOK_ADMIN_TOKEN` | empty | Enables authenticated webhook management when set with `WEBHOOK_ENCRYPTION_KEY`. |
| `WEBHOOK_API_KEYS` | empty | Optional comma-separated owner API keys; each can delete only its own webhooks. |
| `WEBHOOK_ENCRYPTION_KEY` | empty | Base64-encoded 32-byte key (`openssl rand -base64 32`); keep stable to preserve webhook delivery. |
| `PORT` | `8080` | |
| `CORS_ORIGINS` | `*` | Comma separated. |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error`. Logs are JSON on stdout. |
| `GO_ENV` | empty | `production` disables the local `.env.local` overlay. Set it on every deployed instance. |
| `KEEP_AWAKE_URL` | derived | `none` disables; otherwise `$RENDER_EXTERNAL_URL/healthz` on Render. |

Intervals use Go duration syntax (`10s`, `15m`, `6h`).

Presets:

| `NETWORK` | RPC | Horizon | Passphrase |
| --- | --- | --- | --- |
| `testnet` | `https://soroban-testnet.stellar.org` | `https://horizon-testnet.stellar.org` | `Test SDF Network ; September 2015` |
| `mainnet` | none, set `STELLAR_RPC_URL` | `https://horizon.stellar.org` | `Public Global Stellar Network ; September 2015` |
| `standalone` | `http://localhost:8000/rpc` | `http://localhost:8000` | `Standalone Network ; February 2017` |

## Starting and stopping

Local, with embedded Postgres:

```bash
go run ./cmd/devdb &
cp .env.example .env && set -a && . ./.env && set +a
go run ./cmd/plimsoll-indexer
```

Containers: `docker compose up --build` (Postgres on host port 54329, indexer on
8080). Render: see `render.yaml`; set `SUPPLY_POSTER_SECRET` in the dashboard.

Pending schema migrations (`internal/store/migrations/`) are applied on start,
under an advisory lock, so deploying a new version needs no separate migration
step and several instances can start at once. `schema_migrations` records the
version each database is at. Stop with `SIGINT` or
`SIGTERM`; the HTTP server drains for up to 10 seconds.

## Health checks

`GET /healthz` returns `200` with:

```json
{"status":"ok","ingested_to_ledger":0,"last_event_ledger":0,
 "last_event_at":"...","last_event_age_seconds":0}
```

- `503 {"code":"db_unavailable","message":"database is unavailable"}`: Postgres is unreachable.
- `503 {"code":"progress_unavailable","message":"ingest progress is unavailable"}`: the ingest cursor cannot be read.

Every `4xx` and `5xx` response, including the `404` and `405` the router writes
for an unknown path or method, has the same body:
`{"code":"…","message":"…"}` plus an optional `details` object. `code` is
machine-readable and stable; clients should match it instead of `message`. The
schema is `ErrorResponse` in `internal/api/openapi.yaml`.
- `ingested_to_ledger` advances on every scan, even when nothing is stored, so
  it is **not** evidence that ingestion works. Alert on `last_event_age_seconds`
  instead. The keep-alive workflow fails when it exceeds 28800 (8 hours), which
  is the repost interval plus slack. `last_event_age_seconds` is absent until
  the first event is stored.

Other checks:

- `GET /v1/assets` shows each asset's latest supply snapshot and its age.
- `GET /v1/network` shows the contract ids and URLs the instance is using.
- Verify a snapshot end to end with
  `go run ./cmd/plimsoll-indexer verify --sac <SAC> --indexer http://localhost:8080`;
  it exits `0` on match and `1` on mismatch.

There is no metrics endpoint. Monitoring is `/healthz`, the API and the logs.

## Logs to watch

Logs are JSON (`log/slog`). Key messages:

| Level | Message | Meaning and action |
| --- | --- | --- |
| error | `job failed` (`job`, `err`) | A tick failed; it will retry. Persistent failures need the `err` investigated. |
| error | `horizons disagree about supply` | The two Horizons reported different totals for the same ledger. The post is refused. Do not override: find out which Horizon is wrong. |
| warn | `supply cross-check unavailable` | `HORIZON_CHECK_URL` is down. Posting continues unchecked. |
| warn | `supply not cross-checked: horizons are at different ledgers` | Ledger skew after a retry. Posting continues. Frequent occurrences mean the two Horizons lag each other. |
| warn | `ingest cursor is older than RPC retention...` | Ingestion was down longer than RPC's ~7 days. Events in the gap are gone from RPC; the reconciler repairs the latest state. |
| warn | `start ledger is outside RPC retention` | `START_LEDGER` is too old and was clamped. |
| warn | `rpc node is rate limiting; backing off` (`attempt`, `wait`) | The RPC node answered `429 Too Many Requests`. The request is retried after `Retry-After` when the node sends it, otherwise after a delay that doubles per attempt. Frequent entries mean the node's limit is too low for the ingest interval. |
| warn | `recovered a supply snapshot / reserve report from contract state...` | The reconciler filled a gap. Rows are marked `"source": "chain"` with no `tx_hash`. |
| info | `supply posting disabled (SUPPLY_POSTER_SECRET not set)` | Expected on read-only instances. |
| info | `supply cross-check disabled (HORIZON_CHECK_URL not set)` | The poster signs on one Horizon's word. |
| info | `issuer stellar.toml changed` | The issuer document hash changed; includes `old_hash` and `new_hash`. |
| error | `issuer stellar.toml change webhook delivery failed` | A destination could not receive the TOML change event. |

## Common procedures

**Enable posting.** The account must be registered as `SupplyPoster` in the
reporter registry and funded for fees. Set `SUPPLY_POSTER_SECRET`, restart, and
look for `supply posting enabled` with the poster address. An invalid seed stops
startup with `SUPPLY_POSTER_SECRET is not a valid secret seed`.

**Rotate the poster key.** Register the new account as `SupplyPoster`, set the
new secret, restart, confirm a post succeeds, then revoke the old reporter.

**Ingestion stalled** (`last_event_age_seconds` high):

1. Check `job failed` entries with `job=ingest`.
2. Check the RPC is reachable and `STELLAR_RPC_URL`, `COVERAGE_LEDGER_ID` and
   `REPORTER_REGISTRY_ID` match the deployment (`/v1/network`).
3. Confirm the poster is running, because a healthy poster is what raises new
   `supply_posted` events; a read-only instance can legitimately see none.

**Supply snapshot stale.** Posting needs `SUPPLY_POSTER_SECRET`, a funded
account and a ledger above the last snapshot (the poster never posts at or below
the last snapshot's ledger). Check `job failed` entries with `job=poster`.

**Breakdown returns 404 for an on-chain hash.** The poster saves each
breakdown before it posts it and serves it only once the post is confirmed: by
the successful transaction, by ingesting its `supply_posted` event, or by
reading the ledger's current snapshot back (the poster does this every tick).
A breakdown whose post never landed is deleted after 24 hours
(`DefaultBreakdownGrace`); the poster logs `deleted breakdowns whose post never
reached the chain`. A 404 for a hash that is on chain therefore means the
confirmation has not run yet (check `job failed` entries with `job=poster` and
`job=ingest`) or the breakdown was never stored by this database.

**After a decoder change.** Events the old decoder did not track are already
in `raw_events`. Deploy the new binary, then run
`plimsoll-indexer replay` against the same `DATABASE_URL` once: it prints how
many raw events it decoded. It is idempotent and can run while the indexer is
up.

**Move to another network.** Set `NETWORK` (and `STELLAR_RPC_URL` for mainnet),
the two contract ids and `START_LEDGER` for that deployment. Use a fresh
database: one database holds one network's history.

**Database restore or loss.** The schema recreates itself. Ingestion restarts
from `START_LEDGER` and is clamped to RPC retention; older events cannot be
recovered from RPC, but the reconciler restores the latest supply and report
state from the contracts. A backup that holds `raw_events` can rebuild the
typed tables with `plimsoll-indexer replay`. Breakdowns for old snapshots can be lost: they are
stored only in this database.

## Local network testing

An end-to-end test runs against `stellar/quickstart`; see "End-to-end test" in
[CONTRIBUTING.md](../CONTRIBUTING.md). To point a local instance at it, run
quickstart with `--local` and set `NETWORK=standalone`.
