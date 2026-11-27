# Instructions for coding agents — plimsoll-indexer

You are working on the Plimsoll indexer, a single Go binary. Write complete,
tested code. No placeholders.

## Stack

| Item | Version |
| --- | --- |
| Go | 1.27 |
| go-stellar-sdk | v0.7.3 (`clients/rpcclient`, `protocols/rpc`, `txnbuild`, `xdr`) |
| Postgres driver | pgx v5 |
| TOML | BurntSushi/toml v1.6 |
| Dev database | fergusstrange/embedded-postgres (`cmd/devdb` only) |

## Layout

```
cmd/plimsoll-indexer/   wiring: config → store → workers → HTTP server
cmd/devdb/              embedded Postgres for local runs
internal/config/        env loading
internal/horizon/       supply breakdown from /assets; canonical JSON + sha256
internal/stellartoml/   SEP-1 fetch and parse
internal/soroban/       ScVal helpers, event decoder, simulate→sign→send invoker
internal/coverage/      data model + the contract's selection and bps rules
internal/store/         migrations/NNNN_*.sql (applied once each, in order) + queries
internal/worker/        ingest.go, poster.go, toml.go
internal/api/           HTTP handlers (Go 1.22 mux patterns)
```

## Contract events consumed

| Event | Topics | Data map |
| --- | --- | --- |
| `asset_listed` | sym, sac | code, issuer |
| `supply_posted` | sym, sac | amount i128, ledger u32, breakdown_hash bytes, poster |
| `reserve_posted` | sym, sac, tier u32 | amount i128, as_of u64, reporter, doc_hash bytes, doc_uri string |
| `reporter_set` | sym, reporter | role u32, name string |
| `reporter_metadata_updated` | sym, reporter | name symbol?, website_uri string?, contact_hash bytes? |
| `reporter_revoked` | sym, reporter | — |

## Rules that matter

- `horizon.Breakdown`'s field order **is the hash format**. Changing it changes
  every future on-chain hash. Breakdowns are stored as TEXT and served
  byte-for-byte.
- `internal/coverage` must give the same answer as the contract's `coverage()`.
- Amounts: `*big.Int` ↔ `NUMERIC(40,0)` ↔ JSON strings. No floats.
- Errors are wrapped with context (`fmt.Errorf("...: %w", err)`); logging is
  `log/slog` key/value.
- Timestamps leave the API in UTC.
- Schema changes are a new numbered file in `internal/store/migrations/`;
  never edit a merged one (CONTRIBUTING.md, "Schema migrations").

## Before every commit

```bash
gofmt -l .          # must print nothing
go vet ./...
go test ./...
```

## Git rules

Stage named files only (never `git add .`), one logical unit per commit,
Conventional Commits with scopes `config`, `horizon`, `toml`, `soroban`,
`coverage`, `store`, `worker`, `api`, `cmd`, `ci`, push after each.

## Do not

- [ ] log or return `SUPPLY_POSTER_SECRET`
- [ ] fetch arbitrary URLs without size and time limits
- [ ] post supply for a ledger at or below the last snapshot
- [ ] change event decoding without a matching contracts issue
