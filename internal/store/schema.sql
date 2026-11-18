-- Idempotent schema. Applied on every start.

CREATE TABLE IF NOT EXISTS ingest_cursor (
    name        TEXT PRIMARY KEY,
    cursor      TEXT NOT NULL DEFAULT '',
    ledger      BIGINT NOT NULL DEFAULT 0,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- `ledger` is how far the scan has reached, which advances whether or not a
-- page held anything. These record the last event actually written, so a scan
-- that silently stores nothing is visible from /healthz.
ALTER TABLE ingest_cursor ADD COLUMN IF NOT EXISTS event_ledger BIGINT NOT NULL DEFAULT 0;
ALTER TABLE ingest_cursor ADD COLUMN IF NOT EXISTS event_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS assets (
    sac             TEXT PRIMARY KEY,
    code            TEXT NOT NULL,
    issuer          TEXT NOT NULL,
    listed_ledger   BIGINT NOT NULL,
    listed_at       TIMESTAMPTZ NOT NULL,
    event_id        TEXT NOT NULL UNIQUE,
    toml            JSONB,
    toml_checked_at TIMESTAMPTZ,
    toml_error      TEXT
);

CREATE TABLE IF NOT EXISTS supply_snapshots (
    event_id        TEXT PRIMARY KEY,
    sac             TEXT NOT NULL,
    amount          NUMERIC(40, 0) NOT NULL,
    at_ledger       BIGINT NOT NULL,
    breakdown_hash  TEXT NOT NULL,
    poster          TEXT NOT NULL,
    posted_ledger   BIGINT NOT NULL,
    posted_at       TIMESTAMPTZ NOT NULL,
    tx_hash         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS supply_snapshots_sac_ledger
    ON supply_snapshots (sac, at_ledger DESC);
-- 'event' for a row decoded from a contract event, which carries the
-- transaction that produced it; 'chain' for one read back from contract state
-- by the reconciler, where the originating transaction is not recoverable and
-- tx_hash is empty.
ALTER TABLE supply_snapshots ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'event';

CREATE TABLE IF NOT EXISTS reserve_reports (
    event_id        TEXT PRIMARY KEY,
    sac             TEXT NOT NULL,
    tier            SMALLINT NOT NULL,
    amount          NUMERIC(40, 0) NOT NULL,
    as_of           TIMESTAMPTZ NOT NULL,
    reporter        TEXT NOT NULL,
    doc_hash        TEXT NOT NULL,
    doc_uri         TEXT NOT NULL,
    posted_ledger   BIGINT NOT NULL,
    posted_at       TIMESTAMPTZ NOT NULL,
    tx_hash         TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS reserve_reports_sac_tier_asof
    ON reserve_reports (sac, tier, as_of DESC);
ALTER TABLE reserve_reports ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'event';

CREATE TABLE IF NOT EXISTS reporters (
    address         TEXT PRIMARY KEY,
    role            SMALLINT NOT NULL,
    name            TEXT NOT NULL,
    active          BOOLEAN NOT NULL,
    updated_ledger  BIGINT NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL
);

-- Supply breakdowns the poster computed, keyed by the hash posted on-chain.
-- TEXT, not JSONB: the exact bytes must be served back so the hash checks out.
CREATE TABLE IF NOT EXISTS breakdowns (
    hash        TEXT PRIMARY KEY,
    sac         TEXT NOT NULL,
    body        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
