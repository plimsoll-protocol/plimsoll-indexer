CREATE TABLE vaults (
    sac             TEXT PRIMARY KEY,
    vault_address   TEXT NOT NULL,
    reserve_balance NUMERIC(40, 0) NOT NULL DEFAULT 0,
    threshold_bps   BIGINT NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
