CREATE TABLE issuer_toml_snapshots (
    id          BIGSERIAL PRIMARY KEY,
    issuer      TEXT NOT NULL,
    home_domain TEXT NOT NULL,
    sha256_hash TEXT NOT NULL,
    checked_at  TIMESTAMPTZ NOT NULL
);
CREATE INDEX issuer_toml_snapshots_issuer_id
    ON issuer_toml_snapshots (issuer, id DESC);

CREATE TABLE webhooks (
    id                BIGSERIAL PRIMARY KEY,
    url               TEXT NOT NULL,
    secret_hash       TEXT NOT NULL,
    secret_ciphertext BYTEA NOT NULL,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);
