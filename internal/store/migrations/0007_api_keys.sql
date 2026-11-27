CREATE TABLE api_keys (
    id         BIGSERIAL PRIMARY KEY,
    key_hash   TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
