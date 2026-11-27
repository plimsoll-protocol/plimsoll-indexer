ALTER TABLE issuer_toml_snapshots
    ADD COLUMN IF NOT EXISTS content TEXT;
