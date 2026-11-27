ALTER TABLE webhooks
    ADD COLUMN owner_key_hash TEXT NOT NULL DEFAULT '';
