-- A webhook endpoint belongs to the API key that registered it, so one key can
-- register several destinations and lists only its own. api_key is a SHA-256
-- fingerprint of the bearer token, never the token itself, so a database dump
-- cannot be replayed as a key. Rows created before this migration have an
-- empty fingerprint and stay visible to every key, which keeps existing
-- deployments working without a manual backfill.
ALTER TABLE webhooks ADD COLUMN api_key TEXT NOT NULL DEFAULT '';
CREATE INDEX webhooks_api_key_id ON webhooks (api_key, id);

-- Delivery is tracked per endpoint, so an operator can tell which destination
-- is failing without reading the logs. Every attempt increments deliveries and
-- records the last outcome; a failed attempt also increments failures.
ALTER TABLE webhooks ADD COLUMN last_delivery_at TIMESTAMPTZ;
ALTER TABLE webhooks ADD COLUMN last_delivery_status TEXT;
ALTER TABLE webhooks ADD COLUMN last_delivery_error TEXT;
ALTER TABLE webhooks ADD COLUMN deliveries BIGINT NOT NULL DEFAULT 0;
ALTER TABLE webhooks ADD COLUMN failures BIGINT NOT NULL DEFAULT 0;
