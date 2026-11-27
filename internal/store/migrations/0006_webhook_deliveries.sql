-- Delivery is tracked per endpoint, so an operator can tell which destination
-- is failing without reading the logs. Every attempt increments deliveries and
-- records the last outcome; a failed attempt also increments failures.
--
-- Which key registered an endpoint lives in 0005_webhook_owners.sql; this
-- migration only adds the counters and the lookup an operator needs to find
-- one key's endpoints.
ALTER TABLE webhooks ADD COLUMN last_delivery_at TIMESTAMPTZ;
ALTER TABLE webhooks ADD COLUMN last_delivery_status TEXT;
ALTER TABLE webhooks ADD COLUMN last_delivery_error TEXT;
ALTER TABLE webhooks ADD COLUMN deliveries BIGINT NOT NULL DEFAULT 0;
ALTER TABLE webhooks ADD COLUMN failures BIGINT NOT NULL DEFAULT 0;

CREATE INDEX webhooks_owner_key_hash_id ON webhooks (owner_key_hash, id);
