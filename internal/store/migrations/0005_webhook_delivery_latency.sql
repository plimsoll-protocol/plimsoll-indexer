-- One row per webhook delivery attempt. A delivery that fails is a dead
-- letter until it is retried or discarded; keeping every attempt lets
-- operators compare failures with the latency of the endpoint that produced
-- them.
CREATE TABLE webhook_deliveries (
    id                  BIGSERIAL PRIMARY KEY,
    webhook_id          BIGINT NOT NULL REFERENCES webhooks(id) ON DELETE CASCADE,
    event               TEXT NOT NULL,
    status_code         INTEGER NOT NULL DEFAULT 0,
    error               TEXT,
    delivery_latency_ms BIGINT NOT NULL CHECK (delivery_latency_ms >= 0),
    attempted_at        TIMESTAMPTZ NOT NULL
);
CREATE INDEX webhook_deliveries_webhook_id_failed
    ON webhook_deliveries (webhook_id, id DESC)
    WHERE error IS NOT NULL;
