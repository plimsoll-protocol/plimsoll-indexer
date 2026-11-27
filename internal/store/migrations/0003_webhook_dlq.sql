CREATE TABLE webhook_dlq (
    id          BIGSERIAL PRIMARY KEY,
    webhook_id  BIGINT NOT NULL REFERENCES webhooks(id) ON DELETE CASCADE,
    payload     BYTEA NOT NULL,
    last_error  TEXT NOT NULL,
    attempts    INTEGER NOT NULL CHECK (attempts > 0),
    failed_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX webhook_dlq_webhook_id_id
    ON webhook_dlq (webhook_id, id DESC);
