# Webhooks

The indexer can send `issuer_toml_changed` events when an issuer's SEP-1
`stellar.toml` changes. Each HTTPS destination has a separate signing secret.
One API key may register up to 10 destinations; every event is delivered to all
of that key's destinations, and delivery is tracked per destination.

## Enable webhook management

Set both values in the indexer's environment:

```bash
WEBHOOK_ADMIN_TOKEN=<long-random-admin-token>
WEBHOOK_API_KEYS=<owner-key-1>,<owner-key-2>
WEBHOOK_ENCRYPTION_KEY=$(openssl rand -base64 32)
```

`WEBHOOK_API_KEYS` is an optional comma-separated list of owner API keys. The
administrator token can also create and delete webhooks it owns, and is required
to list all destinations. Each API key can delete only webhooks registered with
that same key. Keep all tokens private. The encryption key must remain stable:
it is used to decrypt signing secrets at delivery time. Losing or rotating it
makes existing webhook secrets unusable. The database stores SHA-256 hashes of
owner API keys and signing secrets, plus an AES-256-GCM-encrypted copy of each
signing secret; it never stores either key in plaintext.

## Register a destination

Register a destination over HTTPS with the administrator token or one of the
configured owner API keys. The key that registers a destination owns it,
recorded only as a SHA-256 fingerprint of the token, never the token itself:

```bash
curl -X POST https://indexer.example/v1/webhooks \
  -H "Authorization: Bearer $WEBHOOK_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"url":"https://receiver.example/plimsoll"}'
```

The `201` response includes the generated `secret` once. Save it securely;
later list requests omit it. Each destination has its own secret, so repeat the
call for every receiver you want to add: a key may register up to **10**
destinations. An attempt beyond that answers `422` with
`{"code":"webhook_limit_reached"}`. A valid key that does not own a webhook
receives `403` from `DELETE /v1/webhooks/{id}`; missing, unknown and
already-deleted IDs receive `404`.

The key is stored only as a SHA-256 fingerprint, never as the token itself.
`GET /v1/webhooks` therefore lists the destinations registered with the
presented key, and `DELETE /v1/webhooks/{id}` only deletes one of them. The
indexer only accepts HTTPS destinations and disables HTTP redirects.

## List endpoints and delivery state

`GET /v1/webhooks` also reports delivery per destination, so a failing receiver
is visible without reading the indexer's logs:

```json
{
  "webhooks": [
    {
      "id": 1,
      "url": "https://receiver.example/plimsoll",
      "created_at": "2026-11-24T15:00:00Z",
      "delivery": {
        "deliveries": 12,
        "failures": 1,
        "last_at": "2026-11-26T09:00:00Z",
        "last_status": "failed",
        "last_error": "webhook returned status 500"
      }
    }
  ]
}
```

Every attempt increments `deliveries`; a failed attempt also increments
`failures` and records `last_status` and `last_error`. A successful delivery
sets `last_status` to `delivered` and omits `last_error`. Events fan out to
every registered destination, so one receiver failing does not stop the others;
the failure is returned to the TOML sync job, which logs it and continues on its
next tick. The indexer only accepts HTTPS destinations and disables HTTP
redirects.

## Verify a delivery

The indexer signs the exact request-body bytes using HMAC-SHA256 and sends the
lowercase hexadecimal digest prefixed by `sha256=` in
`X-Plimsoll-Signature-256`. Verify the raw bytes before parsing or re-encoding
the JSON. For example, in Python:

```python
import hashlib
import hmac

def valid_plimsoll_signature(raw_body: bytes, secret: str, header: str) -> bool:
    if not header.startswith("sha256="):
        return False
    expected = hmac.new(
        secret.encode("utf-8"), raw_body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(header.removeprefix("sha256="), expected)
```

A delivered TOML-change body has this shape:

```json
{
  "event": "issuer_toml_changed",
  "issuer": "GISSUER...",
  "home_domain": "issuer.example",
  "old_hash": "previous sha256 hex digest",
  "new_hash": "current sha256 hex digest",
  "occurred_at": "2026-11-24T15:00:00Z"
}
```

The hashes are SHA-256 over the exact fetched `stellar.toml` response bytes.
An initial successful fetch establishes a baseline and does not produce a
change event. A later successful fetch with a different hash is stored as a
new issuer snapshot, logged as `issuer stellar.toml changed`, and delivered to
each registered destination.
## Coverage alerts

This guide covers issuer TOML change subscriptions at `/v1/webhooks`. Coverage
threshold alerts are a separate feature with their own routes
(`/v1/coverage-webhooks`), table (`coverage_webhooks`) and shared signing secret
(`WEBHOOK_SIGNING_SECRET`); see the "Coverage alerts" section of the
[README](../README.md) for the request body, payload and signature header.

## Failed deliveries

The indexer makes the initial delivery attempt, then retries up to three times
after 1, 2 and 4 seconds. A delivery that still fails is saved in the
`webhook_dlq` table with its original payload, last error, attempt count and
failure time. Inspect failures for a destination with the administrator token:

```bash
curl https://indexer.example/v1/webhooks/12/dlq \
  -H "Authorization: Bearer $WEBHOOK_ADMIN_TOKEN"
```

Retry a delivery using its `id` from that response:

```bash
curl -X POST https://indexer.example/v1/webhooks/12/dlq/34/retry \
  -H "Authorization: Bearer $WEBHOOK_ADMIN_TOKEN"
```

Manual retry uses the same retry policy. On success the DLQ row is removed; on
failure it remains available with the latest error and an updated attempt
count. The list endpoint accepts an optional `limit` query parameter (1-1000,
default 100).

## Inspect failed deliveries

Every delivery attempt is recorded with the HTTP status, any error, and
`delivery_latency_ms`: the time from sending the request to receiving the
complete response. Failed attempts are the webhook's dead-letter queue, and
can be listed with the administrator token:

```bash
curl https://indexer.example/v1/webhooks/1/dlq \
  -H "Authorization: Bearer $WEBHOOK_ADMIN_TOKEN"
```

The response is `{"deliveries": [...]}`, newest first. Use `limit` (1-1000,
default 100) to bound it. A high `delivery_latency_ms` alongside a failure
points at a slow upstream endpoint rather than a rejected signature.
