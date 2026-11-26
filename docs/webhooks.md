# Webhooks

The indexer can send `issuer_toml_changed` events when an issuer's SEP-1
`stellar.toml` changes. Each HTTPS destination has a separate signing secret.

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
configured owner API keys:

```bash
curl -X POST https://indexer.example/v1/webhooks \
  -H "Authorization: Bearer $WEBHOOK_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"url":"https://receiver.example/plimsoll"}'
```

The `201` response includes the generated `secret` once. Save it securely;
later list requests omit it. Use `GET /v1/webhooks` to list destinations and
`DELETE /v1/webhooks/{id}` with the same API key to remove one. A valid key that
does not own the webhook receives `403`; missing, unknown, and already-deleted
IDs receive `404`. The indexer only accepts HTTPS destinations and disables
HTTP redirects.

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
