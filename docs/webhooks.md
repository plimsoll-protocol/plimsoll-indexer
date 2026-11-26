# Webhooks

The indexer can send `issuer_toml_changed` events when an issuer's SEP-1
`stellar.toml` changes. Each HTTPS destination has a separate signing secret.
One API key may register up to 10 destinations; every event is delivered to all
of that key's destinations, and delivery is tracked per destination.

## Enable webhook management

Set both values in the indexer's environment:

```bash
WEBHOOK_ADMIN_TOKEN=<long-random-admin-token>
WEBHOOK_ENCRYPTION_KEY=$(openssl rand -base64 32)
```

Keep both values private. The encryption key must remain stable: it is used to
decrypt signing secrets at delivery time. Losing or rotating it makes existing
webhook secrets unusable. The database stores a SHA-256 hash and an
AES-256-GCM-encrypted copy of each secret; it never stores the plaintext.

## Register a destination

Webhook management requires the administrator bearer token, which identifies
the API key that owns the destinations. Register a destination over HTTPS:

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
`{"code":"webhook_limit_reached"}`.

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
next tick.

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
