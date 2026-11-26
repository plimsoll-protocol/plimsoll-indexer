# API response conventions

The HTTP response schemas are defined in [`openapi.yaml`](../internal/api/openapi.yaml).

## Optional values

Optional properties in API-defined response objects are always present. When a
value is unavailable or not applicable, the API returns JSON `null`; it does
not omit the property. This keeps response shapes stable while distinguishing
an unavailable value from an empty string, zero, or empty collection. Non-null
values retain their existing representation.

The `toml` value is an embedded SEP-1 document, not an API-defined response
object; its own optional properties follow the SEP-1 document shape.

Examples include:

- Asset `toml_checked_at` when TOML has not been checked and `toml_error` when
  no error is recorded.
- Asset `supply` or `coverage` when no corresponding data exists.
- Coverage `bps` and `ratio` when coverage cannot be calculated.
- Report `doc_check` before document verification has run.
- Health `last_event_at` and `last_event_age_seconds` before an event is recorded.

Collections remain empty arrays or objects when they have no entries; they are
not `null`. Fields that are always required and non-null remain unchanged.
