# 3. NDJSON Streaming vs Pagination

Status: Accepted

## Context
Exporting large datasets from the indexer API requires efficient transport.

## Decision
We use standard cursor-based pagination for record endpoints and optional NDJSON streaming for bulk exports.

## Consequences
- Predictable memory consumption.
- Easy client-side integration.
