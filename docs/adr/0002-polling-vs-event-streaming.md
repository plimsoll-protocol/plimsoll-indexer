# 2. Polling vs Event Streaming

Status: Accepted

## Context
Contract events must be ingested reliably from Soroban RPC.

## Decision
We use ledger range polling with state tracking rather than persistent streaming sockets.

## Consequences
- Resilient against network drops and indexer restarts.
- Simple retry logic per ledger sequence.
