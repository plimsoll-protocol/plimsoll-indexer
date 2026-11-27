# 4. Separate Poster vs Unified Worker

Status: Accepted

## Context
Supply snapshot posting requires signing transactions with private keys, whereas ingestion and API serving do not require private keys.

## Decision
We separate the supply poster into an optional service/worker loop controlled by secret configuration, allowing read-only instances to run safely without private keys.

## Consequences
- Enhanced security boundary for private keys.
- Scalable independent deployments.
