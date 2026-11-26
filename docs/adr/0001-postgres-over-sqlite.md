# 1. Postgres over SQLite

Status: Accepted

## Context
We need a persistent relational data store for indexer read models, raw event sourcing logs, and webhooks.

## Decision
We use PostgreSQL as our primary database.

## Consequences
- Robust concurrent connection handling.
- Advanced JSONB support for event details.
- Requires external Postgres instance for deployment.
