-- Every contract event ingestion receives, decoded or not, as RPC returned it.
-- RPC keeps events for about seven days; these let `plimsoll-indexer replay`
-- rebuild the typed tables after a decoder change. closed_at is null when
-- RPC's ledgerClosedAt did not parse.
CREATE TABLE raw_events (
    id          TEXT PRIMARY KEY,
    contract    TEXT NOT NULL,
    ledger      BIGINT NOT NULL,
    closed_at   TIMESTAMPTZ,
    tx_hash     TEXT NOT NULL,
    topics_xdr  TEXT[] NOT NULL,
    value_xdr   TEXT NOT NULL
);
CREATE INDEX raw_events_contract_ledger
    ON raw_events (contract, ledger);
