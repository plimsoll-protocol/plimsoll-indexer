-- Disputes on reserve reports. A dispute challenges one report, identified by
-- (sac, report_as_of); the reporter column names who signed that report and
-- disputer names the watcher who challenged it.
--
-- The current contracts do not emit a dispute event yet. plimsoll-contracts#91
-- proposes `report_disputed` (sac, as_of, disputer, hold_until_ledger) and
-- `dispute_resolved` (sac, as_of); this table is shaped to hold exactly those
-- facts so ingestion can be added without another schema change. Until then
-- the table is empty and GET /v1/assets/{sac}/disputes returns an empty page.
CREATE TABLE disputes (
    id                BIGSERIAL PRIMARY KEY,
    event_id          TEXT NOT NULL UNIQUE,
    sac               TEXT NOT NULL,
    report_as_of      TIMESTAMPTZ NOT NULL,
    reporter          TEXT NOT NULL,
    disputer          TEXT NOT NULL,
    status            TEXT NOT NULL CHECK (status IN ('open', 'resolved')),
    resolution        TEXT NOT NULL DEFAULT '',
    hold_until_ledger BIGINT NOT NULL DEFAULT 0,
    opened_ledger     BIGINT NOT NULL,
    opened_at         TIMESTAMPTZ NOT NULL,
    resolved_ledger   BIGINT,
    resolved_at       TIMESTAMPTZ,
    tx_hash           TEXT NOT NULL DEFAULT ''
);

-- The endpoint reads one asset's disputes newest-opened first, optionally
-- filtered by status. Both indexes lead with the column the query constrains
-- so a page stops after LIMIT rows without sorting the asset's history.
CREATE INDEX disputes_sac_opened
    ON disputes (sac, opened_at DESC, id DESC);
CREATE INDEX disputes_sac_status_opened
    ON disputes (sac, status, opened_at DESC, id DESC);
