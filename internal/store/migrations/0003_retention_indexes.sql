-- The retention job deletes by age. Without these indexes every nightly run
-- scans each table in full to find the rows to remove.
CREATE INDEX IF NOT EXISTS supply_snapshots_posted_at
    ON supply_snapshots (posted_at);
CREATE INDEX IF NOT EXISTS reserve_reports_posted_at
    ON reserve_reports (posted_at);
CREATE INDEX IF NOT EXISTS breakdowns_created_at
    ON breakdowns (created_at);
-- Pruning an old breakdown checks whether any surviving snapshot still refers
-- to it. Without this index that check scans supply_snapshots once per
-- candidate breakdown.
CREATE INDEX IF NOT EXISTS supply_snapshots_breakdown_hash
    ON supply_snapshots (breakdown_hash);
