-- The poster saves a breakdown before it posts it. confirmed_at is set once
-- the breakdown is known to back an on-chain snapshot: post_supply succeeded,
-- the coverage ledger returned its hash, or a snapshot row carries it. Until
-- then it is not served, and it is deleted when it stays unconfirmed past the
-- poster's grace period.
ALTER TABLE breakdowns ADD COLUMN confirmed_at TIMESTAMPTZ;

-- Breakdowns saved before confirmation existed are confirmed by their snapshot.
UPDATE breakdowns b SET confirmed_at = b.created_at
WHERE EXISTS (SELECT 1 FROM supply_snapshots s WHERE s.sac = b.sac AND s.breakdown_hash = b.hash);

CREATE INDEX breakdowns_unconfirmed
    ON breakdowns (created_at) WHERE confirmed_at IS NULL;
