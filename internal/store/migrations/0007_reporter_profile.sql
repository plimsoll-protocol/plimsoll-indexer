-- Registry profile fields newer than the events this indexer consumes today.
--
-- `expires_at` mirrors the ReporterExpirySet proposed for the reporter registry
-- (plimsoll-contracts#92) and `reputation_score` its proposed on-chain score
-- (plimsoll-contracts#72). Both are nullable on purpose: NULL means "the
-- registry has not told us", which is different from a score of zero or a
-- reporter that has already expired. Until a registry deployment exposes them,
-- GET /v1/reporters reports null instead of inventing a value.
ALTER TABLE reporters ADD COLUMN IF NOT EXISTS reputation_score INTEGER;
ALTER TABLE reporters ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;

-- GET /v1/reporters derives each reporter's report and asset counts from the
-- reserve reports they signed, which groups by reporter.
CREATE INDEX IF NOT EXISTS reserve_reports_reporter
    ON reserve_reports (reporter);
