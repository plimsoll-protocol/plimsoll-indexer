-- Report history (GET /v1/assets/{sac}/reports) is read newest-posted first
-- per asset. `reserve_reports_sac_tier_asof` from 0001 orders by (sac, tier,
-- as_of), which the latest-report-per-tier lookup needs but which cannot order
-- posted_ledger: the history query read every report an asset ever posted and
-- sorted them before applying LIMIT.
--
-- This index returns the rows in the order the history query asks for, so the
-- scan stops after LIMIT rows instead of touching the asset's whole history.
CREATE INDEX reserve_reports_sac_posted_ledger
    ON reserve_reports (sac, posted_ledger DESC, as_of DESC);
