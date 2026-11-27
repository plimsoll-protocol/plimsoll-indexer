-- Materialized latest coverage per asset.
--
-- GET /v1/assets used to ask supply_snapshots and reserve_reports for each
-- asset in turn. Those are correlated lookups, and the reports one has to sort
-- every report an asset ever posted. This view materialises the newest supply
-- snapshot and the newest report in each tier, so the API reads one row per
-- asset and tier instead.
--
-- REFRESH MATERIALIZED VIEW (not CONCURRENTLY) runs after each ingest batch,
-- so the view and the tables it summarises move together.
--
-- tier is 0 for an asset with no reports. That keeps the unique index simple
-- (Postgres unique indexes treat NULLs as distinct) and lets the view be
-- refreshed concurrently later if it grows large enough to need it.
CREATE MATERIALIZED VIEW IF NOT EXISTS latest_coverage AS
SELECT
    a.sac,
    s.amount        AS supply_amount,
    s.at_ledger     AS supply_ledger,
    s.posted_at     AS supply_posted_at,
    s.breakdown_hash,
    s.poster,
    s.tx_hash       AS supply_tx_hash,
    s.source        AS supply_source,
    COALESCE(r.tier, 0) AS tier,
    r.amount        AS report_amount,
    r.as_of,
    r.posted_at     AS report_posted_at,
    r.reporter,
    r.doc_hash,
    r.doc_uri,
    r.tx_hash       AS report_tx_hash,
    r.source        AS report_source,
    r.doc_check
FROM assets a
LEFT JOIN LATERAL (
    SELECT amount, at_ledger, posted_at, breakdown_hash, poster, tx_hash, source
    FROM supply_snapshots
    WHERE sac = a.sac
    ORDER BY at_ledger DESC
    LIMIT 1
) s ON true
LEFT JOIN LATERAL (
    SELECT DISTINCT ON (tier)
           tier, amount, as_of, posted_at, reporter, doc_hash, doc_uri, tx_hash, source, doc_check
    FROM reserve_reports
    WHERE sac = a.sac
    ORDER BY tier, as_of DESC
) r ON true;

CREATE UNIQUE INDEX IF NOT EXISTS latest_coverage_sac_tier
    ON latest_coverage (sac, tier);
