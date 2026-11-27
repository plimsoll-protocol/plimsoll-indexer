package store

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// BenchmarkLatestReports measures the read GET /v1/assets performs for every
// asset: first through latest_coverage, then through the DISTINCT ON query it
// replaced. Run it with `go test ./internal/store -run '^$' -bench LatestReports`.
func BenchmarkLatestReports(b *testing.B) {
	s, _ := openStore(b)
	sacs := seedLatestReportsBenchmark(b, s, 250, 60)

	ctx := context.Background()
	b.Run("latest_coverage", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			for _, sac := range sacs {
				if _, err := s.LatestReports(ctx, sac); err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	b.Run("distinct_on_reserve_reports", func(b *testing.B) {
		b.ResetTimer()
		for range b.N {
			for _, sac := range sacs {
				if _, err := s.queryReports(ctx, `
					SELECT DISTINCT ON (tier) tier, amount::text, as_of, posted_at, reporter, doc_hash, doc_uri, tx_hash, source,
					                           COALESCE(doc_check, '')
					FROM reserve_reports WHERE sac = $1
					ORDER BY tier, as_of DESC`, sac); err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

// seedLatestReportsBenchmark writes assets and reports directly, bypassing the
// event path so setup does not refresh the materialized view once per row.
func seedLatestReportsBenchmark(b *testing.B, s *Store, assets, reportsPerAsset int) []string {
	b.Helper()
	ctx := context.Background()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		b.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	sacs := make([]string, 0, assets)
	for i := range assets {
		sac := fmt.Sprintf("CBENCH%055d", i)
		sacs = append(sacs, sac)
		if _, err := tx.Exec(ctx, `
			INSERT INTO assets (sac, code, issuer, listed_ledger, listed_at, event_id)
			VALUES ($1, $2, 'GBENCH', 10, $3, $4)`,
			sac, fmt.Sprintf("B%03d", i), now, fmt.Sprintf("bench-asset-%d", i)); err != nil {
			b.Fatal(err)
		}
		for r := range reportsPerAsset {
			tier := r%3 + 1
			if _, err := tx.Exec(ctx, `
				INSERT INTO reserve_reports
				  (event_id, sac, tier, amount, as_of, reporter, doc_hash, doc_uri, posted_ledger, posted_at, tx_hash, source)
				VALUES ($1, $2, $3, $4, $5, 'GBENCH', 'doc', 'https://example.test/report', $6, $5, 'tx', 'event')`,
				fmt.Sprintf("bench-report-%d-%d", i, r), sac, tier, int64(1_000_000+r),
				now.Add(-time.Duration(r)*time.Hour), int64(10+r)); err != nil {
				b.Fatal(err)
			}
		}
	}
	if _, err := tx.Exec(ctx, `REFRESH MATERIALIZED VIEW latest_coverage`); err != nil {
		b.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		b.Fatal(err)
	}
	return sacs
}
