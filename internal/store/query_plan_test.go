package store

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// seedReports fills reserve_reports with assets x perAsset rows spread over the
// three tiers, so the planner has real statistics instead of a nearly empty
// table it would always seq-scan.
func seedReports(tb testing.TB, conn *pgx.Conn, assets, perAsset int) {
	tb.Helper()
	ctx := context.Background()
	if _, err := conn.Exec(ctx, `
		INSERT INTO reserve_reports
		  (event_id, sac, tier, amount, as_of, reporter, doc_hash, doc_uri, posted_ledger, posted_at, tx_hash)
		SELECT 'ev-' || a.i || '-' || r.i, 'CSAC' || a.i, 1 + (r.i % 3), 1000 + r.i,
		       now() - (r.i || ' seconds')::interval, 'GISS', 'dh', 'https://x', r.i, now(), 'tx' || r.i
		FROM generate_series(1, $1) AS a(i), generate_series(1, $2) AS r(i)`,
		assets, perAsset); err != nil {
		tb.Fatalf("seed reserve_reports: %v", err)
	}
	if _, err := conn.Exec(ctx, `ANALYZE reserve_reports`); err != nil {
		tb.Fatalf("analyze reserve_reports: %v", err)
	}
}

// queryPlan returns the EXPLAIN (COSTS OFF) output for a query, one line per
// row, so a test can assert which access path the planner picked.
func queryPlan(tb testing.TB, conn *pgx.Conn, query string, args ...any) string {
	tb.Helper()
	rows, err := conn.Query(context.Background(), "EXPLAIN (COSTS OFF) "+query, args...)
	if err != nil {
		tb.Fatalf("explain: %v", err)
	}
	lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		tb.Fatalf("explain: %v", err)
	}
	return strings.Join(lines, "\n")
}

func connect(tb testing.TB, url string) *pgx.Conn {
	tb.Helper()
	conn, err := pgx.Connect(context.Background(), url)
	if err != nil {
		tb.Fatalf("connect: %v", err)
	}
	tb.Cleanup(func() { _ = conn.Close(context.Background()) })
	return conn
}

// TestCoverageSelectionUsesIndexes covers issue #107: the coverage selection
// query must reach the composite index instead of scanning the asset's whole
// report history, and report history must be read in index order so LIMIT can
// stop early.
func TestCoverageSelectionUsesIndexes(t *testing.T) {
	s, url := openStore(t)
	conn := connect(t, url)
	seedReports(t, conn, 300, 400)

	latest := queryPlan(t, conn, latestReportsSQL, "CSAC7")
	if strings.Contains(latest, "Seq Scan on reserve_reports") {
		t.Fatalf("coverage selection still scans reserve_reports:\n%s", latest)
	}
	if !strings.Contains(latest, "reserve_reports_sac_tier_asof") {
		t.Fatalf("coverage selection does not use reserve_reports_sac_tier_asof:\n%s", latest)
	}

	history := queryPlan(t, conn, reportHistorySQL, "CSAC7", 100)
	if strings.Contains(history, "Seq Scan on reserve_reports") {
		t.Fatalf("report history still scans reserve_reports:\n%s", history)
	}
	if !strings.Contains(history, "reserve_reports_sac_posted_ledger") {
		t.Fatalf("report history does not use reserve_reports_sac_posted_ledger:\n%s", history)
	}

	// The indexed probes must still pick the newest report of each tier.
	reports, err := s.LatestReports(context.Background(), "CSAC7")
	if err != nil {
		t.Fatalf("latest reports: %v", err)
	}
	if len(reports) != 3 {
		t.Fatalf("latest reports has %d tiers, want 3", len(reports))
	}
	for tier, r := range reports {
		if r.Tier != tier {
			t.Fatalf("tier %d holds a report of tier %d", tier, r.Tier)
		}
		// Seeded amounts are 1000 + row, so a report outside this range came
		// from another asset or another tier.
		if got := r.Amount.Int64(); got <= 1000 || got > 1400 {
			t.Fatalf("tier %d newest report has seeded amount %d out of range", tier, got)
		}
	}
}

// BenchmarkLatestReports measures the coverage selection query for one asset
// out of many, which is what /v1/assets/{sac} and each row of /v1/assets do.
func BenchmarkLatestReports(b *testing.B) {
	s, url := openStore(b)
	conn := connect(b, url)
	seedReports(b, conn, 300, 400)
	ctx := context.Background()

	b.ResetTimer()
	for b.Loop() {
		if _, err := s.LatestReports(ctx, "CSAC7"); err != nil {
			b.Fatal(err)
		}
	}
}
