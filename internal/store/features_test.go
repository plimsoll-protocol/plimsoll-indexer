package store

import (
	"context"
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

const testSAC = "CCHPT4TEJDPZQDSUVW3NP6A35ROGV4WEFZKT45SKWYCFZSUB7R5HIM7S"

func applyForTest(t *testing.T, s *Store, events ...any) {
	t.Helper()
	if err := s.ApplyEvents(context.Background(), "plimsoll-events", "cursor-test", 1000, events); err != nil {
		t.Fatalf("apply events: %v", err)
	}
}

func listed(t *testing.T, sac, code string, ledger uint32, at time.Time) soroban.AssetListed {
	t.Helper()
	return soroban.AssetListed{
		Meta: soroban.Meta{ID: "listed:" + sac, Ledger: ledger, ClosedAt: at.UTC()},
		SAC:  sac, Code: code, Issuer: "GISS",
	}
}

func supplyAt(t *testing.T, id, sac string, amount int64, atLedger, posted uint32, postedAt time.Time, hash string) soroban.SupplyPosted {
	t.Helper()
	return soroban.SupplyPosted{
		Meta:          soroban.Meta{ID: id, Ledger: posted, ClosedAt: postedAt.UTC()},
		SAC:           sac,
		Amount:        big.NewInt(amount),
		AtLedger:      atLedger,
		BreakdownHash: hash,
		Poster:        "GPOSTER",
	}
}

func reportAt(t *testing.T, id, sac string, tier uint32, amount int64, asOf time.Time, posted uint32) soroban.ReservePosted {
	t.Helper()
	return soroban.ReservePosted{
		Meta:     soroban.Meta{ID: id, Ledger: posted, ClosedAt: asOf.UTC()},
		SAC:      sac,
		Tier:     tier,
		Amount:   big.NewInt(amount),
		AsOf:     asOf.UTC(),
		Reporter: "GREPORTER",
		DocHash:  "doc-hash",
		DocURI:   "https://example.org/report",
	}
}

// Ingestion refreshes latest_coverage in the same transaction, so the view
// never serves a figure older than the rows behind it.
func TestLatestCoverageRefreshesAfterEachBatch(t *testing.T) {
	s, _ := openStore(t)
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

	applyForTest(t, s,
		listed(t, testSAC, "PUSD", 10, now),
		supplyAt(t, "supply-1", testSAC, 1000, 10, 10, now, "hash-1"),
		reportAt(t, "report-1", testSAC, coverage.TierTranscribed, 1100, now, 10),
	)

	sp, err := s.LatestSupply(context.Background(), testSAC)
	if err != nil {
		t.Fatal(err)
	}
	if sp == nil || sp.Amount.Int64() != 1000 || sp.Ledger != 10 {
		t.Fatalf("latest supply = %+v", sp)
	}
	reports, err := s.LatestReports(context.Background(), testSAC)
	if err != nil {
		t.Fatal(err)
	}
	if r, ok := reports[coverage.TierTranscribed]; !ok || r.Amount.Int64() != 1100 {
		t.Fatalf("latest reports = %+v", reports)
	}

	// A later batch must be visible immediately, not only after a manual
	// refresh.
	applyForTest(t, s,
		supplyAt(t, "supply-2", testSAC, 2000, 20, 20, now.Add(time.Hour), "hash-2"),
		reportAt(t, "report-2", testSAC, coverage.TierTranscribed, 2100, now.Add(time.Hour), 20),
		reportAt(t, "report-3", testSAC, coverage.TierAuditorSigned, 2050, now.Add(2*time.Hour), 21),
	)
	sp, err = s.LatestSupply(context.Background(), testSAC)
	if err != nil {
		t.Fatal(err)
	}
	if sp == nil || sp.Amount.Int64() != 2000 || sp.Ledger != 20 {
		t.Fatalf("latest supply after second batch = %+v", sp)
	}
	reports, err = s.LatestReports(context.Background(), testSAC)
	if err != nil {
		t.Fatal(err)
	}
	if reports[coverage.TierTranscribed].Amount.Int64() != 2100 {
		t.Errorf("transcribed report = %+v", reports[coverage.TierTranscribed])
	}
	if reports[coverage.TierAuditorSigned].Amount.Int64() != 2050 {
		t.Errorf("auditor report = %+v", reports[coverage.TierAuditorSigned])
	}
}

func TestPruneDeletesOldRowsAndKeepsLiveBreakdowns(t *testing.T) {
	s, url := openStore(t)
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	old := now.AddDate(0, 0, -400)
	recent := now.AddDate(0, 0, -10)

	applyForTest(t, s,
		listed(t, testSAC, "PUSD", 10, old),
		supplyAt(t, "supply-old", testSAC, 1000, 10, 10, old, "hash-old"),
		supplyAt(t, "supply-new", testSAC, 2000, 20, 20, recent, "hash-kept"),
		reportAt(t, "report-old", testSAC, coverage.TierTranscribed, 1000, old, 10),
		reportAt(t, "report-new", testSAC, coverage.TierTranscribed, 2000, recent, 20),
	)
	execSQL(t, url, `INSERT INTO breakdowns (hash, sac, body, created_at) VALUES
		('hash-old', $1, '{"old":true}', $2),
		('hash-kept', $1, '{"kept":true}', $2),
		('hash-unreferenced', $1, '{"unused":true}', $2)`, testSAC, old)

	result, err := s.Prune(context.Background(), now.AddDate(0, 0, -365))
	if err != nil {
		t.Fatal(err)
	}
	if result.SupplySnapshots != 1 || result.ReserveReports != 1 || result.Breakdowns != 2 {
		t.Fatalf("prune result = %+v", result)
	}
	if result.Total() != 4 {
		t.Errorf("total = %d, want 4", result.Total())
	}

	history, err := s.SupplyHistory(context.Background(), testSAC, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].Amount.Int64() != 2000 {
		t.Fatalf("supply history after prune = %+v", history)
	}
	reports, err := s.ReportHistory(context.Background(), testSAC, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].Amount.Int64() != 2000 {
		t.Fatalf("report history after prune = %+v", reports)
	}

	// Prune refreshes the view too, so the latest reads cannot keep serving a
	// snapshot the tables no longer hold.
	latest, err := s.LatestSupply(context.Background(), testSAC)
	if err != nil {
		t.Fatal(err)
	}
	if latest == nil || latest.Amount.Int64() != 2000 {
		t.Fatalf("latest supply after prune = %+v", latest)
	}

	if _, err := s.Breakdown(context.Background(), "hash-old"); !errors.Is(err, ErrNotFound) {
		t.Errorf("old breakdown still served: %v", err)
	}
	if _, err := s.Breakdown(context.Background(), "hash-unreferenced"); !errors.Is(err, ErrNotFound) {
		t.Errorf("unreferenced breakdown still served: %v", err)
	}
	if body, err := s.Breakdown(context.Background(), "hash-kept"); err != nil || string(body) != `{"kept":true}` {
		t.Errorf("live breakdown = %q, %v", body, err)
	}
}

func TestPruneKeepsEverythingWithNoCutoffRows(t *testing.T) {
	s, _ := openStore(t)
	now := time.Now().UTC()
	applyForTest(t, s,
		listed(t, testSAC, "PUSD", 10, now),
		supplyAt(t, "supply-1", testSAC, 1000, 10, 10, now, "hash-1"),
	)
	result, err := s.Prune(context.Background(), now.AddDate(0, 0, -365))
	if err != nil {
		t.Fatal(err)
	}
	if result.Total() != 0 {
		t.Fatalf("pruned %+v", result)
	}
}

func TestReindexCountsThenDeletesFromLedger(t *testing.T) {
	s, _ := openStore(t)
	ctx := context.Background()
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	otherSAC := "CDLZFC3SYJYDZT7K67VZ75HPJVIEUVNIXF47ZG2FB2RMQQVU2HHGCYSC"

	applyForTest(t, s,
		listed(t, testSAC, "PUSD", 10, now),
		listed(t, otherSAC, "PXXX", 30, now),
		supplyAt(t, "supply-10", testSAC, 1000, 10, 10, now, "hash-10"),
		supplyAt(t, "supply-20", testSAC, 2000, 20, 20, now, "hash-20"),
		supplyAt(t, "supply-30", testSAC, 3000, 30, 30, now, "hash-30"),
		reportAt(t, "report-20", testSAC, coverage.TierTranscribed, 2500, now, 20),
	)

	counts, err := s.CountEventsFromLedger(ctx, 20)
	if err != nil {
		t.Fatal(err)
	}
	if counts.Assets != 1 || counts.SupplySnapshots != 2 || counts.ReserveReports != 1 {
		t.Fatalf("counts = %+v", counts)
	}
	if counts.Total() != 4 {
		t.Errorf("total = %d, want 4", counts.Total())
	}

	// Counting must not delete anything.
	before, err := s.SupplyHistory(ctx, testSAC, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(before) != 3 {
		t.Fatalf("dry run changed supply history: %d rows", len(before))
	}

	deleted, err := s.DeleteEventsFromLedger(ctx, 20)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != counts {
		t.Fatalf("deleted %+v, want %+v", deleted, counts)
	}

	after, err := s.SupplyHistory(ctx, testSAC, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 1 || after[0].Amount.Int64() != 1000 {
		t.Fatalf("supply history after reindex delete = %+v", after)
	}
	reports, err := s.ReportHistory(ctx, testSAC, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 0 {
		t.Fatalf("reports after reindex delete = %+v", reports)
	}
	if _, err := s.GetAsset(ctx, otherSAC); !errors.Is(err, ErrNotFound) {
		t.Errorf("asset at ledger 30 still present: %v", err)
	}
	if _, err := s.GetAsset(ctx, testSAC); err != nil {
		t.Errorf("asset at ledger 10 was removed: %v", err)
	}
}

func TestResetIngestCursorMakesNextRunStartFresh(t *testing.T) {
	s, _ := openStore(t)
	ctx := context.Background()
	applyForTest(t, s, listed(t, testSAC, "PUSD", 10, time.Now()))

	p, err := s.Progress(ctx, "plimsoll-events")
	if err != nil {
		t.Fatal(err)
	}
	if p.Cursor == "" {
		t.Fatal("ApplyEvents did not store a cursor")
	}
	if err := s.ResetIngestCursor(ctx, "plimsoll-events"); err != nil {
		t.Fatal(err)
	}
	p, err = s.Progress(ctx, "plimsoll-events")
	if err != nil {
		t.Fatal(err)
	}
	if p.Cursor != "" || p.ScannedLedger != 0 {
		t.Fatalf("progress after reset = %+v", p)
	}
}

func TestVacuumAcceptsOnlyRetentionTables(t *testing.T) {
	s, _ := openStore(t)
	ctx := context.Background()
	if err := s.Vacuum(ctx, "supply_snapshots", "reserve_reports", "breakdowns"); err != nil {
		t.Fatalf("vacuum retention tables: %v", err)
	}
	if err := s.Vacuum(ctx, "assets"); err == nil {
		t.Fatal("vacuum accepted a table outside the retention allowlist")
	}
}
