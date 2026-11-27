package store

import (
	"context"
	"testing"
	"time"
)

func TestStats(t *testing.T) {
	s, url := openStore(t)
	ctx := context.Background()

	day := time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC)
	listed := day.Add(-72 * time.Hour)

	// A1 has supply and a report (covered), A2 supply only, A3 neither.
	for _, a := range []struct{ sac, code string }{{"A1", "PUSD"}, {"A2", "PEUR"}, {"A3", "PGBP"}} {
		execSQL(t, url, `
			INSERT INTO assets (sac, code, issuer, listed_ledger, listed_at, event_id)
			VALUES ($1, $2, 'GISS', 10, $3, $4)`,
			a.sac, a.code, listed, "asset:"+a.sac)
	}
	for _, sac := range []string{"A1", "A2"} {
		execSQL(t, url, `
			INSERT INTO supply_snapshots
			  (event_id, sac, amount, at_ledger, breakdown_hash, poster, posted_ledger, posted_at, tx_hash)
			VALUES ($1, $2, 1000, 20, 'h', 'GPOST', 20, $3, 'tx')`,
			"supply:"+sac, sac, day.Add(-time.Hour))
	}
	// One report today, one yesterday; only the former counts.
	execSQL(t, url, `
		INSERT INTO reserve_reports
		  (event_id, sac, tier, amount, as_of, reporter, doc_hash, doc_uri, posted_ledger, posted_at, tx_hash)
		VALUES ('report:1', 'A1', 2, 1100, $1, 'GISS', 'dh', 'https://x', 21, $2, 'tx'),
		       ('report:2', 'A1', 3, 1200, $3, 'GAUD', 'dh', 'https://x', 22, $3, 'tx')`,
		day.Add(-2*time.Hour), day.Add(time.Hour), day.Add(-time.Hour))

	execSQL(t, url, `
		INSERT INTO reporters (address, role, name, active, updated_ledger, updated_at)
		VALUES ('R1', 1, 'Acme', true, 1, $1),
		       ('R2', 2, 'Old', false, 2, $1),
		       ('R3', 3, 'Poster', true, 3, $1)`, day)

	// Ingest lag is measured from the newest event time across cursors.
	execSQL(t, url, `
		INSERT INTO ingest_cursor (name, cursor, ledger, event_ledger, event_at, updated_at)
		VALUES ('plimsoll-events', 'c', 30, 29, $1, $1),
		       ('other', 'd', 5, 4, $2, $2)`,
		day.Add(2*time.Hour), day.Add(3*time.Hour))

	st, err := s.Stats(ctx, day)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st.TotalAssets != 3 || st.CoveredAssets != 1 {
		t.Errorf("assets total=%d covered=%d, want 3 and 1", st.TotalAssets, st.CoveredAssets)
	}
	if st.TotalReporters != 3 || st.ActiveReporters != 2 {
		t.Errorf("reporters total=%d active=%d, want 3 and 2", st.TotalReporters, st.ActiveReporters)
	}
	if st.ReportsToday != 1 {
		t.Errorf("reportsToday = %d, want 1", st.ReportsToday)
	}
	if st.LastEventAt == nil || !st.LastEventAt.Equal(day.Add(3*time.Hour)) {
		t.Errorf("lastEventAt = %v, want %v", st.LastEventAt, day.Add(3*time.Hour))
	}
}

func TestStatsEmpty(t *testing.T) {
	s, _ := openStore(t)
	st, err := s.Stats(context.Background(), time.Date(2026, 11, 25, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if st != (Stats{}) {
		t.Errorf("stats on empty database = %+v, want zero", st)
	}
}
