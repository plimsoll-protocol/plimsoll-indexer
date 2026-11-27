package coverage

import (
	"math"
	"testing"
	"time"
)

// seriesSupply is one snapshot posted at the given minute offset from t0.
func seriesSupply(amount, ledger int64, minutes int64) Supply {
	return Supply{
		Amount:   n(amount),
		Ledger:   uint32(ledger),
		PostedAt: t0.Add(time.Duration(minutes) * time.Minute),
	}
}

// seriesReport is one report posted and as_of'd at the given minute offsets.
func seriesReport(tier uint32, amount int64, asOf, posted int64) Report {
	return Report{
		Tier:     tier,
		Amount:   n(amount),
		AsOf:     t0.Add(time.Duration(asOf) * time.Minute),
		PostedAt: t0.Add(time.Duration(posted) * time.Minute),
		Reporter: "GREPORTER",
	}
}

func TestSeriesUsesLatestSupplyAndBestReportAtEachPoint(t *testing.T) {
	supply := []Supply{
		// Deliberately out of order to prove the builder sorts.
		seriesSupply(2000, 120, 150),
		seriesSupply(1000, 100, -60),
	}
	reports := []Report{
		seriesReport(TierTranscribed, 800, -120, -30),
		seriesReport(TierIssuerSigned, 2100, 120, 135),
		seriesReport(TierAuditorSigned, 1500, 60, 90),
	}
	got := Series(supply, reports, TierTranscribed, t0, t0.Add(3*time.Hour), time.Hour)
	if len(got) != 4 {
		t.Fatalf("got %d points, want 4", len(got))
	}
	for i, p := range got {
		if want := t0.Add(time.Duration(i) * time.Hour); !p.At.Equal(want) {
			t.Errorf("point %d at %v, want %v", i, p.At, want)
		}
	}

	// 10:00 and 11:00: only the first supply and the transcribed report exist.
	if c := got[0].Coverage; c == nil || c.SupplyLedger != 100 || c.Tier != TierTranscribed || c.Bps != 8000 {
		t.Fatalf("10:00 coverage = %+v", c)
	}
	if c := got[1].Coverage; c == nil || c.SupplyLedger != 100 || c.Tier != TierTranscribed {
		t.Fatalf("11:00 coverage = %+v", c)
	}
	// 12:00: the auditor report is now known, but the new supply is not yet.
	if c := got[2].Coverage; c == nil || c.SupplyLedger != 100 || c.Tier != TierAuditorSigned || c.Bps != 15_000 {
		t.Fatalf("12:00 coverage = %+v", c)
	}
	// 13:00: both the newer supply and the issuer report (freshest as_of) apply.
	c := got[3].Coverage
	if c == nil || c.SupplyLedger != 120 || c.Tier != TierIssuerSigned || c.Bps != 10_500 {
		t.Fatalf("13:00 coverage = %+v", c)
	}
	if c.Reserves.Int64() != 2100 || c.Supply.Int64() != 2000 {
		t.Fatalf("amounts = %v/%v", c.Reserves, c.Supply)
	}
	if c.ReportAsOf != t0.Add(120*time.Minute) || c.SupplyPostedAt != t0.Add(150*time.Minute) {
		t.Fatalf("timestamps = %v/%v", c.ReportAsOf, c.SupplyPostedAt)
	}
}

func TestSeriesRespectsMinTier(t *testing.T) {
	supply := []Supply{seriesSupply(100, 1, -10)}
	reports := []Report{
		seriesReport(TierTranscribed, 120, -10, -5),
		seriesReport(TierIssuerSigned, 120, -8, -4),
	}
	got := Series(supply, reports, TierAuditorSigned, t0, t0, time.Hour)
	if len(got) != 1 || got[0].Coverage != nil {
		t.Fatalf("auditor-only series = %+v", got)
	}
	got = Series(supply, reports, TierIssuerSigned, t0, t0, time.Hour)
	if len(got) != 1 || got[0].Coverage == nil || got[0].Coverage.Tier != TierIssuerSigned {
		t.Fatalf("issuer-only series = %+v", got)
	}
}

func TestSeriesPointsAreEmptyBeforeFirstReportAndSupply(t *testing.T) {
	// Supply exists before the window but no report does; the report arrives
	// after the last point.
	supply := []Supply{seriesSupply(100, 1, -600)}
	reports := []Report{seriesReport(TierTranscribed, 100, 120, 130)}
	got := Series(supply, reports, TierTranscribed, t0, t0.Add(2*time.Hour), time.Hour)
	if len(got) != 3 {
		t.Fatalf("got %d points", len(got))
	}
	for i, p := range got {
		if p.Coverage != nil {
			t.Errorf("point %d = %+v, want nil coverage", i, p.Coverage)
		}
	}

	// A zero-supply asset is covered as on-chain: bps saturates and nothing is owed.
	zero := []Supply{{Amount: n(0), Ledger: 1, PostedAt: t0}}
	known := []Report{seriesReport(TierTranscribed, 0, -5, -5)}
	got = Series(zero, known, TierTranscribed, t0, t0, time.Hour)
	if len(got) != 1 || got[0].Coverage == nil {
		t.Fatalf("zero-supply series = %+v", got)
	}
	if got[0].Coverage.Bps != math.MaxUint32 || !got[0].Coverage.NothingIsOwed {
		t.Fatalf("zero-supply coverage = %+v", got[0].Coverage)
	}
}

// A later post with an older as_of does not replace the fresher report for its
// tier, which is how LatestReports picks the report coverage() reads.
func TestSeriesPerTierKeepsFreshestAsOf(t *testing.T) {
	supply := []Supply{seriesSupply(100, 1, -60)}
	reports := []Report{
		seriesReport(TierTranscribed, 120, 10, 20),
		seriesReport(TierTranscribed, 999, 5, 30),
	}
	got := Series(supply, reports, TierTranscribed, t0.Add(45*time.Minute), t0.Add(45*time.Minute), time.Hour)
	if len(got) != 1 || got[0].Coverage == nil || got[0].Coverage.Reserves.Int64() != 120 {
		t.Fatalf("coverage = %+v", got[0])
	}
}

func TestSeriesBoundariesAreInclusive(t *testing.T) {
	supply := []Supply{seriesSupply(100, 1, 0), seriesSupply(200, 2, 60)}
	reports := []Report{seriesReport(TierTranscribed, 100, 0, 0)}
	got := Series(supply, reports, TierTranscribed, t0, t0.Add(time.Hour), time.Hour)
	if len(got) != 2 {
		t.Fatalf("got %d points", len(got))
	}
	if got[0].Coverage == nil || got[0].Coverage.SupplyLedger != 1 {
		t.Fatalf("first point ignored rows posted exactly at from: %+v", got[0])
	}
	if got[1].Coverage == nil || got[1].Coverage.SupplyLedger != 2 {
		t.Fatalf("last point ignored rows posted exactly at to: %+v", got[1])
	}
}

// A partial trailing step still yields the points that fall at or before to.
func TestSeriesStopsBeforeToWhenRangeIsNotAMultiple(t *testing.T) {
	got := Series(nil, nil, TierTranscribed, t0, t0.Add(90*time.Minute), time.Hour)
	if len(got) != 2 {
		t.Fatalf("got %d points, want 2", len(got))
	}
	if !got[1].At.Equal(t0.Add(time.Hour)) {
		t.Fatalf("last point at %v", got[1].At)
	}
}

func TestSeriesRejectsInvalidWindows(t *testing.T) {
	if got := Series(nil, nil, TierTranscribed, t0, t0.Add(time.Hour), 0); got != nil {
		t.Errorf("zero interval returned %d points", len(got))
	}
	if got := Series(nil, nil, TierTranscribed, t0, t0.Add(-time.Hour), time.Hour); got != nil {
		t.Errorf("reversed window returned %d points", len(got))
	}
	supply := []Supply{seriesSupply(100, 1, -10)}
	if got := Series(supply, nil, TierTranscribed, t0, t0, time.Hour); len(got) != 1 || got[0].Coverage != nil {
		t.Errorf("single instant = %+v", got)
	}
}
