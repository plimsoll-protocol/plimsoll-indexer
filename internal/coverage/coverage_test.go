package coverage

import (
	"math"
	"math/big"
	"testing"
	"time"
)

var t0 = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

func n(v int64) *big.Int { return big.NewInt(v) }

func TestBPS(t *testing.T) {
	cases := []struct {
		reserves, supply int64
		want             uint32
	}{
		{1, 3, 3333},
		{100, 100, 10_000},
		{105, 100, 10_500},
		{0, 0, math.MaxUint32},
		{-5, 100, 0},
		{math.MaxInt64, 1, math.MaxUint32},
	}
	for _, c := range cases {
		if got := BPS(n(c.reserves), n(c.supply)); got != c.want {
			t.Errorf("BPS(%d,%d)=%d want %d", c.reserves, c.supply, got, c.want)
		}
	}
}

func reports() map[uint32]Report {
	return map[uint32]Report{
		TierAuditorSigned: {Tier: TierAuditorSigned, Amount: n(90), AsOf: t0.Add(-1000 * time.Second)},
		TierTranscribed:   {Tier: TierTranscribed, Amount: n(101), AsOf: t0.Add(-10 * time.Second)},
	}
}

func TestBestPrefersFreshest(t *testing.T) {
	r, ok := Best(reports(), TierTranscribed)
	if !ok || r.Tier != TierTranscribed {
		t.Fatalf("got %+v", r)
	}
	r, ok = Best(reports(), TierAuditorSigned)
	if !ok || r.Tier != TierAuditorSigned {
		t.Fatalf("got %+v", r)
	}
	if _, ok := Best(map[uint32]Report{}, TierTranscribed); ok {
		t.Fatal("found a report in an empty map")
	}
}

func TestBestTieGoesToStrongerTier(t *testing.T) {
	m := map[uint32]Report{
		TierTranscribed:  {Tier: TierTranscribed, Amount: n(1), AsOf: t0},
		TierIssuerSigned: {Tier: TierIssuerSigned, Amount: n(2), AsOf: t0},
	}
	r, _ := Best(m, TierTranscribed)
	if r.Tier != TierIssuerSigned {
		t.Fatalf("got tier %d", r.Tier)
	}
}

func TestComputeAndCovered(t *testing.T) {
	sp := &Supply{Amount: n(100), Ledger: 10, PostedAt: t0}
	res := Compute(sp, reports(), TierTranscribed)
	if res == nil || res.Bps != 10_100 {
		t.Fatalf("got %+v", res)
	}
	day := 24 * time.Hour
	if !res.Covered(t0, 10_000, day) {
		t.Error("should be covered")
	}
	if res.Covered(t0, 10_200, day) {
		t.Error("ratio check ignored")
	}
	if res.Covered(t0.Add(2*day), 10_000, day) {
		t.Error("freshness check ignored")
	}
	if Compute(nil, reports(), TierTranscribed) != nil {
		t.Error("computed without supply")
	}
	var nilRes *Result
	if nilRes.Covered(t0, 0, day) {
		t.Error("nil result covered")
	}
}

func TestCoverageStatus(t *testing.T) {
	sp := &Supply{Amount: n(100), PostedAt: t0}
	rep := Report{Amount: n(101), AsOf: t0}
	day := 24 * time.Hour

	covered, reason := CoverageStatus(sp, map[uint32]Report{TierTranscribed: rep}, TierTranscribed, t0, 10_000, day)
	if !covered || reason != "covered" {
		t.Fatalf("covered: got %v %q", covered, reason)
	}

	if _, reason := CoverageStatus(nil, map[uint32]Report{TierTranscribed: rep}, TierTranscribed, t0, 0, day); reason != "no_supply" {
		t.Errorf("no supply: got %q", reason)
	}
	if _, reason := CoverageStatus(sp, map[uint32]Report{}, TierTranscribed, t0, 0, day); reason != "no_report" {
		t.Errorf("no report: got %q", reason)
	}
	if _, reason := CoverageStatus(sp, map[uint32]Report{TierTranscribed: rep}, TierTranscribed, t0.Add(2*day), 0, day); reason != "stale_supply" {
		t.Errorf("stale supply: got %q", reason)
	}
	staleRep := Report{Amount: n(101), AsOf: t0.Add(-2 * day)}
	if _, reason := CoverageStatus(sp, map[uint32]Report{TierTranscribed: staleRep}, TierTranscribed, t0, 0, day); reason != "stale_report" {
		t.Errorf("stale report: got %q", reason)
	}
	if _, reason := CoverageStatus(sp, map[uint32]Report{TierTranscribed: rep}, TierTranscribed, t0, 10_200, day); reason != "under_backed" {
		t.Errorf("under backed: got %q", reason)
	}
}

func TestNames(t *testing.T) {
	if TierName(TierIssuerSigned) != "issuer_signed" || RoleName(RoleSupplyPoster) != "supply_poster" {
		t.Fatal("names changed")
	}
	if TierName(99) != "unknown" || RoleName(99) != "unknown" {
		t.Fatal("unknown handling changed")
	}
}
