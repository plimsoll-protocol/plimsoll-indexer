package graphql

import (
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
)

func toAsset(a coverage.Asset) *Asset {
	out := &Asset{
		Sac:          a.SAC,
		Code:         a.Code,
		Issuer:       a.Issuer,
		ListedLedger: int(a.ListedLedger),
		ListedAt:     a.ListedAt.UTC(),
		Reports:      []*Report{},
	}
	if len(a.Toml) > 0 {
		toml := string(a.Toml)
		out.Toml = &toml
	}
	if a.TomlCheckedAt != nil {
		at := a.TomlCheckedAt.UTC()
		out.TomlCheckedAt = &at
	}
	if a.TomlError != "" {
		msg := a.TomlError
		out.TomlError = &msg
	}
	return out
}

func toSupply(sp *coverage.Supply) *Supply {
	if sp == nil {
		return nil
	}
	return &Supply{
		Amount:        sp.Amount.String(),
		Ledger:        int(sp.Ledger),
		PostedAt:      sp.PostedAt.UTC(),
		BreakdownHash: sp.BreakdownHash,
		Poster:        sp.Poster,
		TxHash:        sp.TxHash,
		Source:        sp.Source,
	}
}

func toSupplyList(list []coverage.Supply) []*Supply {
	out := make([]*Supply, 0, len(list))
	for i := range list {
		out = append(out, toSupply(&list[i]))
	}
	return out
}

func toReport(r coverage.Report) *Report {
	out := &Report{
		Tier:     coverage.TierName(r.Tier),
		Amount:   r.Amount.String(),
		AsOf:     r.AsOf.UTC(),
		PostedAt: r.PostedAt.UTC(),
		Reporter: r.Reporter,
		DocHash:  r.DocHash,
		DocURI:   r.DocURI,
		TxHash:   r.TxHash,
		Source:   r.Source,
	}
	if r.DocCheck != "" {
		check := r.DocCheck
		out.DocCheck = &check
	}
	return out
}

func toReportsMap(latest map[uint32]coverage.Report) []*Report {
	tiers := make([]uint32, 0, len(latest))
	for tier := range latest {
		tiers = append(tiers, tier)
	}
	sort.Slice(tiers, func(i, j int) bool { return tiers[i] < tiers[j] })
	out := make([]*Report, 0, len(tiers))
	for _, tier := range tiers {
		out = append(out, toReport(latest[tier]))
	}
	return out
}

func toReportList(list []coverage.Report) []*Report {
	out := make([]*Report, 0, len(list))
	for _, r := range list {
		out = append(out, toReport(r))
	}
	return out
}

func toReporter(r coverage.Reporter) *Reporter {
	return &Reporter{
		Address:   r.Address,
		Role:      coverage.RoleName(r.Role),
		Name:      r.Name,
		UpdatedAt: r.UpdatedAt.UTC(),
	}
}

func toCoverage(r *coverage.Result, now time.Time) *Coverage {
	if r == nil {
		return nil
	}
	out := &Coverage{
		Tier:             coverage.TierName(r.Tier),
		Supply:           r.Supply.String(),
		Reserves:         r.Reserves.String(),
		SupplyLedger:     int(r.SupplyLedger),
		SupplyPostedAt:   r.SupplyPostedAt.UTC(),
		ReportAsOf:       r.ReportAsOf.UTC(),
		ReportAgeSeconds: int(now.Sub(r.ReportAsOf).Seconds()),
		SupplyAgeSeconds: int(now.Sub(r.SupplyPostedAt).Seconds()),
		Reporter:         r.ReportReporter,
		DocURI:           r.ReportDocURI,
	}
	if !r.NothingIsOwed {
		bps := int(r.Bps)
		out.Bps = &bps
		if r.Bps != math.MaxUint32 {
			ratio := strconv.FormatFloat(float64(r.Bps)/100, 'f', 2, 64) + "%"
			out.Ratio = &ratio
		}
	}
	return out
}
