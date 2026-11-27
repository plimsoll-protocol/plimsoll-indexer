package coverage

import (
	"sort"
	"time"
)

// Point is coverage at one instant in a series. Coverage is nil when no supply
// snapshot or no qualifying report was known at that instant.
type Point struct {
	At       time.Time
	Coverage *Result
}

// Series builds coverage at every interval boundary from from to to, inclusive
// of both ends. Each point uses only what was known then: the supply snapshot
// with the latest PostedAt at or before the point, and, per tier, the report
// with the latest AsOf posted at or before the point. Best and BPS then select
// and round exactly as Compute does, so a point matches what the contract would
// have reported at that time.
//
// Supplies posted at the same instant resolve to the higher ledger, then to the
// later row in the input; reports resolve to the later AsOf, and a later post
// wins an equal AsOf. Returns nil when interval is not positive or to is before
// from.
func Series(supply []Supply, reports []Report, minTier uint32, from, to time.Time, interval time.Duration) []Point {
	if interval <= 0 || to.Before(from) {
		return nil
	}

	supply = append([]Supply(nil), supply...)
	sort.SliceStable(supply, func(i, j int) bool {
		if supply[i].PostedAt.Equal(supply[j].PostedAt) {
			return supply[i].Ledger < supply[j].Ledger
		}
		return supply[i].PostedAt.Before(supply[j].PostedAt)
	})

	reports = append([]Report(nil), reports...)
	sort.SliceStable(reports, func(i, j int) bool {
		if reports[i].PostedAt.Equal(reports[j].PostedAt) {
			return reports[i].AsOf.Before(reports[j].AsOf)
		}
		return reports[i].PostedAt.Before(reports[j].PostedAt)
	})

	latestPerTier := make(map[uint32]Report, 3)
	var latestSupply *Supply
	si, ri := 0, 0
	points := make([]Point, 0, int(to.Sub(from)/interval)+1)
	for at := from; !at.After(to); at = at.Add(interval) {
		for si < len(supply) && !supply[si].PostedAt.After(at) {
			sp := supply[si]
			latestSupply = &sp
			si++
		}
		for ri < len(reports) && !reports[ri].PostedAt.After(at) {
			rep := reports[ri]
			// Per tier the newest as_of wins, the same rule LatestReports
			// applies to pick the report coverage() would read.
			if old, ok := latestPerTier[rep.Tier]; !ok || !rep.AsOf.Before(old.AsOf) {
				latestPerTier[rep.Tier] = rep
			}
			ri++
		}
		points = append(points, Point{
			At:       at,
			Coverage: Compute(latestSupply, latestPerTier, minTier),
		})
	}
	return points
}
