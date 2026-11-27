package graphql

import (
	"context"
	"fmt"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
)

func (r *queryResolver) assetWithData(ctx context.Context, a coverage.Asset, minTier int) (*Asset, error) {
	out := toAsset(a)
	out.MinTier = minTier
	sp, err := r.Store.LatestSupply(ctx, a.SAC)
	if err != nil {
		return nil, err
	}
	out.Supply = toSupply(sp)
	reports, err := r.Store.LatestReports(ctx, a.SAC)
	if err != nil {
		return nil, err
	}
	out.Reports = toReportsMap(reports)
	return out, nil
}

func resolveLimit(limit *int) int {
	if limit == nil {
		return 100
	}
	switch {
	case *limit < 1:
		return 100
	case *limit > 1000:
		return 1000
	default:
		return *limit
	}
}

func resolveMinTier(minTier *int) (int, error) {
	if minTier == nil {
		return int(coverage.TierTranscribed), nil
	}
	if *minTier < 1 || *minTier > 3 {
		return 0, fmt.Errorf("minTier must be 1, 2 or 3")
	}
	return *minTier, nil
}
