package graphql

import (
	"context"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
)

// Reader is what the GraphQL resolvers need from storage.
type Reader interface {
	ListAssets(ctx context.Context) ([]coverage.Asset, error)
	GetAsset(ctx context.Context, sac string) (coverage.Asset, error)
	LatestSupply(ctx context.Context, sac string) (*coverage.Supply, error)
	SupplyHistory(ctx context.Context, sac string, limit int) ([]coverage.Supply, error)
	LatestReports(ctx context.Context, sac string) (map[uint32]coverage.Report, error)
	ReportHistory(ctx context.Context, sac string, limit int) ([]coverage.Report, error)
	Reporters(ctx context.Context) ([]coverage.Reporter, error)
}

// Resolver is the dependency injection root for the generated schema.
type Resolver struct {
	Store Reader
	Now   func() time.Time
}

func (r *Resolver) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}
