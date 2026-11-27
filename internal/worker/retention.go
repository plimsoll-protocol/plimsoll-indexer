package worker

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

// retentionStore is the storage surface the retention job needs.
type retentionStore interface {
	Prune(ctx context.Context, cutoff time.Time) (store.PruneResult, error)
	Vacuum(ctx context.Context, tables ...string) error
}

// Retention deletes history older than Days and reclaims the space. It runs
// nightly on the same ticker pattern as the other jobs.
type Retention struct {
	Store retentionStore
	// Days is the retention window. Zero means keep everything forever.
	Days int
	Log  *slog.Logger
	Now  func() time.Time
}

// Run applies the retention policy once. It is a no-op when retention is
// disabled.
func (r *Retention) Run(ctx context.Context) error {
	if r.Days <= 0 {
		return nil
	}
	now := time.Now
	if r.Now != nil {
		now = r.Now
	}
	cutoff := now().UTC().AddDate(0, 0, -r.Days)

	result, err := r.Store.Prune(ctx, cutoff)
	if err != nil {
		return fmt.Errorf("prune data older than %s: %w", cutoff.Format(time.RFC3339), err)
	}
	if result.Total() == 0 {
		r.Log.Debug("retention: nothing older than cutoff", "cutoff", cutoff)
		return nil
	}
	r.Log.Info("retention deleted rows",
		"cutoff", cutoff,
		"supply_snapshots", result.SupplySnapshots,
		"reserve_reports", result.ReserveReports,
		"breakdowns", result.Breakdowns,
		"total", result.Total())

	var tables []string
	if result.SupplySnapshots > 0 {
		tables = append(tables, "supply_snapshots")
	}
	if result.ReserveReports > 0 {
		tables = append(tables, "reserve_reports")
	}
	if result.Breakdowns > 0 {
		tables = append(tables, "breakdowns")
	}
	if err := r.Store.Vacuum(ctx, tables...); err != nil {
		return fmt.Errorf("vacuum after prune: %w", err)
	}
	return nil
}
