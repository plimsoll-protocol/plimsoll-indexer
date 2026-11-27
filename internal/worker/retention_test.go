package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

type fakeRetentionStore struct {
	cutoff    time.Time
	result    store.PruneResult
	pruneErr  error
	vacuumed  []string
	vacuumErr error
	pruned    bool
}

func (f *fakeRetentionStore) Prune(_ context.Context, cutoff time.Time) (store.PruneResult, error) {
	f.pruned = true
	f.cutoff = cutoff
	return f.result, f.pruneErr
}

func (f *fakeRetentionStore) Vacuum(_ context.Context, tables ...string) error {
	f.vacuumed = append(f.vacuumed, tables...)
	return f.vacuumErr
}

func newRetention(db *fakeRetentionStore, days int, now time.Time) *Retention {
	return &Retention{
		Store: db,
		Days:  days,
		Log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:   func() time.Time { return now },
	}
}

func TestRetentionDisabledDoesNothing(t *testing.T) {
	db := &fakeRetentionStore{result: store.PruneResult{SupplySnapshots: 1}}
	if err := newRetention(db, 0, time.Now()).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if db.pruned {
		t.Fatal("retention pruned with Days=0")
	}
}

func TestRetentionPrunesAndVacuumsAffectedTables(t *testing.T) {
	now := time.Date(2026, 11, 24, 3, 0, 0, 0, time.UTC)
	db := &fakeRetentionStore{result: store.PruneResult{
		SupplySnapshots: 3,
		ReserveReports:  2,
		Breakdowns:      1,
	}}
	if err := newRetention(db, 365, now).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantCutoff := now.AddDate(0, 0, -365)
	if !db.cutoff.Equal(wantCutoff) {
		t.Errorf("cutoff = %s, want %s", db.cutoff, wantCutoff)
	}
	wantTables := []string{"supply_snapshots", "reserve_reports", "breakdowns"}
	if len(db.vacuumed) != len(wantTables) {
		t.Fatalf("vacuumed %v, want %v", db.vacuumed, wantTables)
	}
	for i := range wantTables {
		if db.vacuumed[i] != wantTables[i] {
			t.Errorf("vacuumed[%d] = %q, want %q", i, db.vacuumed[i], wantTables[i])
		}
	}
}

func TestRetentionSkipsVacuumWhenNothingDeleted(t *testing.T) {
	db := &fakeRetentionStore{}
	if err := newRetention(db, 365, time.Now()).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(db.vacuumed) != 0 {
		t.Fatalf("vacuumed %v after deleting nothing", db.vacuumed)
	}
}

func TestRetentionStopsOnPruneError(t *testing.T) {
	db := &fakeRetentionStore{pruneErr: errors.New("database down")}
	if err := newRetention(db, 365, time.Now()).Run(context.Background()); err == nil {
		t.Fatal("want prune error")
	}
	if len(db.vacuumed) != 0 {
		t.Fatal("vacuum ran after a failed prune")
	}
}

func TestRetentionReportsVacuumError(t *testing.T) {
	db := &fakeRetentionStore{
		result:    store.PruneResult{SupplySnapshots: 1},
		vacuumErr: errors.New("vacuum refused"),
	}
	if err := newRetention(db, 365, time.Now()).Run(context.Background()); err == nil {
		t.Fatal("want vacuum error")
	}
}
