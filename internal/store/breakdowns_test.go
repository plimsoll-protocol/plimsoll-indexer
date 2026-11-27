package store

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"testing"
	"testing/fstest"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

const (
	breakdownSAC    = "CCHPT4TEJDPZQDSUVW3NP6A35ROGV4WEFZKT45SKWYCFZSUB7R5HIM7S"
	breakdownPoster = "GDACI55IMI5DDZWMZ2EAXWK57AV75VE4626YYBAHQJYDV7QFPNNFK6HJ"
)

func saveBreakdown(t *testing.T, s *Store, hash, body string) {
	t.Helper()
	if err := s.SaveBreakdown(context.Background(), hash, breakdownSAC, []byte(body)); err != nil {
		t.Fatal(err)
	}
}

func wantServed(t *testing.T, s *Store, hash, body string) {
	t.Helper()
	got, err := s.Breakdown(context.Background(), hash)
	if err != nil {
		t.Fatalf("breakdown %s: %v, want it served", hash, err)
	}
	if string(got) != body {
		t.Fatalf("breakdown %s = %q, want %q byte for byte", hash, got, body)
	}
}

func wantNotServed(t *testing.T, s *Store, hash string) {
	t.Helper()
	if got, err := s.Breakdown(context.Background(), hash); !errors.Is(err, ErrNotFound) {
		t.Fatalf("breakdown %s = %q, %v; want ErrNotFound", hash, got, err)
	}
}

func wantStored(t *testing.T, s *Store, hash string, want bool) {
	t.Helper()
	got, err := s.HasBreakdown(context.Background(), hash)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("HasBreakdown(%s) = %v, want %v", hash, got, want)
	}
}

// backdate moves a breakdown's created_at into the past.
func backdate(t *testing.T, url, hash string, by time.Duration) {
	t.Helper()
	execSQL(t, url, `UPDATE breakdowns SET created_at = now() - make_interval(secs => $2) WHERE hash = $1`,
		hash, by.Seconds())
}

func TestSavedBreakdownIsServedOnlyOnceConfirmed(t *testing.T) {
	s, _ := openStore(t)
	body := `{"asset":"PUSD","total":"5"}`
	saveBreakdown(t, s, "h1", body)

	wantStored(t, s, "h1", true)
	wantNotServed(t, s, "h1")

	if err := s.ConfirmBreakdown(context.Background(), "h1"); err != nil {
		t.Fatal(err)
	}
	wantServed(t, s, "h1", body)
}

// Ingesting the supply_posted event for a post confirms its breakdown, so
// a post confirmed by the chain is served even if the poster never saw the
// transaction succeed.
func TestSupplyPostedEventConfirmsTheBreakdown(t *testing.T) {
	s, _ := openStore(t)
	body := `{"total":"7"}`
	saveBreakdown(t, s, "h1", body)
	saveBreakdown(t, s, "h2", `{"total":"8"}`)

	ev := soroban.SupplyPosted{
		Meta:          soroban.Meta{ID: "0000000042-0000000001", Ledger: 42, ClosedAt: time.Now().UTC(), TxHash: "tx1"},
		SAC:           breakdownSAC,
		Amount:        big.NewInt(7),
		AtLedger:      41,
		BreakdownHash: "h1",
		Poster:        breakdownPoster,
	}
	if err := s.ApplyEvents(context.Background(), "test", "c1", 42, nil, []any{ev}); err != nil {
		t.Fatal(err)
	}
	wantServed(t, s, "h1", body)
	wantNotServed(t, s, "h2")
}

// The reconciler reading a snapshot back from the ledger confirms it too.
func TestChainSupplyConfirmsTheBreakdown(t *testing.T) {
	s, _ := openStore(t)
	body := `{"total":"9"}`
	saveBreakdown(t, s, "h1", body)

	_, err := s.SaveChainSupply(context.Background(), breakdownSAC, coverage.Supply{
		Amount: big.NewInt(9), Ledger: 50, PostedAt: time.Now().UTC(), BreakdownHash: "h1", Poster: breakdownPoster,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantServed(t, s, "h1", body)
}

func TestDeleteUnconfirmedBreakdowns(t *testing.T) {
	ctx := context.Background()
	s, url := openStore(t)

	// failed: a post that never reached the chain, saved two days ago.
	saveBreakdown(t, s, "failed", `{"total":"1"}`)
	backdate(t, url, "failed", 48*time.Hour)
	// recent: unconfirmed, but its post may still be in flight.
	saveBreakdown(t, s, "recent", `{"total":"2"}`)
	backdate(t, url, "recent", time.Hour)
	// posted: its post succeeded two days ago.
	saveBreakdown(t, s, "posted", `{"total":"3"}`)
	backdate(t, url, "posted", 48*time.Hour)
	if err := s.ConfirmBreakdown(ctx, "posted"); err != nil {
		t.Fatal(err)
	}
	// snapshotted: never confirmed, but a snapshot row carries its hash.
	saveBreakdown(t, s, "snapshotted", `{"total":"4"}`)
	backdate(t, url, "snapshotted", 48*time.Hour)
	execSQL(t, url, `UPDATE breakdowns SET confirmed_at = NULL WHERE hash = 'snapshotted'`)
	execSQL(t, url, `INSERT INTO supply_snapshots
		(event_id, sac, amount, at_ledger, breakdown_hash, poster, posted_ledger, posted_at, tx_hash)
		VALUES ('ev-s', $1, 4, 60, 'snapshotted', $2, 61, now(), 'tx-s')`, breakdownSAC, breakdownPoster)

	n, err := s.DeleteUnconfirmedBreakdowns(ctx, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("deleted %d, want 1", n)
	}
	wantStored(t, s, "failed", false)
	wantStored(t, s, "recent", true)
	wantStored(t, s, "posted", true)
	wantStored(t, s, "snapshotted", true)
	wantServed(t, s, "posted", `{"total":"3"}`)
}

// Saving a breakdown again for a retried post restarts its grace period, so a
// retry that succeeds after the first attempt's grace has passed keeps it.
func TestSavingAgainRestartsTheGracePeriod(t *testing.T) {
	ctx := context.Background()
	s, url := openStore(t)
	saveBreakdown(t, s, "retried", `{"total":"5"}`)
	backdate(t, url, "retried", 48*time.Hour)
	saveBreakdown(t, s, "retried", `{"total":"5"}`)

	if n, err := s.DeleteUnconfirmedBreakdowns(ctx, 24*time.Hour); err != nil || n != 0 {
		t.Fatalf("deleted %d (%v), want 0", n, err)
	}
	wantStored(t, s, "retried", true)

	// Saving a confirmed breakdown again leaves it confirmed.
	if err := s.ConfirmBreakdown(ctx, "retried"); err != nil {
		t.Fatal(err)
	}
	saveBreakdown(t, s, "retried", `{"total":"5"}`)
	wantServed(t, s, "retried", `{"total":"5"}`)
}

// migrationsBeforeConfirmation is the schema as it stood before
// the breakdown_confirmation migration, so a test can build a database an older
// indexer left behind.
func migrationsBeforeConfirmation(t *testing.T) fs.FS {
	t.Helper()
	ms, err := loadMigrations(migrationsFS())
	if err != nil {
		t.Fatal(err)
	}
	older := fstest.MapFS{}
	for _, m := range ms {
		if m.Name == "breakdown_confirmation" {
			return older
		}
		older[fmt.Sprintf("%04d_%s.sql", m.Version, m.Name)] = &fstest.MapFile{Data: []byte(m.SQL)}
	}
	t.Fatal("no breakdown_confirmation migration")
	return nil
}

// Breakdowns saved before confirmation existed are confirmed by the migration
// that adds it when a snapshot carries their hash; the rest stay unserved.
func TestExistingBreakdownsAreConfirmedBySnapshotsOnUpgrade(t *testing.T) {
	ctx := context.Background()
	url := newDatabase(t)
	if err := migrate(ctx, testPool(t, url), migrationsBeforeConfirmation(t)); err != nil {
		t.Fatal(err)
	}
	execSQL(t, url, `INSERT INTO breakdowns (hash, sac, body) VALUES
		('old-posted', $1, '{"total":"6"}'), ('old-orphan', $1, '{"total":"7"}')`, breakdownSAC)
	execSQL(t, url, `INSERT INTO supply_snapshots
		(event_id, sac, amount, at_ledger, breakdown_hash, poster, posted_ledger, posted_at, tx_hash)
		VALUES ('ev-old', $1, 6, 10, 'old-posted', $2, 11, now(), 'tx-old')`, breakdownSAC, breakdownPoster)

	s, err := Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	wantServed(t, s, "old-posted", `{"total":"6"}`)
	wantNotServed(t, s, "old-orphan")
	wantStored(t, s, "old-orphan", true)
}
