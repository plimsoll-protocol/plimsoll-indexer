package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

// The tests in this file need a real Postgres. postgres_test.go starts one for
// the package and skips each test when `go test -short` is set.

const testSAC = "CSACSTORETEST"

func eventMeta(id string, ledger uint32, at time.Time) soroban.Meta {
	return soroban.Meta{ID: id, Ledger: ledger, ClosedAt: at.UTC(), TxHash: "tx-" + id}
}

// tableCounts returns the row count of every table named, so two states of the
// database can be compared without knowing which table a bug touched.
func tableCounts(t *testing.T, url string, tables ...string) map[string]int {
	t.Helper()
	p := testPool(t, url)
	out := make(map[string]int, len(tables))
	for _, table := range tables {
		var n int
		if err := p.QueryRow(context.Background(), `SELECT count(*) FROM `+table).Scan(&n); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		out[table] = n
	}
	return out
}

func TestApplyEventsIsIdempotent(t *testing.T) {
	s, url := openStore(t)
	ctx := context.Background()
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

	events := []any{
		soroban.AssetListed{Meta: eventMeta("ev-asset", 10, base), SAC: testSAC, Code: "PUSD", Issuer: "GISS"},
		soroban.SupplyPosted{
			Meta: eventMeta("ev-supply", 11, base.Add(time.Minute)), SAC: testSAC,
			Amount: big.NewInt(1_000_000), AtLedger: 9, BreakdownHash: "hash-1", Poster: "GPOST",
		},
		soroban.ReservePosted{
			Meta: eventMeta("ev-report", 12, base.Add(2*time.Minute)), SAC: testSAC,
			Tier: 2, Amount: big.NewInt(1_020_000), AsOf: base, Reporter: "GREP",
			DocHash: "doc-1", DocURI: "https://issuer.example/report",
		},
		soroban.ReporterSet{Meta: eventMeta("ev-reporter", 13, base.Add(3*time.Minute)), Reporter: "GREP", Role: 2, Name: "Reporter"},
		soroban.ReporterRevoked{Meta: eventMeta("ev-revoked", 14, base.Add(4*time.Minute)), Reporter: "GREP"},
	}

	if err := s.ApplyEvents(ctx, "plimsoll-events", "cursor-1", 12, events); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	tables := []string{"assets", "supply_snapshots", "reserve_reports", "reporters", "ingest_cursor"}
	first := tableCounts(t, url, tables...)
	if first["assets"] != 1 || first["supply_snapshots"] != 1 || first["reserve_reports"] != 1 || first["reporters"] != 1 {
		t.Fatalf("first apply stored %v, want one row per table", first)
	}
	progress, err := s.Progress(ctx, "plimsoll-events")
	if err != nil {
		t.Fatal(err)
	}

	// The same page delivered twice by a retried RPC call must be a no-op.
	if err := s.ApplyEvents(ctx, "plimsoll-events", "cursor-1", 12, events); err != nil {
		t.Fatalf("second apply: %v", err)
	}
	second := tableCounts(t, url, tables...)
	for _, table := range tables {
		if first[table] != second[table] {
			t.Errorf("%s has %d rows after re-apply, want %d", table, second[table], first[table])
		}
	}
	again, err := s.Progress(ctx, "plimsoll-events")
	if err != nil {
		t.Fatal(err)
	}
	if again.Cursor != progress.Cursor || again.ScannedLedger != progress.ScannedLedger ||
		again.EventLedger != progress.EventLedger || !again.EventAt.Equal(progress.EventAt) {
		t.Fatalf("progress changed on re-apply: %+v then %+v", progress, again)
	}

	// The re-applied events left exactly one snapshot and one report.
	supply, err := s.SupplyHistory(ctx, testSAC, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(supply) != 1 || supply[0].Amount.Cmp(big.NewInt(1_000_000)) != 0 || supply[0].Ledger != 9 {
		t.Fatalf("supply history = %+v", supply)
	}
	reports, err := s.ReportHistory(ctx, testSAC, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(reports) != 1 || reports[0].Tier != 2 || reports[0].Amount.Cmp(big.NewInt(1_020_000)) != 0 {
		t.Fatalf("report history = %+v", reports)
	}
}

func TestLatestReportsReturnsNewestPerTier(t *testing.T) {
	s, _ := openStore(t)
	ctx := context.Background()
	base := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

	report := func(id string, tier uint32, amount int64, asOf time.Time) soroban.ReservePosted {
		return soroban.ReservePosted{
			Meta: eventMeta(id, 20, asOf), SAC: testSAC, Tier: tier,
			Amount: big.NewInt(amount), AsOf: asOf, Reporter: fmt.Sprintf("GREP%d", tier),
			DocHash: "doc-" + id, DocURI: "https://issuer.example/" + id,
		}
	}
	events := []any{
		report("ev-t1-old", 1, 900, base),
		report("ev-t1-new", 1, 950, base.Add(48*time.Hour)),
		report("ev-t2", 2, 980, base.Add(24*time.Hour)),
		report("ev-t3", 3, 1_000, base.Add(12*time.Hour)),
	}
	if err := s.ApplyEvents(ctx, "plimsoll-events", "c", 20, events); err != nil {
		t.Fatal(err)
	}

	latest, err := s.LatestReports(ctx, testSAC)
	if err != nil {
		t.Fatal(err)
	}
	if len(latest) != 3 {
		t.Fatalf("LatestReports returned %d tiers, want 3", len(latest))
	}
	if got := latest[1]; got.Amount.Cmp(big.NewInt(950)) != 0 || !got.AsOf.Equal(base.Add(48*time.Hour)) {
		t.Errorf("tier 1 = amount %v as_of %v, want the newer report", got.Amount, got.AsOf)
	}
	if got := latest[2]; got.Amount.Cmp(big.NewInt(980)) != 0 || !got.AsOf.Equal(base.Add(24*time.Hour)) {
		t.Errorf("tier 2 = amount %v as_of %v", got.Amount, got.AsOf)
	}
	if got := latest[3]; got.Amount.Cmp(big.NewInt(1_000)) != 0 || !got.AsOf.Equal(base.Add(12*time.Hour)) {
		t.Errorf("tier 3 = amount %v as_of %v", got.Amount, got.AsOf)
	}
}

func TestBreakdownBytesRoundTripExactly(t *testing.T) {
	s, _ := openStore(t)
	ctx := context.Background()
	// The exact bytes matter: sha256(body) is the on-chain hash, so spacing,
	// field order and the trailing newline must survive storage.
	body := []byte("{\n  \"total\": \"10200000000\",\n  \"buckets\": [\"a\", \"b\"]\n}\n")
	sum := sha256.Sum256(body)
	hash := fmt.Sprintf("%x", sum)

	if err := s.SaveBreakdown(ctx, hash, testSAC, body); err != nil {
		t.Fatal(err)
	}
	got, err := s.Breakdown(ctx, hash)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, body) {
		t.Fatalf("Breakdown returned %q, want %q", got, body)
	}
	if after := sha256.Sum256(got); after != sum {
		t.Fatalf("hash of served body = %x, want %x", after, sum)
	}

	// A second save for the same hash keeps the first body: the hash is the
	// identity, and overwriting it would break an already-posted snapshot.
	if err := s.SaveBreakdown(ctx, hash, testSAC, []byte(`{"total":"0"}`)); err != nil {
		t.Fatal(err)
	}
	if got, err = s.Breakdown(ctx, hash); err != nil {
		t.Fatal(err)
	} else if !bytes.Equal(got, body) {
		t.Fatalf("Breakdown after duplicate save = %q, want the original", got)
	}
}

func TestPoolConfigAppliesSecretsPassword(t *testing.T) {
	const url = "postgres://plimsoll:from-url@localhost:54329/plimsoll?sslmode=disable"

	cfg, err := poolConfig(url, "from-secrets")
	if err != nil {
		t.Fatalf("poolConfig: %v", err)
	}
	if cfg.ConnConfig.Password != "from-secrets" {
		t.Errorf("password = %q, want the secrets-backend password", cfg.ConnConfig.Password)
	}
	if cfg.ConnConfig.User != "plimsoll" || cfg.ConnConfig.Database != "plimsoll" {
		t.Errorf("user/database changed: %q/%q", cfg.ConnConfig.User, cfg.ConnConfig.Database)
	}
	if cfg.ConnConfig.RuntimeParams["timezone"] != "UTC" {
		t.Errorf("timezone = %q, want UTC", cfg.ConnConfig.RuntimeParams["timezone"])
	}
}

func TestPoolConfigKeepsURLPassword(t *testing.T) {
	cfg, err := poolConfig("postgres://plimsoll:from-url@localhost:54329/plimsoll", "")
	if err != nil {
		t.Fatalf("poolConfig: %v", err)
	}
	if cfg.ConnConfig.Password != "from-url" {
		t.Errorf("password = %q, want the one DATABASE_URL carries", cfg.ConnConfig.Password)
	}
}

func TestPoolConfigRejectsBadURL(t *testing.T) {
	if _, err := poolConfig("://not-a-url", ""); err == nil {
		t.Fatal("poolConfig on a malformed URL = nil error, want error")
	} else if !strings.Contains(err.Error(), "parse database url") {
		t.Errorf("error = %q, want it to name the failure", err)
	}
}
