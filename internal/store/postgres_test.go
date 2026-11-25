package store

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

// The store tests run against a real Postgres: embedded-postgres downloads the
// binaries once (cached in ~/.embedded-postgres-go) and starts one server for
// the package. Each test gets a database of its own from newDatabase.
// `go test -short` skips them.

// adminURL is the server's maintenance database; empty when the server is not
// running, in which case database tests skip.
var adminURL string

var databaseSeq atomic.Int64

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	os.Exit(runWithPostgres(m))
}

func runWithPostgres(m *testing.M) int {
	dir, err := os.MkdirTemp("", "plimsoll-store-test-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "embedded postgres:", err)
		return 1
	}
	defer os.RemoveAll(dir)

	port, err := freePort()
	if err != nil {
		fmt.Fprintln(os.Stderr, "embedded postgres:", err)
		return 1
	}
	var serverLog bytes.Buffer
	pg := embeddedpostgres.NewDatabase(embeddedpostgres.DefaultConfig().
		Port(port).
		RuntimePath(filepath.Join(dir, "runtime")).
		DataPath(filepath.Join(dir, "data")).
		BinariesPath(filepath.Join(dir, "bin")).
		StartTimeout(60 * time.Second).
		Logger(&serverLog))
	if err := pg.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "embedded postgres: %v\n%s", err, serverLog.String())
		return 1
	}
	defer func() {
		if err := pg.Stop(); err != nil {
			fmt.Fprintln(os.Stderr, "embedded postgres stop:", err)
		}
	}()

	adminURL = fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", port)
	return m.Run()
}

func freePort() (uint32, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return uint32(l.Addr().(*net.TCPAddr).Port), nil
}

// newDatabase creates an empty database for one test, drops it when the test
// ends, and returns its URL.
func newDatabase(t testing.TB) string {
	t.Helper()
	if adminURL == "" {
		t.Skip("embedded postgres is not running (go test -short)")
	}
	ctx := context.Background()
	name := fmt.Sprintf("store_test_%d_%d", os.Getpid(), databaseSeq.Add(1))
	admin, err := pgx.Connect(ctx, adminURL)
	if err != nil {
		t.Fatalf("connect to embedded postgres: %v", err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		conn, err := pgx.Connect(context.Background(), adminURL)
		if err != nil {
			t.Errorf("drop database %s: %v", name, err)
			return
		}
		defer conn.Close(context.Background())
		if _, err := conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
			t.Errorf("drop database %s: %v", name, err)
		}
	})
	return fmt.Sprintf("postgres://postgres:postgres@localhost:%s/%s?sslmode=disable", portOf(adminURL), name)
}

// openStore opens a Store on a fresh database, applying the schema the way
// the indexer does on start.
func openStore(t testing.TB) (*Store, string) {
	t.Helper()
	url := newDatabase(t)
	s, err := Open(context.Background(), url)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(s.Close)
	return s, url
}

func portOf(url string) string {
	cfg, err := pgx.ParseConfig(url)
	if err != nil {
		panic(err)
	}
	return fmt.Sprint(cfg.Port)
}

// execSQL runs statements on the database at url outside any Store.
func execSQL(t *testing.T, url, sql string, args ...any) {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("exec %.60q: %v", sql, err)
	}
}

// testPool is a connection pool on the database at url, closed with the test.
func testPool(t *testing.T, url string) *pgxpool.Pool {
	t.Helper()
	p, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func TestIssuerTomlSnapshotsDetectChanges(t *testing.T) {
	s, _ := openStore(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 24, 15, 0, 0, 0, time.UTC)

	old, changed, err := s.SaveIssuerTomlSnapshot(ctx, "GISSUER", "issuer.example", "hash-one", at)
	if err != nil || old != "" || changed {
		t.Fatalf("initial snapshot: old=%q changed=%v err=%v", old, changed, err)
	}
	old, changed, err = s.SaveIssuerTomlSnapshot(ctx, "GISSUER", "issuer.example", "hash-one", at.Add(time.Hour))
	if err != nil || old != "hash-one" || changed {
		t.Fatalf("same snapshot: old=%q changed=%v err=%v", old, changed, err)
	}
	old, changed, err = s.SaveIssuerTomlSnapshot(ctx, "GISSUER", "issuer.example", "hash-two", at.Add(2*time.Hour))
	if err != nil || old != "hash-one" || !changed {
		t.Fatalf("changed snapshot: old=%q changed=%v err=%v", old, changed, err)
	}
	old, changed, err = s.SaveIssuerTomlSnapshot(ctx, "GISSUER", "issuer.example", "hash-two", at.Add(3*time.Hour))
	if err != nil || old != "hash-two" || changed {
		t.Fatalf("repeated snapshot: old=%q changed=%v err=%v", old, changed, err)
	}

	var count int
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM issuer_toml_snapshots WHERE issuer = $1`, "GISSUER").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("stored %d snapshots, want 2", count)
	}
}

func TestWebhookPersistence(t *testing.T) {
	s, _ := openStore(t)
	ctx := context.Background()
	ciphertext := []byte{1, 2, 3, 4}
	created, err := s.CreateWebhook(ctx, "https://receiver.example/hook", "sha256-hash", ciphertext)
	if err != nil {
		t.Fatal(err)
	}
	if created.ID < 1 || created.SecretHash != "sha256-hash" || !bytes.Equal(created.SecretCiphertext, ciphertext) {
		t.Fatalf("created webhook = %+v", created)
	}

	list, err := s.ListWebhooks(ctx)
	if err != nil || len(list) != 1 || list[0].ID != created.ID ||
		list[0].URL != created.URL || list[0].SecretHash != created.SecretHash ||
		!bytes.Equal(list[0].SecretCiphertext, ciphertext) {
		t.Fatalf("listed webhooks = %+v, %v", list, err)
	}
	deleted, err := s.DeleteWebhook(ctx, created.ID)
	if err != nil || !deleted {
		t.Fatalf("delete webhook: deleted=%v err=%v", deleted, err)
	}
	deleted, err = s.DeleteWebhook(ctx, created.ID)
	if err != nil || deleted {
		t.Fatalf("delete missing webhook: deleted=%v err=%v", deleted, err)
	}
}

func TestApplyEventsIsIdempotentAcrossReconnect(t *testing.T) {
	s, _ := openStore(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 25, 5, 0, 0, 0, time.UTC)

	supply := soroban.SupplyPosted{
		Meta:          soroban.Meta{ID: "supply-event-1", Ledger: 100, ClosedAt: at},
		SAC:           "CSAC",
		Amount:        big.NewInt(500),
		AtLedger:      99,
		BreakdownHash: "hash",
		Poster:        "GPOSTER",
	}
	reserve := soroban.ReservePosted{
		Meta:     soroban.Meta{ID: "reserve-event-1", Ledger: 101, ClosedAt: at.Add(time.Minute)},
		SAC:      "CSAC",
		Tier:     1,
		Amount:   big.NewInt(300),
		AsOf:     at,
		Reporter: "GREPORTER",
		DocHash:  "doc-hash",
		DocURI:   "https://example.com/report",
	}

	if err := s.ApplyEvents(ctx, "plimsoll-events", "cursor-1", 101, []any{supply, reserve}); err != nil {
		t.Fatalf("apply first page: %v", err)
	}
	// A reconnect can return the tail of the first page again alongside new data.
	nextSupply := supply
	nextSupply.ID = "supply-event-2"
	nextSupply.Ledger = 102
	nextReserve := reserve
	nextReserve.ID = "reserve-event-2"
	nextReserve.Ledger = 102
	nextReserve.AsOf = at.Add(time.Minute)
	if err := s.ApplyEvents(ctx, "plimsoll-events", "cursor-2", 102,
		[]any{supply, reserve, nextSupply, nextReserve}); err != nil {
		t.Fatalf("apply replayed page: %v", err)
	}

	for _, table := range []string{"supply_snapshots", "reserve_reports"} {
		var count int
		if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM `+table).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 2 {
			t.Errorf("%s has %d rows after replay, want 2 (one replayed and one new event)", table, count)
		}
	}

	progress, err := s.Progress(ctx, "plimsoll-events")
	if err != nil || progress.Cursor != "cursor-2" {
		t.Fatalf("progress after replay = %+v, %v", progress, err)
	}
}

func TestEventIDMigrationRepairsLegacyTables(t *testing.T) {
	url := newDatabase(t)
	schema := strings.ReplaceAll(legacySchema(t),
		"event_id        TEXT PRIMARY KEY,", "event_id        TEXT NOT NULL,")
	if !strings.Contains(schema, "event_id        TEXT NOT NULL,") {
		t.Fatal("legacy schema did not contain event ID primary keys")
	}
	execSQL(t, url, schema)
	execSQL(t, url, `
		INSERT INTO supply_snapshots
		  (event_id, sac, amount, at_ledger, breakdown_hash, poster, posted_ledger, posted_at, tx_hash)
		VALUES
		  ('supply-duplicate', 'CSAC', 5, 10, 'hash', 'GPOSTER', 10, now(), 'tx'),
		  ('supply-duplicate', 'CSAC', 5, 10, 'hash', 'GPOSTER', 10, now(), 'tx')`)
	execSQL(t, url, `
		INSERT INTO reserve_reports
		  (event_id, sac, tier, amount, as_of, reporter, doc_hash, doc_uri, posted_ledger, posted_at, tx_hash)
		VALUES
		  ('reserve-duplicate', 'CSAC', 1, 5, now(), 'GREPORTER', 'hash', 'https://example.com',
		   10, now(), 'tx'),
		  ('reserve-duplicate', 'CSAC', 1, 5, now(), 'GREPORTER', 'hash', 'https://example.com',
		   10, now(), 'tx')`)

	s, err := Open(context.Background(), url)
	if err != nil {
		t.Fatalf("open legacy database: %v", err)
	}
	defer s.Close()

	for _, table := range []string{"supply_snapshots", "reserve_reports"} {
		var count int
		if err := s.pool.QueryRow(context.Background(),
			`SELECT count(*) FROM `+table).Scan(&count); err != nil {
			t.Fatalf("count %s: %v", table, err)
		}
		if count != 1 {
			t.Errorf("%s has %d rows after migration, want 1", table, count)
		}
	}
	var constraints int
	if err := s.pool.QueryRow(context.Background(), `
		SELECT count(*)
		FROM pg_constraint
		WHERE conname IN ('supply_snapshots_event_id_unique', 'reserve_reports_event_id_unique')`,
	).Scan(&constraints); err != nil {
		t.Fatal(err)
	}
	if constraints != 2 {
		t.Fatalf("created %d event ID constraints, want 2", constraints)
	}
}
