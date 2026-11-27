package store

import (
	"bytes"
	"context"
	"errors"
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

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
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
		Version(embeddedpostgres.V16).
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

func TestAssetPaginationKeyset(t *testing.T) {
	s, url := openStore(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 25, 2, 0, 0, 0, time.UTC)
	for i, asset := range []struct {
		sac, code, issuer string
	}{
		{"C1", "PUSD", "G1"},
		{"C2", "PUSD", "G2"},
		{"C3", "USDX", "G3"},
		{"C4", "USDX", "G4"},
		{"C5", "USDX", "G5"},
	} {
		execSQL(t, url, `
			INSERT INTO assets (sac, code, issuer, listed_ledger, listed_at, event_id)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			asset.sac, asset.code, asset.issuer, i+1, at, fmt.Sprintf("listed-%d", i))
	}

	first, err := s.ListAssetsPage(ctx, 3, "", "")
	if err != nil || len(first) != 3 ||
		first[0].Code != "PUSD" || first[1].Issuer != "G2" || first[2].Issuer != "G3" {
		t.Fatalf("first page = %+v, err=%v", first, err)
	}
	middle, err := s.ListAssetsPage(ctx, 3, first[2].Code, first[2].Issuer)
	if err != nil || len(middle) != 2 || middle[0].Issuer != "G4" || middle[1].Issuer != "G5" {
		t.Fatalf("middle page = %+v, err=%v", middle, err)
	}
	last, err := s.ListAssetsPage(ctx, 3, middle[1].Code, middle[1].Issuer)
	if err != nil || len(last) != 0 {
		t.Fatalf("last page = %+v, err=%v", last, err)
	}
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
	ownerKeyHash := "owner-key-hash"
	created, err := s.CreateWebhook(ctx, "https://receiver.example/hook", "sha256-hash", ciphertext, ownerKeyHash)
	if err != nil {
		t.Fatal(err)
	}
	if created.ID < 1 || created.SecretHash != "sha256-hash" || created.OwnerKeyHash != ownerKeyHash ||
		!bytes.Equal(created.SecretCiphertext, ciphertext) {
		t.Fatalf("created webhook = %+v", created)
	}

	if err := s.DeleteWebhook(ctx, created.ID, "different-owner", false); !errors.Is(err, ErrForbidden) {
		t.Fatalf("delete webhook as non-owner: err=%v, want ErrForbidden", err)
	}
	if err := s.DeleteWebhook(ctx, created.ID, ownerKeyHash, false); err != nil {
		t.Fatalf("delete webhook as owner: %v", err)
	}
	if err := s.DeleteWebhook(ctx, created.ID, ownerKeyHash, false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("delete missing webhook: err=%v, want ErrNotFound", err)
	}

	list, err := s.ListWebhooks(ctx)
	if err != nil || len(list) != 0 {
		t.Fatalf("listed webhooks after delete = %+v, %v", list, err)
	}
	if _, err := s.CreateWebhook(ctx, "https://receiver.example/hook", "sha256-hash", ciphertext, ownerKeyHash); err != nil {
		t.Fatal(err)
	}
	list, err = s.ListWebhooks(ctx)
	if err != nil || len(list) != 1 ||
		list[0].URL != created.URL || list[0].SecretHash != created.SecretHash ||
		!bytes.Equal(list[0].SecretCiphertext, ciphertext) {
		t.Fatalf("listed webhooks = %+v, %v", list, err)
	}

	var legacyID int64
	err = s.pool.QueryRow(ctx, `
		INSERT INTO webhooks (url, secret_hash, secret_ciphertext)
		VALUES ('https://legacy.example/hook', 'legacy-hash', $1)
		RETURNING id`, ciphertext).Scan(&legacyID)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteWebhook(ctx, legacyID, "owner-key-hash", false); !errors.Is(err, ErrForbidden) {
		t.Fatalf("delete legacy webhook as owner key: err=%v, want ErrForbidden", err)
	}
	if err := s.DeleteWebhook(ctx, legacyID, "admin-key-hash", true); err != nil {
		t.Fatalf("delete legacy webhook as admin: %v", err)
	}
}

// TestWebhookPerKeyLimitAndDeliveryCounters covers the multi-endpoint rules:
// one key holds at most MaxWebhooksPerKey endpoints, each key sees only its
// own, and every delivery attempt is counted against the endpoint it went to.
func TestWebhookPerKeyLimitAndDeliveryCounters(t *testing.T) {
	s, _ := openStore(t)
	ctx := context.Background()
	ciphertext := []byte{1, 2, 3, 4}

	var created Webhook
	for i := range MaxWebhooksPerKey {
		w, err := s.CreateWebhook(ctx, fmt.Sprintf("https://receiver.example/%d", i), "hash", ciphertext, "key-one")
		if err != nil {
			t.Fatalf("create endpoint %d: %v", i, err)
		}
		created = w
	}
	if _, err := s.CreateWebhook(ctx, "https://receiver.example/over", "hash", ciphertext, "key-one"); !errors.Is(err, ErrWebhookLimit) {
		t.Fatalf("create past the limit: err=%v, want ErrWebhookLimit", err)
	}
	// The limit is per key: another key starts with its own quota.
	if _, err := s.CreateWebhook(ctx, "https://receiver.example/other", "hash", ciphertext, "key-two"); err != nil {
		t.Fatalf("create for a second key: %v", err)
	}

	mine, err := s.ListWebhooksForKey(ctx, "key-one")
	if err != nil || len(mine) != MaxWebhooksPerKey {
		t.Fatalf("key-one lists %d endpoints (%v), want %d", len(mine), err, MaxWebhooksPerKey)
	}
	theirs, err := s.ListWebhooksForKey(ctx, "key-two")
	if err != nil || len(theirs) != 1 || theirs[0].OwnerKeyHash != "key-two" {
		t.Fatalf("key-two lists %+v (%v), want only its own endpoint", theirs, err)
	}
	// The dispatcher still sees every endpoint.
	if all, err := s.ListWebhooks(ctx); err != nil || len(all) != MaxWebhooksPerKey+1 {
		t.Fatalf("all endpoints = %d (%v), want %d", len(all), err, MaxWebhooksPerKey+1)
	}

	at := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	if err := s.RecordWebhookDelivery(ctx, created.ID, at, ""); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordWebhookDelivery(ctx, created.ID, at.Add(time.Minute), "webhook returned status 500"); err != nil {
		t.Fatal(err)
	}
	mine, err = s.ListWebhooksForKey(ctx, "key-one")
	if err != nil {
		t.Fatal(err)
	}
	var got Webhook
	for _, w := range mine {
		if w.ID == created.ID {
			got = w
		}
	}
	failedAt := at.Add(time.Minute)
	if got.Deliveries != 2 || got.Failures != 1 || got.LastDeliveryStatus != "failed" ||
		got.LastDeliveryError != "webhook returned status 500" ||
		got.LastDeliveryAt == nil || !got.LastDeliveryAt.Equal(failedAt) {
		t.Fatalf("delivery state = %+v, want two attempts and one failure", got)
	}
	// A later success updates the status and clears the error, not the counts.
	if err := s.RecordWebhookDelivery(ctx, created.ID, at.Add(2*time.Minute), ""); err != nil {
		t.Fatal(err)
	}
	mine, err = s.ListWebhooksForKey(ctx, "key-one")
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range mine {
		if w.ID == created.ID {
			got = w
		}
	}
	if got.Deliveries != 3 || got.Failures != 1 || got.LastDeliveryStatus != "delivered" || got.LastDeliveryError != "" {
		t.Fatalf("delivery state after success = %+v", got)
	}
}

func TestReporterSetPersistsDisplayNameAndPreservesMetadata(t *testing.T) {
	s, _ := openStore(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 25, 5, 0, 0, 0, time.UTC)
	displayName, websiteURI, contactHash := "Acme Inc.", "https://acme.example", "aabbcc"

	registered := soroban.ReporterSet{
		Meta:     soroban.Meta{ID: "reporter-set-1", Ledger: 100, ClosedAt: at},
		Reporter: "GREPORTER",
		Role:     coverage.RoleAuditor,
		Name:     "Acme",
	}
	if err := s.ApplyEvents(ctx, "plimsoll-events", "cursor-1", 100, nil, []any{registered}); err != nil {
		t.Fatalf("apply registration: %v", err)
	}
	metadataEvent := soroban.ReporterMetadataUpdated{
		Meta:        soroban.Meta{ID: "reporter-metadata-1", Ledger: 101, ClosedAt: at.Add(time.Minute)},
		Reporter:    registered.Reporter,
		DisplayName: &displayName,
		WebsiteURI:  &websiteURI,
		ContactHash: &contactHash,
	}
	if err := s.ApplyEvents(ctx, "plimsoll-events", "cursor-2", 101, nil, []any{metadataEvent}); err != nil {
		t.Fatalf("apply reporter metadata: %v", err)
	}

	updated := registered
	updated.ID = "reporter-set-2"
	updated.Ledger = 102
	updated.ClosedAt = at.Add(2 * time.Minute)
	updated.Role = coverage.RoleTranscriber
	updated.Name = "Acme Updated"
	if err := s.ApplyEvents(ctx, "plimsoll-events", "cursor-3", 102, nil, []any{updated}); err != nil {
		t.Fatalf("apply update: %v", err)
	}

	reporters, err := s.Reporters(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(reporters) != 1 {
		t.Fatalf("got %d reporters, want 1", len(reporters))
	}
	got := reporters[0]
	if got.Name != "Acme Updated" || got.DisplayName != displayName ||
		got.Role != coverage.RoleTranscriber || got.WebsiteURI == nil ||
		*got.WebsiteURI != websiteURI || got.ContactHash == nil ||
		*got.ContactHash != contactHash {
		t.Fatalf("reporter metadata after update = %+v", got)
	}

	if _, err := s.SaveChainReporter(ctx, coverage.Reporter{
		Address: registered.Reporter, Role: updated.Role, Name: updated.Name,
		DisplayName: displayName, WebsiteURI: &websiteURI, ContactHash: &contactHash,
	}); err != nil {
		t.Fatalf("save reporter state: %v", err)
	}
	if _, err := s.SaveChainReporter(ctx, coverage.Reporter{
		Address: registered.Reporter, Role: updated.Role, Name: updated.Name,
	}); err != nil {
		t.Fatalf("clear reporter state metadata: %v", err)
	}
	reporters, err = s.Reporters(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if reporters[0].DisplayName != updated.Name || reporters[0].WebsiteURI != nil || reporters[0].ContactHash != nil {
		t.Fatalf("reporter metadata after chain clear = %+v", reporters[0])
	}
}

func TestReporterMetadataEventKeepsAddressForReconciliation(t *testing.T) {
	s, _ := openStore(t)
	ctx := context.Background()
	displayName := "Acme Inc."
	event := soroban.ReporterMetadataUpdated{
		Meta:        soroban.Meta{ID: "reporter-metadata-only", Ledger: 100, ClosedAt: time.Now().UTC()},
		Reporter:    "GREPORTER",
		DisplayName: &displayName,
	}
	if err := s.ApplyEvents(ctx, "plimsoll-events", "cursor-metadata-only", 100, nil, []any{event}); err != nil {
		t.Fatal(err)
	}
	reporters, err := s.Reporters(ctx)
	if err != nil || len(reporters) != 0 {
		t.Fatalf("metadata-only event exposed an incomplete reporter: %+v, %v", reporters, err)
	}
	addresses, err := s.AllReporterAddresses(ctx)
	if err != nil || len(addresses) != 1 || addresses[0] != event.Reporter {
		t.Fatalf("metadata-only event lost its reporter address: %v, %v", addresses, err)
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

	if err := s.ApplyEvents(ctx, "plimsoll-events", "cursor-1", 101, nil, []any{supply, reserve}); err != nil {
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
	if err := s.ApplyEvents(ctx, "plimsoll-events", "cursor-2", 102, nil,
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

func TestLatestSuppliesAndReportsBySAC(t *testing.T) {
	s, _ := openStore(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 25, 1, 0, 0, 0, time.UTC)

	for _, supply := range []coverage.Supply{
		{Amount: big.NewInt(100), Ledger: 10, PostedAt: at, BreakdownHash: "old", Poster: "G1"},
		{Amount: big.NewInt(200), Ledger: 20, PostedAt: at.Add(time.Hour), BreakdownHash: "new", Poster: "G1"},
	} {
		if _, err := s.SaveChainSupply(ctx, "CSAC1", supply); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.SaveChainSupply(ctx, "CSAC2", coverage.Supply{
		Amount: big.NewInt(300), Ledger: 30, PostedAt: at, BreakdownHash: "two", Poster: "G2",
	}); err != nil {
		t.Fatal(err)
	}

	reports := []coverage.Report{
		{Tier: coverage.TierIssuerSigned, Amount: big.NewInt(110), AsOf: at, PostedAt: at, Reporter: "G1", DocURI: "old"},
		{Tier: coverage.TierIssuerSigned, Amount: big.NewInt(210), AsOf: at.Add(time.Hour), PostedAt: at.Add(time.Hour), Reporter: "G1", DocURI: "new"},
		{Tier: coverage.TierAuditorSigned, Amount: big.NewInt(220), AsOf: at, PostedAt: at, Reporter: "G2", DocURI: "audit"},
	}
	for _, report := range reports {
		if _, err := s.SaveChainReport(ctx, "CSAC1", report); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.SaveChainReport(ctx, "CSAC2", coverage.Report{
		Tier: coverage.TierTranscribed, Amount: big.NewInt(290), AsOf: at, PostedAt: at, Reporter: "G3", DocURI: "two",
	}); err != nil {
		t.Fatal(err)
	}

	supplies, err := s.LatestSupplies(ctx, []string{"CSAC1", "CSAC2", "CSAC3"})
	if err != nil {
		t.Fatal(err)
	}
	if supplies["CSAC1"] == nil || supplies["CSAC1"].Ledger != 20 || supplies["CSAC1"].Amount.String() != "200" {
		t.Fatalf("CSAC1 supply = %+v", supplies["CSAC1"])
	}
	if supplies["CSAC2"] == nil || supplies["CSAC2"].Ledger != 30 {
		t.Fatalf("CSAC2 supply = %+v", supplies["CSAC2"])
	}
	if supplies["CSAC3"] != nil {
		t.Fatalf("missing CSAC3 supply should be nil: %+v", supplies["CSAC3"])
	}

	bySAC, err := s.LatestReportsBySAC(ctx, []string{"CSAC1", "CSAC2", "CSAC3"})
	if err != nil {
		t.Fatal(err)
	}
	if len(bySAC["CSAC1"]) != 2 {
		t.Fatalf("CSAC1 reports = %+v", bySAC["CSAC1"])
	}
	if got := bySAC["CSAC1"][coverage.TierIssuerSigned].Amount.String(); got != "210" {
		t.Fatalf("latest issuer_signed amount = %q, want 210", got)
	}
	if len(bySAC["CSAC2"]) != 1 {
		t.Fatalf("CSAC2 reports = %+v", bySAC["CSAC2"])
	}
}
