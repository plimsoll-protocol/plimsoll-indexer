package api

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	embeddedpostgres "github.com/fergusstrange/embedded-postgres"
	"github.com/jackc/pgx/v5"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

var (
	testAdminURL string
	testDBSeq    atomic.Int64
)

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		os.Exit(m.Run())
	}
	os.Exit(runWithPostgres(m))
}

func runWithPostgres(m *testing.M) int {
	dir, err := os.MkdirTemp("", "plimsoll-api-test-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "embedded postgres:", err)
		return 1
	}
	defer os.RemoveAll(dir)

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintln(os.Stderr, "embedded postgres port:", err)
		return 1
	}
	port := uint32(l.Addr().(*net.TCPAddr).Port)
	_ = l.Close()

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

	testAdminURL = fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", port)
	return m.Run()
}

func openIntegrationStore(t *testing.T) *store.Store {
	t.Helper()
	if testAdminURL == "" {
		t.Skip("embedded postgres is not running (go test -short)")
	}
	ctx := context.Background()
	name := fmt.Sprintf("api_test_%d_%d", os.Getpid(), testDBSeq.Add(1))
	admin, err := pgx.Connect(ctx, testAdminURL)
	if err != nil {
		t.Fatalf("connect to embedded postgres: %v", err)
	}
	defer admin.Close(ctx)
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	t.Cleanup(func() {
		conn, err := pgx.Connect(context.Background(), testAdminURL)
		if err != nil {
			t.Errorf("drop database %s: %v", name, err)
			return
		}
		defer conn.Close(context.Background())
		_, _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	cfg, err := pgx.ParseConfig(testAdminURL)
	if err != nil {
		t.Fatal(err)
	}
	dbURL := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/%s?sslmode=disable", cfg.Port, name)
	s, err := store.Open(ctx, dbURL)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(s.Close)
	return s
}

func TestVaultEndpointIntegration(t *testing.T) {
	db := openIntegrationStore(t)
	ctx := context.Background()

	// List the asset via event ingestion so the integration test exercises the
	// full store + API path for an asset with and without an indexed vault.
	listed := soroban.AssetListed{
		Meta:   soroban.Meta{ID: "ev-asset-1", Ledger: 100, ClosedAt: now},
		SAC:    sac,
		Code:   "PUSD",
		Issuer: "GBIE3ANCRVCBWETUZXWYKRMP27LQVJHX757XXAQUT3LYHVTNFPPPXEY4",
	}
	if err := db.ApplyEvents(ctx, "plimsoll-events", "cur-1", 100, []any{listed}); err != nil {
		t.Fatalf("apply asset_listed: %v", err)
	}

	srv := httptest.NewServer((&Server{
		Store: db,
		CORS:  []string{"*"},
		Log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:   func() time.Time { return now },
	}).Handler())
	defer srv.Close()

	// Listed asset with no indexed vault returns 404.
	get(t, srv, "/v1/assets/"+sac+"/vault", http.StatusNotFound)

	const vaultAddr = "CC3SUECAJSAC4PBV4QLUII7X5Y4WNVL5YGTBAQ4CHCAH6R3QLRYJUZHQ"
	if err := db.SaveVault(ctx, coverage.Vault{
		SAC:            sac,
		VaultAddress:   vaultAddr,
		ReserveBalance: big.NewInt(7_500_0000000),
		ThresholdBps:   10000,
		UpdatedAt:      now,
	}); err != nil {
		t.Fatalf("save vault: %v", err)
	}

	resp, err := http.Get(srv.URL + "/v1/assets/" + sac + "/vault")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var got struct {
		SAC            string `json:"sac"`
		VaultAddress   string `json:"vault_address"`
		ReserveBalance string `json:"reserve_balance"`
		ThresholdBps   uint32 `json:"threshold_bps"`
		UpdatedAt      string `json:"updated_at"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got.SAC != sac || got.VaultAddress != vaultAddr ||
		got.ReserveBalance != "75000000000" || got.ThresholdBps != 10000 ||
		got.UpdatedAt != now.Format(time.RFC3339) {
		t.Fatalf("unexpected vault integration response: %+v", got)
	}
}
