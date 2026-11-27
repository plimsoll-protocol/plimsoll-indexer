package horizon

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

const usdcRecord = `{"_embedded":{"records":[{
  "asset_code":"USDC",
  "balances":{"authorized":"268509083.9282711","authorized_to_maintain_liabilities":"0.0000000","unauthorized":"1.5"},
  "claimable_balances_amount":"25747.5652831",
  "liquidity_pools_amount":"5329035.9202534",
  "contracts_amount":"76649744.8499999"
}]}}`

func TestParseAmount(t *testing.T) {
	cases := map[string]string{
		"0.0000000":         "0",
		"1":                 "10000000",
		"1.5":               "15000000",
		"12.3456789":        "123456789",
		"":                  "0",
		"92233720368.54775": "922337203685477500",
	}
	for in, want := range cases {
		got, err := ParseAmount(in)
		if err != nil {
			t.Fatalf("%q: %v", in, err)
		}
		if got.String() != want {
			t.Errorf("%q: got %s want %s", in, got, want)
		}
	}
	for _, bad := range []string{"-1", "1.12345678", "abc"} {
		if _, err := ParseAmount(bad); err == nil {
			t.Errorf("%q: expected error", bad)
		}
	}
}

func TestSupplySumsEveryBucket(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			_, _ = w.Write([]byte(`{"history_latest_ledger": 61000000}`))
		case "/assets":
			if r.URL.Query().Get("asset_code") != "USDC" {
				t.Errorf("unexpected query %s", r.URL.RawQuery)
			}
			_, _ = w.Write([]byte(usdcRecord))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	b, err := NewClient(srv.URL).Supply(context.Background(), "USDC", "GISSUER")
	if err != nil {
		t.Fatal(err)
	}
	if b.Ledger != 61000000 {
		t.Errorf("ledger %d", b.Ledger)
	}
	// 268509083.9282711 + 1.5 + 25747.5652831 + 5329035.9202534 + 76649744.8499999
	if b.Total != "3505136137638075" {
		t.Errorf("total %s", b.Total)
	}
	if b.Unauthorized != "15000000" {
		t.Errorf("unauthorized %s", b.Unauthorized)
	}
	if b.Asset != "USDC:GISSUER" {
		t.Errorf("asset %s", b.Asset)
	}
}

// The ledger in a breakdown must be the one the balances were read at. Horizon
// stamps /assets with Latest-Ledger; reading the root separately could report a
// ledger that closed in between and mislabel the balances.
func TestSupplyUsesTheLedgerServedInTheLatestLedgerHeader(t *testing.T) {
	var assets atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/":
			// A root read would see a later ledger than the assets response.
			_, _ = w.Write([]byte(`{"history_latest_ledger": 61000005}`))
		case "/assets":
			assets.Add(1)
			w.Header().Set("Latest-Ledger", "61000003")
			_, _ = w.Write([]byte(usdcRecord))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	b, err := NewClient(srv.URL).Supply(context.Background(), "USDC", "GISSUER")
	if err != nil {
		t.Fatal(err)
	}
	if b.Ledger != 61000003 {
		t.Errorf("ledger %d, want the ledger the assets were served at (61000003)", b.Ledger)
	}
	if got := assets.Load(); got != 1 {
		t.Errorf("read /assets %d times, want 1 when the header names the ledger", got)
	}
}

// A Horizon (or proxy) that omits Latest-Ledger cannot say which ledger it
// served, so the read is bracketed by root queries. If the root moves across the
// read, no honest label exists and Supply says so instead of guessing.
func TestSupplyDetectsAMovingLedgerWhenTheHeaderIsMissing(t *testing.T) {
	var roots atomic.Uint32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			fmt.Fprintf(w, `{"history_latest_ledger": %d}`, 61_000_000+roots.Add(1))
			return
		}
		_, _ = w.Write([]byte(usdcRecord))
	}))
	defer srv.Close()

	_, err := NewClient(srv.URL).Supply(context.Background(), "USDC", "GISSUER")
	if !errors.Is(err, ErrLedgerMoved) {
		t.Fatalf("want ErrLedgerMoved, got %v", err)
	}
}

func TestSupplyUnknownAsset(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			_, _ = w.Write([]byte(`{"history_latest_ledger": 5}`))
			return
		}
		_, _ = w.Write([]byte(`{"_embedded":{"records":[]}}`))
	}))
	defer srv.Close()
	_, err := NewClient(srv.URL).Supply(context.Background(), "NOPE", "GISSUER")
	if !errors.Is(err, ErrAssetNotFound) {
		t.Fatalf("got %v", err)
	}
}

func TestHashIsSha256OfCanonical(t *testing.T) {
	b := Breakdown{Asset: "X:G", Ledger: 7, Total: "1"}
	sum := sha256.Sum256(b.Canonical())
	if b.Hash() != hex.EncodeToString(sum[:]) {
		t.Fatal("hash mismatch")
	}
	// Field order is fixed by the struct, so the encoding is stable.
	want := `{"asset":"X:G","ledger":7,"authorized":"","authorized_to_maintain_liabilities":"","unauthorized":"","claimable_balances":"","liquidity_pools":"","contracts":"","total":"1"}`
	if string(b.Canonical()) != want {
		t.Fatalf("canonical form changed:\n%s", b.Canonical())
	}
}

func TestHomeDomain(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"home_domain":"circle.com"}`))
	}))
	defer srv.Close()
	d, err := NewClient(srv.URL).HomeDomain(context.Background(), "GA5Z")
	if err != nil || d != "circle.com" {
		t.Fatalf("got %q, %v", d, err)
	}
}
