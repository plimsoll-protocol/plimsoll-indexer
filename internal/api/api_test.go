package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

var now = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

const sac = "CCHPT4TEJDPZQDSUVW3NP6A35ROGV4WEFZKT45SKWYCFZSUB7R5HIM7S"

type fake struct {
	assets  []coverage.Asset
	supply  map[string]*coverage.Supply
	reports map[string]map[uint32]coverage.Report
	bodies  map[string][]byte
}

func (f *fake) Ping(context.Context) error { return nil }
func (f *fake) ListAssets(context.Context) ([]coverage.Asset, error) {
	return f.assets, nil
}
func (f *fake) GetAsset(_ context.Context, s string) (coverage.Asset, error) {
	for _, a := range f.assets {
		if a.SAC == s {
			return a, nil
		}
	}
	return coverage.Asset{}, store.ErrNotFound
}
func (f *fake) LatestSupply(_ context.Context, s string) (*coverage.Supply, error) {
	return f.supply[s], nil
}
func (f *fake) SupplyHistory(_ context.Context, s string, _ int) ([]coverage.Supply, error) {
	if sp := f.supply[s]; sp != nil {
		return []coverage.Supply{*sp}, nil
	}
	return nil, nil
}
func (f *fake) LatestReports(_ context.Context, s string) (map[uint32]coverage.Report, error) {
	if r := f.reports[s]; r != nil {
		return r, nil
	}
	return map[uint32]coverage.Report{}, nil
}
func (f *fake) ReportHistory(_ context.Context, s string, _ int) ([]coverage.Report, error) {
	var out []coverage.Report
	for _, r := range f.reports[s] {
		out = append(out, r)
	}
	return out, nil
}
func (f *fake) Reporters(context.Context) ([]coverage.Reporter, error) {
	return []coverage.Reporter{{Address: "GAUD", Role: coverage.RoleAuditor, Name: "Acme"}}, nil
}
func (f *fake) Breakdown(_ context.Context, h string) ([]byte, error) {
	if b, ok := f.bodies[h]; ok {
		return b, nil
	}
	return nil, store.ErrNotFound
}
func (f *fake) Progress(context.Context, string) (store.Progress, error) {
	return store.Progress{
		Cursor:        "c",
		ScannedLedger: 42,
		EventLedger:   40,
		EventAt:       now.Add(-90 * time.Second),
	}, nil
}

func server() *httptest.Server {
	f := &fake{
		assets: []coverage.Asset{{SAC: sac, Code: "PUSD", Issuer: "GISS"}},
		supply: map[string]*coverage.Supply{
			sac: {Amount: big.NewInt(1_000_0000000), Ledger: 100, PostedAt: now.Add(-time.Hour)},
		},
		reports: map[string]map[uint32]coverage.Report{
			sac: {coverage.TierIssuerSigned: {
				Tier: coverage.TierIssuerSigned, Amount: big.NewInt(1_020_0000000),
				AsOf: now.Add(-2 * time.Hour), Reporter: "GISS", DocURI: "https://x",
			}},
		},
		bodies: map[string][]byte{"abc": []byte(`{"total":"1"}`)},
	}
	s := &Server{
		Store: f,
		CORS:  []string{"*"},
		Log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:   func() time.Time { return now },
	}
	return httptest.NewServer(s.Handler())
}

func get(t *testing.T, srv *httptest.Server, path string, want int) map[string]any {
	t.Helper()
	resp, err := http.Get(srv.URL + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != want {
		t.Fatalf("%s: status %d want %d", path, resp.StatusCode, want)
	}
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return out
}

func TestGetAssetCoverage(t *testing.T) {
	srv := server()
	defer srv.Close()
	body := get(t, srv, "/v1/assets/"+sac, 200)
	cov := body["coverage"].(map[string]any)
	if cov["bps"].(float64) != 10_200 || cov["ratio"] != "102.00%" || cov["tier"] != "issuer_signed" {
		t.Fatalf("coverage %+v", cov)
	}
	if cov["report_age_seconds"].(float64) != 7200 {
		t.Errorf("age %v", cov["report_age_seconds"])
	}
	if body["supply"].(map[string]any)["amount"] != "10000000000" {
		t.Errorf("supply %+v", body["supply"])
	}
}

func TestMinTierExcludesWeakerReports(t *testing.T) {
	srv := server()
	defer srv.Close()
	body := get(t, srv, "/v1/assets/"+sac+"?min_tier=3", 200)
	if body["coverage"] != nil {
		t.Fatalf("expected no coverage at auditor tier, got %+v", body["coverage"])
	}
	get(t, srv, "/v1/assets/"+sac+"?min_tier=9", 400)
}

func TestListAndMisc(t *testing.T) {
	srv := server()
	defer srv.Close()
	list := get(t, srv, "/v1/assets", 200)
	if len(list["assets"].([]any)) != 1 {
		t.Fatal("expected one asset")
	}
	get(t, srv, "/v1/assets/CNOPE", 404)
	rep := get(t, srv, "/v1/reporters", 200)
	if rep["reporters"].([]any)[0].(map[string]any)["role"] != "auditor" {
		t.Fatal("role name missing")
	}
	h := get(t, srv, "/healthz", 200)
	if h["ingested_to_ledger"].(float64) != 42 {
		t.Fatal("health missing ledger")
	}
	// A scan far ahead of the last stored event is the shape of silently
	// broken ingestion, so health has to report both.
	if h["last_event_ledger"].(float64) != 40 {
		t.Errorf("last_event_ledger = %v", h["last_event_ledger"])
	}
	if h["last_event_age_seconds"].(float64) != 90 {
		t.Errorf("last_event_age_seconds = %v", h["last_event_age_seconds"])
	}
	get(t, srv, "/v1/assets/"+sac+"/supply?limit=0", 400)
	get(t, srv, "/v1/assets/"+sac+"/reports", 200)
}

func TestBreakdownServedVerbatim(t *testing.T) {
	srv := server()
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/v1/breakdowns/abc")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(b) != `{"total":"1"}` {
		t.Fatalf("body %q", b)
	}
	get(t, srv, "/v1/breakdowns/zzz", 404)
}

func TestCORSHeader(t *testing.T) {
	srv := server()
	defer srv.Close()
	req, _ := http.NewRequest(http.MethodOptions, srv.URL+"/v1/assets", nil)
	req.Header.Set("Origin", "https://plimsoll.example")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent || resp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("status %d, header %q", resp.StatusCode, resp.Header.Get("Access-Control-Allow-Origin"))
	}
}
