package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

const testSAC = "CCHPT4TEJDPZQDSUVW3NP6A35ROGV4WEFZKT45SKWYCFZSUB7R5HIM7S"

var testNow = time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)

type fakeReader struct {
	assets  []coverage.Asset
	supply  map[string]*coverage.Supply
	reports map[string]map[uint32]coverage.Report
}

func (f *fakeReader) ListAssets(context.Context) ([]coverage.Asset, error) {
	return f.assets, nil
}

func (f *fakeReader) GetAsset(_ context.Context, sac string) (coverage.Asset, error) {
	for _, a := range f.assets {
		if a.SAC == sac {
			return a, nil
		}
	}
	return coverage.Asset{}, store.ErrNotFound
}

func (f *fakeReader) LatestSupply(_ context.Context, sac string) (*coverage.Supply, error) {
	return f.supply[sac], nil
}

func (f *fakeReader) SupplyHistory(_ context.Context, sac string, _ int) ([]coverage.Supply, error) {
	if sp := f.supply[sac]; sp != nil {
		return []coverage.Supply{*sp}, nil
	}
	return nil, nil
}

func (f *fakeReader) LatestReports(_ context.Context, sac string) (map[uint32]coverage.Report, error) {
	if r := f.reports[sac]; r != nil {
		return r, nil
	}
	return map[uint32]coverage.Report{}, nil
}

func (f *fakeReader) ReportHistory(_ context.Context, sac string, _ int) ([]coverage.Report, error) {
	var out []coverage.Report
	for _, r := range f.reports[sac] {
		out = append(out, r)
	}
	return out, nil
}

func (f *fakeReader) Reporters(context.Context) ([]coverage.Reporter, error) {
	return []coverage.Reporter{{
		Address: "GAUD", Role: coverage.RoleAuditor, Name: "Acme", UpdatedAt: testNow,
	}}, nil
}

func testHandler(production bool) http.Handler {
	f := &fakeReader{
		assets: []coverage.Asset{{
			SAC: testSAC, Code: "PUSD", Issuer: "GISS", ListedLedger: 10,
			ListedAt: testNow.Add(-time.Hour), Toml: []byte(`{"name":"PUSD"}`),
		}},
		supply: map[string]*coverage.Supply{
			testSAC: {
				Amount: big.NewInt(1_000_0000000), Ledger: 100,
				PostedAt: testNow.Add(-time.Hour), BreakdownHash: "hash",
				Poster: "GPOSTER", TxHash: "tx", Source: "event",
			},
		},
		reports: map[string]map[uint32]coverage.Report{
			testSAC: {coverage.TierIssuerSigned: {
				Tier: coverage.TierIssuerSigned, Amount: big.NewInt(1_020_0000000),
				AsOf: testNow.Add(-2 * time.Hour), PostedAt: testNow.Add(-2 * time.Hour),
				Reporter: "GISS", DocHash: "doc", DocURI: "https://x", Source: "event",
			}},
		},
	}
	return NewHandlerWithNow(f, production, func() time.Time { return testNow })
}

func postGraphQL(t *testing.T, h http.Handler, query string) map[string]any {
	t.Helper()
	body, err := json.Marshal(map[string]string{"query": query})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/graphql", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return out
}

func TestGraphQLAssetsAndCoverage(t *testing.T) {
	h := testHandler(false)
	out := postGraphQL(t, h, `{
		assets {
			sac code listedLedger
			toml
			supply { amount ledger breakdownHash source }
			reports { tier amount }
			coverage { bps ratio tier supply reserves reportAgeSeconds }
		}
	}`)
	if errs, ok := out["errors"]; ok {
		t.Fatalf("errors: %v", errs)
	}
	assets := out["data"].(map[string]any)["assets"].([]any)
	if len(assets) != 1 {
		t.Fatalf("assets = %v", assets)
	}
	a := assets[0].(map[string]any)
	if a["sac"] != testSAC || a["toml"] != `{"name":"PUSD"}` {
		t.Errorf("asset = %+v", a)
	}
	if a["supply"].(map[string]any)["amount"] != "10000000000" {
		t.Errorf("supply = %+v", a["supply"])
	}
	reports := a["reports"].([]any)
	if len(reports) != 1 || reports[0].(map[string]any)["tier"] != "issuer_signed" {
		t.Errorf("reports = %v", reports)
	}
	cov := a["coverage"].(map[string]any)
	if cov["bps"].(float64) != 10_200 || cov["ratio"] != "102.00%" || cov["tier"] != "issuer_signed" {
		t.Errorf("coverage = %+v", cov)
	}
	if cov["reportAgeSeconds"].(float64) != 7200 {
		t.Errorf("reportAgeSeconds = %v", cov["reportAgeSeconds"])
	}
}

func TestGraphQLMinTierAndMissingAsset(t *testing.T) {
	h := testHandler(false)
	out := postGraphQL(t, h, `{
		assets { coverage(minTier: 3) { tier } }
		asset(sac: "CNOPE") { sac }
	}`)
	if errs, ok := out["errors"]; ok {
		t.Fatalf("errors: %v", errs)
	}
	data := out["data"].(map[string]any)
	if data["asset"] != nil {
		t.Errorf("missing asset = %v, want null", data["asset"])
	}
	cov := data["assets"].([]any)[0].(map[string]any)["coverage"]
	if cov != nil {
		t.Errorf("coverage at tier 3 = %v, want null", cov)
	}

	out = postGraphQL(t, h, `{ assets(minTier: 3) { coverage { tier } } }`)
	if errs, ok := out["errors"]; ok {
		t.Fatalf("assets(minTier) errors: %v", errs)
	}
	cov = out["data"].(map[string]any)["assets"].([]any)[0].(map[string]any)["coverage"]
	if cov != nil {
		t.Errorf("coverage from assets(minTier: 3) = %v, want null", cov)
	}

	out = postGraphQL(t, h, `{ assets { coverage(minTier: 9) { tier } } }`)
	if _, ok := out["errors"]; !ok {
		t.Fatal("want an error for an invalid minTier")
	}
}

func TestGraphQLHistoryAndReporters(t *testing.T) {
	h := testHandler(false)
	out := postGraphQL(t, h, `{
		snapshots(sac: "`+testSAC+`") { amount }
		supplyHistory(sac: "`+testSAC+`", limit: 1) { amount }
		reports(sac: "`+testSAC+`") { tier }
		reportHistory(sac: "`+testSAC+`", limit: 1) { tier }
		reporters { address role name }
	}`)
	if errs, ok := out["errors"]; ok {
		t.Fatalf("errors: %v", errs)
	}
	data := out["data"].(map[string]any)
	for _, field := range []string{"snapshots", "supplyHistory", "reports", "reportHistory"} {
		if len(data[field].([]any)) != 1 {
			t.Errorf("%s = %v", field, data[field])
		}
	}
	reporters := data["reporters"].([]any)
	if len(reporters) != 1 || reporters[0].(map[string]any)["role"] != "auditor" {
		t.Errorf("reporters = %v", reporters)
	}
}

func TestGraphQLIntrospectionIsDisabledInProduction(t *testing.T) {
	query := `{ __schema { queryType { name } } }`

	dev := postGraphQL(t, testHandler(false), query)
	if _, ok := dev["errors"]; ok {
		t.Fatalf("development introspection failed: %v", dev["errors"])
	}

	prod := postGraphQL(t, testHandler(true), query)
	if _, ok := prod["errors"]; !ok {
		t.Fatalf("production introspection returned data: %v", prod["data"])
	}
}

func TestGraphQLMethodNotAllowedWithoutTransport(t *testing.T) {
	h := testHandler(false)
	req := httptest.NewRequest(http.MethodDelete, "/graphql", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code == http.StatusOK {
		t.Fatalf("DELETE /graphql returned 200: %s", rec.Body.String())
	}
}
