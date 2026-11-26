package api

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/webhook"
)

var now = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

const sac = "CCHPT4TEJDPZQDSUVW3NP6A35ROGV4WEFZKT45SKWYCFZSUB7R5HIM7S"

// breakdownHash is what a SHA-256 breakdown hash looks like: 64 hex characters.
var breakdownHash = strings.Repeat("a", 64)

// contractAddress builds a well-formed contract strkey from a tag byte, so a
// test can hold several distinct addresses.
func contractAddress(t *testing.T, tag byte) string {
	t.Helper()
	raw := make([]byte, 32)
	raw[0] = tag
	s, err := strkey.Encode(strkey.VersionByteContract, raw)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// unlistedSAC is a well-formed contract strkey that fake holds no asset for.
func unlistedSAC(t *testing.T) string { return contractAddress(t, 0x01) }

type fake struct {
	assets   []coverage.Asset
	supply   map[string]*coverage.Supply
	reports  map[string]map[uint32]coverage.Report
	bodies   map[string][]byte
	disputes map[string][]coverage.Dispute
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
func (f *fake) Disputes(_ context.Context, s, status string, _, offset int) ([]coverage.Dispute, error) {
	var filtered []coverage.Dispute
	for _, d := range f.disputes[s] {
		if status != "" && d.Status != status {
			continue
		}
		filtered = append(filtered, d)
	}
	if offset >= len(filtered) {
		return nil, nil
	}
	return filtered[offset:], nil
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

type webhookManagerFake struct {
	webhooks []store.Webhook
}

func (f *webhookManagerFake) CreateWebhook(_ context.Context, url, hash string, ciphertext []byte) (store.Webhook, error) {
	item := store.Webhook{
		ID: int64(len(f.webhooks) + 1), URL: url, SecretHash: hash,
		SecretCiphertext: ciphertext, CreatedAt: now,
	}
	f.webhooks = append(f.webhooks, item)
	return item, nil
}

func (f *webhookManagerFake) ListWebhooks(context.Context) ([]store.Webhook, error) {
	return f.webhooks, nil
}

func (f *webhookManagerFake) DeleteWebhook(_ context.Context, id int64) (bool, error) {
	for i, item := range f.webhooks {
		if item.ID == id {
			f.webhooks = append(f.webhooks[:i], f.webhooks[i+1:]...)
			return true, nil
		}
	}
	return false, nil
}

func server() *httptest.Server {
	return serverWithWebhooks(nil, nil, "")
}

// serverFor serves an arbitrary fake, so a test can set up more assets or
// disputes than the default fixture.
func serverFor(f *fake) *httptest.Server {
	s := &Server{
		Store: f,
		CORS:  []string{"*"},
		Log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:   func() time.Time { return now },
	}
	return httptest.NewServer(s.Handler())
}

func serverWithWebhooks(manager WebhookManager, codec *webhook.SecretCodec, token string) *httptest.Server {
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
		bodies: map[string][]byte{breakdownHash: []byte(`{"total":"1"}`)},
	}
	s := &Server{
		Store:             f,
		WebhookStore:      manager,
		WebhookCodec:      codec,
		WebhookAdminToken: token,
		CORS:              []string{"*"},
		Log:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:               func() time.Time { return now },
		Changelog:         testChangelog,
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

// HEAD answers existence with a status code and no body, so a client can
// check a listing without downloading the asset document.
func TestHeadAsset(t *testing.T) {
	srv := server()
	defer srv.Close()
	for _, tc := range []struct {
		name string
		path string
		want int
	}{
		{"listed", "/v1/assets/" + sac, http.StatusOK},
		{"unlisted", "/v1/assets/" + unlistedSAC(t), http.StatusNotFound},
		{"malformed", "/v1/assets/not-a-sac", http.StatusBadRequest},
	} {
		req, err := http.NewRequest(http.MethodHead, srv.URL+tc.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if resp.StatusCode != tc.want {
			t.Errorf("%s: status %d want %d", tc.name, resp.StatusCode, tc.want)
		}
		if len(body) != 0 {
			t.Errorf("%s: body %q, want empty", tc.name, body)
		}
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
	get(t, srv, "/v1/assets/CNOPE", 400)
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
	resp, err := http.Get(srv.URL + "/v1/breakdowns/" + breakdownHash)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(b) != `{"total":"1"}` {
		t.Fatalf("body %q", b)
	}
	get(t, srv, "/v1/breakdowns/"+strings.Repeat("0", 64), 404)
}

// Every path id is checked before it reaches storage: malformed ids are a
// client error, while well-formed but unlisted ids are a missing resource.
func TestMalformedPathIDs(t *testing.T) {
	srv := server()
	defer srv.Close()
	for _, path := range []string{
		"/v1/assets/not-a-sac",
		"/v1/assets/CNOPE",
		"/v1/assets/not-a-sac/supply",
		"/v1/assets/not-a-sac/reports",
		"/v1/assets/not-a-sac/disputes",
	} {
		body := get(t, srv, path, 400)
		if body["code"] != "invalid_sac" || body["message"] == nil {
			t.Errorf("%s: expected an invalid_sac error, got %+v", path, body)
		}
	}
	for _, hash := range []string{"abc", strings.Repeat("a", 63), strings.Repeat("z", 64)} {
		body := get(t, srv, "/v1/breakdowns/"+hash, 400)
		if body["code"] != "invalid_breakdown_hash" || body["message"] == nil {
			t.Errorf("hash %q: expected an invalid_breakdown_hash error, got %+v", hash, body)
		}
	}
}

func TestUnlistedPathIDs(t *testing.T) {
	srv := server()
	defer srv.Close()
	unknown := unlistedSAC(t)
	for _, path := range []string{
		"/v1/assets/" + unknown,
		"/v1/assets/" + unknown + "/supply",
		"/v1/assets/" + unknown + "/reports",
		"/v1/assets/" + unknown + "/disputes",
	} {
		body := get(t, srv, path, 404)
		if body["code"] != "asset_not_listed" || body["message"] != "asset not listed" {
			t.Errorf("%s: body %+v", path, body)
		}
	}
}

// failingStore breaks Ping so the health handler's error path can be checked.
type failingStore struct{ Reader }

func (failingStore) Ping(context.Context) error { return errors.New("down") }

// Every failure, including the plain-text 404 and 405 net/http writes for an
// unknown path or method, carries the same structured body.
func TestStructuredErrorBody(t *testing.T) {
	srv := server()
	defer srv.Close()
	cases := []struct {
		method, path, code, parameter string
		status                        int
	}{
		{http.MethodGet, "/v1/assets/CNOPE", "invalid_sac", "", 400},
		{http.MethodGet, "/v1/assets/" + sac + "?min_tier=9", "invalid_min_tier", "min_tier", 400},
		{http.MethodGet, "/v1/assets/" + sac + "/supply?limit=0", "invalid_limit", "limit", 400},
		{http.MethodGet, "/v1/breakdowns/abc", "invalid_breakdown_hash", "", 400},
		{http.MethodGet, "/v1/breakdowns/" + strings.Repeat("0", 64), "breakdown_not_found", "", 404},
		{http.MethodGet, "/v1/assets/" + unlistedSAC(t), "asset_not_listed", "", 404},
		{http.MethodGet, "/v1/nope", "not_found", "", 404},
		{http.MethodPost, "/v1/assets", "method_not_allowed", "", 405},
		{http.MethodPost, "/v1/webhooks", "webhooks_not_configured", "", 503},
	}
	for _, tc := range cases {
		req, err := http.NewRequest(tc.method, srv.URL+tc.path, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		var body errorBody
		decodeErr := json.NewDecoder(resp.Body).Decode(&body)
		resp.Body.Close()
		if resp.StatusCode != tc.status {
			t.Errorf("%s %s: status %d want %d", tc.method, tc.path, resp.StatusCode, tc.status)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
			t.Errorf("%s %s: Content-Type %q", tc.method, tc.path, ct)
		}
		if decodeErr != nil {
			t.Errorf("%s %s: decode: %v", tc.method, tc.path, decodeErr)
			continue
		}
		if body.Code != tc.code || body.Message == "" {
			t.Errorf("%s %s: body %+v want code %q", tc.method, tc.path, body, tc.code)
		}
		if tc.parameter != "" && body.Details["parameter"] != tc.parameter {
			t.Errorf("%s %s: details %+v want parameter %q", tc.method, tc.path, body.Details, tc.parameter)
		}
	}
}

func TestHealthErrorBody(t *testing.T) {
	s := &Server{
		Store: failingStore{&fake{}},
		CORS:  []string{"*"},
		Log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:   func() time.Time { return now },
	}
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()
	body := get(t, srv, "/healthz", 503)
	if body["code"] != "db_unavailable" || body["message"] == nil {
		t.Fatalf("body %+v", body)
	}
}

// A listed asset keeps answering 200 from both history endpoints.
func TestListedAssetHistory(t *testing.T) {
	srv := server()
	defer srv.Close()
	if got := get(t, srv, "/v1/assets/"+sac+"/supply", 200)["supply"].([]any); len(got) != 1 {
		t.Fatalf("supply history %+v", got)
	}
	if got := get(t, srv, "/v1/assets/"+sac+"/reports", 200)["reports"].([]any); len(got) != 1 {
		t.Fatalf("report history %+v", got)
	}
}

func TestCoveredAssetsEndpoint(t *testing.T) {
	second := contractAddress(t, 0x02)
	stale := contractAddress(t, 0x03)
	under := contractAddress(t, 0x04)
	f := &fake{
		assets: []coverage.Asset{
			{SAC: sac, Code: "PUSD", Issuer: "GISS"},
			{SAC: second, Code: "QUSD", Issuer: "GISS"},
			{SAC: stale, Code: "STALE", Issuer: "GISS"},
			{SAC: under, Code: "UNDER", Issuer: "GISS"},
		},
		supply: map[string]*coverage.Supply{
			sac:    {Amount: big.NewInt(1_000), Ledger: 100, PostedAt: now.Add(-time.Hour)},
			second: {Amount: big.NewInt(1_000), Ledger: 100, PostedAt: now.Add(-time.Hour)},
			stale:  {Amount: big.NewInt(1_000), Ledger: 100, PostedAt: now.Add(-48 * time.Hour)},
			under:  {Amount: big.NewInt(1_000), Ledger: 100, PostedAt: now.Add(-time.Hour)},
		},
		reports: map[string]map[uint32]coverage.Report{
			sac:    {coverage.TierIssuerSigned: {Tier: coverage.TierIssuerSigned, Amount: big.NewInt(1_000), AsOf: now.Add(-2 * time.Hour), Reporter: "GISS"}},
			second: {coverage.TierIssuerSigned: {Tier: coverage.TierIssuerSigned, Amount: big.NewInt(2_000), AsOf: now.Add(-2 * time.Hour), Reporter: "GISS"}},
			stale:  {coverage.TierIssuerSigned: {Tier: coverage.TierIssuerSigned, Amount: big.NewInt(1_000), AsOf: now.Add(-2 * time.Hour), Reporter: "GISS"}},
			under:  {coverage.TierIssuerSigned: {Tier: coverage.TierIssuerSigned, Amount: big.NewInt(500), AsOf: now.Add(-2 * time.Hour), Reporter: "GISS"}},
		},
	}
	srv := serverFor(f)
	defer srv.Close()

	body := get(t, srv, "/v1/assets/covered", 200)
	assets := body["assets"].([]any)
	if len(assets) != 2 {
		t.Fatalf("covered assets = %d, want sac and second", len(assets))
	}
	if assets[0].(map[string]any)["sac"] != sac || assets[1].(map[string]any)["sac"] != second {
		t.Fatalf("covered assets = %+v, want the two fresh, backed assets", assets)
	}

	if got := get(t, srv, "/v1/assets/covered?min_bps=10001", 200)["assets"].([]any); len(got) != 1 {
		t.Fatalf("min_bps=10001 returned %d assets, want only the over-backed one", len(got))
	}
	if got := get(t, srv, "/v1/assets/covered?max_age=1h", 200)["assets"].([]any); len(got) != 0 {
		t.Fatalf("max_age=1h returned %d assets, want none", len(got))
	}
	if got := get(t, srv, "/v1/assets/covered?min_tier=3", 200)["assets"].([]any); len(got) != 0 {
		t.Fatalf("min_tier=3 returned %d assets, want none", len(got))
	}

	page := get(t, srv, "/v1/assets/covered?limit=1", 200)
	if len(page["assets"].([]any)) != 1 || page["next_offset"].(float64) != 1 {
		t.Fatalf("first page = %+v, want one asset and next_offset 1", page)
	}
	page = get(t, srv, "/v1/assets/covered?limit=1&offset=1", 200)
	if len(page["assets"].([]any)) != 1 || page["next_offset"] != nil {
		t.Fatalf("second page = %+v, want one asset and no next page", page)
	}

	get(t, srv, "/v1/assets/covered?min_bps=abc", 400)
	get(t, srv, "/v1/assets/covered?max_age=0", 400)
	get(t, srv, "/v1/assets/covered?offset=-1", 400)
}

func TestAssetDisputes(t *testing.T) {
	resolvedLedger := uint32(160)
	resolvedAt := now.Add(-time.Hour)
	f := &fake{
		assets: []coverage.Asset{{SAC: sac, Code: "PUSD", Issuer: "GISS"}},
		disputes: map[string][]coverage.Dispute{
			sac: {
				{SAC: sac, Reporter: "GISS", Disputer: "GWATCH", Status: "open",
					ReportAsOf: now.Add(-3 * time.Hour), HoldUntilLedger: 200,
					OpenedLedger: 150, OpenedAt: now.Add(-2 * time.Hour), TxHash: "tx-open"},
				{SAC: sac, Reporter: "GISS", Disputer: "GWATCH", Status: "resolved",
					Resolution: "upheld", ReportAsOf: now.Add(-6 * time.Hour), HoldUntilLedger: 190,
					OpenedLedger: 140, OpenedAt: now.Add(-3 * time.Hour),
					ResolvedLedger: &resolvedLedger, ResolvedAt: &resolvedAt, TxHash: "tx-resolved"},
			},
		},
	}
	srv := serverFor(f)
	defer srv.Close()

	all := get(t, srv, "/v1/assets/"+sac+"/disputes", 200)["disputes"].([]any)
	if len(all) != 2 {
		t.Fatalf("disputes = %d, want 2", len(all))
	}
	open := get(t, srv, "/v1/assets/"+sac+"/disputes?status=open", 200)["disputes"].([]any)
	if len(open) != 1 || open[0].(map[string]any)["status"] != "open" {
		t.Fatalf("open disputes = %+v, want one open", open)
	}
	if open[0].(map[string]any)["disputer"] != "GWATCH" || open[0].(map[string]any)["reporter"] != "GISS" {
		t.Errorf("dispute actors missing: %+v", open[0])
	}
	resolved := get(t, srv, "/v1/assets/"+sac+"/disputes?status=resolved", 200)["disputes"].([]any)
	if len(resolved) != 1 || resolved[0].(map[string]any)["resolution"] != "upheld" {
		t.Fatalf("resolved disputes = %+v, want the upheld one", resolved)
	}

	page := get(t, srv, "/v1/assets/"+sac+"/disputes?limit=1", 200)
	if len(page["disputes"].([]any)) != 1 || page["next_offset"].(float64) != 1 {
		t.Fatalf("first page = %+v, want one dispute and next_offset 1", page)
	}
	if body := get(t, srv, "/v1/assets/"+sac+"/disputes?status=pending", 400); body["code"] != "invalid_status" {
		t.Fatalf("invalid status body = %+v", body)
	}
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

func TestOpenAPISpec(t *testing.T) {
	srv := server()
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/openapi.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), "openapi: 3.1.0") {
		t.Fatalf("expected openapi 3.1.0 spec, got: %s", string(body))
	}
	if !strings.Contains(string(body), "operationId: headAsset") {
		t.Error("expected the spec to document the HEAD asset operation")
	}
}

func TestWebhookSecretReturnedOnlyAtCreation(t *testing.T) {
	manager := &webhookManagerFake{}
	codec, err := webhook.NewSecretCodec(base64.StdEncoding.EncodeToString(make([]byte, 32)))
	if err != nil {
		t.Fatal(err)
	}
	srv := serverWithWebhooks(manager, codec, "admin-token")
	defer srv.Close()

	request := func(method, path, body string, authorized bool) *http.Response {
		t.Helper()
		req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		if authorized {
			req.Header.Set("Authorization", "Bearer admin-token")
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	unauthorized := request(http.MethodPost, "/v1/webhooks", `{"url":"https://receiver.example/hook"}`, false)
	var unauthorizedBody errorBody
	if err := json.NewDecoder(unauthorized.Body).Decode(&unauthorizedBody); err != nil {
		t.Fatal(err)
	}
	unauthorized.Body.Close()
	if unauthorized.StatusCode != http.StatusUnauthorized || unauthorizedBody.Code != "unauthorized" {
		t.Fatalf("unauthorized create = %d %+v, want 401 unauthorized", unauthorized.StatusCode, unauthorizedBody)
	}

	created := request(http.MethodPost, "/v1/webhooks", `{"url":"https://receiver.example/hook"}`, true)
	defer created.Body.Close()
	if created.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", created.StatusCode)
	}
	var response struct {
		ID     int64  `json:"id"`
		Secret string `json:"secret"`
	}
	if err := json.NewDecoder(created.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Secret == "" || len(manager.webhooks) != 1 {
		t.Fatalf("creation response %q, stored webhooks %d", response.Secret, len(manager.webhooks))
	}
	stored := manager.webhooks[0]
	if stored.SecretHash != webhook.SecretHash(response.Secret) {
		t.Fatal("stored secret hash does not match the one-time returned secret")
	}
	decrypted, err := codec.Decrypt(stored.SecretCiphertext)
	if err != nil || decrypted != response.Secret {
		t.Fatalf("stored signing secret = %q, %v", decrypted, err)
	}

	list := request(http.MethodGet, "/v1/webhooks", "", true)
	defer list.Body.Close()
	listBody, err := io.ReadAll(list.Body)
	if err != nil {
		t.Fatal(err)
	}
	if list.StatusCode != http.StatusOK || strings.Contains(string(listBody), response.Secret) ||
		strings.Contains(string(listBody), "secret_hash") || strings.Contains(string(listBody), "secret_ciphertext") {
		t.Fatalf("list response disclosed secret or failed: status=%d body=%s", list.StatusCode, listBody)
	}
}
