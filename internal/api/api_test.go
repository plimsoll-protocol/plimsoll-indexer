package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stellar/go-stellar-sdk/strkey"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/metrics"
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
	assets           []coverage.Asset
	supply           map[string]*coverage.Supply
	reports          map[string]map[uint32]coverage.Report
	bodies           map[string][]byte
	batchSupplyCalls int
	batchReportCalls int
	exportPages      [][]store.ReportExportRow
	exportCalls      int
	exportFrom       *time.Time
	exportTo         *time.Time
	exportCursors    []*store.ReportExportCursor
	progress         map[string]store.Progress
	attestation      map[string]store.Attestation
	created          []coverage.Webhook
	deleted          []int64
	assets  []coverage.Asset
	supply  map[string]*coverage.Supply
	reports map[string]map[uint32]coverage.Report
	bodies  map[string][]byte
	// stored is what EventsSince replays, as if it had been ingested.
	stored []any
	assets             []coverage.Asset
	supply             map[string]*coverage.Supply
	reports            map[string]map[uint32]coverage.Report
	bodies             map[string][]byte
	getAssetCalls      int
	supplyHistoryCalls int
	reportHistoryCalls int
}

func (f *fake) Ping(context.Context) error { return nil }
func (f *fake) ListAssets(context.Context) ([]coverage.Asset, error) {
	return f.assets, nil
}
func (f *fake) SearchAssets(_ context.Context, q string) ([]coverage.Asset, error) {
	var out []coverage.Asset
	qLower := strings.ToLower(q)
	for _, a := range f.assets {
		if strings.Contains(strings.ToLower(a.Code), qLower) ||
			strings.Contains(strings.ToLower(a.Issuer), qLower) {
			out = append(out, a)
		}
	}
	if len(out) > 20 {
		out = out[:20]
	}
	return out, nil
}
func (f *fake) GetAsset(_ context.Context, s string) (coverage.Asset, error) {
	f.getAssetCalls++
	for _, a := range f.assets {
		if a.SAC == s {
			return a, nil
		}
	}
	return coverage.Asset{}, store.ErrNotFound
}
func (f *fake) LatestSupplies(_ context.Context, sacs []string) (map[string]*coverage.Supply, error) {
	f.batchSupplyCalls++
	out := make(map[string]*coverage.Supply)
	for _, s := range sacs {
		if sp := f.supply[s]; sp != nil {
			out[s] = sp
		}
	}
	return out, nil
}
func (f *fake) SupplyHistory(_ context.Context, s string, _ int) ([]coverage.Supply, error) {
	f.supplyHistoryCalls++
	if sp := f.supply[s]; sp != nil {
		return []coverage.Supply{*sp}, nil
	}
	return nil, nil
}
func (f *fake) LatestReportsBySAC(_ context.Context, sacs []string) (map[string]map[uint32]coverage.Report, error) {
	f.batchReportCalls++
	out := make(map[string]map[uint32]coverage.Report)
	for _, s := range sacs {
		if r := f.reports[s]; r != nil {
			out[s] = r
		}
func (f *fake) LatestReports(_ context.Context, s string) (map[uint32]coverage.Report, error) {
	if r := f.reports[s]; r != nil {
		return r, nil
	}
	return map[uint32]coverage.Report{}, nil
}
func (f *fake) ReportHistory(_ context.Context, s string, _ int) ([]coverage.Report, error) {
	f.reportHistoryCalls++
	var out []coverage.Report
	for _, r := range f.reports[s] {
		out = append(out, r)
	}
	return out, nil
}
func (f *fake) LatestReports(_ context.Context, s string) (map[uint32]coverage.Report, error) {
	out := make(map[uint32]coverage.Report)
	for _, rep := range f.reports[s] {
		if cur, ok := out[rep.Tier]; !ok || rep.AsOf.After(cur.AsOf) {
			out[rep.Tier] = rep.Report
		}
	}
	return out, nil
}
func (f *fake) ReportExportPage(_ context.Context, _ string, from, to *time.Time, after *store.ReportExportCursor, _ int) ([]store.ReportExportRow, error) {
	if f.exportCalls == 0 {
		f.exportFrom, f.exportTo = from, to
	}
	f.exportCalls++
	f.exportCursors = append(f.exportCursors, after)
	pageIndex := f.exportCalls - 1
	if pageIndex >= len(f.exportPages) {
		return nil, nil
	}
	return f.exportPages[pageIndex], nil
}
func (f *fake) LatestSupply(_ context.Context, sac string) (*coverage.Supply, error) {
	return f.supply[sac], nil
}

func (f *fake) LatestReports(_ context.Context, sac string) (map[uint32]coverage.Report, error) {
	return f.reports[sac], nil
}

func (f *fake) Reporters(context.Context) ([]coverage.Reporter, error) {
	websiteURI, contactHash := "https://acme.example", "sha256:contact"
	return []coverage.Reporter{{
		Address: "GAUD", Role: coverage.RoleAuditor, Name: "Acme", DisplayName: "Acme Inc.",
		WebsiteURI: &websiteURI, ContactHash: &contactHash,
	}}, nil
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
func (f *fake) Progress(_ context.Context, name string) (store.Progress, error) {
	if p, ok := f.progress[name]; ok {
		return p, nil
	}
	return store.Progress{}, nil
}

func (f *fake) CreateCoverageWebhook(_ context.Context, w coverage.Webhook) (coverage.Webhook, error) {
	w.ID = int64(len(f.created) + 1)
	w.CreatedAt = now
	f.created = append(f.created, w)
	return w, nil
}
func (f *fake) DeleteCoverageWebhook(_ context.Context, id int64) error {
	for _, w := range f.created {
		if w.ID == id {
			f.deleted = append(f.deleted, id)
			return nil
		}
	}
	return store.ErrNotFound
}
func (f *fake) Attestation(_ context.Context, s string) (store.Attestation, error) {
	if a, ok := f.attestation[s]; ok {
		return a, nil
	}
	return store.Attestation{}, store.ErrNotFound
}

func TestHealthAggregatesContractStreamProgress(t *testing.T) {
	f := &fake{
		progress: map[string]store.Progress{
			store.LegacyIngestCursorName: {
				ScannedLedger: 90,
				EventLedger:   80,
				EventAt:       now.Add(-time.Hour),
			},
			store.ContractIngestCursorName("coverage"): {
				ScannedLedger: 120,
				EventLedger:   100,
				EventAt:       now.Add(-2 * time.Minute),
			},
			store.ContractIngestCursorName("registry"): {
				ScannedLedger: 110,
				EventLedger:   105,
				EventAt:       now.Add(-time.Minute),
			},
		},
	}
	srv := httptest.NewServer((&Server{
		Store: f,
		Network: NetworkInfo{
			CoverageLedgerID:   "coverage",
			ReporterRegistryID: "registry",
		},
		Now: func() time.Time { return now },
	}).Handler())
	defer srv.Close()

	body := get(t, srv, "/healthz", http.StatusOK)
	if body["ingested_to_ledger"].(float64) != 110 {
		t.Errorf("ingested_to_ledger = %v, want lagging stream ledger 110", body["ingested_to_ledger"])
	}
	if body["last_event_ledger"].(float64) != 105 {
		t.Errorf("last_event_ledger = %v, want 105", body["last_event_ledger"])
	}
	if body["last_event_age_seconds"].(float64) != 60 {
		t.Errorf("last_event_age_seconds = %v, want 60", body["last_event_age_seconds"])
	}
}

func (f *fake) EventsSince(_ context.Context, after string, limit int) ([]any, error) {
	var out []any
	for _, ev := range f.stored {
		meta, ok := ev.(soroban.WithMeta)
		if !ok || meta.EventMeta().ID <= after {
			continue
		}
		out = append(out, ev)
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

// newFake builds the fixture the handlers are tested against: one listed asset
// with one supply snapshot and one report.
func newFake() *fake {
	return &fake{
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
}

type webhookManagerFake struct {
	webhooks []store.Webhook
}

func (f *webhookManagerFake) CreateWebhook(_ context.Context, url, hash string, ciphertext []byte, ownerKeyHash string) (store.Webhook, error) {
	owned := 0
	for _, item := range f.webhooks {
		if item.OwnerKeyHash == ownerKeyHash {
			owned++
		}
	}
	if owned >= store.MaxWebhooksPerKey {
		return store.Webhook{}, store.ErrWebhookLimit
	}
	item := store.Webhook{
		ID: int64(len(f.webhooks) + 1), URL: url, SecretHash: hash,
		SecretCiphertext: ciphertext, OwnerKeyHash: ownerKeyHash, CreatedAt: now,
	}
	f.webhooks = append(f.webhooks, item)
	return item, nil
}

func (f *webhookManagerFake) ListWebhooks(context.Context) ([]store.Webhook, error) {
	return f.webhooks, nil
}

func (f *webhookManagerFake) ListWebhooksForKey(_ context.Context, ownerKeyHash string) ([]store.Webhook, error) {
	out := make([]store.Webhook, 0, len(f.webhooks))
	for _, item := range f.webhooks {
		if item.OwnerKeyHash == ownerKeyHash || item.OwnerKeyHash == "" {
			out = append(out, item)
		}
	}
	return out, nil
}

func (f *webhookManagerFake) DeleteWebhook(_ context.Context, id int64, ownerKeyHash string, allowLegacyAdmin bool) error {
	for i, item := range f.webhooks {
		if item.ID == id {
			if item.OwnerKeyHash != ownerKeyHash && !(item.OwnerKeyHash == "" && allowLegacyAdmin) {
				return store.ErrForbidden
			}
			f.webhooks = append(f.webhooks[:i], f.webhooks[i+1:]...)
			return nil
		}
	}
	return store.ErrNotFound
}

func server() *httptest.Server {
	return serverWithWebhooks(nil, nil, "")
}

func newFake() *fake {
	f := &fake{
		assets: []coverage.Asset{{SAC: sac, Code: "PUSD", Issuer: "GISS"}},
		supply: map[string][]coverage.Supply{
			sac: {
				{Amount: big.NewInt(1_000_0000000), Ledger: 100, PostedAt: supplyNow},
				{Amount: big.NewInt(9_00000000), Ledger: 90, PostedAt: supplyNow.Add(-time.Hour)},
				{Amount: big.NewInt(8_00000000), Ledger: 80, PostedAt: supplyNow.Add(-2 * time.Hour)},
				{Amount: big.NewInt(7_00000000), Ledger: 70, PostedAt: supplyNow.Add(-3 * time.Hour)},
			},
		},
		reports: map[string][]fakeReport{
			sac: {
				{Report: coverage.Report{Tier: coverage.TierIssuerSigned, Amount: big.NewInt(1_020_0000000),
					AsOf: now.Add(-2 * time.Hour), PostedAt: now.Add(-2 * time.Hour), Reporter: "GISS", DocURI: "https://x"},
					PostedLedger: 50},
				{Report: coverage.Report{Tier: coverage.TierIssuerSigned, Amount: big.NewInt(1_010_0000000),
					AsOf: now.Add(-4 * time.Hour), PostedAt: now.Add(-4 * time.Hour), Reporter: "GISS", DocURI: "https://x"},
					PostedLedger: 40},
				{Report: coverage.Report{Tier: coverage.TierTranscribed, Amount: big.NewInt(1_005_0000000),
					AsOf: now.Add(-6 * time.Hour), PostedAt: now.Add(-6 * time.Hour), Reporter: "GISS", DocURI: "https://x"},
					PostedLedger: 30},
			},
		},
		bodies: map[string][]byte{breakdownHash: []byte(`{"total":"1"}`)},
		progress: map[string]store.Progress{
			store.LegacyIngestCursorName: {
				ScannedLedger: 42,
				EventLedger:   40,
				EventAt:       now.Add(-90 * time.Second),
			},
		},
		attestation: map[string]store.Attestation{
			sac: {
				Hash:        "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08",
				FirstSeenAt: now.Add(-48 * time.Hour),
				ChangedAt:   &now,
			},
		},
	}
	return f
}

// coverageServer enables the coverage alert administration endpoints, which
// need the admin token and the shared signing secret.
func coverageServer(token string) *httptest.Server {
	f := newFake()
	s := &Server{
		Store:                f,
		CoverageWebhooks:     f,
		WebhookAdminToken:    token,
		WebhookSigningSecret: "test-signing-secret",
		Log:                  slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:                  func() time.Time { return now },
	}
	return httptest.NewServer(s.Handler())
}

func serverWithWebhooks(manager WebhookManager, codec *webhook.SecretCodec, token string) *httptest.Server {
	f := newFake()
	return serverWithFake(f, manager, codec, token)
}

func serverWithFake(f Reader, manager WebhookManager, codec *webhook.SecretCodec, token string) *httptest.Server {
	s := &Server{
		Store:             f,
		WebhookStore:      manager,
		WebhookCodec:      codec,
		WebhookAdminToken: token,
		WebhookAPIKeys:    []string{"owner-token", "other-owner-token"},
		CORS:              []string{"*"},
		Log:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:               func() time.Time { return now },
		Changelog:         testChangelog,
	}
	return httptest.NewServer(s.Handler())
}

// serverFor runs the API over a specific store, so a test can vary the supply
// and reports behind an asset.
func serverFor(f *fake) *httptest.Server {
	s := &Server{
		Store:     f,
		CORS:      []string{"*"},
		Log:       slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:       func() time.Time { return now },
		Changelog: testChangelog,
	}
	return httptest.NewServer(s.Handler())
}

func (m mockAPIStore) GetAssetReporters(ctx context.Context, sac string) ([]store.Reporter, error) {
	return m.reporters, nil
}

func TestGetAssetCoverage(t *testing.T) {
	srv := server()
	defer srv.Close()
	body := get(t, srv, "/v1/assets/"+sac, 200)
	for _, field := range []string{"toml_checked_at", "toml_error"} {
		value, ok := body[field]
		if !ok || value != nil {
			t.Errorf("%s = %v (present: %t), want null", field, value, ok)
		}
	}
	// The issuer's attestation document is folded into the stored SEP-1 object.
	toml := body["toml"].(map[string]any)
	if toml["attestation_hash"] != "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08" ||
		toml["attestation_first_seen_at"] != now.Add(-48*time.Hour).Format(time.RFC3339) ||
		toml["attestation_changed_at"] != now.Format(time.RFC3339) {
		t.Errorf("toml attestation = %+v", toml)
	}
	report := body["reports"].(map[string]any)["issuer_signed"].(map[string]any)
	if value, ok := report["doc_check"]; !ok || value != nil {
		t.Errorf("doc_check = %v (present: %t), want null", value, ok)
	}
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
	return store.Webhook{}, store.ErrNotFound
}
func (m mockAPIStore) StreamAuditTrail(ctx context.Context, fn func(store.AuditRecord) error) error {
	for _, r := range m.audit {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}
func (m mockAPIStore) StreamCSVExports(ctx context.Context, fn func(store.ExportRecord) error) error {
	return nil
}

func TestListAndMisc(t *testing.T) {
	srv := server()
	defer srv.Close()
	list := get(t, srv, "/v1/assets", 200)
	if len(list["assets"].([]any)) != 1 {
		t.Fatal("expected one asset")
	}

	// Search endpoint tests
	get(t, srv, "/v1/assets/search", 400)
	get(t, srv, "/v1/assets/search?q=", 400)
	get(t, srv, "/v1/assets/search?q=   ", 400)

	res := get(t, srv, "/v1/assets/search?q=pusd", 200)
	results := res["assets"].([]any)
	if len(results) != 1 {
		t.Fatalf("want 1 search result, got %d", len(results))
	}

	resNone := get(t, srv, "/v1/assets/search?q=nonexistent", 200)
	if len(resNone["assets"].([]any)) != 0 {
		t.Fatalf("want 0 search results, got %d", len(resNone["assets"].([]any)))
	}
	get(t, srv, "/v1/assets/CNOPE", 400)
	rep := get(t, srv, "/v1/reporters", 200)
	reporter := rep["reporters"].([]any)[0].(map[string]any)
	if reporter["role"] != "auditor" || reporter["display_name"] != "Acme Inc." ||
		reporter["website_uri"] != "https://acme.example" || reporter["contact_hash"] != "sha256:contact" {
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
	s := &Server{Store: mockAPIStore{reporters: rep}}
	r := chi.NewRouter()
	r.Get("/v1/assets/{sac}/reporters", s.ListAssetReporters)

	req := httptest.NewRequest(http.MethodGet, "/v1/assets/SAC1/reporters", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		Fatalf := t.Fatalf
		Fatalf("status = %d", rec.Code)
	}
}

func TestStreamAuditNDJSON(t *testing.T) {
	s := &Server{Store: mockAPIStore{audit: []store.AuditRecord{{ID: 1, Action: "test", Timestamp: "now"}}}}
	r := chi.NewRouter()
	r.Get("/v1/audit", s.StreamAudit)

	req := httptest.NewRequest(http.MethodGet, "/v1/audit", nil)
	req.Header.Set("Accept", "application/x-ndjson")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	get(t, srv, "/v1/assets?limit=0", 400)
	get(t, srv, "/v1/assets?limit=201", 400)
	get(t, srv, "/v1/assets?limit=nope", 400)
	get(t, srv, "/v1/assets?cursor=not-a-cursor", 400)
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

func TestMalformedHistorySACDoesNotQueryStore(t *testing.T) {
	f := &fake{}
	srv := serverWithFake(f, nil, nil, "")
	defer srv.Close()

	for _, path := range []string{
		"/v1/assets/" + strings.Repeat("a", 63) + "/supply",
		"/v1/assets/" + strings.Repeat("a", 63) + "/reports",
	} {
		body := get(t, srv, path, 400)
		if body["error"] != "sac must be a 32-byte contract strkey" {
			t.Errorf("%s: unexpected error: %v", path, body["error"])
		}
	}
	if f.getAssetCalls != 0 || f.supplyHistoryCalls != 0 || f.reportHistoryCalls != 0 {
		t.Fatalf("malformed SAC reached store: GetAsset=%d SupplyHistory=%d ReportHistory=%d",
			f.getAssetCalls, f.supplyHistoryCalls, f.reportHistoryCalls)
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

func TestReportExportStreamsPagesAndAppliesDateFilters(t *testing.T) {
	f := &fake{
		assets: []coverage.Asset{{SAC: sac}},
		exportPages: [][]store.ReportExportRow{
			makeExportRows(500),
			makeExportRows(1),
		},
	}
	f.exportPages[0][len(f.exportPages[0])-1].Cursor = store.ReportExportCursor{Ledger: 599, EventID: "event-599"}
	f.exportPages[1][0].Cursor = store.ReportExportCursor{Ledger: 600, EventID: "event-600"}
	srv := httptest.NewServer((&Server{
		Store: f,
		Log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}).Handler())
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/v1/assets/" + sac + "/reports/export.csv?from=2026-10-01&to=2026-10-05")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Disposition"); got != `attachment; filename="reports-`+sac+`.csv"` {
		t.Fatalf("Content-Disposition = %q", got)
	}
	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/csv") {
		t.Fatalf("Content-Type = %q", got)
	}
	records, err := csv.NewReader(resp.Body).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	// One header and every row of both pages.
	if len(records) != 502 || strings.Join(records[0], ",") != "ledger,timestamp,reporter,bps,tier,doc_uri" {
		t.Fatalf("CSV has %d records and header %v", len(records), records[0])
	}
	if f.exportCalls != 2 || f.exportCursors[1] == nil ||
		f.exportCursors[1].Ledger != 599 || f.exportCursors[1].EventID != "event-599" {
		t.Fatalf("page calls/cursor = %d / %+v", f.exportCalls, f.exportCursors)
	}
	// to is exclusive, so to=2026-10-05 asks for everything before 2026-10-06.
	if f.exportFrom == nil || !f.exportFrom.Equal(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)) ||
		f.exportTo == nil || !f.exportTo.Equal(time.Date(2026, 10, 6, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("date bounds = %v / %v", f.exportFrom, f.exportTo)
	}
}

// adminRequest sends a webhook-administration request and returns the status
// and decoded body (nil for 204).
func adminRequest(t *testing.T, srv *httptest.Server, method, path, token, body string) (int, map[string]any) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, srv.URL+path, r)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return resp.StatusCode, nil
	}
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func makeExportRows(n int) []store.ReportExportRow {
	rows := make([]store.ReportExportRow, n)
	bps := uint32(10_200)
	for i := range rows {
		rows[i] = store.ReportExportRow{
			Ledger: uint32(100 + i), Timestamp: now.Add(-2 * time.Hour),
			Reporter: "GREPORTER", Bps: &bps,
			Tier: coverage.TierIssuerSigned, DocURI: "https://example.test/report",
			Cursor: store.ReportExportCursor{Ledger: int64(100 + i), EventID: "event"},
		}
	}
	return rows
}

// adminRequest sends a webhook-administration request and returns the status
// and decoded body (nil for 204).
func adminRequest(t *testing.T, srv *httptest.Server, method, path, token, body string) (int, map[string]any) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, srv.URL+path, r)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return resp.StatusCode, nil
	}
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestWebhookAdminEndpoints(t *testing.T) {
	srv := coverageServer("test-admin")
	defer srv.Close()

	valid := `{"url":"https://integrator.example/hook","sac":"CSAC","min_bps":9500,"min_tier":2}`
	if got, _ := adminRequest(t, srv, http.MethodPost, "/v1/coverage-webhooks", "", valid); got != http.StatusUnauthorized {
		t.Fatalf("no token: status %d, want 401", got)
	}
	if got, _ := adminRequest(t, srv, http.MethodPost, "/v1/coverage-webhooks", "wrong", valid); got != http.StatusUnauthorized {
		t.Fatalf("wrong token: status %d, want 401", got)
	}

	status, body := adminRequest(t, srv, http.MethodPost, "/v1/coverage-webhooks", "test-admin", valid)
	if status != http.StatusCreated {
		t.Fatalf("create: status %d (%v), want 201", status, body)
	}
	if body["id"].(float64) != 1 || body["url"] != "https://integrator.example/hook" ||
		body["sac"] != "CSAC" || body["min_bps"].(float64) != 9500 || body["min_tier"].(float64) != 2 {
		t.Fatalf("created webhook = %+v", body)
	}
	if body["created_at"] == nil {
		t.Fatal("created webhook has no created_at")
	}

	for name, payload := range map[string]string{
		"non-http url": `{"url":"ftp://integrator.example/hook","sac":"CSAC","min_bps":9500,"min_tier":2}`,
		"no sac":       `{"url":"https://integrator.example/hook","min_bps":9500,"min_tier":2}`,
		"bad bps":      `{"url":"https://integrator.example/hook","sac":"CSAC","min_bps":0,"min_tier":2}`,
		"bad tier":     `{"url":"https://integrator.example/hook","sac":"CSAC","min_bps":9500,"min_tier":4}`,
		"not json":     `nope`,
	} {
		if got, _ := adminRequest(t, srv, http.MethodPost, "/v1/coverage-webhooks", "test-admin", payload); got != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", name, got)
		}
	}

	if got, _ := adminRequest(t, srv, http.MethodDelete, "/v1/coverage-webhooks/1", "test-admin", ""); got != http.StatusNoContent {
		t.Fatalf("delete: status %d, want 204", got)
	}
	if got, _ := adminRequest(t, srv, http.MethodDelete, "/v1/coverage-webhooks/99", "test-admin", ""); got != http.StatusNotFound {
		t.Fatalf("delete unknown: status %d, want 404", got)
	}
	if got, _ := adminRequest(t, srv, http.MethodDelete, "/v1/coverage-webhooks/abc", "test-admin", ""); got != http.StatusBadRequest {
		t.Fatalf("delete non-numeric: status %d, want 400", got)
	}
}

func TestWebhookPerKeyLimitAndScopedListing(t *testing.T) {
	manager := &webhookManagerFake{}
	codec, err := webhook.NewSecretCodec(base64.StdEncoding.EncodeToString(make([]byte, 32)))
	if err != nil {
		t.Fatal(err)
	}
	srv := serverWithWebhooks(manager, codec, "admin-token")
	defer srv.Close()

	post := func(token, url string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/webhooks", strings.NewReader(`{"url":"`+url+`"}`))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	for i := range store.MaxWebhooksPerKey {
		resp := post("owner-token", "https://receiver.example/"+strconv.Itoa(i))
		resp.Body.Close()
		if resp.StatusCode != http.StatusCreated {
			t.Fatalf("endpoint %d: status %d, want 201", i, resp.StatusCode)
		}
	}
	over := post("owner-token", "https://receiver.example/over")
	var overBody errorBody
	if err := json.NewDecoder(over.Body).Decode(&overBody); err != nil {
		t.Fatal(err)
	}
	over.Body.Close()
	if over.StatusCode != http.StatusUnprocessableEntity || overBody.Code != "webhook_limit_reached" {
		t.Fatalf("over the limit = %d %+v, want 422 webhook_limit_reached", over.StatusCode, overBody)
	}
	other := post("other-owner-token", "https://receiver.example/other")
	other.Body.Close()
	if other.StatusCode != http.StatusCreated {
		t.Fatalf("second key: status %d, want 201", other.StatusCode)
	}

	list := func(token string) []map[string]any {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, srv.URL+"/v1/webhooks", nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var body struct {
			Webhooks []map[string]any `json:"webhooks"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return body.Webhooks
	}

	mine := list("owner-token")
	if len(mine) != store.MaxWebhooksPerKey {
		t.Fatalf("owner lists %d endpoints, want %d", len(mine), store.MaxWebhooksPerKey)
	}
	for _, w := range mine {
		if w["url"] == "https://receiver.example/other" {
			t.Fatal("a key listed another key's endpoint")
		}
		if _, ok := w["delivery"]; !ok {
			t.Fatalf("endpoint %v has no delivery state", w["url"])
		}
	}
	if all := list("admin-token"); len(all) != store.MaxWebhooksPerKey+1 {
		t.Fatalf("admin lists %d endpoints, want %d", len(all), store.MaxWebhooksPerKey+1)
	}
}

func TestWebhookAdminDisabled(t *testing.T) {
	s := &Server{
		Store: &fake{},
		Log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:   func() time.Time { return now },
	}
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()
	status, body := adminRequest(t, srv, http.MethodPost, "/v1/coverage-webhooks", "anything", `{}`)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("disabled administration: status %d (%v), want 503", status, body)
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
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode, resp.Header
}

func hasVaryOrigin(header http.Header) bool {
	for _, v := range header.Values("Vary") {
		for _, part := range strings.Split(v, ",") {
			if strings.TrimSpace(part) == "Origin" {
				return true
			}
		}
	}
	return false
}

func TestCORSWildcardOrigin(t *testing.T) {
	srv := server()
	defer srv.Close()
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		_, header := corsRequest(t, srv, method, "/v1/assets", "https://plimsoll.example")
		if got := header.Get("Access-Control-Allow-Origin"); got != "*" {
			t.Fatalf("%s: Access-Control-Allow-Origin = %q, want *", method, got)
		}
	}
	status, header := corsRequest(t, srv, http.MethodOptions, "/v1/assets", "https://plimsoll.example")
	if status != http.StatusNoContent {
		t.Fatalf("preflight status %d", status)
	}
	if header.Get("Access-Control-Allow-Headers") == "" || header.Get("Access-Control-Max-Age") == "" {
		t.Fatalf("preflight missing allow-headers (%q) or max-age (%q)",
			header.Get("Access-Control-Allow-Headers"), header.Get("Access-Control-Max-Age"))
	}
}

func TestCORSAllowedOrigin(t *testing.T) {
	srv := serverWithCORS("https://app.example")
	defer srv.Close()
	status, header := corsRequest(t, srv, http.MethodGet, "/v1/assets", "https://app.example")
	if status != http.StatusOK {
		t.Fatalf("status %d", status)
	}
	if got := header.Get("Access-Control-Allow-Origin"); got != "https://app.example" {
		t.Fatalf("Access-Control-Allow-Origin = %q", got)
	}
	if !hasVaryOrigin(header) {
		t.Fatalf("Vary = %q, want Origin", header.Values("Vary"))
	}
}

func TestCORSDisallowedOrigin(t *testing.T) {
	srv := serverWithCORS("https://app.example")
	defer srv.Close()
	for _, method := range []string{http.MethodGet, http.MethodOptions} {
		status, header := corsRequest(t, srv, method, "/v1/assets", "https://evil.example")
		// The response is still served; the browser is what refuses it.
		if method == http.MethodGet && status != http.StatusOK {
			t.Fatalf("status %d", status)
		}
		if got := header.Get("Access-Control-Allow-Origin"); got != "" {
			t.Fatalf("%s: Access-Control-Allow-Origin = %q, want none", method, got)
		}
		// Without Vary a cache could serve this refused answer to an
		// allowed origin.
		if !hasVaryOrigin(header) {
			t.Fatalf("%s: Vary = %q, want Origin", method, header.Values("Vary"))
		}
	}
}

func TestCORSPreflight(t *testing.T) {
	srv := serverWithCORS("https://app.example")
	defer srv.Close()
	status, header := corsRequest(t, srv, http.MethodOptions, "/v1/assets", "https://app.example")
	if status != http.StatusNoContent {
		t.Fatalf("status %d", status)
	}
	if got := header.Get("Access-Control-Allow-Origin"); got != "https://app.example" {
		t.Fatalf("Access-Control-Allow-Origin = %q", got)
	}
	if got := header.Get("Access-Control-Allow-Headers"); !strings.Contains(got, "Content-Type") {
		t.Fatalf("Access-Control-Allow-Headers = %q", got)
	}
	if got := header.Get("Access-Control-Max-Age"); got != "600" {
		t.Fatalf("Access-Control-Max-Age = %q", got)
	}
	if got := header.Get("Access-Control-Allow-Methods"); !strings.Contains(got, http.MethodGet) {
		t.Fatalf("Access-Control-Allow-Methods = %q", got)
	}
}

func TestVersionEndpoint(t *testing.T) {
	srv := server()
	defer srv.Close()
	body := get(t, srv, "/v1/version", 200)
	if body["version"] != "v1.2.3" || body["commit"] != "abc123" || body["go"] != "go1.27" {
		t.Fatalf("version %+v", body)
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
	if !strings.Contains(string(body), "operationId: getCoverageNow") ||
		!strings.Contains(string(body), "CoverageNow:") {
		t.Error("expected the spec to document the current coverage endpoint and its schema")
	}
}

// unreadable is a store whose amount reads fail, as the real store's do when a
// stored NUMERIC does not parse.
type unreadable struct{ *fake }

var errUnreadable = errors.New(`supply snapshot at ledger 100: amount "NaN" is not an integer`)

func (unreadable) LatestSupplies(context.Context, []string) (map[string]*coverage.Supply, error) {
	return nil, errUnreadable
}

func (unreadable) LatestSupplies(context.Context, []string) (map[string]*coverage.Supply, error) {
	return nil, errUnreadable
}
func (unreadable) SupplyHistory(context.Context, string, int) ([]coverage.Supply, error) {
	return nil, errUnreadable
}
func (unreadable) LatestReportsBySAC(context.Context, []string) (map[string]map[uint32]coverage.Report, error) {
	return nil, errUnreadable
}
func (unreadable) LatestReportsBySAC(context.Context, []string) (map[string]map[uint32]coverage.Report, error) {
	return nil, errUnreadable
}
func (unreadable) ReportHistory(context.Context, string, int) ([]coverage.Report, error) {
	return nil, errUnreadable
}

// An amount the store cannot read fails the request instead of being served,
// so no endpoint reports a corrupted figure as zero supply or zero reserves.
func TestUnreadableAmountsFailTheRequest(t *testing.T) {
	s := &Server{
		Store: unreadable{&fake{assets: []coverage.Asset{{SAC: sac, Code: "PUSD", Issuer: "GISS"}}}},
		Log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:   func() time.Time { return now },
	}
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	for _, path := range []string{
		"/v1/assets",
		"/v1/assets/" + sac,
		"/v1/assets/" + sac + "/supply",
		"/v1/assets/" + sac + "/reports",
	} {
		body := get(t, srv, path, http.StatusInternalServerError)
		if body["supply"] != nil || body["coverage"] != nil {
			t.Errorf("%s served figures alongside the error: %+v", path, body)
		}
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

	request := func(method, path, body, token string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	unauthorized := request(http.MethodPost, "/v1/webhooks", `{"url":"https://receiver.example/hook"}`, "")
	var unauthorizedBody errorBody
	if err := json.NewDecoder(unauthorized.Body).Decode(&unauthorizedBody); err != nil {
		t.Fatal(err)
	}
	unauthorized.Body.Close()
	if unauthorized.StatusCode != http.StatusUnauthorized || unauthorizedBody.Code != "unauthorized" {
		t.Fatalf("unauthorized create = %d %+v, want 401 unauthorized", unauthorized.StatusCode, unauthorizedBody)
	}

	created := request(http.MethodPost, "/v1/webhooks", `{"url":"https://receiver.example/hook"}`, "owner-token")
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
	if stored.OwnerKeyHash != webhook.SecretHash("owner-token") {
		t.Fatal("stored owner key hash does not match the creating API key")
	}
	decrypted, err := codec.Decrypt(stored.SecretCiphertext)
	if err != nil || decrypted != response.Secret {
		t.Fatalf("stored signing secret = %q, %v", decrypted, err)
	}

	list := request(http.MethodGet, "/v1/webhooks", "", "admin-token")
	defer list.Body.Close()
	listBody, err := io.ReadAll(list.Body)
	if err != nil {
		t.Fatal(err)
	}
	if list.StatusCode != http.StatusOK || strings.Contains(string(listBody), response.Secret) ||
		strings.Contains(string(listBody), "secret_hash") || strings.Contains(string(listBody), "secret_ciphertext") {
		t.Fatalf("list response disclosed secret or failed: status=%d body=%s", list.StatusCode, listBody)
	}

	deleteWebhook := func(token, path string) *http.Response {
		t.Helper()
		return request(http.MethodDelete, path, "", token)
	}
	notOwner := deleteWebhook("other-owner-token", "/v1/webhooks/"+strconv.FormatInt(response.ID, 10))
	var forbidden errorBody
	if err := json.NewDecoder(notOwner.Body).Decode(&forbidden); err != nil {
		t.Fatal(err)
	}
	notOwner.Body.Close()
	if notOwner.StatusCode != http.StatusForbidden || forbidden.Code != "webhook_forbidden" {
		t.Fatalf("non-owner delete = %d %+v, want 403 webhook_forbidden", notOwner.StatusCode, forbidden)
	}

	unknown := deleteWebhook("owner-token", "/v1/webhooks/999")
	unknown.Body.Close()
	if unknown.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown webhook delete = %d, want 404", unknown.StatusCode)
	}

	deleted := deleteWebhook("owner-token", "/v1/webhooks/"+strconv.FormatInt(response.ID, 10))
	deleted.Body.Close()
	if deleted.StatusCode != http.StatusNoContent {
		t.Fatalf("owner delete = %d, want 204", deleted.StatusCode)
	}
	again := deleteWebhook("owner-token", "/v1/webhooks/"+strconv.FormatInt(response.ID, 10))
	var missing errorBody
	if err := json.NewDecoder(again.Body).Decode(&missing); err != nil {
		t.Fatal(err)
	}
	again.Body.Close()
	if again.StatusCode != http.StatusNotFound || missing.Code != "webhook_not_found" {
		t.Fatalf("repeated delete = %d %+v, want 404 webhook_not_found", again.StatusCode, missing)
	}
}

func TestDocsUI(t *testing.T) {
	srv := server()
	defer srv.Close()
	for _, path := range []string{"/docs", "/docs/"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s status %d want 200", path, resp.StatusCode)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), "swagger-ui") || !strings.Contains(string(body), "/openapi.yaml") {
			t.Fatalf("%s expected Swagger UI referencing /openapi.yaml, got: %s", path, string(body))
		}
	}
}

func TestMetricsEndpoint(t *testing.T) {
	srv := server()
	defer srv.Close()

	// Ensure vec metrics have at least one sample so they appear in /metrics output
	metrics.DBQueryDuration.WithLabelValues("ping").Observe(0.001)
	metrics.EventsIngestedTotal.WithLabelValues("asset_listed").Inc()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d want 200", resp.StatusCode)
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	body := string(bodyBytes)

	// Verify standard Go runtime metrics are exposed
	if !strings.Contains(body, "go_goroutines") && !strings.Contains(body, "go_info") {
		t.Error("expected Go runtime metrics in /metrics response")
	}

	// Verify custom metrics are present
	customMetrics := []string{
		"plimsoll_ingest_lag_ledgers",
		"plimsoll_reports_per_minute",
		"plimsoll_assets_active",
		"plimsoll_db_query_duration_seconds",
		"plimsoll_reports_ingested_total",
		"plimsoll_events_ingested_total",
		"plimsoll_ingest_last_event_age_seconds",
	}
	for _, m := range customMetrics {
		if !strings.Contains(body, m) {
			t.Errorf("expected metric %q in /metrics output", m)
		}
	}
}
