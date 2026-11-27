// Package api serves Plimsoll's read API over HTTP.
//
// @title Plimsoll Indexer API
// @version 1.0.0
// @description OpenAPI 3.0 specification for Plimsoll read API over HTTP.
// @license.name Apache 2.0
// @license.url http://www.apache.org/licenses/LICENSE-2.0.html
// @BasePath /
package api

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"mime"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stellar/go-stellar-sdk/strkey"

	"github.com/plimsoll-protocol/plimsoll-indexer/docs"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/changelog"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/events"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/metrics"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/webhook"
)

//go:embed openapi.yaml
var openAPISpec []byte

// is_covered has no default arguments on chain, so the shortcut endpoint owns
// its defaults: 100% backing, fresh within a day, at any report tier.
const (
	defaultCoveredMinBps uint32 = 10_000
	defaultCoveredMaxAge        = 24 * time.Hour
)

// Reader is what the API needs from storage.
type Reader interface {
	Ping(ctx context.Context) error
	ListAssets(ctx context.Context) ([]coverage.Asset, error)
	SearchAssets(ctx context.Context, q string) ([]coverage.Asset, error)
	GetAsset(ctx context.Context, sac string) (coverage.Asset, error)
	BatchCoverage(ctx context.Context, sacs []string) (map[string]store.CoverageData, error)
	LatestSupply(ctx context.Context, sac string) (*coverage.Supply, error)
	LatestSupplies(ctx context.Context, sacs []string) (map[string]*coverage.Supply, error)
	SupplyHistory(ctx context.Context, sac string, limit int) ([]coverage.Supply, error)
	SupplyHistoryCount(ctx context.Context, sac string) (int64, error)
	LatestReports(ctx context.Context, sac string) (map[uint32]coverage.Report, error)
	LatestReportsBySAC(ctx context.Context, sacs []string) (map[string]map[uint32]coverage.Report, error)
	ReportHistory(ctx context.Context, sac string, limit int) ([]coverage.Report, error)
	ReportExportPage(ctx context.Context, sac string, from, to *time.Time, after *store.ReportExportCursor, limit int) ([]store.ReportExportRow, error)
	SeriesSupply(ctx context.Context, sac string, from, to time.Time) ([]coverage.Supply, error)
	SeriesReports(ctx context.Context, sac string, from, to time.Time) ([]coverage.Report, error)
	ReportHistoryCount(ctx context.Context, sac string) (int64, error)
	Reporters(ctx context.Context) ([]coverage.Reporter, error)
	Disputes(ctx context.Context, sac, status string, limit, offset int) ([]coverage.Dispute, error)
	Breakdown(ctx context.Context, hash string) ([]byte, error)
	Attestation(ctx context.Context, sac string) (store.Attestation, error)
	AuditHistory(ctx context.Context, sac string, after *store.AuditCursor, limit int) ([]store.AuditEvent, error)
	Progress(ctx context.Context, name string) (store.Progress, error)
	// EventsSince backs Last-Event-ID resume from stored events.
	EventsSince(ctx context.Context, after string, limit int) ([]any, error)
}

// CoverageWebhookStore is what the API needs to manage coverage threshold
// registrations.
type CoverageWebhookStore interface {
	CreateCoverageWebhook(ctx context.Context, w coverage.Webhook) (coverage.Webhook, error)
	DeleteCoverageWebhook(ctx context.Context, id int64) error
}

type assetPageReader interface {
	ListAssetsPage(ctx context.Context, limit int, afterCode, afterIssuer string) ([]coverage.Asset, error)
}

type assetCursor struct {
	Version int    `json:"v"`
	Code    string `json:"code"`
	Issuer  string `json:"issuer"`
	Stats(ctx context.Context, dayStart time.Time) (store.Stats, error)
	GetVault(ctx context.Context, sac string) (coverage.Vault, error)
	LatestIssuerToml(ctx context.Context, issuer string) (coverage.IssuerTomlSnapshot, error)
}

// NetworkInfo is served at /v1/network so clients can discover contract ids.
type NetworkInfo struct {
	NetworkPassphrase  string `json:"network_passphrase"`
	RPCURL             string `json:"rpc_url"`
	HorizonURL         string `json:"horizon_url"`
	CoverageLedgerID   string `json:"coverage_ledger_id"`
	ReporterRegistryID string `json:"reporter_registry_id"`
}

type Server struct {
	Store   Reader
	Network NetworkInfo
	CORS    []string
	Log     *slog.Logger
	Now     func() time.Time

	// WebhookStore, WebhookCodec and WebhookAdminToken manage issuer TOML
	// change subscriptions at /v1/webhooks. All three must be set; otherwise
	// those endpoints answer 503.
	// GraphQL, when set, is mounted at /graphql. It is nil unless
	// ENABLE_GRAPHQL=true.
	GraphQL http.Handler
	Store             Reader
	WebhookStore      WebhookManager
	WebhookRetryer    WebhookRetryer
	WebhookAdminToken string
	WebhookAPIKeys    []string
	WebhookCodec      *webhook.SecretCodec

	// CoverageWebhooks and WebhookSigningSecret enable coverage threshold
	// alerts at /v1/coverage-webhooks, administered with the same token.
	CoverageWebhooks     CoverageWebhookStore
	WebhookSigningSecret string

	// Changelog is the breaking-change history served at /v1/changelog. It is
	// loaded from the hand-maintained CHANGELOG.yaml at start-up.
	Changelog []changelog.Entry
	Network           NetworkInfo
	CORS              []string
	Metrics           *metrics.Metrics
	Log               *slog.Logger
	Now               func() time.Time

	// Events is the live broadcaster fed by ingestion. Nil answers 503.
	Events *events.Hub
	// EventHeartbeat is how often an idle stream gets a keepalive comment;
	// EventWriteTimeout bounds one socket write. Both have defaults.
	EventHeartbeat    time.Duration
	EventWriteTimeout time.Duration
	// Done, when closed, ends streaming handlers so graceful shutdown does
	// not wait on connections that never go idle.
	Done <-chan struct{}
}

// WebhookManager manages the endpoints of one API key. apiKey is the
// fingerprint of the bearer token that made the request, so a key can only
// list and delete its own endpoints.
type WebhookManager interface {
	CreateWebhook(ctx context.Context, url, secretHash string, ciphertext []byte, ownerKeyHash string) (store.Webhook, error)
	ListWebhooks(ctx context.Context) ([]store.Webhook, error)
	ListWebhooksForKey(ctx context.Context, ownerKeyHash string) ([]store.Webhook, error)
	DeleteWebhook(ctx context.Context, id int64, ownerKeyHash string, allowLegacyAdmin bool) error
	CreateWebhook(context.Context, string, string, []byte) (store.Webhook, error)
	ListWebhooks(context.Context) ([]store.Webhook, error)
	GetWebhook(context.Context, int64) (store.Webhook, bool, error)
	DeleteWebhook(context.Context, int64) (bool, error)
	ListWebhookDLQ(context.Context, int64, int) ([]store.WebhookDLQ, error)
}

type WebhookRetryer interface {
	RetryWebhookDLQ(context.Context, int64, int64) (bool, error)
	statsMu    sync.Mutex
	statsEntry *statsEntry
}

// statsCacheTTL is how long /v1/stats serves an aggregate set before reading
// the database again.
const statsCacheTTL = 60 * time.Second

type statsEntry struct {
	stats   store.Stats
	expires time.Time
}

// WebhookDLQReader exposes failed delivery attempts. It is separate from
// WebhookManager so management-only stores need not implement it.
type WebhookDLQReader interface {
	GetWebhook(context.Context, int64) (store.Webhook, bool, error)
	ListWebhookDLQ(context.Context, int64, int) ([]store.WebhookDelivery, error)
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /metrics", s.metrics)
	mux.HandleFunc("GET /openapi.yaml", s.openAPI)
	mux.HandleFunc("GET /docs", s.docsUI)
	mux.HandleFunc("GET /docs/", s.docsUI)
	mux.HandleFunc("GET /v1/network", s.network)
	mux.HandleFunc("GET /v1/version", s.version)
	mux.HandleFunc("GET /v1/assets", s.listAssets)
	mux.HandleFunc("GET /v1/assets/search", s.searchAssets)
	mux.HandleFunc("POST /v1/assets/batch-covered", s.batchCoverage)
	mux.HandleFunc("GET /v1/assets/{sac}", s.getAsset)
	mux.HandleFunc("HEAD /v1/assets/{sac}", s.headAsset)
	mux.HandleFunc("GET /v1/assets/{sac}/supply", s.supplyHistory)
	mux.HandleFunc("GET /v1/assets/{sac}/reports", s.reportHistory)
	mux.HandleFunc("GET /v1/assets/{sac}/reports/export.csv", s.reportExport)
	mux.HandleFunc("GET /v1/assets/{sac}/coverage/now", s.coverageNow)
	mux.HandleFunc("GET /v1/assets/{sac}/coverage", s.coverageSeries)
	mux.HandleFunc("GET /v1/assets/{sac}/vault", s.getAssetVault)
	mux.HandleFunc("GET /v1/issuers/{issuer}/toml", s.getIssuerToml)
	mux.HandleFunc("GET /v1/assets/{sac}/audit", s.auditHistory)
	mux.HandleFunc("GET /v1/reporters", s.reporters)
	mux.HandleFunc("GET /v1/changelog", s.changelog)
	mux.HandleFunc("GET /v1/stats", s.stats)
	mux.HandleFunc("GET /v1/breakdowns/{hash}", s.breakdown)
	mux.HandleFunc("GET /v1/events", s.events)
	if s.GraphQL != nil {
		mux.Handle("/graphql", s.GraphQL)
	}
	mux.HandleFunc("POST /v1/webhooks", s.createWebhook)
	mux.HandleFunc("GET /v1/webhooks", s.listWebhooks)
	mux.HandleFunc("GET /v1/webhooks/{id}/dlq", s.listWebhookDLQ)
	mux.HandleFunc("POST /v1/webhooks/{id}/dlq/{delivery_id}/retry", s.retryWebhookDLQ)
	mux.HandleFunc("DELETE /v1/webhooks/{id}", s.deleteWebhook)
	mux.HandleFunc("POST /v1/coverage-webhooks", s.createCoverageWebhook)
	mux.HandleFunc("DELETE /v1/coverage-webhooks/{id}", s.deleteCoverageWebhook)
	return s.compress(s.cors(errorEnvelope(mux)))
}

func (s *Server) compress(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cw := &compressionWriter{
			ResponseWriter: w,
			encoding:       acceptedEncoding(strings.Join(r.Header.Values("Accept-Encoding"), ",")),
			logger:         s.Log,
			method:         r.Method,
		}
		next.ServeHTTP(cw, r)
		if err := cw.finish(); err != nil && s.Log != nil {
			s.Log.Error("write compressed API response", "err", err)
		}
	})
	if s.Metrics != nil {
		mux.Handle("GET /metrics", s.Metrics.Handler())
		return s.Metrics.Middleware(s.cors(mux))
	// The route table is the only list of endpoints: serving and documentation
	// both come from it (see routes.go).
	for _, rt := range routes() {
		mux.HandleFunc(rt.Method+" "+rt.Pattern, func(w http.ResponseWriter, r *http.Request) {
			rt.Handle(s, w, r)
		})
	}
	return s.cors(mux)
}

// ---- JSON shapes ----------------------------------------------------------

type supplyJSON struct {
	Amount string `json:"amount"`
	coverage.Supply
}

type reportJSON struct {
	Tier     string  `json:"tier"`
	Amount   string  `json:"amount"`
	DocCheck *string `json:"doc_check"`
	coverage.Report
}

type coverageJSON struct {
	// Bps is null when nothing is owed (zero supply).
	Bps            *uint32   `json:"bps"`
	Ratio          *string   `json:"ratio"`
	Tier           string    `json:"tier"`
	Supply         string    `json:"supply"`
	Reserves       string    `json:"reserves"`
	SupplyLedger   uint32    `json:"supply_ledger"`
	SupplyPostedAt time.Time `json:"supply_posted_at"`
	ReportAsOf     time.Time `json:"report_as_of"`
	ReportAgeSecs  int64     `json:"report_age_seconds"`
	SupplyAgeSecs  int64     `json:"supply_age_seconds"`
	Reporter       string    `json:"reporter"`
	DocURI         string    `json:"doc_uri"`
}

type assetJSON struct {
	coverage.Asset
	Toml          json.RawMessage        `json:"toml"`
	TomlCheckedAt *time.Time             `json:"toml_checked_at"`
	TomlError     *string                `json:"toml_error"`
	Supply        *supplyJSON            `json:"supply"`
	Reports       map[string]*reportJSON `json:"reports"`
	Coverage      *coverageJSON          `json:"coverage"`
}

// coverageNowJSON is the shortcut view of the live coverage figure: the bps,
// tier, as_of and reporter that /v1/assets/{sac} reports under "coverage",
// without the supply, reports and history behind them.
type coverageNowJSON struct {
	// Bps is null when nothing is owed (zero supply), as in coverageJSON.
	Bps      *uint32   `json:"bps"`
	Tier     string    `json:"tier"`
	AsOf     time.Time `json:"as_of"`
	Reporter string    `json:"reporter"`
}

type batchCoverageItemJSON struct {
	SAC      string        `json:"sac"`
	Coverage *coverageJSON `json:"coverage"`
}

// seriesPointJSON is coverage at one instant of a series. Coverage is null
// when nothing qualified at that instant.
type seriesPointJSON struct {
	At       time.Time     `json:"at"`
	Coverage *coverageJSON `json:"coverage"`
}

type seriesJSON struct {
	SAC      string            `json:"sac"`
	MinTier  string            `json:"min_tier"`
	Interval string            `json:"interval"`
	From     time.Time         `json:"from"`
	To       time.Time         `json:"to"`
	Points   []seriesPointJSON `json:"points"`
}

type statsJSON struct {
	TotalAssets      int64 `json:"totalAssets"`
	CoveredAssets    int64 `json:"coveredAssets"`
	TotalReporters   int64 `json:"totalReporters"`
	ActiveReporters  int64 `json:"activeReporters"`
	ReportsToday     int64 `json:"reportsToday"`
	IngestLagSeconds int64 `json:"ingestLagSeconds"`
}

type reporterJSON struct {
	Role string `json:"role"`
	coverage.Reporter
}

type vaultJSON struct {
	ReserveBalance string `json:"reserve_balance"`
	coverage.Vault
}

type webhookJSON struct {
	ID        int64        `json:"id"`
	URL       string       `json:"url"`
	CreatedAt time.Time    `json:"created_at"`
	Delivery  deliveryJSON `json:"delivery"`
}

// deliveryJSON is the per-endpoint delivery state. Events fan out to every
// endpoint, so each one succeeds or fails on its own.
type deliveryJSON struct {
	Deliveries int64      `json:"deliveries"`
	Failures   int64      `json:"failures"`
	LastAt     *time.Time `json:"last_at"`
	LastStatus string     `json:"last_status,omitempty"`
	LastError  string     `json:"last_error,omitempty"`
}

type createdWebhookJSON struct {
	webhookJSON
	Secret string `json:"secret"`
}

type webhookDLQJSON struct {
	ID        int64           `json:"id"`
	Payload   json.RawMessage `json:"payload"`
	LastError string          `json:"last_error"`
	Attempts  int             `json:"attempts"`
	FailedAt  time.Time       `json:"failed_at"`
type webhookDeliveryJSON struct {
	ID                int64     `json:"id"`
	WebhookID         int64     `json:"webhook_id"`
	Event             string    `json:"event"`
	StatusCode        int       `json:"status_code"`
	Error             string    `json:"error"`
	DeliveryLatencyMS int64     `json:"delivery_latency_ms"`
	AttemptedAt       time.Time `json:"attempted_at"`
}

func toSupply(sp *coverage.Supply) *supplyJSON {
	if sp == nil {
		return nil
	}
	return &supplyJSON{Amount: sp.Amount.String(), Supply: *sp}
}

func toReport(r coverage.Report) *reportJSON {
	out := &reportJSON{Tier: coverage.TierName(r.Tier), Amount: r.Amount.String(), Report: r}
	if r.DocCheck != "" {
		out.DocCheck = &r.DocCheck
	}
	return out
}

func toWebhookJSON(w store.Webhook) webhookJSON {
	return webhookJSON{
		ID:        w.ID,
		URL:       w.URL,
		CreatedAt: w.CreatedAt,
		Delivery: deliveryJSON{
			Deliveries: w.Deliveries,
			Failures:   w.Failures,
			LastAt:     w.LastDeliveryAt,
			LastStatus: w.LastDeliveryStatus,
			LastError:  w.LastDeliveryError,
		},
	}
}

func (s *Server) toCoverage(r *coverage.Result) *coverageJSON {
	return toCoverageAt(r, s.Now())
}

// toCoverageAt ages a coverage result relative to now, which for a series is
// the point's own instant rather than the request time.
func toCoverageAt(r *coverage.Result, now time.Time) *coverageJSON {
	if r == nil {
		return nil
	}
	out := &coverageJSON{
		Tier:           coverage.TierName(r.Tier),
		Supply:         r.Supply.String(),
		Reserves:       r.Reserves.String(),
		SupplyLedger:   r.SupplyLedger,
		SupplyPostedAt: r.SupplyPostedAt,
		ReportAsOf:     r.ReportAsOf,
		ReportAgeSecs:  int64(now.Sub(r.ReportAsOf).Seconds()),
		SupplyAgeSecs:  int64(now.Sub(r.SupplyPostedAt).Seconds()),
		Reporter:       r.ReportReporter,
		DocURI:         r.ReportDocURI,
	}
	if !r.NothingIsOwed {
		bps := r.Bps
		out.Bps = &bps
		if bps != math.MaxUint32 {
			ratio := strconv.FormatFloat(float64(bps)/100, 'f', 2, 64) + "%"
			out.Ratio = &ratio
		}
	}
	return out
}

// ---- handlers -------------------------------------------------------------

// @Summary Health check and indexer sync status
// @Description Reports database reachability and progress of the contract event ingestion loop.
// @Tags system
// @Produce json
// @Success 200 {object} map[string]any
// @Failure 503 {object} map[string]string
// @Router /healthz [get]
func (s *Server) health(w http.ResponseWriter, r *http.Request) {
	if err := s.Store.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, codeDBUnavailable, "database is unavailable")
		return
	}
	p, err := s.Store.Progress(r.Context(), store.LegacyIngestCursorName)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, codeProgressUnavailable, "ingest progress is unavailable")
		return
	}
	if s.Network.CoverageLedgerID != "" && s.Network.ReporterRegistryID != "" {
		ids := []string{s.Network.CoverageLedgerID, s.Network.ReporterRegistryID}
		scanned := make([]uint32, 0, len(ids))
		var eventLedger uint32
		eventAt := p.EventAt
		for _, id := range ids {
			stream, err := s.Store.Progress(r.Context(), store.ContractIngestCursorName(id))
			if err != nil {
				writeError(w, http.StatusServiceUnavailable, codeProgressUnavailable, "ingest progress is unavailable")
				return
			}
			scanned = append(scanned, stream.ScannedLedger)
			if stream.EventLedger > eventLedger {
				eventLedger = stream.EventLedger
			}
			if stream.EventAt.After(eventAt) {
				eventAt = stream.EventAt
			}
		}
		p.ScannedLedger = scanned[0]
		for _, ledger := range scanned[1:] {
			if ledger < p.ScannedLedger {
				p.ScannedLedger = ledger
			}
		}
		if eventLedger > p.EventLedger {
			p.EventLedger = eventLedger
		}
		p.EventAt = eventAt
	}
	body := map[string]any{
		"status":                 "ok",
		"ingested_to_ledger":     p.ScannedLedger,
		"last_event_ledger":      p.EventLedger,
		"last_event_at":          nil,
		"last_event_age_seconds": nil,
	}
	// A scan that keeps advancing while no event lands is how ingestion failed
	// silently before, so say when an event was last written and let callers
	// alert on it.
	if !p.EventAt.IsZero() {
		body["last_event_at"] = p.EventAt.UTC()
		body["last_event_age_seconds"] = int64(s.Now().Sub(p.EventAt).Seconds())
	}
	writeJSON(w, http.StatusOK, body)
}

// @Summary Prometheus metrics endpoint
// @Description Exposes standard Go runtime metrics plus custom indexer metrics.
// @Tags metrics
// @Produce text/plain
// @Success 200 {string} string "Prometheus text exposition format"
// @Router /metrics [get]
func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	if s.Store != nil {
		if assets, err := s.Store.ListAssets(r.Context()); err == nil {
			metrics.ActiveAssets.Set(float64(len(assets)))
		}
		if p, err := s.Store.Progress(r.Context(), "plimsoll-events"); err == nil && !p.EventAt.IsZero() {
			now := time.Now()
			if s.Now != nil {
				now = s.Now()
			}
			metrics.IngestLastEventAgeSeconds.Set(float64(now.Sub(p.EventAt).Seconds()))
		}
	}
	promhttp.Handler().ServeHTTP(w, r)
}

// @Summary Get OpenAPI 3.0 specification
// @Description Serves the OpenAPI specification in YAML format.
// @Tags docs
// @Produce application/yaml
// @Success 200 {string} string "OpenAPI YAML specification"
// @Router /openapi.yaml [get]
func (s *Server) openAPI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(docs.OpenAPISpec)
}

const swaggerUIHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Plimsoll Indexer API Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css" />
</head>
<body>
<div id="swagger-ui"></div>
<script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js" crossorigin></script>
<script>
  window.onload = () => {
    window.ui = SwaggerUIBundle({
      url: '/openapi.yaml',
      dom_id: '#swagger-ui',
    });
  };
</script>
</body>
</html>`

// @Summary Interactive API documentation
// @Description Serves Swagger UI documentation for the indexer API.
// @Tags docs
// @Produce html
// @Success 200 {string} string "HTML documentation page"
// @Router /docs [get]
func (s *Server) docsUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(swaggerUIHTML))
}

// @Summary Discover network parameters and contract IDs
// @Description Discover network parameters and contract IDs for client configuration.
// @Tags network
// @Produce json
// @Success 200 {object} NetworkInfo
// @Router /v1/network [get]
func (s *Server) network(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Network)
func (s *Server) network(w http.ResponseWriter, r *http.Request) {
	s.writeConditionalJSON(w, r, http.StatusOK, s.Network)
}

func (s *Server) version(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Build)
}

// @Summary List all tracked assets with coverage and reports
// @Description Returns all assets currently tracked on the coverage ledger.
// @Tags assets
// @Produce json
// @Param min_tier query int false "Minimum report tier (1=transcribed, 2=issuer_signed, 3=auditor_signed)" Enums(1, 2, 3) default(1)
// @Success 200 {object} map[string]any
// @Failure 400 {object} map[string]string
// @Router /v1/assets [get]
func (s *Server) listAssets(w http.ResponseWriter, r *http.Request) {
	minTier, ok := parseTier(w, r)
	if !ok {
		return
	}
	limit, ok := parseAssetLimit(w, r)
	if !ok {
		return
	}
	cursor, ok := parseAssetCursor(w, r)
	if !ok {
		return
	}

	var assets []coverage.Asset
	var err error
	if pageStore, ok := s.Store.(assetPageReader); ok {
		afterCode, afterIssuer := "", ""
		if cursor != nil {
			afterCode, afterIssuer = cursor.Code, cursor.Issuer
		}
		assets, err = pageStore.ListAssetsPage(r.Context(), limit+1, afterCode, afterIssuer)
	} else {
		all, listErr := s.Store.ListAssets(r.Context())
		if listErr != nil {
			err = listErr
		} else {
			sortAssets(all)
			start := 0
			if cursor != nil {
				for i := range all {
					if all[i].Code > cursor.Code ||
						(all[i].Code == cursor.Code && all[i].Issuer > cursor.Issuer) {
						start = i
						break
					}
					if i == len(all)-1 {
						start = len(all)
					}
				}
			}
			end := start + limit + 1
			if end > len(all) {
				end = len(all)
			}
			assets = all[start:end]
		}
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	hasNext := len(assets) > limit
	if hasNext {
		assets = assets[:limit]
	}
	sacs := make([]string, 0, len(assets))
	for _, a := range assets {
		sacs = append(sacs, a.SAC)
	}
	supplies, err := s.Store.LatestSupplies(r.Context(), sacs)
	if err != nil {
		s.fail(w, err)
		return
	}
	reports, err := s.Store.LatestReportsBySAC(r.Context(), sacs)
	if err != nil {
		s.fail(w, err)
		return
	}
	out := make([]assetJSON, 0, len(assets))
	for _, a := range assets {
		aj, err := s.assembleWithData(r.Context(), a, supplies[a.SAC], reports[a.SAC], minTier)
		if err != nil {
			s.fail(w, err)
			return
		}
		out = append(out, aj)
	}
	body := map[string]any{"assets": out}
	if hasNext {
		last := assets[len(assets)-1]
		body["next_cursor"] = encodeAssetCursor(assetCursor{
			Version: 1,
			Code:    last.Code,
			Issuer:  last.Issuer,
		})
	}
	writeJSON(w, http.StatusOK, body)
}

func (s *Server) searchAssets(w http.ResponseWriter, r *http.Request) {
	q := strings.TrimSpace(r.URL.Query().Get("q"))
	if q == "" {
		writeError(w, http.StatusBadRequest, "query parameter q is required and cannot be empty")
		return
	}
	minTier, ok := parseTier(w, r)
	if !ok {
		return
	}
	assets, err := s.Store.SearchAssets(r.Context(), q)
	if err != nil {
		s.fail(w, err)
		return
	}
	if len(assets) > 20 {
		assets = assets[:20]
	}
	out := make([]assetJSON, 0, len(assets))
	for _, a := range assets {
		aj, err := s.assemble(r.Context(), a, minTier)
		if err != nil {
			s.fail(w, err)
			return
		}
		out = append(out, aj)
	}
	writeJSON(w, http.StatusOK, map[string]any{"assets": out})
}

// @Summary Get asset details, latest supply, reports, and coverage
// @Description Returns the specified asset with its coverage calculation and report history.
// @Tags assets
// @Produce json
// @Param sac path string true "Stellar Asset Contract address"
// @Param min_tier query int false "Minimum report tier" Enums(1, 2, 3) default(1)
// @Success 200 {object} map[string]any
// @Failure 400 {object} map[string]string
// @Failure 404 {object} map[string]string
// @Router /v1/assets/{sac} [get]
func (s *Server) getAsset(w http.ResponseWriter, r *http.Request) {
	// Go 1.22+'s ServeMux rejects a literal GET /v1/assets/covered alongside
	// HEAD /v1/assets/{sac}: the literal is more specific by path but less
	// specific by method, so neither pattern wins. Dispatch the reserved name
	// here instead of registering a conflicting literal route.
	if r.PathValue("sac") == "covered" {
		s.coveredAssets(w, r)
		return
	}
	sac, ok := parseSAC(w, r)
	if !ok {
		return
	}
	minTier, ok := parseTier(w, r)
	if !ok {
		return
	}
	a, err := s.Store.GetAsset(r.Context(), sac)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, codeAssetNotListed, "asset not listed")
		return
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	aj, err := s.assemble(r.Context(), a, minTier)
	if err != nil {
		s.fail(w, err)
		return
	}
	s.writeConditionalJSON(w, r, http.StatusOK, aj)
}

// headAsset answers whether an asset is listed without building the asset
// document, so clients can check existence cheaply. It never writes a body of
// its own: HEAD has no response body on the wire regardless of what a handler
// writes, and this one only needs a status.

// @Summary Check whether an asset is listed
// @Description Returns 200 when the asset is listed and 404 when it is not, so clients can check existence without downloading the full asset document. The response never has a body.
// @ID headAsset
// @Tags assets
// @Param sac path string true "Stellar Asset Contract address"
// @Success 200 "Asset is listed. No response body."
// @Failure 400 "sac is not a contract strkey. No response body."
// @Failure 404 "Asset is not listed. No response body."
// @Router /v1/assets/{sac} [head]
func (s *Server) headAsset(w http.ResponseWriter, r *http.Request) {
	sac, ok := parseSAC(w, r)
	if !ok {
		return
	}
	_, err := s.Store.GetAsset(r.Context(), sac)
	if errors.Is(err, store.ErrNotFound) {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) batchCoverage(w http.ResponseWriter, r *http.Request) {
	const maxAssets = 50
	const maxBodyBytes = 4096

	body, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil || len(body) > maxBodyBytes {
		writeError(w, http.StatusBadRequest, codeInvalidBody, "request body must be a JSON array of 1 to 50 SAC addresses")
		return
	}
	var sacs []string
	if err := json.Unmarshal(body, &sacs); err != nil || len(sacs) == 0 || len(sacs) > maxAssets {
		writeError(w, http.StatusBadRequest, codeInvalidBody, "request body must be a JSON array of 1 to 50 SAC addresses")
		return
	}
	for _, sac := range sacs {
		if !strkey.IsValidContractAddress(sac) {
			writeError(w, http.StatusBadRequest, codeInvalidSAC, "each asset must be a contract strkey")
			return
		}
	}

	data, err := s.Store.BatchCoverage(r.Context(), sacs)
	if err != nil {
		s.fail(w, err)
		return
	}
	out := make([]batchCoverageItemJSON, 0, len(sacs))
	for _, sac := range sacs {
		item := data[sac]
		out = append(out, batchCoverageItemJSON{
			SAC:      sac,
			Coverage: s.toCoverage(coverage.Compute(item.Supply, item.Reports, coverage.TierTranscribed)),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"assets": out})
}

func (s *Server) assemble(ctx context.Context, a coverage.Asset, minTier uint32) (assetJSON, error) {
	supplies, err := s.Store.LatestSupplies(ctx, []string{a.SAC})
	if err != nil {
		return assetJSON{}, err
	}
	reportsBySAC, err := s.Store.LatestReportsBySAC(ctx, []string{a.SAC})
	if err != nil {
		return assetJSON{}, err
	}
	return s.assembleWithData(ctx, a, supplies[a.SAC], reportsBySAC[a.SAC], minTier)
}

// assembleWithData builds the asset document from figures that were already
// read for a whole page, so listing N assets does not issue four queries per
// asset.
func (s *Server) assembleWithData(ctx context.Context, a coverage.Asset, sp *coverage.Supply, reports map[uint32]coverage.Report, minTier uint32) (assetJSON, error) {
	att, attErr := s.Store.Attestation(ctx, a.SAC)
	if attErr != nil && !errors.Is(attErr, store.ErrNotFound) {
		return assetJSON{}, attErr
	}
	tomlBody, err := withAttestation(a.Toml, att, attErr == nil)
	if err != nil {
		return nil, nil, err
	}
	out := s.assembleWithData(a, supplies[a.SAC], reportsBySAC[a.SAC], minTier)
	out.Toml = tomlBody
	return out, nil
}

func (s *Server) assembleWithData(a coverage.Asset, sp *coverage.Supply, reports map[uint32]coverage.Report, minTier uint32) assetJSON {
	return sp, reports, nil
}

func (s *Server) assembleFrom(a coverage.Asset, minTier uint32, sp *coverage.Supply, reports map[uint32]coverage.Report) assetJSON {
	out := assetJSON{
		Asset:         a,
		Toml:          json.RawMessage("null"),
		TomlCheckedAt: a.TomlCheckedAt,
		Supply:        toSupply(sp),
		Reports:       map[string]*reportJSON{},
		Coverage:      s.toCoverage(coverage.Compute(sp, reports, minTier)),
	}
	if a.TomlError != "" {
		out.TomlError = &a.TomlError
	}
	if len(a.Toml) > 0 {
		out.Toml = a.Toml
	}
	for tier, rep := range reports {
		out.Reports[coverage.TierName(tier)] = toReport(rep)
	}
	return out, nil
}

// withAttestation adds the issuer's attestation document fields to the stored
// SEP-1 object. The object is re-encoded, so its key order is that of
// encoding/json; nothing else about it changes.
func withAttestation(raw []byte, att store.Attestation, found bool) (json.RawMessage, error) {
	if !found && len(raw) == 0 {
		return json.RawMessage("null"), nil
	}
	obj := map[string]json.RawMessage{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &obj); err != nil {
			return nil, fmt.Errorf("decode stored stellar.toml: %w", err)
		}
	}
	if found {
		hash, _ := json.Marshal(att.Hash)
		obj["attestation_hash"] = hash
		firstSeen, _ := json.Marshal(att.FirstSeenAt.UTC().Format(time.RFC3339))
		obj["attestation_first_seen_at"] = firstSeen
		if att.ChangedAt != nil {
			changed, _ := json.Marshal(att.ChangedAt.UTC().Format(time.RFC3339))
			obj["attestation_changed_at"] = changed
		}
	}
	body, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("encode asset toml: %w", err)
	}
	return body, nil
}

// @Summary Get historical supply snapshots for an asset
// @Description Returns historical supply snapshots posted on chain for an asset.
// @Tags supply
// @Produce json
// @Param sac path string true "Stellar Asset Contract address"
// @Param limit query int false "Max snapshots to return" default(50)
// @Success 200 {object} map[string]any
// @Router /v1/assets/{sac}/supply [get]
func (s *Server) supplyHistory(w http.ResponseWriter, r *http.Request) {
	sac, ok := parseSAC(w, r)
	if !ok {
		return
	}
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	if !s.requireAsset(w, r, sac) {
		return
	}
	total, err := s.Store.SupplyHistoryCount(r.Context(), sac)
	if err != nil {
		s.fail(w, err)
		return
	}
	list, err := s.Store.SupplyHistory(r.Context(), sac, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	out := make([]*supplyJSON, 0, len(list))
	for i := range list {
		out = append(out, toSupply(&list[i]))
	}
	writeJSON(w, http.StatusOK, map[string]any{"supply": out, "next_before": nextBefore})
	w.Header().Set("X-Total-Count", strconv.FormatInt(total, 10))
	writeJSON(w, http.StatusOK, map[string]any{"supply": out})
}

// @Summary Get historical reserve reports for an asset
// @Description Returns historical reserve reports submitted for an asset.
// @Tags reports
// @Produce json
// @Param sac path string true "Stellar Asset Contract address"
// @Param limit query int false "Max reports to return" default(50)
// @Success 200 {object} map[string]any
// @Router /v1/assets/{sac}/reports [get]
func (s *Server) reportHistory(w http.ResponseWriter, r *http.Request) {
	sac, ok := parseSAC(w, r)
	if !ok {
		return
	}
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	if !s.requireAsset(w, r, sac) {
		return
	}
	total, err := s.Store.ReportHistoryCount(r.Context(), sac)
	if err != nil {
		s.fail(w, err)
		return
	}
	list, err := s.Store.ReportHistory(r.Context(), sac, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	out := make([]*reportJSON, 0, len(list))
	for _, rep := range list {
		out = append(out, toReport(rep))
	}
	writeJSON(w, http.StatusOK, map[string]any{"reports": out, "next_before": nextBefore})
}

func (s *Server) covered(w http.ResponseWriter, r *http.Request) {
	minBps, ok := parseMinBps(w, r)
	if !ok {
		return
	}
	maxAge, ok := parseMaxAge(w, r)
func (s *Server) coverageSeries(w http.ResponseWriter, r *http.Request) {
func (s *Server) getAssetVault(w http.ResponseWriter, r *http.Request) {
	sac, ok := parseSAC(w, r)
	if !ok {
		return
	}
	minTier, ok := parseTier(w, r)
	if !ok {
		return
	}
	sp, err := s.Store.LatestSupply(r.Context(), r.PathValue("sac"))
	v, err := s.Store.GetVault(r.Context(), sac)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, codeVaultNotFound, "vault not found")
		return
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	reports, err := s.Store.LatestReports(r.Context(), r.PathValue("sac"))
	if err != nil {
		s.fail(w, err)
		return
	}
	covered, reason := coverage.CoverageStatus(sp, reports, minTier, s.Now(), minBps, maxAge)
	writeJSON(w, http.StatusOK, map[string]any{"covered": covered, "reason": reason})
}

const reportExportPageSize = 500

func (s *Server) reportExport(w http.ResponseWriter, r *http.Request) {
	sac, ok := parseSAC(w, r)
	if !ok {
		return
	}
	from, to, ok := parseDateRange(w, r)
	if !ok {
		return
	}
	if !s.requireAsset(w, r, sac) {
		return
	}

	page, err := s.Store.ReportExportPage(r.Context(), sac, from, to, nil, reportExportPageSize)
	to, ok := parseTimeQuery(w, r, "to", s.Now().UTC())
	if !ok {
		return
	}
	from, ok := parseTimeQuery(w, r, "from", to.Add(-24*time.Hour))
	if !ok {
		return
	}
	interval, rawInterval, ok := parseInterval(w, r)
	if !ok {
		return
	}
	if !from.Before(to) {
		writeError(w, http.StatusBadRequest, "from must be before to")
		return
	}
	if to.Sub(from)/interval+1 > maxSeriesPoints {
		writeError(w, http.StatusBadRequest, "range has more than 1000 points; use a larger interval")
		return
	}
	if !s.requireAsset(w, r, sac) {
		return
	}
	supply, err := s.Store.SeriesSupply(r.Context(), sac, from, to)
	balance := "0"
	if v.ReserveBalance != nil {
		balance = v.ReserveBalance.String()
	}
	v.UpdatedAt = v.UpdatedAt.UTC()
	writeJSON(w, http.StatusOK, vaultJSON{ReserveBalance: balance, Vault: v})
}

func (s *Server) getIssuerToml(w http.ResponseWriter, r *http.Request) {
	issuer := r.PathValue("issuer")
	if !strkey.IsValidEd25519PublicKey(issuer) {
		writeError(w, http.StatusBadRequest, codeInvalidIssuer, "issuer must be a Stellar account public key")
		return
	}
	snap, err := s.Store.LatestIssuerToml(r.Context(), issuer)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, codeIssuerTomlNotFound, "issuer toml not found")
		return
	}
	if err != nil {
		s.fail(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/csv; charset=utf-8")
	w.Header().Set("Content-Disposition", `attachment; filename="reports-`+sac+`.csv"`)
	writer := csv.NewWriter(w)
	if err := writer.Write([]string{"ledger", "timestamp", "reporter", "bps", "tier", "doc_uri"}); err != nil {
		s.Log.Error("write report export", "err", err)
		return
	}

	var cursor *store.ReportExportCursor
	for {
		for _, row := range page {
			bps := ""
			if row.Bps != nil {
				bps = strconv.FormatUint(uint64(*row.Bps), 10)
			}
			record := []string{
				strconv.FormatUint(uint64(row.Ledger), 10),
				row.Timestamp.UTC().Format(time.RFC3339Nano),
				row.Reporter,
				bps,
				coverage.TierName(row.Tier),
				row.DocURI,
			}
			if err := writer.Write(record); err != nil {
				s.Log.Error("write report export", "err", err)
				return
			}
		}
		if len(page) < reportExportPageSize {
			break
		}
		next := page[len(page)-1].Cursor
		cursor = &next
		page, err = s.Store.ReportExportPage(r.Context(), sac, from, to, cursor, reportExportPageSize)
		if err != nil {
			s.Log.Error("read report export page", "err", err)
			return
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		s.Log.Error("flush report export", "err", err)
	}
}

// coverageNow answers the current coverage figure alone, for clients that poll
// bps and do not need the asset document or the history behind it.
func (s *Server) coverageNow(w http.ResponseWriter, r *http.Request) {
	sac, ok := parseSAC(w, r)
	if !ok {
		return
	}
	if !s.requireAsset(w, r, sac) {
		return
	}
	supplies, err := s.Store.LatestSupplies(r.Context(), []string{sac})
	reports, err := s.Store.SeriesReports(r.Context(), sac, from, to)
	if err != nil {
		s.fail(w, err)
		return
	}
	reportsBySAC, err := s.Store.LatestReportsBySAC(r.Context(), []string{sac})
	if err != nil {
		s.fail(w, err)
		return
	}
	res := coverage.Compute(supplies[sac], reportsBySAC[sac], coverage.TierTranscribed)
	if res == nil {
		writeError(w, http.StatusNotFound, codeCoverageNotFound, "no live coverage report")
		return
	}
	out := coverageNowJSON{
		Tier:     coverage.TierName(res.Tier),
		AsOf:     res.ReportAsOf.UTC(),
		Reporter: res.ReportReporter,
	}
	if !res.NothingIsOwed {
		bps := res.Bps
		out.Bps = &bps
	points := coverage.Series(supply, reports, minTier, from, to, interval)
	out := seriesJSON{
		SAC:      sac,
		MinTier:  coverage.TierName(minTier),
		Interval: rawInterval,
		From:     from,
		To:       to,
		Points:   make([]seriesPointJSON, 0, len(points)),
	}
	for _, p := range points {
		out.Points = append(out.Points, seriesPointJSON{
			At:       p.At.UTC(),
			Coverage: toCoverageAt(p.Coverage, p.At),
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// @Summary List authorized reporters
// @Description Returns all active registered reporters and their roles.
// @Tags reporters
// @Produce json
// @Success 200 {object} map[string]any
// @Router /v1/reporters [get]
	snap.FetchedAt = snap.FetchedAt.UTC()
	w.Header().Set("Last-Modified", snap.FetchedAt.Format(http.TimeFormat))
	writeJSON(w, http.StatusOK, snap)
	w.Header().Set("X-Total-Count", strconv.FormatInt(total, 10))
	writeJSON(w, http.StatusOK, map[string]any{"reports": out})
}

func (s *Server) auditHistory(w http.ResponseWriter, r *http.Request) {
	sac, ok := parseSAC(w, r)
	if !ok {
		return
	}
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	var after *store.AuditCursor
	if token := r.URL.Query().Get("cursor"); token != "" {
		after, ok = parseAuditCursor(token)
		if !ok {
			writeErrorDetails(w, http.StatusBadRequest, codeInvalidCursor,
				"cursor must be a valid audit cursor", map[string]any{"parameter": "cursor"})
			return
		}
	}
	if !s.requireAsset(w, r, sac) {
		return
	}
	events, err := s.Store.AuditHistory(r.Context(), sac, after, limit+1)
	if err != nil {
		s.fail(w, err)
		return
	}
	hasMore := len(events) > limit
	if hasMore {
		events = events[:limit]
	}
	var nextCursor string
	if hasMore {
		last := events[len(events)-1]
		nextCursor = makeAuditCursor(store.AuditCursor{Ledger: last.Ledger, ID: last.ID})
	}
	writeJSON(w, http.StatusOK, struct {
		Events     []store.AuditEvent `json:"events"`
		NextCursor string             `json:"next_cursor,omitempty"`
	}{Events: events, NextCursor: nextCursor})
}

func (s *Server) reporters(w http.ResponseWriter, r *http.Request) {
	list, err := s.Store.Reporters(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	out := make([]reporterJSON, 0, len(list))
	for _, rep := range list {
		out = append(out, reporterJSON{Role: coverage.RoleName(rep.Role), Reporter: rep})
	}
	writeJSON(w, http.StatusOK, map[string]any{"reporters": out})
}

// coveredAssets lists only the assets whose current coverage passes the same
// freshness and ratio checks as the contract's is_covered. The contract has no
// defaults for min_bps and max_age, so this endpoint documents and applies its
// own: 100% backing and 24 hours, the pair the Plimsoll app treats as covered.
func (s *Server) coveredAssets(w http.ResponseWriter, r *http.Request) {
	minTier, ok := parseTier(w, r)
	if !ok {
		return
	}
	minBps, ok := parseMinBps(w, r)
	if !ok {
		return
	}
	maxAge, ok := parseMaxAge(w, r)
	if !ok {
		return
	}
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	offset, ok := parseOffset(w, r)
	if !ok {
		return
	}

	assets, err := s.Store.ListAssets(r.Context())
func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	st, err := s.cachedStats(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	matched := make([]assetJSON, 0, len(assets))
	for _, a := range assets {
		sp, reports, err := s.coverageInputs(r.Context(), a.SAC)
		if err != nil {
			s.fail(w, err)
			return
		}
		result := coverage.Compute(sp, reports, minTier)
		if !result.Covered(s.Now(), minBps, maxAge) {
			continue
		}
		matched = append(matched, s.assembleFrom(a, minTier, sp, reports))
	}

	page, next := pageOf(matched, limit, offset)
	body := map[string]any{"assets": page}
	if next != nil {
		body["next_offset"] = *next
	}
	writeJSON(w, http.StatusOK, body)
}

// disputeHistory lists one asset's disputes, newest-opened first. It returns
// 404 for an unlisted asset, like the supply and report histories, instead of
// an empty list that looks the same as an asset with no disputes.
func (s *Server) disputeHistory(w http.ResponseWriter, r *http.Request) {
	sac, ok := parseSAC(w, r)
	if !ok {
		return
	}
	status, ok := parseDisputeStatus(w, r)
	if !ok {
		return
	}
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	offset, ok := parseOffset(w, r)
	if !ok {
		return
	}
	if !s.requireAsset(w, r, sac) {
		return
	}
	list, err := s.Store.Disputes(r.Context(), sac, status, limit+1, offset)
	if err != nil {
		s.fail(w, err)
		return
	}
	var next *int
	if len(list) > limit {
		list = list[:limit]
		n := offset + limit
		next = &n
	}
	out := make([]coverage.Dispute, 0, len(list))
	out = append(out, list...)
	body := map[string]any{"disputes": out}
	if next != nil {
		body["next_offset"] = *next
	}
	writeJSON(w, http.StatusOK, body)
}

// @Summary List breaking changes per released API version
// @Description Machine-readable changelog, newest version first, generated from CHANGELOG.yaml.
// @ID getChangelog
// @Tags changelog
// @Produce json
// @Success 200 {object} map[string]any
// @Router /v1/changelog [get]
func (s *Server) changelog(w http.ResponseWriter, _ *http.Request) {
	entries := s.Changelog
	if entries == nil {
		entries = []changelog.Entry{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"changelog": entries})
}

// @Summary Get canonical supply breakdown by hash
// @Description Serves the canonical JSON breakdown stored for a posted supply snapshot.
// @Tags breakdowns
// @Produce json
// @Param hash path string true "Hex-encoded SHA-256 hash of the breakdown JSON"
// @Success 200 {string} string "Canonical JSON breakdown"
// @Failure 404 {object} map[string]string
// @Router /v1/breakdowns/{hash} [get]
	var lag int64
	if st.LastEventAt != nil {
		if secs := int64(s.Now().Sub(*st.LastEventAt).Seconds()); secs > 0 {
			lag = secs
		}
	}
	writeJSON(w, http.StatusOK, statsJSON{
		TotalAssets:      st.TotalAssets,
		CoveredAssets:    st.CoveredAssets,
		TotalReporters:   st.TotalReporters,
		ActiveReporters:  st.ActiveReporters,
		ReportsToday:     st.ReportsToday,
		IngestLagSeconds: lag,
	})
}

// cachedStats serves the aggregates from memory until statsCacheTTL has
// passed. Failures are not cached, so a transient database error does not keep
// /v1/stats broken for a minute.
func (s *Server) cachedStats(ctx context.Context) (store.Stats, error) {
	now := s.Now()
	s.statsMu.Lock()
	e := s.statsEntry
	s.statsMu.Unlock()
	if e != nil && now.Before(e.expires) {
		return e.stats, nil
	}
	dayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	st, err := s.Store.Stats(ctx, dayStart)
	if err != nil {
		return store.Stats{}, err
	}
	s.statsMu.Lock()
	s.statsEntry = &statsEntry{stats: st, expires: now.Add(statsCacheTTL)}
	s.statsMu.Unlock()
	return st, nil
}

func (s *Server) breakdown(w http.ResponseWriter, r *http.Request) {
	hash := r.PathValue("hash")
	if !isBreakdownHash(hash) {
		writeError(w, http.StatusBadRequest, codeInvalidBreakdownHash, "hash must be 64 hex characters")
		return
	}
	body, err := s.Store.Breakdown(r.Context(), hash)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, codeBreakdownNotFound, "unknown breakdown hash")
		return
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	// Served byte-for-byte so sha256(body) equals the on-chain hash.
	s.writeConditional(w, r, http.StatusOK, "application/json", body)
}

// ---- webhooks -------------------------------------------------------------

// @Summary Register an issuer TOML change webhook
// @Description Register a webhook to receive notifications when issuer TOML changes. Secret is returned only once at creation.
// @Tags webhooks
// @Accept json
// @Produce json
// @Param body body map[string]string true "Webhook configuration with url"
// @Success 201 {object} map[string]any
// @Failure 400 {object} map[string]string
// @Failure 401 {object} map[string]string
// @Failure 503 {object} map[string]string
// @Router /v1/webhooks [post]
func (s *Server) createWebhook(w http.ResponseWriter, r *http.Request) {
	if !s.webhookConfigured(w) {
		return
	}
	ownerKeyHash, _, ok := s.authorizeWebhookAPIKey(w, r)
	if !ok {
		return
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, 4097))
	if err != nil || len(body) > 4096 {
		writeError(w, http.StatusBadRequest, codeInvalidBody, "request body must be valid JSON under 4KB")
		return
	}
	var input struct {
		URL string `json:"url"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidBody, "request body must contain a webhook URL")
		return
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, codeInvalidBody, "request body must contain one JSON object")
		return
	}
	if err := webhook.ValidateURL(input.URL); err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidWebhookURL, "url must be a valid HTTPS URL")
		return
	}
	secret, err := webhook.GenerateSecret()
	if err != nil {
		s.fail(w, err)
		return
	}
	ciphertext, err := s.WebhookCodec.Encrypt(secret)
	if err != nil {
		s.fail(w, err)
		return
	}
	created, err := s.WebhookStore.CreateWebhook(r.Context(), input.URL, webhook.SecretHash(secret), ciphertext, ownerKeyHash)
	if errors.Is(err, store.ErrWebhookLimit) {
		writeErrorDetails(w, http.StatusUnprocessableEntity, codeWebhookLimitReached,
			fmt.Sprintf("an API key may register at most %d webhook endpoints", store.MaxWebhooksPerKey),
			map[string]any{"limit": store.MaxWebhooksPerKey})
		return
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, createdWebhookJSON{
		webhookJSON: toWebhookJSON(created),
		Secret:      secret,
	})
}

// @Summary List registered webhooks without their secrets
// @Description Returns all registered webhook destinations without sensitive secret material.
// @Tags webhooks
// @Produce json
// @Success 200 {object} map[string]any
// @Failure 401 {object} map[string]string
// @Failure 503 {object} map[string]string
// @Router /v1/webhooks [get]
func (s *Server) listWebhooks(w http.ResponseWriter, r *http.Request) {
	if !s.webhookConfigured(w) {
		return
	}
	ownerKeyHash, isAdmin, ok := s.authorizeWebhookAPIKey(w, r)
	if !ok {
		return
	}
	// A key sees the endpoints it registered; the admin token sees every
	// endpoint, which is what an operator needs when one destination fails.
	var webhooks []store.Webhook
	var err error
	if isAdmin {
		webhooks, err = s.WebhookStore.ListWebhooks(r.Context())
	} else {
		webhooks, err = s.WebhookStore.ListWebhooksForKey(r.Context(), ownerKeyHash)
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	out := make([]webhookJSON, 0, len(webhooks))
	for _, item := range webhooks {
		out = append(out, toWebhookJSON(item))
	}
	writeJSON(w, http.StatusOK, map[string]any{"webhooks": out})
}

// @Summary Delete a registered webhook
// @Description Removes a registered webhook by ID.
// @Tags webhooks
// @Param id path int true "Webhook ID"
// @Success 204 "Webhook deleted"
// @Failure 400 {object} map[string]string
// @Failure 401 {object} map[string]string
// @Failure 404 {object} map[string]string
// @Failure 503 {object} map[string]string
// @Router /v1/webhooks/{id} [delete]
func (s *Server) listWebhookDLQ(w http.ResponseWriter, r *http.Request) {
	if !s.webhookConfigured(w) || !s.authorizeWebhookAdmin(w, r) {
		return
	}
	reader, ok := s.WebhookStore.(WebhookDLQReader)
	if !ok {
		writeError(w, http.StatusServiceUnavailable, codeWebhooksNotConfigured, "webhook delivery history is not configured")
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeErrorDetails(w, http.StatusBadRequest, codeInvalidWebhookID,
			"webhook id must be a positive integer", map[string]any{"id": r.PathValue("id")})
		return
	}
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	_, found, err := reader.GetWebhook(r.Context(), id)
	if err != nil {
		s.fail(w, err)
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, codeWebhookNotFound, "webhook not found")
		return
	}
	items, err := reader.ListWebhookDLQ(r.Context(), id, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	out := make([]webhookDeliveryJSON, 0, len(items))
	for _, item := range items {
		out = append(out, webhookDeliveryJSON{
			ID:                item.ID,
			WebhookID:         item.WebhookID,
			Event:             item.Event,
			StatusCode:        item.StatusCode,
			Error:             item.Error,
			DeliveryLatencyMS: item.DeliveryLatencyMS,
			AttemptedAt:       item.AttemptedAt.UTC(),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"deliveries": out})
}

func (s *Server) deleteWebhook(w http.ResponseWriter, r *http.Request) {
	if !s.webhookConfigured(w) {
		return
	}
	ownerKeyHash, isAdmin, ok := s.authorizeWebhookAPIKey(w, r)
	if !ok {
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeErrorDetails(w, http.StatusBadRequest, codeInvalidWebhookID,
			"webhook id must be a positive integer", map[string]any{"id": r.PathValue("id")})
		return
	}
	err = s.WebhookStore.DeleteWebhook(r.Context(), id, ownerKeyHash, isAdmin)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, codeWebhookNotFound, "webhook not found")
		return
	}
	if errors.Is(err, store.ErrForbidden) {
		writeError(w, http.StatusForbidden, codeWebhookForbidden, "API key does not own this webhook")
		return
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) listWebhookDLQ(w http.ResponseWriter, r *http.Request) {
	if !s.webhookConfigured(w) || !s.authorizeWebhookAdmin(w, r) {
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id < 1 {
		writeError(w, http.StatusBadRequest, "webhook id must be a positive integer")
		return
	}
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	_, found, err := s.WebhookStore.GetWebhook(r.Context(), id)
	if err != nil {
		s.fail(w, err)
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "webhook not found")
		return
	}
	items, err := s.WebhookStore.ListWebhookDLQ(r.Context(), id, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	out := make([]webhookDLQJSON, 0, len(items))
	for _, item := range items {
		out = append(out, webhookDLQJSON{
			ID: item.ID, Payload: json.RawMessage(item.Payload), LastError: item.LastError,
			Attempts: item.Attempts, FailedAt: item.FailedAt.UTC(),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"deliveries": out})
}

func (s *Server) retryWebhookDLQ(w http.ResponseWriter, r *http.Request) {
	if !s.webhookConfigured(w) || !s.authorizeWebhookAdmin(w, r) {
		return
	}
	webhookID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || webhookID < 1 {
		writeError(w, http.StatusBadRequest, "webhook id must be a positive integer")
		return
	}
	deliveryID, err := strconv.ParseInt(r.PathValue("delivery_id"), 10, 64)
	if err != nil || deliveryID < 1 {
		writeError(w, http.StatusBadRequest, "delivery id must be a positive integer")
		return
	}
	if s.WebhookRetryer == nil {
		writeError(w, http.StatusServiceUnavailable, "webhook retry is not configured")
		return
	}
	found, err := s.WebhookRetryer.RetryWebhookDLQ(r.Context(), webhookID, deliveryID)
	if err != nil {
		s.fail(w, err)
		return
	}
	if !found {
		writeError(w, http.StatusNotFound, "webhook DLQ entry not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "delivered"})
}

func (s *Server) webhookConfigured(w http.ResponseWriter) bool {
	if s.WebhookStore == nil || s.WebhookCodec == nil || s.WebhookAdminToken == "" {
		writeError(w, http.StatusServiceUnavailable, codeWebhooksNotConfigured, "webhook management is not configured")
		return false
	}
	return true
}

func (s *Server) authorizeWebhookAPIKey(w http.ResponseWriter, r *http.Request) (string, bool, bool) {
	token, ok := bearerToken(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, codeUnauthorized, "unauthorized")
		return "", false, false
	}
	if sameToken(token, s.WebhookAdminToken) {
		return webhook.KeyID(token), true, true
	}
	for _, key := range s.WebhookAPIKeys {
		if sameToken(token, key) {
			return webhook.KeyID(token), false, true
		}
	}
	writeError(w, http.StatusUnauthorized, codeUnauthorized, "unauthorized")
	return "", false, false
}

func bearerToken(r *http.Request) (string, bool) {
	header := r.Header.Get("Authorization")
	token := strings.TrimPrefix(header, "Bearer ")
	return token, token != header && token != ""
}

func sameToken(token, expected string) bool {
	return len(token) == len(expected) &&
		subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1
}

// ---- coverage webhooks ----------------------------------------------------

// coverageWebhookRequest is the body of POST /v1/coverage-webhooks.
type coverageWebhookRequest struct {
	URL     string `json:"url"`
	SAC     string `json:"sac"`
	MinBps  uint32 `json:"min_bps"`
	MinTier uint32 `json:"min_tier"`
}

func (s *Server) createCoverageWebhook(w http.ResponseWriter, r *http.Request) {
	if !s.requireCoverageAdmin(w, r) {
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	var req coverageWebhookRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, codeInvalidBody, "body must be a JSON object")
		return
	}
	req.URL = strings.TrimSpace(req.URL)
	if code, msg := validateCoverageWebhook(req); code != "" {
		writeError(w, http.StatusBadRequest, code, msg)
		return
	}
	wh, err := s.CoverageWebhooks.CreateCoverageWebhook(r.Context(), coverage.Webhook{
		URL: req.URL, SAC: req.SAC, MinBps: req.MinBps, MinTier: req.MinTier,
	})
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, wh)
}

func (s *Server) deleteCoverageWebhook(w http.ResponseWriter, r *http.Request) {
	if !s.requireCoverageAdmin(w, r) {
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeErrorDetails(w, http.StatusBadRequest, codeInvalidWebhookID,
			"webhook id must be a positive integer", map[string]any{"id": r.PathValue("id")})
		return
	}
	err = s.CoverageWebhooks.DeleteCoverageWebhook(r.Context(), id)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, codeWebhookNotFound, "webhook not found")
		return
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// requireCoverageAdmin checks the bearer token on coverage alert
// administration requests. It writes the response itself and returns false
// when the request is refused.
func (s *Server) requireCoverageAdmin(w http.ResponseWriter, r *http.Request) bool {
	if s.WebhookAdminToken == "" || s.CoverageWebhooks == nil || s.WebhookSigningSecret == "" {
		writeError(w, http.StatusServiceUnavailable, codeWebhooksNotConfigured, "coverage webhook administration is disabled")
		return false
	}
	const prefix = "Bearer "
	token := ""
	if auth := r.Header.Get("Authorization"); strings.HasPrefix(auth, prefix) {
		token = strings.TrimSpace(auth[len(prefix):])
	}
	if subtle.ConstantTimeCompare([]byte(token), []byte(s.WebhookAdminToken)) != 1 {
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeError(w, http.StatusUnauthorized, codeUnauthorized, "invalid admin token")
		return false
	}
	return true
}

// validateCoverageWebhook returns an error code and message, both empty when
// the request is valid.
func validateCoverageWebhook(req coverageWebhookRequest) (code, msg string) {
	u, err := url.Parse(req.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return codeInvalidWebhookURL, "url must be an absolute http or https URL"
	}
	if req.SAC == "" {
		return codeInvalidSAC, "sac is required"
	}
	if req.MinBps < 1 || req.MinBps > 10_000 {
		return codeInvalidBody, "min_bps must be between 1 and 10000"
	}
	if req.MinTier < 1 || req.MinTier > 3 {
		return codeInvalidBody, "min_tier must be 1, 2 or 3"
	}
	return "", ""
}

// ---- helpers --------------------------------------------------------------

// parseSAC reads the {sac} path value and rejects anything that is not a
// contract strkey, so unlisted ids answer 404 instead of passing arbitrary
// strings through to storage.
func parseSAC(w http.ResponseWriter, r *http.Request) (string, bool) {
	sac := r.PathValue("sac")
	if !strkey.IsValidContractAddress(sac) {
		writeError(w, http.StatusBadRequest, codeInvalidSAC, "sac must be a contract strkey")
		return "", false
	}
	return sac, true
}

// requireAsset answers 404 unless the asset is listed, so its history
// endpoints match /v1/assets/{sac} instead of returning an empty list.
func (s *Server) requireAsset(w http.ResponseWriter, r *http.Request, sac string) bool {
	_, err := s.Store.GetAsset(r.Context(), sac)
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, codeAssetNotListed, "asset not listed")
		return false
	}
	if err != nil {
		s.fail(w, err)
		return false
	}
	return true
}

// isBreakdownHash reports whether hash is a SHA-256 digest in hex.
func isBreakdownHash(hash string) bool {
	if len(hash) != 64 {
		return false
	}
	_, err := hex.DecodeString(hash)
	return err == nil
}

func parseTier(w http.ResponseWriter, r *http.Request) (uint32, bool) {
	v := r.URL.Query().Get("min_tier")
	if v == "" {
		return coverage.TierTranscribed, true
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil || n < 1 || n > 3 {
		writeErrorDetails(w, http.StatusBadRequest, codeInvalidMinTier,
			"min_tier must be 1, 2 or 3", map[string]any{"parameter": "min_tier", "value": v})
		return 0, false
	}
	return uint32(n), true
}

func parseAssetLimit(w http.ResponseWriter, r *http.Request) (int, bool) {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return 50, true
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > 200 {
		writeError(w, http.StatusBadRequest, codeInvalidLimit, "limit must be between 1 and 200")
		return 0, false
	}
	return n, true
}

func parseAssetCursor(w http.ResponseWriter, r *http.Request) (*assetCursor, bool) {
	v := r.URL.Query().Get("cursor")
	if v == "" {
		return nil, true
	}
	raw, err := base64.RawURLEncoding.DecodeString(v)
	if err != nil || base64.RawURLEncoding.EncodeToString(raw) != v {
		writeError(w, http.StatusBadRequest, codeInvalidCursor, "invalid cursor")
		return nil, false
	}
	var cursor assetCursor
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cursor); err != nil || cursor.Version != 1 || cursor.Code == "" || cursor.Issuer == "" {
		writeError(w, http.StatusBadRequest, codeInvalidCursor, "invalid cursor")
		return nil, false
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		writeError(w, http.StatusBadRequest, codeInvalidCursor, "invalid cursor")
		return nil, false
	}
	return &cursor, true
}

func encodeAssetCursor(cursor assetCursor) string {
	raw, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func sortAssets(assets []coverage.Asset) {
	sort.Slice(assets, func(i, j int) bool {
		if assets[i].Code != assets[j].Code {
			return assets[i].Code < assets[j].Code
		}
		return assets[i].Issuer < assets[j].Issuer
	})
const (
	minSeriesInterval = 5 * time.Minute
	maxSeriesInterval = 7 * 24 * time.Hour
	maxSeriesPoints   = 1000
)

// parseTimeQuery reads a UTC RFC 3339 query parameter, falling back to def.
func parseTimeQuery(w http.ResponseWriter, r *http.Request, name string, def time.Time) (time.Time, bool) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return def, true
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		writeError(w, http.StatusBadRequest, name+" must be an RFC 3339 timestamp")
		return time.Time{}, false
	}
	return t.UTC(), true
}

// parseInterval reads the series step. Go's duration syntax has no day unit, so
// a trailing d (as in 7d) is expanded before parsing.
func parseInterval(w http.ResponseWriter, r *http.Request) (time.Duration, string, bool) {
	raw := r.URL.Query().Get("interval")
	if raw == "" {
		return time.Hour, "1h", true
	}
	var (
		d   time.Duration
		err error
	)
	if days, ok := strings.CutSuffix(raw, "d"); ok {
		n, convErr := strconv.Atoi(days)
		if convErr != nil {
			err = convErr
		} else {
			d = time.Duration(n) * 24 * time.Hour
		}
	} else {
		d, err = time.ParseDuration(raw)
	}
	if err != nil || d < minSeriesInterval || d > maxSeriesInterval {
		writeError(w, http.StatusBadRequest, "interval must be between 5m and 7d")
		return 0, "", false
	}
	return d, raw, true
}

func parseLimit(w http.ResponseWriter, r *http.Request) (int, bool) {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return 100, true
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > 1000 {
		writeErrorDetails(w, http.StatusBadRequest, codeInvalidLimit,
			"limit must be between 1 and 1000", map[string]any{"parameter": "limit", "value": v})
		return 0, false
	}
	return n, true
}

func parseDateRange(w http.ResponseWriter, r *http.Request) (*time.Time, *time.Time, bool) {
	parse := func(name string) (*time.Time, bool) {
		value := r.URL.Query().Get(name)
		if value == "" {
			return nil, true
		}
		date, err := time.Parse("2006-01-02", value)
		if err != nil {
			writeError(w, http.StatusBadRequest, codeInvalidDate, name+" must be a date in YYYY-MM-DD format")
			return nil, false
		}
		date = date.UTC()
		if name == "to" {
			date = date.AddDate(0, 0, 1)
		}
		return &date, true
	}
	from, ok := parse("from")
	if !ok {
		return nil, nil, false
	}
	to, ok := parse("to")
	if !ok {
		return nil, nil, false
	}
	if from != nil && to != nil && !from.Before(*to) {
		writeError(w, http.StatusBadRequest, codeInvalidDateRange, "from must be on or before to")
		return nil, nil, false
	}
	return from, to, true
func makeAuditCursor(cursor store.AuditCursor) string {
	payload := strconv.AppendUint(nil, uint64(cursor.Ledger), 10)
	payload = append(payload, ':')
	payload = append(payload, cursor.ID...)
	return base64.RawURLEncoding.EncodeToString(payload)
}

func parseAuditCursor(token string) (*store.AuditCursor, bool) {
	payload, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, false
	}
	ledgerText, id, found := strings.Cut(string(payload), ":")
	ledger, err := strconv.ParseUint(ledgerText, 10, 32)
	if !found || err != nil || id == "" {
		return nil, false
	}
	return &store.AuditCursor{Ledger: uint32(ledger), ID: id}, true
}

func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// A wildcard answer is identical for every origin, so a cache may reuse
		// it. A restricted answer depends on the request's origin, and must say
		// so even when the origin is refused: otherwise a cache can store the
		// refused response (which has no Access-Control-Allow-Origin) and serve
		// it to an allowed origin.
		wildcard := false
		for _, allowed := range s.CORS {
			if allowed == "*" {
				wildcard = true
				break
			}
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		w.Header().Set("Access-Control-Expose-Headers", "X-Total-Count")
		if r.Method == http.MethodOptions {
			w.Header().Set("Access-Control-Allow-Headers", corsAllowHeaders)
			w.Header().Set("Access-Control-Max-Age", corsMaxAge)
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	s.Log.Error("request failed", "err", err)
	writeError(w, http.StatusInternalServerError, codeInternal, "internal error")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// ---- errors ---------------------------------------------------------------

// errorBody is the body of every 4xx and 5xx response. Code is stable and
// machine-readable so clients can switch on it, Message is for humans, and
// Details is optional context, omitted when there is nothing to add.
type errorBody struct {
	Code    string         `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

// Error codes are part of the API: clients match them literally, so renaming
// one is a breaking change.
const (
	codeNotFound              = "not_found"
	codeMethodNotAllowed      = "method_not_allowed"
	codeInvalidSAC            = "invalid_sac"
	codeAssetNotListed        = "asset_not_listed"
	codeInvalidBreakdownHash  = "invalid_breakdown_hash"
	codeBreakdownNotFound     = "breakdown_not_found"
	codeCoverageNotFound      = "coverage_not_found"
	codeInvalidMinTier        = "invalid_min_tier"
	codeInvalidLimit          = "invalid_limit"
	codeInvalidDate           = "invalid_date"
	codeInvalidOffset         = "invalid_offset"
	codeInvalidStatus         = "invalid_status"
	codeInvalidMinBps         = "invalid_min_bps"
	codeInvalidMaxAge         = "invalid_max_age"
	codeInvalidCursor         = "invalid_cursor"
	codeInvalidBody           = "invalid_body"
	codeInvalidCursor         = "invalid_cursor"
	codeInvalidDateRange      = "invalid_date_range"
	codeInvalidWebhookURL     = "invalid_webhook_url"
	codeInvalidWebhookID      = "invalid_webhook_id"
	codeWebhookNotFound       = "webhook_not_found"
	codeWebhookLimitReached   = "webhook_limit_reached"
	codeWebhookForbidden      = "webhook_forbidden"
	codeWebhooksNotConfigured = "webhooks_not_configured"
	codeUnauthorized          = "unauthorized"
	codeAuthUnavailable       = "auth_unavailable"
	codeInternal              = "internal_error"
	codeAPIAuthNotConfigured  = "api_auth_not_configured"
	codeDBUnavailable         = "db_unavailable"
	codeProgressUnavailable   = "progress_unavailable"
	codeVaultNotFound         = "vault_not_found"
	codeInvalidIssuer         = "invalid_issuer"
	codeIssuerTomlNotFound    = "issuer_toml_not_found"
)

// errorEnvelope rewrites the plain-text 404 and 405 that net/http's mux writes
// for an unknown path or method, so every failure leaves this API with the same
// JSON shape. Responses that are already JSON pass through untouched.
func errorEnvelope(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ew := &errorWriter{ResponseWriter: w}
		next.ServeHTTP(ew, r)
		ew.flush()
	})
}

type errorWriter struct {
	http.ResponseWriter
	status  int
	written bool
	replace bool
}

func (w *errorWriter) WriteHeader(status int) {
	if w.written {
		return
	}
	w.written = true
	w.status = status
	if status >= http.StatusBadRequest && !isJSON(w.Header().Get("Content-Type")) {
		// A non-JSON error, which net/http writes itself. Hold the status back
		// and let flush answer with the structured body instead.
		w.replace = true
		return
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *errorWriter) Write(p []byte) (int, error) {
	if !w.written {
		w.WriteHeader(http.StatusOK)
	}
	if w.replace {
		return len(p), nil
	}
	return w.ResponseWriter.Write(p)
}

// flush writes the structured body for a held-back non-JSON error, once the
// handler has finished. It writes nothing for responses that passed through.
func (w *errorWriter) flush() {
	if !w.replace {
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.ResponseWriter.WriteHeader(w.status)
	_ = json.NewEncoder(w.ResponseWriter).Encode(errorBody{
		Code:    codeForStatus(w.status),
		Message: http.StatusText(w.status),
	})
}

func isJSON(contentType string) bool {
	mediaType, _, err := mime.ParseMediaType(contentType)
	return err == nil && mediaType == "application/json"
}

// codeForStatus names the errors only net/http writes, so they carry a code
// like every handler error does.
func codeForStatus(status int) string {
	switch status {
	case http.StatusNotFound:
		return codeNotFound
	case http.StatusMethodNotAllowed:
		return codeMethodNotAllowed
	case http.StatusInternalServerError:
		return codeInternal
	}
	return strings.ReplaceAll(strings.ToLower(http.StatusText(status)), " ", "_")
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorBody{Code: code, Message: message})
}

// writeErrorDetails is writeError with context identifying what was rejected.
func writeErrorDetails(w http.ResponseWriter, status int, code, message string, details map[string]any) {
	writeJSON(w, status, errorBody{Code: code, Message: message, Details: details})
// writeConditionalJSON writes v with an ETag and answers 304 Not Modified when
// the request already holds this representation.
func (s *Server) writeConditionalJSON(w http.ResponseWriter, r *http.Request, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		s.fail(w, err)
		return
	}
	// json.Encoder, which writeJSON uses, terminates every value with a newline;
	// keep it so conditional and unconditional responses are byte-identical.
	s.writeConditional(w, r, status, "application/json", append(body, '\n'))
}

// writeConditional writes body with an ETag derived from its hash and answers
// 304 Not Modified, without a body, when If-None-Match names that ETag. The
// validator is the hash of the exact bytes served, so it changes whenever the
// representation does and never when it does not.
func (s *Server) writeConditional(w http.ResponseWriter, r *http.Request, status int, contentType string, body []byte) {
	sum := sha256.Sum256(body)
	etag := `"` + hex.EncodeToString(sum[:]) + `"`
	w.Header().Set("ETag", etag)
	if etagMatches(r.Header.Get("If-None-Match"), etag) {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// etagMatches reports whether an If-None-Match header names etag. The
// comparison is the weak one RFC 9110 requires for If-None-Match: a W/ prefix
// and surrounding whitespace are ignored, the header may list several tags, and
// "*" matches any current representation.
func etagMatches(header, etag string) bool {
	for _, candidate := range strings.Split(header, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "*" || strings.TrimPrefix(candidate, "W/") == etag {
			return true
		}
	}
	return false
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
