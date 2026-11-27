// Package api serves Plimsoll's read API over HTTP.
package api

import (
	"context"
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
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/changelog"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/events"
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
	LatestSupply(ctx context.Context, sac string) (*coverage.Supply, error)
	LatestSupplies(ctx context.Context, sacs []string) (map[string]*coverage.Supply, error)
	SupplyHistory(ctx context.Context, sac string, limit int) ([]coverage.Supply, error)
	LatestReports(ctx context.Context, sac string) (map[uint32]coverage.Report, error)
	LatestReportsBySAC(ctx context.Context, sacs []string) (map[string]map[uint32]coverage.Report, error)
	ReportHistory(ctx context.Context, sac string, limit int) ([]coverage.Report, error)
	ReportExportPage(ctx context.Context, sac string, from, to *time.Time, after *store.ReportExportCursor, limit int) ([]store.ReportExportRow, error)
	Reporters(ctx context.Context) ([]coverage.Reporter, error)
	Disputes(ctx context.Context, sac, status string, limit, offset int) ([]coverage.Dispute, error)
	Breakdown(ctx context.Context, hash string) ([]byte, error)
	Attestation(ctx context.Context, sac string) (store.Attestation, error)
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
	WebhookStore      WebhookManager
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
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /openapi.yaml", s.openAPI)
	mux.HandleFunc("GET /v1/network", s.network)
	mux.HandleFunc("GET /v1/version", s.version)
	mux.HandleFunc("GET /v1/assets", s.listAssets)
	mux.HandleFunc("GET /v1/assets/search", s.searchAssets)
	mux.HandleFunc("GET /v1/assets/{sac}", s.getAsset)
	mux.HandleFunc("HEAD /v1/assets/{sac}", s.headAsset)
	mux.HandleFunc("GET /v1/assets/{sac}/supply", s.supplyHistory)
	mux.HandleFunc("GET /v1/assets/{sac}/reports", s.reportHistory)
	mux.HandleFunc("GET /v1/assets/{sac}/reports/export.csv", s.reportExport)
	mux.HandleFunc("GET /v1/assets/{sac}/coverage/now", s.coverageNow)
	mux.HandleFunc("GET /v1/reporters", s.reporters)
	mux.HandleFunc("GET /v1/changelog", s.changelog)
	mux.HandleFunc("GET /v1/breakdowns/{hash}", s.breakdown)
	mux.HandleFunc("GET /v1/events", s.events)
	mux.HandleFunc("POST /v1/webhooks", s.createWebhook)
	mux.HandleFunc("GET /v1/webhooks", s.listWebhooks)
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

type reporterJSON struct {
	Role string `json:"role"`
	coverage.Reporter
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
	if r == nil {
		return nil
	}
	now := s.Now()
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

func (s *Server) openAPI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(openAPISpec)
}

func (s *Server) network(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Network)
}

func (s *Server) version(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Build)
}

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
	writeJSON(w, http.StatusOK, aj)
}

// headAsset answers whether an asset is listed without building the asset
// document, so clients can check existence cheaply. It never writes a body of
// its own: HEAD has no response body on the wire regardless of what a handler
// writes, and this one only needs a status.
func (s *Server) headAsset(w http.ResponseWriter, r *http.Request) {
	sac, ok := parseSAC(w, r)
	if !ok {
		return
	}
	if _, err := s.Store.GetAsset(r.Context(), sac); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, http.StatusNotFound, codeAssetNotListed, "asset not listed")
			return
		}
		s.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
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
	list, err := s.Store.SupplyHistory(r.Context(), sac, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	out := make([]*supplyJSON, 0, len(list))
	for i := range list {
		out = append(out, toSupply(&list[i]))
	}
	writeJSON(w, http.StatusOK, map[string]any{"supply": out})
}

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
	list, err := s.Store.ReportHistory(r.Context(), sac, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	out := make([]*reportJSON, 0, len(list))
	for _, rep := range list {
		out = append(out, toReport(rep))
	}
	writeJSON(w, http.StatusOK, map[string]any{"reports": out})
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
	}
	writeJSON(w, http.StatusOK, out)
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

func (s *Server) changelog(w http.ResponseWriter, _ *http.Request) {
	entries := s.Changelog
	if entries == nil {
		entries = []changelog.Entry{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"changelog": entries})
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
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

// ---- webhooks -------------------------------------------------------------

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
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
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
}
