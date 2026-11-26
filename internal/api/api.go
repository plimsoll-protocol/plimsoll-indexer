// Package api serves Plimsoll's read API over HTTP.
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
	"io"
	"log/slog"
	"math"
	"mime"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/stellar/go-stellar-sdk/strkey"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/changelog"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/webhook"
)

//go:embed openapi.yaml
var openAPISpec []byte

// Reader is what the API needs from storage.
type Reader interface {
	Ping(ctx context.Context) error
	ListAssets(ctx context.Context) ([]coverage.Asset, error)
	GetAsset(ctx context.Context, sac string) (coverage.Asset, error)
	LatestSupplies(ctx context.Context, sacs []string) (map[string]*coverage.Supply, error)
	SupplyHistory(ctx context.Context, sac string, limit int) ([]coverage.Supply, error)
	LatestReportsBySAC(ctx context.Context, sacs []string) (map[string]map[uint32]coverage.Report, error)
	ReportHistory(ctx context.Context, sac string, limit int) ([]coverage.Report, error)
	ReportExportPage(ctx context.Context, sac string, from, to *time.Time, after *store.ReportExportCursor, limit int) ([]store.ReportExportRow, error)
	Reporters(ctx context.Context) ([]coverage.Reporter, error)
	Breakdown(ctx context.Context, hash string) ([]byte, error)
	Progress(ctx context.Context, name string) (store.Progress, error)
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
	Store             Reader
	WebhookStore      WebhookManager
	WebhookAdminToken string
	WebhookAPIKeys    []string
	WebhookCodec      *webhook.SecretCodec
	Network           NetworkInfo
	CORS              []string
	Log               *slog.Logger
	Now               func() time.Time
	// Changelog is the breaking-change history served at /v1/changelog. It is
	// loaded from the hand-maintained CHANGELOG.yaml at start-up.
	Changelog []changelog.Entry
}

type WebhookManager interface {
	CreateWebhook(context.Context, string, string, []byte, string) (store.Webhook, error)
	ListWebhooks(context.Context) ([]store.Webhook, error)
	DeleteWebhook(ctx context.Context, id int64, ownerKeyHash string, allowLegacyAdmin bool) error
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /openapi.yaml", s.openAPI)
	mux.HandleFunc("GET /v1/network", s.network)
	mux.HandleFunc("GET /v1/assets", s.listAssets)
	mux.HandleFunc("GET /v1/assets/{sac}", s.getAsset)
	mux.HandleFunc("HEAD /v1/assets/{sac}", s.headAsset)
	mux.HandleFunc("GET /v1/assets/{sac}/supply", s.supplyHistory)
	mux.HandleFunc("GET /v1/assets/{sac}/reports", s.reportHistory)
	mux.HandleFunc("GET /v1/assets/{sac}/reports/export.csv", s.reportExport)
	mux.HandleFunc("GET /v1/assets/{sac}/coverage/now", s.coverageNow)
	mux.HandleFunc("GET /v1/reporters", s.reporters)
	mux.HandleFunc("GET /v1/changelog", s.changelog)
	mux.HandleFunc("GET /v1/breakdowns/{hash}", s.breakdown)
	mux.HandleFunc("POST /v1/webhooks", s.createWebhook)
	mux.HandleFunc("GET /v1/webhooks", s.listWebhooks)
	mux.HandleFunc("DELETE /v1/webhooks/{id}", s.deleteWebhook)
	return s.cors(errorEnvelope(mux))
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
	ID        int64     `json:"id"`
	URL       string    `json:"url"`
	CreatedAt time.Time `json:"created_at"`
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
		out = append(out, s.assembleWithData(a, supplies[a.SAC], reports[a.SAC], minTier))
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

func (s *Server) getAsset(w http.ResponseWriter, r *http.Request) {
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
	return s.assembleWithData(a, supplies[a.SAC], reportsBySAC[a.SAC], minTier), nil
}

func (s *Server) assembleWithData(a coverage.Asset, sp *coverage.Supply, reports map[uint32]coverage.Report, minTier uint32) assetJSON {
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
	return out
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
	supply, err := s.Store.LatestSupply(r.Context(), sac)
	if err != nil {
		s.fail(w, err)
		return
	}
	reports, err := s.Store.LatestReports(r.Context(), sac)
	if err != nil {
		s.fail(w, err)
		return
	}
	res := coverage.Compute(supply, reports, coverage.TierTranscribed)
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
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, createdWebhookJSON{
		webhookJSON: webhookJSON{ID: created.ID, URL: created.URL, CreatedAt: created.CreatedAt},
		Secret:      secret,
	})
}

func (s *Server) listWebhooks(w http.ResponseWriter, r *http.Request) {
	if !s.webhookConfigured(w) || !s.authorizeWebhookAdmin(w, r) {
		return
	}
	webhooks, err := s.WebhookStore.ListWebhooks(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	out := make([]webhookJSON, 0, len(webhooks))
	for _, item := range webhooks {
		out = append(out, webhookJSON{ID: item.ID, URL: item.URL, CreatedAt: item.CreatedAt})
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

func (s *Server) authorizeWebhookAdmin(w http.ResponseWriter, r *http.Request) bool {
	token, ok := bearerToken(r)
	if !ok || !sameToken(token, s.WebhookAdminToken) {
		writeError(w, http.StatusUnauthorized, codeUnauthorized, "unauthorized")
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
		return webhookAPIKeyHash(token), true, true
	}
	for _, key := range s.WebhookAPIKeys {
		if sameToken(token, key) {
			return webhookAPIKeyHash(token), false, true
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

func webhookAPIKeyHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
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
		writeError(w, http.StatusBadRequest, "limit must be between 1 and 200")
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
		writeError(w, http.StatusBadRequest, "invalid cursor")
		return nil, false
	}
	var cursor assetCursor
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cursor); err != nil || cursor.Version != 1 || cursor.Code == "" || cursor.Issuer == "" {
		writeError(w, http.StatusBadRequest, "invalid cursor")
		return nil, false
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid cursor")
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
			writeError(w, http.StatusBadRequest, name+" must be a date in YYYY-MM-DD format")
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
		writeError(w, http.StatusBadRequest, "from must be on or before to")
		return nil, nil, false
	}
	return from, to, true
}

func (s *Server) cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		for _, allowed := range s.CORS {
			if allowed == "*" || allowed == origin {
				w.Header().Set("Access-Control-Allow-Origin", allowed)
				w.Header().Set("Vary", "Origin")
				break
			}
		}
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type")
		if r.Method == http.MethodOptions {
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
	codeInvalidBody           = "invalid_body"
	codeInvalidWebhookURL     = "invalid_webhook_url"
	codeInvalidWebhookID      = "invalid_webhook_id"
	codeWebhookNotFound       = "webhook_not_found"
	codeWebhookForbidden      = "webhook_forbidden"
	codeWebhooksNotConfigured = "webhooks_not_configured"
	codeUnauthorized          = "unauthorized"
	codeInternal              = "internal_error"
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
