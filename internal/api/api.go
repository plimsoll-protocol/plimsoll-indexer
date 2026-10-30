// Package api serves Plimsoll's read API over HTTP.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

// Reader is what the API needs from storage.
type Reader interface {
	Ping(ctx context.Context) error
	ListAssets(ctx context.Context) ([]coverage.Asset, error)
	GetAsset(ctx context.Context, sac string) (coverage.Asset, error)
	LatestSupply(ctx context.Context, sac string) (*coverage.Supply, error)
	SupplyHistory(ctx context.Context, sac string, limit int) ([]coverage.Supply, error)
	LatestReports(ctx context.Context, sac string) (map[uint32]coverage.Report, error)
	ReportHistory(ctx context.Context, sac string, limit int) ([]coverage.Report, error)
	Reporters(ctx context.Context) ([]coverage.Reporter, error)
	Breakdown(ctx context.Context, hash string) ([]byte, error)
	Progress(ctx context.Context, name string) (store.Progress, error)
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
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("GET /v1/network", s.network)
	mux.HandleFunc("GET /v1/assets", s.listAssets)
	mux.HandleFunc("GET /v1/assets/{sac}", s.getAsset)
	mux.HandleFunc("GET /v1/assets/{sac}/supply", s.supplyHistory)
	mux.HandleFunc("GET /v1/assets/{sac}/reports", s.reportHistory)
	mux.HandleFunc("GET /v1/reporters", s.reporters)
	mux.HandleFunc("GET /v1/breakdowns/{hash}", s.breakdown)
	return s.cors(mux)
}

// ---- JSON shapes ----------------------------------------------------------

type supplyJSON struct {
	Amount string `json:"amount"`
	coverage.Supply
}

type reportJSON struct {
	Tier   string `json:"tier"`
	Amount string `json:"amount"`
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
	Toml     json.RawMessage        `json:"toml"`
	Supply   *supplyJSON            `json:"supply"`
	Reports  map[string]*reportJSON `json:"reports"`
	Coverage *coverageJSON          `json:"coverage"`
}

type reporterJSON struct {
	Role string `json:"role"`
	coverage.Reporter
}

func toSupply(sp *coverage.Supply) *supplyJSON {
	if sp == nil {
		return nil
	}
	return &supplyJSON{Amount: sp.Amount.String(), Supply: *sp}
}

func toReport(r coverage.Report) *reportJSON {
	return &reportJSON{Tier: coverage.TierName(r.Tier), Amount: r.Amount.String(), Report: r}
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
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "db_unavailable"})
		return
	}
	p, err := s.Store.Progress(r.Context(), "plimsoll-events")
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "progress_unavailable"})
		return
	}
	body := map[string]any{
		"status":             "ok",
		"ingested_to_ledger": p.ScannedLedger,
		"last_event_ledger":  p.EventLedger,
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

func (s *Server) network(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, s.Network)
}

func (s *Server) listAssets(w http.ResponseWriter, r *http.Request) {
	minTier, ok := parseTier(w, r)
	if !ok {
		return
	}
	assets, err := s.Store.ListAssets(r.Context())
	if err != nil {
		s.fail(w, err)
		return
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
	minTier, ok := parseTier(w, r)
	if !ok {
		return
	}
	a, err := s.Store.GetAsset(r.Context(), r.PathValue("sac"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "asset not listed")
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

func (s *Server) assemble(ctx context.Context, a coverage.Asset, minTier uint32) (assetJSON, error) {
	sp, err := s.Store.LatestSupply(ctx, a.SAC)
	if err != nil {
		return assetJSON{}, err
	}
	reports, err := s.Store.LatestReports(ctx, a.SAC)
	if err != nil {
		return assetJSON{}, err
	}
	out := assetJSON{
		Asset:    a,
		Toml:     json.RawMessage("null"),
		Supply:   toSupply(sp),
		Reports:  map[string]*reportJSON{},
		Coverage: s.toCoverage(coverage.Compute(sp, reports, minTier)),
	}
	if len(a.Toml) > 0 {
		out.Toml = a.Toml
	}
	for tier, rep := range reports {
		out.Reports[coverage.TierName(tier)] = toReport(rep)
	}
	return out, nil
}

func (s *Server) supplyHistory(w http.ResponseWriter, r *http.Request) {
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	list, err := s.Store.SupplyHistory(r.Context(), r.PathValue("sac"), limit)
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
	limit, ok := parseLimit(w, r)
	if !ok {
		return
	}
	list, err := s.Store.ReportHistory(r.Context(), r.PathValue("sac"), limit)
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

func (s *Server) breakdown(w http.ResponseWriter, r *http.Request) {
	body, err := s.Store.Breakdown(r.Context(), r.PathValue("hash"))
	if errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, "unknown breakdown hash")
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

// ---- helpers --------------------------------------------------------------

func parseTier(w http.ResponseWriter, r *http.Request) (uint32, bool) {
	v := r.URL.Query().Get("min_tier")
	if v == "" {
		return coverage.TierTranscribed, true
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil || n < 1 || n > 3 {
		writeError(w, http.StatusBadRequest, "min_tier must be 1, 2 or 3")
		return 0, false
	}
	return uint32(n), true
}

func parseLimit(w http.ResponseWriter, r *http.Request) (int, bool) {
	v := r.URL.Query().Get("limit")
	if v == "" {
		return 100, true
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > 1000 {
		writeError(w, http.StatusBadRequest, "limit must be between 1 and 1000")
		return 0, false
	}
	return n, true
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
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) fail(w http.ResponseWriter, err error) {
	s.Log.Error("request failed", "err", err)
	writeError(w, http.StatusInternalServerError, "internal error")
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
