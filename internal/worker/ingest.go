// Package worker runs the indexer's periodic jobs.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/metrics"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

const (
	// reporterCursorName is the cursor of the registry watcher. It is separate
	// from cursorName so the tight reporter loop and the main ingest loop do
	// not move each other's position.
	reporterCursorName = "reporters"
	pageLimit          = 200
	// CursorName is the ingest cursor the indexer resumes from.
	CursorName = "plimsoll-events"
	cursorName = CursorName
	pageLimit  = 200
)

// EventSource is the RPC surface ingestion needs.
type EventSource interface {
	GetEvents(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error)
	GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error)
}

type eventSource = EventSource

// eventStore is the storage surface ingestion needs.
type eventStore interface {
	Progress(ctx context.Context, name string) (store.Progress, error)
	ApplyEvents(ctx context.Context, name, cursor string, ledger uint32, raw []soroban.RawEvent, events []any) error
	ApplyEvents(ctx context.Context, name, cursor string, ledger uint32, events []any) error
	LatestReporterReports(ctx context.Context) ([]store.ReporterLatestReport, error)
}

type ingestMetrics interface {
	SetIngestProgress(ingested, latest uint32)
}

// Ingester copies Plimsoll contract events from RPC into Postgres.
type Ingester struct {
	RPC         EventSource
	RPCs        []EventSource
	RPCURLs     []string
	Store       eventStore
	ContractIDs []string
	// RegistryID is the reporter registry contract. RunReporters watches it
	// alone, on its own cursor, so registrations are indexed promptly.
	RegistryID  string
	StartLedger uint32
	Workers     int
	Log         *slog.Logger
	currentRPC  int
	reportTimes []time.Time
	RPC                    eventSource
	Store                  eventStore
	ContractIDs            []string
	StartLedger            uint32
	ReporterStaleThreshold time.Duration
	Log                    *slog.Logger
	// OnProgress, when set, is called after each page with the ledger the
	// scan has reached. Reindex uses it to report progress every 1 000
	// ledgers; normal ingestion leaves it nil.
	OnProgress func(scannedLedger uint32)
	Metrics     ingestMetrics
}

// Run pulls every available page once. Call it on a ticker.
func (in *Ingester) Run(ctx context.Context) error {
	contractIDs := uniqueStrings(in.ContractIDs)
	if len(contractIDs) == 0 {
		return nil
	}
	if len(contractIDs) == 1 {
		return in.runStream(ctx, store.LegacyIngestCursorName, contractIDs, store.Progress{})
	}

	legacyProgress, err := in.Store.Progress(ctx, store.LegacyIngestCursorName)
	if err != nil {
		return fmt.Errorf("read legacy progress: %w", err)
	}
	streams := make([]ingestStream, len(contractIDs))
	for i, contractID := range contractIDs {
		streams[i] = ingestStream{
			name:        store.ContractIngestCursorName(contractID),
			contractIDs: []string{contractID},
		}
	}
	workers := in.Workers
	if workers <= 0 {
		workers = 2
	}
	if workers > len(streams) {
		workers = len(streams)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	jobs := make(chan ingestStream, len(streams))
	for _, stream := range streams {
		jobs <- stream
	}
	close(jobs)

	var wg sync.WaitGroup
	var once sync.Once
	var runErr error
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for stream := range jobs {
				if err := in.runStream(ctx, stream.name, stream.contractIDs, legacyProgress); err != nil {
					once.Do(func() {
						runErr = err
						cancel()
					})
					return
				}
			}
		}()
	}
	wg.Wait()
	if runErr != nil {
		return runErr
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("ingest streams: %w", err)
	}
	return nil
}

type ingestStream struct {
	name        string
	contractIDs []string
}

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	unique := make([]string, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		unique = append(unique, value)
	}
	return unique
}

func (in *Ingester) runStream(ctx context.Context, cursorName string, contractIDs []string, fallback store.Progress) error {
	progress, err := in.Store.Progress(ctx, cursorName)
	if err != nil {
		return fmt.Errorf("read progress for %s: %w", cursorName, err)
	}
	latestLedger := progress.ScannedLedger
	if in.Metrics != nil {
		latest, err := in.RPC.GetLatestLedger(ctx)
		if err != nil {
			in.Log.Warn("could not refresh ingest metrics", "err", err)
		} else {
			latestLedger = latest.Sequence
			in.Metrics.SetIngestProgress(progress.ScannedLedger, latestLedger)
		}
	}
	cursor := progress.Cursor
	if cursor == "" && progress.ScannedLedger == 0 {
		cursor = fallback.Cursor
		if cursor != "" {
			progress = fallback
		}
	}
	recovered := false
	for {
		req := protocol.GetEventsRequest{
			Filters: []protocol.EventFilter{{
				EventType:   protocol.EventTypeSet{protocol.EventTypeContract: nil},
				ContractIDs: contractIDs,
			}},
			Pagination: &protocol.PaginationOptions{Limit: pageLimit},
		}
		if cursor != "" {
			c, err := protocol.ParseCursor(cursor)
			if err != nil {
				return fmt.Errorf("parse cursor %q: %w", cursor, err)
			}
			req.Pagination.Cursor = &c
		} else {
			start, err := in.startLedger(ctx, contractIDs)
			if err != nil {
				return err
			}
			req.StartLedger = start
		}

		resp, err := in.getEvents(ctx, req)
		if err != nil {
			// A cursor older than RPC's retention window is refused, and
			// retrying the same cursor can only be refused again, so ingestion
			// would stay wedged until someone cleared the cursor by hand.
			// Restart from the oldest ledger RPC still serves. Events in the
			// gap are gone from RPC for good; the warning is the record that
			// they were skipped. Recover once per run so a persistent refusal
			// cannot spin.
			if cursor != "" && !recovered && outsideRetention(err) {
				in.Log.Warn("ingest cursor is older than RPC retention; restarting from the oldest ledger RPC serves and skipping the gap",
					"cursor", cursor, "stream", cursorName, "scanned_to_ledger", progress.ScannedLedger,
					"last_event_ledger", progress.EventLedger, "error", err)
				cursor, recovered = "", true
				continue
			}
			return fmt.Errorf("getEvents: %w", err)
		}
		raw, decoded, err := decodePage(resp.Events)
		if err != nil {
			return err
		}
		if err := in.Store.ApplyEvents(ctx, cursorName, resp.Cursor, resp.LatestLedger, raw, decoded); err != nil {
			return fmt.Errorf("apply events: %w", err)
		}
		if resp.LatestLedger > 0 {
			if resp.LatestLedger >= progress.ScannedLedger && progress.ScannedLedger > 0 {
				metrics.IngestLagLedgers.Set(float64(resp.LatestLedger - progress.ScannedLedger))
			} else {
				metrics.IngestLagLedgers.Set(0)
			}
		}
		for _, ev := range decoded {
			switch ev.(type) {
			case soroban.AssetListed:
				metrics.EventsIngestedTotal.WithLabelValues("asset_listed").Inc()
			case soroban.SupplyPosted:
				metrics.EventsIngestedTotal.WithLabelValues("supply_posted").Inc()
			case soroban.ReservePosted:
				metrics.EventsIngestedTotal.WithLabelValues("reserve_posted").Inc()
				metrics.ReportsIngestedTotal.Inc()
				in.recordReportTime(time.Now())
			case soroban.ReporterSet:
				metrics.EventsIngestedTotal.WithLabelValues("reporter_set").Inc()
			case soroban.ReporterRevoked:
				metrics.EventsIngestedTotal.WithLabelValues("reporter_revoked").Inc()
		if in.OnProgress != nil {
			if c, err := protocol.ParseCursor(resp.Cursor); err == nil {
				in.OnProgress(c.Ledger)
			}
		if resp.LatestLedger > latestLedger {
			latestLedger = resp.LatestLedger
		}
		if in.Metrics != nil {
			in.Metrics.SetIngestProgress(resp.LatestLedger, latestLedger)
		}
		if len(decoded) > 0 {
			in.Log.Info("ingested events", "stream", cursorName, "count", len(decoded), "latest_ledger", resp.LatestLedger)
		}
		cursor = resp.Cursor
		if len(resp.Events) < pageLimit {
			return in.checkStaleReporters(ctx)
		}
	}
}

// RunReporters indexes reporter_set and reporter_revoked events from the
// registry contract on their own cursor.
//
// The main ingest loop already stores these events, but only on
// INGEST_INTERVAL (10s by default), which is two ledgers. A registration is
// metadata, not history, so waiting for the periodic reconcile (15m) or even
// another two ledgers is avoidable: this loop runs on REPORTER_INTERVAL (5s by
// default, about one ledger), which puts a new reporter at /v1/reporters
// within one ledger of registration. Writing the same event twice is
// harmless: rows are keyed by event id and reporter changes are ordered by
// ledger.
func (in *Ingester) RunReporters(ctx context.Context) error {
	if in.RegistryID == "" {
		return errors.New("reporter registry id is not configured")
	}
	progress, err := in.Store.Progress(ctx, reporterCursorName)
	if err != nil {
		return fmt.Errorf("read reporter progress: %w", err)
	}
	cursor := progress.Cursor
	recovered := false
	for {
		req := protocol.GetEventsRequest{
			Filters: []protocol.EventFilter{{
				EventType:   protocol.EventTypeSet{protocol.EventTypeContract: nil},
				ContractIDs: []string{in.RegistryID},
			}},
			Pagination: &protocol.PaginationOptions{Limit: pageLimit},
		}
		if cursor != "" {
			c, err := protocol.ParseCursor(cursor)
			if err != nil {
				return fmt.Errorf("parse reporter cursor %q: %w", cursor, err)
			}
			req.Pagination.Cursor = &c
		} else {
			start, err := in.startLedger(ctx, []string{in.RegistryID})
			if err != nil {
				return err
			}
			req.StartLedger = start
		}

		resp, err := in.RPC.GetEvents(ctx, req)
		if err != nil {
			if cursor != "" && !recovered && outsideRetention(err) {
				in.Log.Warn("reporter cursor is older than RPC retention; restarting from the oldest ledger RPC serves",
					"cursor", cursor, "error", err)
				cursor, recovered = "", true
				continue
			}
			return fmt.Errorf("getEvents for reporters: %w", err)
		}
		raw, decoded, err := decodePage(resp.Events)
		if err != nil {
			return err
		}
		events := reporterEvents(decoded)
		if err := in.Store.ApplyEvents(ctx, reporterCursorName, resp.Cursor, resp.LatestLedger, raw, events); err != nil {
			return fmt.Errorf("apply reporter events: %w", err)
		}
		if len(events) > 0 {
			in.Log.Info("indexed reporter events", "count", len(events), "latest_ledger", resp.LatestLedger)
		}
		cursor = resp.Cursor
		if len(resp.Events) < pageLimit {
			return nil
		}
	}
}

// reporterEvents keeps only the registry events the reporters table is built
// from, so another event type on the same contract cannot move this cursor's
// work along.
func reporterEvents(events []any) []any {
	out := make([]any, 0, len(events))
	for _, ev := range events {
		switch ev.(type) {
		case soroban.ReporterSet, soroban.ReporterRevoked:
			out = append(out, ev)
		}
	}
	return out
func (in *Ingester) recordReportTime(now time.Time) {
	cutoff := now.Add(-time.Minute)
	valid := 0
	for _, t := range in.reportTimes {
		if t.After(cutoff) {
			in.reportTimes[valid] = t
			valid++
		}
	}
	in.reportTimes = append(in.reportTimes[:valid], now)
	metrics.ReportsPerMinute.Set(float64(len(in.reportTimes)))
func (in *Ingester) checkStaleReporters(ctx context.Context) error {
	if in.ReporterStaleThreshold == 0 {
		return nil
	}
	reports, err := in.Store.LatestReporterReports(ctx)
	if err != nil {
		return fmt.Errorf("check stale reporters: %w", err)
	}
	now := time.Now()
	for _, report := range reports {
		age := now.Sub(report.AsOf)
		if age > in.ReporterStaleThreshold {
			in.Log.Warn("reporter report is stale",
				"reporter", report.Reporter,
				"age", age.Round(time.Second).String(),
				"report_as_of", report.AsOf.UTC(),
				"threshold", in.ReporterStaleThreshold.String())
		}
	}
	return nil
}

// outsideRetention reports whether RPC refused the request because the ledger
// it asked for has aged out. RPC answers -32600 ("invalid request") for this
// alongside genuinely malformed requests, so the message is the only thing
// that distinguishes it.
func outsideRetention(err error) bool {
	return strings.Contains(err.Error(), "must be within the ledger range")
}

// decodePage turns one page of RPC events into the typed events this indexer
// stores. raw keeps every event in the page, including the ones the decoder
// does not track, so they can be decoded later (see store.ReplayRawEvents).
//
// Every event a page carries is kept. RPC used to mark events raised by calls
// that later failed with inSuccessfulContractCall=false, and stopped sending
// the field once it only ever returned events from successful calls. Filtering
// on it silently dropped every event once the field went missing, which wedged
// ingestion, so the filter is gone rather than defaulted either way.
func decodePage(events []protocol.EventInfo) ([]soroban.RawEvent, []any, error) {
	raw := make([]soroban.RawEvent, 0, len(events))
	decoded := make([]any, 0, len(events))
	for _, e := range events {
		ev, err := soroban.Decode(e)
		if err != nil {
			return nil, nil, err
		}
		raw = append(raw, soroban.NewRawEvent(e))
		if ev != nil {
			decoded = append(decoded, ev)
		}
	}
	return raw, decoded, nil
}

// startLedger clamps the configured start to RPC's retention window.
func (in *Ingester) startLedger(ctx context.Context, contractIDs []string) (uint32, error) {
	latest, err := in.RPC.GetLatestLedger(ctx)
func (in *Ingester) startLedger(ctx context.Context) (uint32, error) {
	latest, err := in.getLatestLedger(ctx)
	if err != nil {
		return 0, fmt.Errorf("getLatestLedger: %w", err)
	}
	start := in.StartLedger
	if start == 0 || start > latest.Sequence {
		start = latest.Sequence
	}
	// RPC keeps roughly 7 days; asking for older ledgers is an error.
	probe, err := in.getEvents(ctx, protocol.GetEventsRequest{
		StartLedger: latest.Sequence,
		Filters:     []protocol.EventFilter{{ContractIDs: contractIDs}},
		Pagination:  &protocol.PaginationOptions{Limit: 1},
	})
	if err == nil && start < probe.OldestLedger {
		in.Log.Warn("start ledger is outside RPC retention; earlier events are lost",
			"start_ledger", start, "oldest_ledger", probe.OldestLedger)
		start = probe.OldestLedger
	}
	return start, nil
}

func (in *Ingester) sources() []EventSource {
	if len(in.RPCs) > 0 {
		return in.RPCs
	}
	if in.RPC != nil {
		return []EventSource{in.RPC}
	}
	return nil
}

func (in *Ingester) endpointName(i int) string {
	if i >= 0 && i < len(in.RPCURLs) && in.RPCURLs[i] != "" {
		return in.RPCURLs[i]
	}
	return fmt.Sprintf("endpoint-%d", i)
}

func (in *Ingester) getEvents(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
	srcs := in.sources()
	if len(srcs) <= 1 {
		src := in.RPC
		if len(srcs) == 1 {
			src = srcs[0]
		}
		if src == nil {
			return protocol.GetEventsResponse{}, errors.New("no RPC source configured")
		}
		return src.GetEvents(ctx, req)
	}

	start := in.currentRPC
	n := len(srcs)
	var lastErr error
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		resp, err := srcs[idx].GetEvents(ctx, req)
		if err == nil {
			in.currentRPC = idx
			in.RPC = srcs[idx]
			return resp, nil
		}
		if ctx.Err() != nil {
			return protocol.GetEventsResponse{}, ctx.Err()
		}
		if outsideRetention(err) {
			return protocol.GetEventsResponse{}, err
		}
		lastErr = err
		next := (idx + 1) % n
		in.Log.Warn("rpc endpoint connection failure, failing over to next endpoint",
			"failed_endpoint", in.endpointName(idx),
			"next_endpoint", in.endpointName(next),
			"error", err,
		)
		in.currentRPC = next
		in.RPC = srcs[next]
	}
	in.currentRPC = 0
	in.RPC = srcs[0]
	return protocol.GetEventsResponse{}, fmt.Errorf("all %d RPC endpoints failed: %w", n, lastErr)
}

func (in *Ingester) getLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error) {
	srcs := in.sources()
	if len(srcs) <= 1 {
		src := in.RPC
		if len(srcs) == 1 {
			src = srcs[0]
		}
		if src == nil {
			return protocol.GetLatestLedgerResponse{}, errors.New("no RPC source configured")
		}
		return src.GetLatestLedger(ctx)
	}

	start := in.currentRPC
	n := len(srcs)
	var lastErr error
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		resp, err := srcs[idx].GetLatestLedger(ctx)
		if err == nil {
			in.currentRPC = idx
			in.RPC = srcs[idx]
			return resp, nil
		}
		if ctx.Err() != nil {
			return protocol.GetLatestLedgerResponse{}, ctx.Err()
		}
		lastErr = err
		next := (idx + 1) % n
		in.Log.Warn("rpc endpoint connection failure, failing over to next endpoint",
			"failed_endpoint", in.endpointName(idx),
			"next_endpoint", in.endpointName(next),
			"error", err,
		)
		in.currentRPC = next
		in.RPC = srcs[next]
	}
	in.currentRPC = 0
	in.RPC = srcs[0]
	return protocol.GetLatestLedgerResponse{}, fmt.Errorf("all %d RPC endpoints failed: %w", n, lastErr)
}
