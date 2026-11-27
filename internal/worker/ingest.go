// Package worker runs the indexer's periodic jobs.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

const (
	// reporterCursorName is the cursor of the registry watcher. It is separate
	// from cursorName so the tight reporter loop and the main ingest loop do
	// not move each other's position.
	reporterCursorName = "reporters"
	pageLimit          = 200
)

// eventSource is the RPC surface ingestion needs.
type eventSource interface {
	GetEvents(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error)
	GetLatestLedger(ctx context.Context) (protocol.GetLatestLedgerResponse, error)
}

// eventStore is the storage surface ingestion needs.
type eventStore interface {
	Progress(ctx context.Context, name string) (store.Progress, error)
	ApplyEvents(ctx context.Context, name, cursor string, ledger uint32, raw []soroban.RawEvent, events []any) error
}

// Ingester copies Plimsoll contract events from RPC into Postgres.
type Ingester struct {
	RPC         eventSource
	Store       eventStore
	ContractIDs []string
	// RegistryID is the reporter registry contract. RunReporters watches it
	// alone, on its own cursor, so registrations are indexed promptly.
	RegistryID  string
	StartLedger uint32
	Workers     int
	Log         *slog.Logger
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

		resp, err := in.RPC.GetEvents(ctx, req)
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
		if len(decoded) > 0 {
			in.Log.Info("ingested events", "stream", cursorName, "count", len(decoded), "latest_ledger", resp.LatestLedger)
		}
		cursor = resp.Cursor
		if len(resp.Events) < pageLimit {
			return nil
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
	if err != nil {
		return 0, fmt.Errorf("getLatestLedger: %w", err)
	}
	start := in.StartLedger
	if start == 0 || start > latest.Sequence {
		start = latest.Sequence
	}
	// RPC keeps roughly 7 days; asking for older ledgers is an error.
	probe, err := in.RPC.GetEvents(ctx, protocol.GetEventsRequest{
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
