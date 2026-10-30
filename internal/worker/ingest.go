// Package worker runs the indexer's periodic jobs.
package worker

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/stellar/go-stellar-sdk/clients/rpcclient"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

const (
	cursorName = "plimsoll-events"
	pageLimit  = 200
)

// Ingester copies Plimsoll contract events from RPC into Postgres.
type Ingester struct {
	RPC         *rpcclient.Client
	Store       *store.Store
	ContractIDs []string
	StartLedger uint32
	Log         *slog.Logger
}

// Run pulls every available page once. Call it on a ticker.
func (in *Ingester) Run(ctx context.Context) error {
	progress, err := in.Store.Progress(ctx, cursorName)
	if err != nil {
		return fmt.Errorf("read progress: %w", err)
	}
	cursor := progress.Cursor
	for {
		req := protocol.GetEventsRequest{
			Filters: []protocol.EventFilter{{
				EventType:   protocol.EventTypeSet{protocol.EventTypeContract: nil},
				ContractIDs: in.ContractIDs,
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
			start, err := in.startLedger(ctx)
			if err != nil {
				return err
			}
			req.StartLedger = start
		}

		resp, err := in.RPC.GetEvents(ctx, req)
		if err != nil {
			return fmt.Errorf("getEvents: %w", err)
		}
		decoded, err := decodePage(resp.Events)
		if err != nil {
			return err
		}
		if err := in.Store.ApplyEvents(ctx, cursorName, resp.Cursor, resp.LatestLedger, decoded); err != nil {
			return fmt.Errorf("apply events: %w", err)
		}
		if len(decoded) > 0 {
			in.Log.Info("ingested events", "count", len(decoded), "latest_ledger", resp.LatestLedger)
		}
		cursor = resp.Cursor
		if len(resp.Events) < pageLimit {
			return nil
		}
	}
}

// decodePage turns one page of RPC events into the typed events this indexer
// stores, dropping the ones it does not track.
//
// Every event a page carries is kept. RPC used to mark events raised by calls
// that later failed with inSuccessfulContractCall=false, and stopped sending
// the field once it only ever returned events from successful calls. Filtering
// on it silently dropped every event once the field went missing, which wedged
// ingestion, so the filter is gone rather than defaulted either way.
func decodePage(events []protocol.EventInfo) ([]any, error) {
	decoded := make([]any, 0, len(events))
	for _, e := range events {
		ev, err := soroban.Decode(e)
		if err != nil {
			return nil, err
		}
		if ev != nil {
			decoded = append(decoded, ev)
		}
	}
	return decoded, nil
}

// startLedger clamps the configured start to RPC's retention window.
func (in *Ingester) startLedger(ctx context.Context) (uint32, error) {
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
		Filters:     []protocol.EventFilter{{ContractIDs: in.ContractIDs}},
		Pagination:  &protocol.PaginationOptions{Limit: 1},
	})
	if err == nil && start < probe.OldestLedger {
		in.Log.Warn("start ledger is outside RPC retention; earlier events are lost",
			"start_ledger", start, "oldest_ledger", probe.OldestLedger)
		start = probe.OldestLedger
	}
	return start, nil
}
