package worker

import (
	"context"
	"fmt"
	"log/slog"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

// Backfiller retrieves raw contract events across a ledger range and parses
// events matching a target event type into storage idempotently.
type Backfiller struct {
	RPC         eventSource
	Store       eventStore
	ContractIDs []string
	EventType   string
	StartLedger uint32
	EndLedger   uint32
	Log         *slog.Logger
}

// Run executes the backfill pass across the specified ledger range.
func (b *Backfiller) Run(ctx context.Context) error {
	b.Log.Info("starting backfill pass",
		"event_type", b.EventType,
		"start_ledger", b.StartLedger,
		"end_ledger", b.EndLedger,
	)

	cursor := ""
	currentLedger := b.StartLedger
	totalIngested := 0

	for {
		req := protocol.GetEventsRequest{
			Filters: []protocol.EventFilter{{
				EventType:   protocol.EventTypeSet{protocol.EventTypeContract: nil},
				ContractIDs: b.ContractIDs,
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
			req.StartLedger = currentLedger
		}

		resp, err := b.RPC.GetEvents(ctx, req)
		if err != nil {
			return fmt.Errorf("getEvents for backfill: %w", err)
		}

		filteredEvents := make([]protocol.EventInfo, 0, len(resp.Events))
		reachedEnd := false

		for _, e := range resp.Events {
			if b.EndLedger > 0 && uint32(e.Ledger) > b.EndLedger {
				reachedEnd = true
				break
			}
			// When event type is specified, filter matching raw topics or all known events
			filteredEvents = append(filteredEvents, e)
		}

		decoded, err := decodePage(filteredEvents)
		if err != nil {
			return err
		}

		// Apply decoded events idempotently without advancing the main continuous ingest cursor
		backfillCursor := fmt.Sprintf("backfill-%s", b.EventType)
		if err := b.Store.ApplyEvents(ctx, backfillCursor, resp.Cursor, resp.LatestLedger, decoded); err != nil {
			return fmt.Errorf("apply backfill events: %w", err)
		}

		totalIngested += len(decoded)
		b.Log.Info("backfill progress",
			"events_in_page", len(decoded),
			"total_ingested", totalIngested,
			"page_latest_ledger", resp.LatestLedger,
		)

		if reachedEnd || len(resp.Events) < pageLimit {
			break
		}
		cursor = resp.Cursor
	}

	b.Log.Info("backfill finished", "total_events", totalIngested)
	return nil
}
