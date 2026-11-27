package worker

import (
	"context"
	"fmt"
	"log/slog"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

// replayStore is the storage surface a replay needs. It is deliberately
// narrower than eventStore: a replay must never move the ingest cursor, so it
// cannot reuse ApplyEvents.
type replayStore interface {
	ApplyReplayEvents(ctx context.Context, events []any) (store.ReplaySummary, error)
}

// replayProgressEvery is how many ledgers of the requested range pass between
// progress logs.
const replayProgressEvery = 1000

// Replayer re-ingests a fixed ledger range and upserts the events it finds.
//
// It is safe to run beside the normal ingest loop: rows are keyed by event id,
// reporter changes are only applied when they are not older than what is
// stored, and the ingest cursor is left where it was. Running the same range
// twice therefore produces the same state.
type Replayer struct {
	RPC         eventSource
	Store       replayStore
	ContractIDs []string
	Log         *slog.Logger
}

// Run re-ingests [from, to] once and returns what it processed. In a dry run it
// writes nothing, logs what would be applied, and Store may be nil.
func (r *Replayer) Run(ctx context.Context, from, to uint32, dryRun bool) (store.ReplaySummary, error) {
	var total store.ReplaySummary
	switch {
	case from == 0:
		return total, fmt.Errorf("replay: a first ledger is required")
	case to < from:
		return total, fmt.Errorf("replay: last ledger %d is before first ledger %d", to, from)
	case !dryRun && r.Store == nil:
		return total, fmt.Errorf("replay: a database is required unless --dry-run is set")
	}

	cursor := ""
	// scanned is the highest ledger any returned event came from. Progress is
	// reported off it, so an empty range still ends with a complete line. The
	// counters are uint64 so a range ending near the largest ledger sequence
	// cannot wrap.
	scanned := uint64(from)
	nextProgress := scanned + replayProgressEvery

	for {
		req := protocol.GetEventsRequest{
			Filters: []protocol.EventFilter{{
				EventType:   protocol.EventTypeSet{protocol.EventTypeContract: nil},
				ContractIDs: r.ContractIDs,
			}},
			Pagination: &protocol.PaginationOptions{Limit: pageLimit},
		}
		if cursor == "" {
			req.StartLedger = from
		} else {
			c, err := protocol.ParseCursor(cursor)
			if err != nil {
				return total, fmt.Errorf("replay: parse cursor %q: %w", cursor, err)
			}
			req.Pagination.Cursor = &c
		}

		resp, err := r.RPC.GetEvents(ctx, req)
		if err != nil {
			return total, fmt.Errorf("replay: getEvents from ledger %d: %w", from, err)
		}

		page := make([]any, 0, len(resp.Events))
		pastEnd := false
		for _, e := range resp.Events {
			ledger := uint64(e.Ledger)
			if ledger > scanned {
				scanned = ledger
			}
			if ledger < uint64(from) {
				continue
			}
			if ledger > uint64(to) {
				// RPC has no usable end bound once a cursor is supplied, so
				// the range is enforced here: events past it are dropped and
				// the walk stops after this page.
				pastEnd = true
				continue
			}
			ev, err := soroban.Decode(e)
			if err != nil {
				return total, err
			}
			if ev != nil {
				page = append(page, ev)
			}
		}

		if len(page) > 0 {
			if dryRun {
				pageSummary := store.SummarizeReplay(page)
				total.Add(pageSummary)
				r.Log.Info("replay would upsert",
					"events", pageSummary.Total(), "assets", pageSummary.Assets,
					"supply", pageSummary.Supplies, "reports", pageSummary.Reports,
					"reporters", pageSummary.Reporters, "revoked_reporters", pageSummary.Revoked,
					"scanned_to", min(scanned, uint64(to)))
			} else {
				applied, err := r.Store.ApplyReplayEvents(ctx, page)
				if err != nil {
					return total, fmt.Errorf("replay: apply events: %w", err)
				}
				total.Add(applied)
				r.Log.Debug("replay batch", "from", from, "to", to,
					"scanned_to", min(scanned, uint64(to)), "events", applied.Total())
			}
		}

		for nextProgress <= scanned && nextProgress <= uint64(to) {
			r.Log.Info("replay progress", "from", from, "to", to,
				"scanned_to", nextProgress, "events", total.Total(), "dry_run", dryRun)
			nextProgress += replayProgressEvery
		}

		next := resp.Cursor
		if pastEnd || len(resp.Events) < pageLimit || next == "" || next == cursor {
			break
		}
		cursor = next
	}

	r.Log.Info("replay complete", "from", from, "to", to, "dry_run", dryRun,
		"events", total.Total(), "assets", total.Assets, "supply", total.Supplies,
		"reports", total.Reports, "reporters", total.Reporters, "revoked_reporters", total.Revoked)
	return total, nil
}
