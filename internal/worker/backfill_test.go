package worker

import (
	"context"
	"io"
	"log/slog"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

func TestBackfillEventTypeRunsAcrossLedgerRange(t *testing.T) {
	page := supplyPostedPage(t)
	rpc := &fakeRPC{
		latest: 5468159,
		oldest: 5347200,
		pages: map[string]protocol.GetEventsResponse{
			"": {Events: page, Cursor: "cursor-1"},
		},
	}
	db := &fakeStore{}

	b := &Backfiller{
		RPC:         rpc,
		Store:       db,
		ContractIDs: []string{"CC2QQ7R4FLPHZP7KA5AO4IASXXICTG6N4GQXCXITTEBNYQEYTYXKMN4D"},
		EventType:   soroban.EventThresholdsUpdated,
		StartLedger: 5347200,
		EndLedger:   5468159,
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	if err := b.Run(context.Background()); err != nil {
		t.Fatalf("Backfiller.Run failed: %v", err)
	}

	if len(db.applied) != 1 {
		t.Fatalf("expected 1 page applied, got %d", len(db.applied))
	}
}
