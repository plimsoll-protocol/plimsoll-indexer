package worker

import (
	"context"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

// Events outside the requested range are dropped, and a replay never moves the
// ingest cursor, so it is safe beside the normal ingest loop.
func TestReplayUpsertsOnlyEventsInTheRange(t *testing.T) {
	page := supplyPostedPage(t)
	inRange := page[0]
	inRange.ID = "event-in-range"
	inRange.Ledger = 120
	pastEnd := page[0]
	pastEnd.ID = "event-past-end"
	pastEnd.Ledger = 500

	rpc := &fakeRPC{
		latest: 500,
		pages: map[string]protocol.GetEventsResponse{
			"": {Events: []protocol.EventInfo{inRange, pastEnd}, Cursor: "replay-cursor-1"},
		},
	}
	db := &fakeStore{}
	replayer := &Replayer{
		RPC:         rpc,
		Store:       db,
		ContractIDs: []string{"CCONTRACT"},
		Log:         testLogger(),
	}

	summary, err := replayer.Run(context.Background(), 100, 150, false)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Supplies != 1 || summary.Total() != 1 {
		t.Errorf("summary = %+v, want one supply event", summary)
	}
	if len(db.replayed) != 1 || len(db.replayed[0]) != 1 {
		t.Fatalf("replayed %v, want one page with one event", db.replayed)
	}
	got, ok := db.replayed[0][0].(soroban.SupplyPosted)
	if !ok || got.ID != "event-in-range" {
		t.Fatalf("replayed %T %+v, want the in-range supply event", db.replayed[0][0], got)
	}
	if len(db.cursors) != 0 {
		t.Error("a replay must not move the ingest cursor")
	}
}

func TestReplayDryRunWritesNothing(t *testing.T) {
	page := supplyPostedPage(t)
	page[0].Ledger = 120
	rpc := &fakeRPC{
		latest: 120,
		pages: map[string]protocol.GetEventsResponse{
			"": {Events: page, Cursor: "replay-cursor-1"},
		},
	}
	// No store: a dry run only needs RPC.
	replayer := &Replayer{RPC: rpc, ContractIDs: []string{"CCONTRACT"}, Log: testLogger()}

	summary, err := replayer.Run(context.Background(), 100, 150, true)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Supplies != 1 {
		t.Errorf("summary = %+v, want one supply event", summary)
	}
}

func TestReplayRejectsAnInvertedRange(t *testing.T) {
	replayer := &Replayer{RPC: &fakeRPC{}, Log: testLogger()}
	if _, err := replayer.Run(context.Background(), 200, 100, true); err == nil {
		t.Fatal("want an error when --to is before --from")
	}
	if _, err := replayer.Run(context.Background(), 0, 100, true); err == nil {
		t.Fatal("want an error when --from is zero")
	}
}
