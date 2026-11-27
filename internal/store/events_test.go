package store

import (
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

type recordingPublisher struct {
	batches [][]any
}

func (p *recordingPublisher) Publish(events []any) {
	p.batches = append(p.batches, append([]any(nil), events...))
}

// A page replayed after a reconnect must not be fanned out again: a live
// subscriber would otherwise see the same event twice.
func TestApplyEventsPublishesOnlyNewEvents(t *testing.T) {
	s, _ := openStore(t)
	pub := &recordingPublisher{}
	s.Publisher = pub
	ctx := context.Background()
	at := time.Date(2026, 11, 25, 6, 0, 0, 0, time.UTC)

	supply := soroban.SupplyPosted{
		Meta:     soroban.Meta{ID: "0000000000000000100-0000000000", Ledger: 100, ClosedAt: at, TxHash: "tx"},
		SAC:      "CSAC",
		Amount:   big.NewInt(500),
		AtLedger: 99, BreakdownHash: "hash", Poster: "GPOSTER",
	}
	reserve := soroban.ReservePosted{
		Meta:   soroban.Meta{ID: "0000000000000000101-0000000000", Ledger: 101, ClosedAt: at, TxHash: "tx2"},
		SAC:    "CSAC",
		Tier:   coverage.TierIssuerSigned,
		Amount: big.NewInt(300),
		AsOf:   at, Reporter: "GREPORTER", DocHash: "doc", DocURI: "https://example.com",
	}

	if err := s.ApplyEvents(ctx, "plimsoll-events", "c1", 101, []any{supply, reserve}); err != nil {
		t.Fatal(err)
	}
	if len(pub.batches) != 1 || len(pub.batches[0]) != 2 {
		t.Fatalf("first page published %v, want one batch of two", pub.batches)
	}

	// The tail of a page comes back after a reconnect alongside one new event.
	next := reserve
	next.ID = "0000000000000000102-0000000000"
	next.Ledger = 102
	if err := s.ApplyEvents(ctx, "plimsoll-events", "c2", 102, []any{supply, reserve, next}); err != nil {
		t.Fatal(err)
	}
	if len(pub.batches) != 2 || len(pub.batches[1]) != 1 {
		t.Fatalf("replayed page published %v, want one batch of one", pub.batches)
	}
	if got := pub.batches[1][0].(soroban.ReservePosted); got.ID != next.ID {
		t.Fatalf("published %q, want only the new event %q", got.ID, next.ID)
	}
}

// EventsSince is the resume path: stored events in chain order, from storage,
// with reconciler-recovered rows left out because they never landed as events.
func TestEventsSinceReplaysStoredEventsInOrder(t *testing.T) {
	s, _ := openStore(t)
	ctx := context.Background()
	at := time.Date(2026, 11, 25, 7, 0, 0, 0, time.UTC)
	const (
		oldestID = "0000000000000000099-0000000000"
		assetID  = "0000000000000000100-0000000000"
		supplyID = "0000000000000000102-0000000000"
		reportID = "0000000000000000103-0000000000"
	)

	asset := soroban.AssetListed{
		Meta: soroban.Meta{ID: assetID, Ledger: 100, ClosedAt: at},
		SAC:  "CSAC", Code: "PUSD", Issuer: "GISS",
	}
	supply := soroban.SupplyPosted{
		Meta:     soroban.Meta{ID: supplyID, Ledger: 102, ClosedAt: at.Add(2 * time.Minute), TxHash: "tx-s"},
		SAC:      "CSAC",
		Amount:   big.NewInt(500),
		AtLedger: 101, BreakdownHash: "hash", Poster: "GPOSTER",
	}
	report := soroban.ReservePosted{
		Meta:   soroban.Meta{ID: reportID, Ledger: 103, ClosedAt: at.Add(3 * time.Minute), TxHash: "tx-r"},
		SAC:    "CSAC",
		Tier:   coverage.TierAuditorSigned,
		Amount: big.NewInt(300),
		AsOf:   at, Reporter: "GREPORTER", DocHash: "doc", DocURI: "https://example.com",
	}
	if err := s.ApplyEvents(ctx, "plimsoll-events", "c1", 103, []any{asset, supply, report}); err != nil {
		t.Fatal(err)
	}
	// A figure the reconciler read back from contract state outlives the event
	// window, but it is not something a stream subscriber missed.
	inserted, err := s.SaveChainSupply(ctx, "CSAC", coverage.Supply{
		Amount: big.NewInt(700), Ledger: 200, PostedAt: at.Add(time.Hour),
		BreakdownHash: "chain-hash", Poster: "GPOSTER",
	})
	if err != nil || !inserted {
		t.Fatalf("save chain supply: inserted=%v err=%v", inserted, err)
	}

	all, err := s.EventsSince(ctx, oldestID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("replayed %d events, want 3 (chain row excluded): %v", len(all), all)
	}
	if got, ok := all[0].(soroban.AssetListed); !ok || got.ID != assetID {
		t.Fatalf("first replayed event = %#v", all[0])
	}
	if got, ok := all[1].(soroban.SupplyPosted); !ok || got.ID != supplyID ||
		got.Amount.Cmp(big.NewInt(500)) != 0 || got.AtLedger != 101 || got.TxHash != "tx-s" {
		t.Fatalf("second replayed event = %#v", all[1])
	}
	if got, ok := all[2].(soroban.ReservePosted); !ok || got.ID != reportID ||
		got.Tier != coverage.TierAuditorSigned || !got.AsOf.Equal(at) || got.AsOf.Location() != time.UTC {
		t.Fatalf("third replayed event = %#v", all[2])
	}

	after, err := s.EventsSince(ctx, supplyID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(after) != 1 || after[0].(soroban.ReservePosted).ID != reportID {
		t.Fatalf("events after supply = %#v", after)
	}

	// No id means no resume; it must not turn into a full-history replay.
	if none, err := s.EventsSince(ctx, "", 10); err != nil || len(none) != 0 {
		t.Fatalf("empty Last-Event-ID replayed %v (err %v)", none, err)
	}

	limited, err := s.EventsSince(ctx, oldestID, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(limited) != 2 {
		t.Fatalf("limited replay returned %d events, want 2", len(limited))
	}
	if limited[1].(soroban.SupplyPosted).ID != supplyID {
		t.Fatalf("limited replay kept %#v, want the oldest two", limited)
	}
}
