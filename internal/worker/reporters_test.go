package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

// reporterSetPage is one getEvents page carrying a single reporter_set event,
// in the shape testnet RPC serves it.
func reporterSetPage(t *testing.T, name string) []protocol.EventInfo {
	t.Helper()
	reporter := must(t)(soroban.AddressVal("GDACI55IMI5DDZWMZ2EAXWK57AV75VE4626YYBAHQJYDV7QFPNNFK6HJ"))
	value := mapVal(t, map[string]xdr.ScVal{
		"role": soroban.U32Val(coverage.RoleAuditor),
		"name": strVal(t, name),
	})
	body := fmt.Sprintf(rpcPageWithoutSuccessFlag,
		b64(t, symVal(t, soroban.EventReporterSet)), b64(t, reporter), b64(t, value))
	var page []protocol.EventInfo
	if err := json.Unmarshal([]byte(body), &page); err != nil {
		t.Fatal(err)
	}
	return page
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// A registration is indexed by the watcher on its own cursor, so the API can
// answer with the new reporter without waiting for the 15-minute reconcile or
// the next two-ledger ingest tick.
func TestReporterWatcherIndexesRegistrationsImmediately(t *testing.T) {
	const name = "Acme Audits"
	page := reporterSetPage(t, name)
	page[0].ID = "reporter-registration-1"
	page[0].Ledger = 900
	rpc := &fakeRPC{
		latest: 900,
		pages: map[string]protocol.GetEventsResponse{
			"": {Events: page, Cursor: "reporter-cursor-1"},
		},
	}
	db := &fakeStore{}
	ingester := &Ingester{
		RPC:        rpc,
		Store:      db,
		RegistryID: "CREGISTRY",
		Log:        testLogger(),
	}

	if err := ingester.RunReporters(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(db.applied) != 1 || len(db.applied[0]) != 1 {
		t.Fatalf("applied %v, want one page with one reporter event", db.applied)
	}
	got, ok := db.applied[0][0].(soroban.ReporterSet)
	if !ok {
		t.Fatalf("stored %T, want soroban.ReporterSet", db.applied[0][0])
	}
	if got.Reporter == "" || got.Role != coverage.RoleAuditor || got.Name != name {
		t.Errorf("reporter event = %+v", got)
	}
	if db.names[0] != reporterCursorName {
		t.Errorf("applied under cursor %q, want %q", db.names[0], reporterCursorName)
	}
	if db.cursors[0] != "reporter-cursor-1" || db.ledgers[0] != 900 {
		t.Errorf("cursor=%q ledger=%d", db.cursors[0], db.ledgers[0])
	}
}

// The watcher only stores reporter events: another event from the registry
// contract must not be written to the reporters table.
func TestReporterWatcherDropsOtherRegistryEvents(t *testing.T) {
	page := reporterSetPage(t, "Acme Audits")
	page[0].ID = "other-event"
	page[0].TopicXDR[0] = b64(t, symVal(t, "registry_paused"))
	rpc := &fakeRPC{
		latest: 900,
		pages: map[string]protocol.GetEventsResponse{
			"": {Events: page, Cursor: "reporter-cursor-1"},
		},
	}
	db := &fakeStore{}
	ingester := &Ingester{RPC: rpc, Store: db, RegistryID: "CREGISTRY", Log: testLogger()}

	if err := ingester.RunReporters(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(db.applied) != 1 || len(db.applied[0]) != 0 {
		t.Fatalf("applied %v, want an empty page", db.applied)
	}
	// The cursor still advances, so the event is not re-read every tick.
	if db.cursors[0] != "reporter-cursor-1" {
		t.Errorf("cursor = %q, want it to advance", db.cursors[0])
	}
}

func TestReporterWatcherNeedsARegistry(t *testing.T) {
	ingester := &Ingester{RPC: &fakeRPC{}, Store: &fakeStore{}, Log: testLogger()}
	if err := ingester.RunReporters(context.Background()); err == nil {
		t.Fatal("want an error when no registry contract is configured")
	}
}
