package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

// A getEvents page in the shape testnet RPC serves today: no
// inSuccessfulContractCall field at all. Unmarshalling leaves that bool false,
// so filtering on it drops every event and ingestion stops without raising an
// error anywhere.
const rpcPageWithoutSuccessFlag = `[
  {
    "type": "contract",
    "ledger": 5453220,
    "ledgerClosedAt": "2026-10-29T16:02:20Z",
    "contractId": "CC2QQ7R4FLPHZP7KA5AO4IASXXICTG6N4GQXCXITTEBNYQEYTYXKMN4D",
    "id": "0023421401557905408-0000000000",
    "operationIndex": 0,
    "transactionIndex": 3,
    "txHash": "ec9f24cf8f1a805ef9fa98c298df69c29732079af49df66f34a3e9a63f20d711",
    "topic": [%q, %q],
    "value": %q
  }
]`

func symVal(t *testing.T, s string) xdr.ScVal {
	t.Helper()
	sym := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
}

func mapVal(t *testing.T, fields map[string]xdr.ScVal) xdr.ScVal {
	t.Helper()
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	m := make(xdr.ScMap, 0, len(keys))
	for _, k := range keys {
		m = append(m, xdr.ScMapEntry{Key: symVal(t, k), Val: fields[k]})
	}
	mp := &m
	return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &mp}
}

func b64(t *testing.T, v xdr.ScVal) string {
	t.Helper()
	s, err := xdr.MarshalBase64(v)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func must(t *testing.T) func(xdr.ScVal, error) xdr.ScVal {
	t.Helper()
	return func(v xdr.ScVal, err error) xdr.ScVal {
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

func supplyPostedPage(t *testing.T) []protocol.EventInfo {
	t.Helper()
	sac := must(t)(soroban.AddressVal("CCHPT4TEJDPZQDSUVW3NP6A35ROGV4WEFZKT45SKWYCFZSUB7R5HIM7S"))
	value := mapVal(t, map[string]xdr.ScVal{
		"amount":         must(t)(soroban.I128Val(big.NewInt(9_999_000000000))),
		"breakdown_hash": must(t)(soroban.BytesN32Val(strings.Repeat("ab", 32))),
		"ledger":         soroban.U32Val(5453219),
		"poster":         must(t)(soroban.AddressVal("GDACI55IMI5DDZWMZ2EAXWK57AV75VE4626YYBAHQJYDV7QFPNNFK6HJ")),
	})
	body := fmt.Sprintf(rpcPageWithoutSuccessFlag,
		b64(t, symVal(t, soroban.EventSupplyPosted)), b64(t, sac), b64(t, value))

	var page []protocol.EventInfo
	if err := json.Unmarshal([]byte(body), &page); err != nil {
		t.Fatal(err)
	}
	if page[0].InSuccessfulContractCall {
		t.Fatal("fixture must leave inSuccessfulContractCall false")
	}
	return page
}

func TestDecodePageKeepsEventsWithoutSuccessFlag(t *testing.T) {
	raw, decoded, err := decodePage(supplyPostedPage(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) != 1 {
		t.Fatalf("want 1 raw event, got %d", len(raw))
	}
	if len(decoded) != 1 {
		t.Fatalf("want 1 decoded event, got %d", len(decoded))
	}
	sp, ok := decoded[0].(soroban.SupplyPosted)
	if !ok {
		t.Fatalf("got %T", decoded[0])
	}
	if sp.AtLedger != 5453219 || sp.Amount.Int64() != 9_999_000000000 {
		t.Errorf("%+v", sp)
	}
}

// An event the decoder does not track yields no typed row, but it is kept raw
// exactly as RPC sent it, so a later decoder can still read it.
func TestDecodePageKeepsUntrackedEventsRaw(t *testing.T) {
	page := supplyPostedPage(t)
	page[0].TopicXDR[0] = b64(t, symVal(t, "admin_transferred"))
	raw, decoded, err := decodePage(page)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 0 {
		t.Fatalf("want no typed row for an untracked event, got %d", len(decoded))
	}
	if len(raw) != 1 {
		t.Fatalf("want the untracked event kept raw, got %d raw events", len(raw))
	}
	r := raw[0]
	if r.ID != page[0].ID || r.Contract != page[0].ContractID || r.TxHash != page[0].TransactionHash ||
		r.Ledger != uint32(page[0].Ledger) || r.ValueXDR != page[0].ValueXDR ||
		strings.Join(r.TopicXDR, ",") != strings.Join(page[0].TopicXDR, ",") {
		t.Fatalf("raw event %+v does not match RPC's %+v", r, page[0])
	}
	if got := r.ClosedAt.Format(time.RFC3339); got != page[0].LedgerClosedAt {
		t.Errorf("closed at %s, want %s", got, page[0].LedgerClosedAt)
	}
}

// ---- Run ------------------------------------------------------------------

type fakeRPC struct {
	// pages keyed by the cursor asked for; "" is a start-ledger request.
	pages   map[string]protocol.GetEventsResponse
	errs    map[string]error
	latest  uint32
	asked   []string
	oldest  uint32
	askedSL []uint32
}

func (f *fakeRPC) GetEvents(_ context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
	key := ""
	if req.Pagination != nil && req.Pagination.Cursor != nil {
		key = req.Pagination.Cursor.String()
	}
	if key == "" {
		f.askedSL = append(f.askedSL, req.StartLedger)
	}
	f.asked = append(f.asked, key)
	if err := f.errs[key]; err != nil {
		return protocol.GetEventsResponse{}, err
	}
	resp := f.pages[key]
	if resp.LatestLedger == 0 {
		resp.LatestLedger = f.latest
	}
	if resp.OldestLedger == 0 {
		resp.OldestLedger = f.oldest
	}
	return resp, nil
}

func (f *fakeRPC) GetLatestLedger(context.Context) (protocol.GetLatestLedgerResponse, error) {
	if err := f.errs["latest"]; err != nil {
		return protocol.GetLatestLedgerResponse{}, err
	}
	return protocol.GetLatestLedgerResponse{Sequence: f.latest}, nil
}

type fakeStore struct {
	progress store.Progress
	raw      [][]soroban.RawEvent
	applied  [][]any
	names    []string
	cursors  []string
	ledgers  []uint32
	replayed [][]any
	progress     store.Progress
	applied      [][]any
	cursors      []string
	ledgers      []uint32
	reports      []store.ReporterLatestReport
	reportErr    error
	reportChecks int
}

func (f *fakeStore) Progress(context.Context, string) (store.Progress, error) {
	return f.progress, nil
}

func (f *fakeStore) ApplyEvents(_ context.Context, name, cursor string, ledger uint32, raw []soroban.RawEvent, events []any) error {
	f.raw = append(f.raw, raw)
	f.applied = append(f.applied, events)
	f.names = append(f.names, name)
	f.cursors = append(f.cursors, cursor)
	f.ledgers = append(f.ledgers, ledger)
	return nil
}

func (f *fakeStore) ApplyReplayEvents(_ context.Context, events []any) (store.ReplaySummary, error) {
	f.replayed = append(f.replayed, events)
	return store.SummarizeReplay(events), nil
func (f *fakeStore) LatestReporterReports(context.Context) ([]store.ReporterLatestReport, error) {
	f.reportChecks++
	return f.reports, f.reportErr
}

func newIngester(rpc *fakeRPC, db *fakeStore) *Ingester {
	return &Ingester{
		RPC:                    rpc,
		Store:                  db,
		ContractIDs:            []string{"CC2QQ7R4FLPHZP7KA5AO4IASXXICTG6N4GQXCXITTEBNYQEYTYXKMN4D"},
		StartLedger:            5034380,
		ReporterStaleThreshold: 30 * 24 * time.Hour,
		Log:                    slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestRunWarnsForStaleReporterOncePerCycle(t *testing.T) {
	db := &fakeStore{reports: []store.ReporterLatestReport{
		{Reporter: "GSTALE", AsOf: time.Now().Add(-48 * time.Hour)},
		{Reporter: "GFRESH", AsOf: time.Now().Add(-time.Hour)},
	}}
	var logs strings.Builder
	in := newIngester(&fakeRPC{latest: 100}, db)
	in.ReporterStaleThreshold = 24 * time.Hour
	in.Log = slog.New(slog.NewJSONHandler(&logs, nil))

	if err := in.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if db.reportChecks != 1 {
		t.Fatalf("stale reporter check called %d times, want once", db.reportChecks)
	}
	if !strings.Contains(logs.String(), `"reporter":"GSTALE"`) ||
		!strings.Contains(logs.String(), `"age":"48h`) {
		t.Errorf("stale report warning lacks reporter or age: %s", logs.String())
	}
	if strings.Contains(logs.String(), `"reporter":"GFRESH"`) {
		t.Errorf("fresh reporter unexpectedly warned: %s", logs.String())
	}
}

func TestRunSurfacesStaleReporterCheckError(t *testing.T) {
	db := &fakeStore{reportErr: errors.New("database unavailable")}
	in := newIngester(&fakeRPC{latest: 100}, db)
	err := in.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "database unavailable") {
		t.Fatalf("want stale reporter query error, got %v", err)
	}
}

func TestRunSkipsStaleReporterCheckWhenDisabled(t *testing.T) {
	db := &fakeStore{}
	in := newIngester(&fakeRPC{latest: 100}, db)
	in.ReporterStaleThreshold = 0
	if err := in.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if db.reportChecks != 0 {
		t.Errorf("stale reporter check called %d times, want none when disabled", db.reportChecks)
	}
}

// The exact refusal testnet RPC returns for a cursor that has aged out.
var retentionErr = errors.New("[-32600] startLedger must be within the ledger range: 5347200 - 5468159")

func TestRunRestartsWhenCursorFellOutOfRetention(t *testing.T) {
	page := supplyPostedPage(t)
	stale := "0022785183757369344-0000000000"
	rpc := &fakeRPC{
		latest: 5468159,
		oldest: 5347200,
		errs:   map[string]error{stale: retentionErr},
		pages: map[string]protocol.GetEventsResponse{
			"": {Events: page, Cursor: "0023421401557905408-0000000000"},
		},
	}
	db := &fakeStore{progress: store.Progress{Cursor: stale, ScannedLedger: 5305089, EventLedger: 5305089}}

	if err := newIngester(rpc, db).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(db.applied) != 1 || len(db.applied[0]) != 1 {
		t.Fatalf("want the page after recovery stored, got %v", db.applied)
	}
	// The configured start is long out of retention, so the restart has to ask
	// for the oldest ledger RPC still serves rather than the configured one.
	// (The first entry is the probe startLedger makes to learn the window.)
	if got := rpc.askedSL[len(rpc.askedSL)-1]; got != 5347200 {
		t.Errorf("restarted from ledger %d, want 5347200", got)
	}
}

func TestRunGivesUpWhenRestartIsAlsoRefused(t *testing.T) {
	stale := "0022785183757369344-0000000000"
	rpc := &fakeRPC{
		latest: 5468159,
		oldest: 5347200,
		errs:   map[string]error{stale: retentionErr, "": retentionErr},
	}
	db := &fakeStore{progress: store.Progress{Cursor: stale}}

	err := newIngester(rpc, db).Run(context.Background())
	if err == nil {
		t.Fatal("want an error once the restart is refused too")
	}
	// The stale cursor is tried once and never retried, so a refusal that
	// outlasts the recovery cannot spin.
	stales := 0
	for _, c := range rpc.asked {
		if c == stale {
			stales++
		}
	}
	if stales != 1 {
		t.Errorf("stale cursor asked for %d times, want 1", stales)
	}
}

func TestRunReportsUnrelatedRPCErrors(t *testing.T) {
	rpc := &fakeRPC{latest: 100, oldest: 1, errs: map[string]error{"": errors.New("connection reset")}}
	db := &fakeStore{}
	err := newIngester(rpc, db).Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "connection reset") {
		t.Fatalf("want the RPC error reported, got %v", err)
	}
	if len(db.applied) != 0 {
		t.Error("nothing should be stored when the page failed")
	}
}

type concurrentRPC struct {
	pages   map[string]protocol.EventInfo
	mu      sync.Mutex
	seen    map[string]int
	arrived chan struct{}
	once    sync.Once
}

func (r *concurrentRPC) GetEvents(ctx context.Context, req protocol.GetEventsRequest) (protocol.GetEventsResponse, error) {
	contractID := req.Filters[0].ContractIDs[0]
	if req.Pagination.Limit == 1 {
		return protocol.GetEventsResponse{OldestLedger: 1}, nil
	}

	r.mu.Lock()
	r.seen[contractID]++
	total := len(r.seen)
	r.mu.Unlock()
	if total == 2 {
		r.once.Do(func() { close(r.arrived) })
	}
	select {
	case <-r.arrived:
	case <-ctx.Done():
		return protocol.GetEventsResponse{}, ctx.Err()
	case <-time.After(2 * time.Second):
		return protocol.GetEventsResponse{}, errors.New("contract streams were not concurrent")
	}
	event := r.pages[contractID]
	return protocol.GetEventsResponse{
		Events:       []protocol.EventInfo{event},
		Cursor:       "cursor-" + contractID,
		LatestLedger: 100,
	}, nil
}

func (r *concurrentRPC) GetLatestLedger(context.Context) (protocol.GetLatestLedgerResponse, error) {
	return protocol.GetLatestLedgerResponse{Sequence: 100}, nil
}

type eventTrackingStore struct {
	mu       sync.Mutex
	names    map[string]bool
	eventIDs map[string]int
	raw      [][]soroban.RawEvent
}

func (s *eventTrackingStore) Progress(context.Context, string) (store.Progress, error) {
	return store.Progress{}, nil
}

func (s *eventTrackingStore) ApplyEvents(_ context.Context, name, _ string, _ uint32, _ []soroban.RawEvent, events []any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.names[name] = true
	s.raw = append(s.raw, raw)
	for _, event := range events {
		supply, ok := event.(soroban.SupplyPosted)
		if !ok {
			return fmt.Errorf("unexpected decoded event %T", event)
		}
		s.eventIDs[supply.ID]++
	}
	return nil
}

func TestIngesterRunsContractStreamsConcurrentlyWithoutDuplicates(t *testing.T) {
	page := supplyPostedPage(t)
	ledgerEvent, registryEvent := page[0], page[0]
	ledgerEvent.ID = "ledger-event"
	registryEvent.ID = "registry-event"
	rpc := &concurrentRPC{
		pages: map[string]protocol.EventInfo{
			"ledger-contract":   ledgerEvent,
			"registry-contract": registryEvent,
		},
		seen:    make(map[string]int),
		arrived: make(chan struct{}),
	}
	db := &eventTrackingStore{
		names:    make(map[string]bool),
		eventIDs: make(map[string]int),
	}
	in := &Ingester{
		RPC:         rpc,
		Store:       db,
		ContractIDs: []string{"ledger-contract", "registry-contract"},
		StartLedger: 10,
		Workers:     2,
		Log:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := in.Run(ctx); err != nil {
		t.Fatalf("Run: %v", err)
	}

	for _, id := range []string{"ledger-contract", "registry-contract"} {
		if rpc.seen[id] != 1 {
			t.Errorf("%s stream requested %d pages, want 1", id, rpc.seen[id])
		}
		if !db.names[store.ContractIngestCursorName(id)] {
			t.Errorf("missing checkpoint for %s", id)
		}
	}
	if len(db.eventIDs) != 2 || db.eventIDs["ledger-event"] != 1 || db.eventIDs["registry-event"] != 1 {
		t.Errorf("stored event IDs = %v, want each stream's event exactly once", db.eventIDs)
	}
}

// Run hands the store every event of a page raw, tracked or not, next to the
// typed rows, so both are written in the page's transaction.
func TestRunStoresEveryEventRaw(t *testing.T) {
	page := supplyPostedPage(t)
	untracked := page[0]
	untracked.ID = "0023421401557905408-0000000001"
	untracked.TopicXDR = []string{b64(t, symVal(t, "admin_transferred"))}
	page = append(page, untracked)

	rpc := &fakeRPC{
		latest: 5468159,
		oldest: 5347200,
		pages: map[string]protocol.GetEventsResponse{
			"": {Events: page, Cursor: "0023421401557905408-0000000001"},
		},
	}
	db := &fakeStore{}
	if err := newIngester(rpc, db).Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(db.raw) != 1 || len(db.raw[0]) != 2 {
		t.Fatalf("want both events stored raw in one page, got %v", db.raw)
	}
	if db.raw[0][0].ID != page[0].ID || db.raw[0][1].ID != untracked.ID {
		t.Fatalf("raw events out of order: %+v", db.raw[0])
	}
	if len(db.applied) != 1 || len(db.applied[0]) != 1 {
		t.Fatalf("want one typed row, got %v", db.applied)
type testLogHandler struct {
	records []slog.Record
}

func (h *testLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *testLogHandler) Handle(_ context.Context, r slog.Record) error {
	h.records = append(h.records, r)
	return nil
}
func (h *testLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *testLogHandler) WithGroup(string) slog.Handler      { return h }

func TestFailoverToNextEndpointOnConnectionFailure(t *testing.T) {
	page := supplyPostedPage(t)
	validCursor := "0023421401557905408-0000000000"
	validCursor2 := "0023421401557905408-0000000001"
	rpc0 := &fakeRPC{latest: 5468159, oldest: 5347200, errs: map[string]error{"": errors.New("connection refused")}}
	rpc1 := &fakeRPC{latest: 5468159, oldest: 5347200, pages: map[string]protocol.GetEventsResponse{
		"": {Events: page, Cursor: validCursor},
	}}
	rpc2 := &fakeRPC{latest: 5468159, oldest: 5347200}

	logHandler := &testLogHandler{}
	logger := slog.New(logHandler)

	db := &fakeStore{progress: store.Progress{Cursor: ""}}
	ingester := &Ingester{
		RPCs:        []EventSource{rpc0, rpc1, rpc2},
		RPCURLs:     []string{"http://rpc0", "http://rpc1", "http://rpc2"},
		Store:       db,
		ContractIDs: []string{"CC2QQ7R4FLPHZP7KA5AO4IASXXICTG6N4GQXCXITTEBNYQEYTYXKMN4D"},
		StartLedger: 5347200,
		Log:         logger,
	}

	if err := ingester.Run(context.Background()); err != nil {
		t.Fatalf("Run should have succeeded via failover, got: %v", err)
	}

	if len(db.applied) != 1 || len(db.applied[0]) != 1 {
		t.Fatalf("want events applied from rpc1, got %v", db.applied)
	}
	if ingester.currentRPC != 1 {
		t.Errorf("currentRPC = %d, want 1", ingester.currentRPC)
	}

	// Verify failover was logged at WARN level
	warnFound := false
	for _, rec := range logHandler.records {
		if rec.Level == slog.LevelWarn && strings.Contains(rec.Message, "failing over") {
			warnFound = true
			break
		}
	}
	if !warnFound {
		t.Error("expected failover to be logged at WARN level")
	}

	// Second run should use rpc1 without failing over again
	rpc1.pages[validCursor] = protocol.GetEventsResponse{Events: nil, Cursor: validCursor2}
	db.progress.Cursor = validCursor
	if err := ingester.Run(context.Background()); err != nil {
		t.Fatalf("second Run failed: %v", err)
	}
	if ingester.currentRPC != 1 {
		t.Errorf("expected to stay on rpc1, currentRPC = %d", ingester.currentRPC)
	}
}

func TestFailoverCyclesBackToFirstAfterAllFail(t *testing.T) {
	cursor := "0023421401557905408-0000000000"
	rpc0 := &fakeRPC{latest: 5468159, oldest: 5347200, errs: map[string]error{cursor: errors.New("conn 0 failed")}}
	rpc1 := &fakeRPC{latest: 5468159, oldest: 5347200, errs: map[string]error{cursor: errors.New("conn 1 failed")}}
	rpc2 := &fakeRPC{latest: 5468159, oldest: 5347200, errs: map[string]error{cursor: errors.New("conn 2 failed")}}

	logHandler := &testLogHandler{}
	db := &fakeStore{progress: store.Progress{Cursor: cursor}}
	ingester := &Ingester{
		RPCs:        []EventSource{rpc0, rpc1, rpc2},
		RPCURLs:     []string{"http://rpc0", "http://rpc1", "http://rpc2"},
		Store:       db,
		ContractIDs: []string{"CC2QQ7R4FLPHZP7KA5AO4IASXXICTG6N4GQXCXITTEBNYQEYTYXKMN4D"},
		StartLedger: 5347200,
		Log:         slog.New(logHandler),
	}

	err := ingester.Run(context.Background())
	if err == nil {
		t.Fatal("expected Run to fail when all RPCs fail")
	}
	if !strings.Contains(err.Error(), "all 3 RPC endpoints failed") {
		t.Errorf("error = %v, want 'all 3 RPC endpoints failed'", err)
	}

	// Acceptance criteria: cycling back to the first after all fail
	if ingester.currentRPC != 0 {
		t.Errorf("expected currentRPC to cycle back to 0, got %d", ingester.currentRPC)
	}

	// Check warn logs count for all 3 failovers
	warnCount := 0
	for _, rec := range logHandler.records {
		if rec.Level == slog.LevelWarn && strings.Contains(rec.Message, "failing over") {
			warnCount++
		}
	}
	if warnCount != 3 {
		t.Errorf("warnCount = %d, want 3", warnCount)
	}
}

func TestFailoverGetLatestLedger(t *testing.T) {
	page := supplyPostedPage(t)
	rpc0 := &fakeRPC{errs: map[string]error{"latest": errors.New("network down")}}
	rpc1 := &fakeRPC{latest: 5468159, oldest: 5347200, pages: map[string]protocol.GetEventsResponse{
		"": {Events: page, Cursor: "c1"},
	}}

	logHandler := &testLogHandler{}
	db := &fakeStore{progress: store.Progress{Cursor: ""}}
	ingester := &Ingester{
		RPCs:        []EventSource{rpc0, rpc1},
		RPCURLs:     []string{"http://rpc0", "http://rpc1"},
		Store:       db,
		ContractIDs: []string{"CC2QQ7R4FLPHZP7KA5AO4IASXXICTG6N4GQXCXITTEBNYQEYTYXKMN4D"},
		StartLedger: 0, // forces calling startLedger -> getLatestLedger
		Log:         slog.New(logHandler),
	}

	if err := ingester.Run(context.Background()); err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if len(db.applied) != 1 {
		t.Fatalf("want 1 page applied, got %d", len(db.applied))
	}
	if ingester.currentRPC != 1 {
		t.Errorf("expected currentRPC = 1, got %d", ingester.currentRPC)
	}

	warnFound := false
	for _, rec := range logHandler.records {
		if rec.Level == slog.LevelWarn && strings.Contains(rec.Message, "failing over") {
			warnFound = true
			break
		}
	}
	if !warnFound {
		t.Error("expected failover to be logged at WARN level")
	}
}
