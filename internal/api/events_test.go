package api

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/events"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

// Event ids in the format RPC assigns: a zero-padded TOID, so byte order is
// chain order.
const (
	eventIDOld     = "0023421401557905400-0000000000"
	eventIDSupply  = "0023421401557905408-0000000000"
	eventIDReserve = "0023421401557905409-0000000000"
	eventIDAsset   = "0023421401557905410-0000000000"
)

// newStreamServer serves the API over a fake store with a live hub.
func newStreamServer(t *testing.T, hub *events.Hub, heartbeat time.Duration, opt func(*Server)) (*httptest.Server, *fake) {
	t.Helper()
	f := newFake()
	s := &Server{
		Store:             f,
		Events:            hub,
		CORS:              []string{"*"},
		Log:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:               func() time.Time { return now },
		EventHeartbeat:    heartbeat,
		EventWriteTimeout: 5 * time.Second,
	}
	if opt != nil {
		opt(s)
	}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return srv, f
}

func openEventStream(t *testing.T, srv *httptest.Server, lastID string) (*http.Response, *bufio.Reader) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	if lastID != "" {
		req.Header.Set("Last-Event-ID", lastID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("open event stream: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("content type %q, want text/event-stream", got)
	}
	return resp, bufio.NewReader(resp.Body)
}

type sseFrame struct {
	comment   string
	id        string
	event     string
	dataLines []string
}

// readFrame reads one SSE frame. Comments (heartbeats) come back as frames of
// their own so a test can assert on them.
func readFrame(t *testing.T, r *bufio.Reader) sseFrame {
	t.Helper()
	var f sseFrame
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read SSE stream: %v", err)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if f.comment == "" && f.id == "" && f.event == "" && len(f.dataLines) == 0 {
				continue
			}
			return f
		}
		if strings.HasPrefix(line, ":") {
			f.comment = strings.TrimSpace(line[1:])
			continue
		}
		field, value, _ := strings.Cut(line, ": ")
		switch field {
		case "id":
			f.id = value
		case "event":
			f.event = value
		case "data":
			f.dataLines = append(f.dataLines, value)
		}
	}
}

func readEventFrame(t *testing.T, r *bufio.Reader) sseFrame {
	t.Helper()
	for {
		if f := readFrame(t, r); f.event != "" {
			return f
		}
	}
}

func waitForSubscribers(t *testing.T, hub *events.Hub, want int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if hub.Subscribers() == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("subscribers = %d, want %d", hub.Subscribers(), want)
}

func decodeFrame(t *testing.T, f sseFrame, into any) {
	t.Helper()
	if len(f.dataLines) != 1 {
		t.Fatalf("frame %s has %d data lines, want 1", f.event, len(f.dataLines))
	}
	if err := json.Unmarshal([]byte(f.dataLines[0]), into); err != nil {
		t.Fatalf("decode %s frame: %v", f.event, err)
	}
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func supplyEvent(id string, atLedger uint32) soroban.SupplyPosted {
	return soroban.SupplyPosted{
		Meta:          soroban.Meta{ID: id, Ledger: 100, ClosedAt: now.Add(-time.Minute), TxHash: "tx-1"},
		SAC:           sac,
		Amount:        big.NewInt(500),
		AtLedger:      atLedger,
		BreakdownHash: breakdownHash,
		Poster:        "GPOSTER",
	}
}

func reserveEvent(id string) soroban.ReservePosted {
	return soroban.ReservePosted{
		Meta:     soroban.Meta{ID: id, Ledger: 100, ClosedAt: now.Add(-time.Minute), TxHash: "tx-2"},
		SAC:      sac,
		Tier:     coverage.TierTranscribed,
		Amount:   big.NewInt(300),
		AsOf:     now.Add(-2 * time.Hour),
		Reporter: "GREPORTER",
		DocHash:  "doc-hash",
		DocURI:   "https://example.com/report",
	}
}

// A subscriber sees each ingested event with the same JSON shape its REST
// endpoint serves.
func TestEventStreamDeliversLiveEvents(t *testing.T) {
	hub := events.NewHub(4)
	srv, _ := newStreamServer(t, hub, time.Hour, nil)
	resp, reader := openEventStream(t, srv, "")
	defer resp.Body.Close()
	waitForSubscribers(t, hub, 1)

	hub.Publish([]any{
		supplyEvent(eventIDSupply, 99),
		reserveEvent(eventIDReserve),
		soroban.AssetListed{
			Meta: soroban.Meta{ID: eventIDAsset, Ledger: 101, ClosedAt: now.Add(-time.Minute)},
			SAC:  sac, Code: "PUSD", Issuer: "GISS",
		},
	})

	supplyFrame := readEventFrame(t, reader)
	if supplyFrame.id != eventIDSupply || supplyFrame.event != "supply_posted" {
		t.Fatalf("supply frame id=%q event=%q", supplyFrame.id, supplyFrame.event)
	}
	var supply map[string]any
	decodeFrame(t, supplyFrame, &supply)
	if supply["amount"] != "500" || supply["ledger"].(float64) != 99 ||
		supply["source"] != "event" || supply["tx_hash"] != "tx-1" {
		t.Errorf("supply payload %+v", supply)
	}
	restSupply := get(t, srv, "/v1/assets/"+sac+"/supply", 200)["supply"].([]any)[0].(map[string]any)
	if got, want := strings.Join(sortedKeys(supply), ","), strings.Join(sortedKeys(restSupply), ","); got != want {
		t.Errorf("supply fields = %s, REST fields = %s", got, want)
	}

	reserveFrame := readEventFrame(t, reader)
	if reserveFrame.id != eventIDReserve || reserveFrame.event != "reserve_posted" {
		t.Fatalf("reserve frame id=%q event=%q", reserveFrame.id, reserveFrame.event)
	}
	var report map[string]any
	decodeFrame(t, reserveFrame, &report)
	if report["tier"] != "transcribed" || report["amount"] != "300" || report["doc_uri"] != "https://example.com/report" {
		t.Errorf("report payload %+v", report)
	}
	restReport := get(t, srv, "/v1/assets/"+sac+"/reports", 200)["reports"].([]any)[0].(map[string]any)
	if got, want := strings.Join(sortedKeys(report), ","), strings.Join(sortedKeys(restReport), ","); got != want {
		t.Errorf("report fields = %s, REST fields = %s", got, want)
	}

	assetFrame := readEventFrame(t, reader)
	if assetFrame.id != eventIDAsset || assetFrame.event != "asset_listed" {
		t.Fatalf("asset frame id=%q event=%q", assetFrame.id, assetFrame.event)
	}
	var asset map[string]any
	decodeFrame(t, assetFrame, &asset)
	if asset["sac"] != sac || asset["code"] != "PUSD" || asset["issuer"] != "GISS" ||
		asset["listed_ledger"].(float64) != 101 {
		t.Errorf("asset payload %+v", asset)
	}
	for _, field := range sortedKeys(asset) {
		if _, ok := get(t, srv, "/v1/assets/"+sac, 200)[field]; !ok {
			t.Errorf("asset field %q is not in the REST asset shape", field)
		}
	}
}

// Reconnecting with Last-Event-ID replays what was missed from storage, then
// continues with live events without a gap.
func TestEventStreamResumesFromLastEventID(t *testing.T) {
	hub := events.NewHub(4)
	srv, f := newStreamServer(t, hub, time.Hour, nil)
	f.stored = []any{
		supplyEvent(eventIDOld, 97),
		supplyEvent(eventIDSupply, 99),
		reserveEvent(eventIDReserve),
	}
	resp, reader := openEventStream(t, srv, eventIDOld)
	defer resp.Body.Close()
	waitForSubscribers(t, hub, 1)

	first := readEventFrame(t, reader)
	if first.id != eventIDSupply || first.event != "supply_posted" {
		t.Fatalf("first replayed frame id=%q event=%q", first.id, first.event)
	}
	second := readEventFrame(t, reader)
	if second.id != eventIDReserve || second.event != "reserve_posted" {
		t.Fatalf("second replayed frame id=%q event=%q", second.id, second.event)
	}

	// A live event that lands after the replay must follow the backlog in
	// order and without duplicating it.
	hub.Publish([]any{supplyEvent(eventIDAsset, 102)})
	third := readEventFrame(t, reader)
	if third.id != eventIDAsset || third.event != "supply_posted" {
		t.Fatalf("live frame after replay id=%q event=%q", third.id, third.event)
	}
}

// Closing the response, which is what an EventSource does when the page goes
// away, must release the subscription slot.
func TestEventStreamDisconnectReleasesSubscriber(t *testing.T) {
	hub := events.NewHub(4)
	srv, _ := newStreamServer(t, hub, time.Hour, nil)
	resp, _ := openEventStream(t, srv, "")
	waitForSubscribers(t, hub, 1)

	resp.Body.Close()
	waitForSubscribers(t, hub, 0)
}

// The subscriber cap answers 503 rather than letting streams grow without
// bound.
func TestEventStreamRejectsBeyondSubscriberCap(t *testing.T) {
	hub := events.NewHub(1)
	srv, _ := newStreamServer(t, hub, time.Hour, nil)
	first, _ := openEventStream(t, srv, "")
	waitForSubscribers(t, hub, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(second.Body)
	second.Body.Close()
	if second.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("over-cap status = %d, want 503", second.StatusCode)
	}
	if !strings.Contains(string(body), "too many event subscribers") {
		t.Errorf("over-cap body = %s", body)
	}

	first.Body.Close()
	waitForSubscribers(t, hub, 0)
}

// Idle connections are kept alive with SSE comments.
func TestEventStreamSendsHeartbeatComments(t *testing.T) {
	srv, _ := newStreamServer(t, events.NewHub(2), 10*time.Millisecond, nil)
	resp, reader := openEventStream(t, srv, "")
	defer resp.Body.Close()
	f := readFrame(t, reader)
	if f.comment != "keepalive" {
		t.Fatalf("first idle frame = %+v, want a keepalive comment", f)
	}
}

// Graceful shutdown closes live streams instead of waiting on them.
func TestEventStreamEndsOnShutdown(t *testing.T) {
	done := make(chan struct{})
	srv, _ := newStreamServer(t, events.NewHub(2), time.Hour, func(s *Server) { s.Done = done })
	resp, _ := openEventStream(t, srv, "")
	defer resp.Body.Close()

	close(done)
	if _, err := io.ReadAll(resp.Body); err != nil {
		t.Fatalf("stream did not end cleanly on shutdown: %v", err)
	}
}
