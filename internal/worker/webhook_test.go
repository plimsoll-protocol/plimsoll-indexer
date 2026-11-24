package worker

import (
	"context"
	"encoding/json"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
)

const webhookSAC = "CSACWEBHOOK"

// fakeWebhookStore is an in-memory WebhookStore for notifier tests.
type fakeWebhookStore struct {
	hooks      []coverage.Webhook
	supply     map[string]*coverage.Supply
	reports    map[string]map[uint32]coverage.Report
	deliveries []*fakeDelivery
	nextID     int64
	now        time.Time
	// current, when set, overrides the frozen now so a test can advance the
	// clock it shares with the notifier.
	current func() time.Time
}

type fakeDelivery struct {
	id         int64
	webhookID  int64
	payload    []byte
	attempts   int
	nextAt     time.Time
	delivered  bool
	lastStatus int
}

func newFakeWebhookStore(now time.Time) *fakeWebhookStore {
	return &fakeWebhookStore{
		supply:  map[string]*coverage.Supply{},
		reports: map[string]map[uint32]coverage.Report{},
		now:     now,
	}
}

func (f *fakeWebhookStore) clock() time.Time {
	if f.current != nil {
		return f.current()
	}
	return f.now
}

func (f *fakeWebhookStore) ListWebhooks(context.Context) ([]coverage.Webhook, error) {
	return f.hooks, nil
}

func (f *fakeWebhookStore) LatestSupply(_ context.Context, sac string) (*coverage.Supply, error) {
	return f.supply[sac], nil
}

func (f *fakeWebhookStore) LatestReports(_ context.Context, sac string) (map[uint32]coverage.Report, error) {
	if r := f.reports[sac]; r != nil {
		return r, nil
	}
	return map[uint32]coverage.Report{}, nil
}

func (f *fakeWebhookStore) SetWebhookAlerted(_ context.Context, id int64, alerted bool) error {
	for i := range f.hooks {
		if f.hooks[i].ID == id {
			f.hooks[i].Alerted = alerted
		}
	}
	return nil
}

func (f *fakeWebhookStore) EnqueueWebhookDelivery(_ context.Context, webhookID int64, payload []byte) error {
	f.nextID++
	f.deliveries = append(f.deliveries, &fakeDelivery{
		id: f.nextID, webhookID: webhookID, payload: payload, nextAt: f.clock(),
	})
	return nil
}

func (f *fakeWebhookStore) DueWebhookDeliveries(_ context.Context, limit, maxAttempts int) ([]coverage.WebhookDelivery, error) {
	var out []coverage.WebhookDelivery
	for _, d := range f.deliveries {
		if d.delivered || d.attempts >= maxAttempts || d.nextAt.After(f.clock()) {
			continue
		}
		var url string
		for _, h := range f.hooks {
			if h.ID == d.webhookID {
				url = h.URL
			}
		}
		out = append(out, coverage.WebhookDelivery{
			ID: d.id, WebhookID: d.webhookID, URL: url, Payload: d.payload, Attempts: d.attempts,
		})
		if len(out) == limit {
			break
		}
	}
	return out, nil
}

func (f *fakeWebhookStore) RecordWebhookAttempt(_ context.Context, id int64, status int, _ string, delivered bool, nextAt time.Time) error {
	for _, d := range f.deliveries {
		if d.id == id {
			d.attempts++
			d.lastStatus = status
			d.delivered = delivered
			d.nextAt = nextAt
		}
	}
	return nil
}

// webhookReceiver is an httptest receiver that records requests and answers
// with the given status codes in order, repeating the last one.
type webhookReceiver struct {
	mu      sync.Mutex
	bodies  [][]byte
	signs   []string
	status  []int
	receive int
}

func newWebhookReceiver(status ...int) (*httptest.Server, *webhookReceiver) {
	rec := &webhookReceiver{status: status}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rec.mu.Lock()
		rec.bodies = append(rec.bodies, body)
		rec.signs = append(rec.signs, r.Header.Get("X-Plimsoll-Signature"))
		code := http.StatusOK
		if len(rec.status) > 0 {
			i := rec.receive
			if i >= len(rec.status) {
				i = len(rec.status) - 1
			}
			code = rec.status[i]
		}
		rec.receive++
		rec.mu.Unlock()
		w.WriteHeader(code)
	}))
	return srv, rec
}

func (r *webhookReceiver) requests() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.bodies)
}

var webhookNow = time.Date(2026, 10, 5, 12, 0, 0, 0, time.UTC)

// droppingCoverage installs a supply snapshot and a report whose bps is below
// the webhook's threshold.
func droppingCoverage(store *fakeWebhookStore, bps uint32) {
	store.supply[webhookSAC] = &coverage.Supply{
		Amount: big.NewInt(1_000_000), Ledger: 100, PostedAt: webhookNow,
	}
	reserves := new(big.Int).Mul(big.NewInt(1_000_000), big.NewInt(int64(bps)))
	reserves.Quo(reserves, big.NewInt(10_000))
	store.reports[webhookSAC] = map[uint32]coverage.Report{
		coverage.TierTranscribed: {
			Tier: coverage.TierTranscribed, Amount: reserves, AsOf: webhookNow,
			Reporter: "GREP", DocHash: "h", DocURI: "https://issuer.example/report",
		},
	}
}

func testNotifier(store *fakeWebhookStore, secret string, now *time.Time) *WebhookNotifier {
	store.current = func() time.Time { return *now }
	return &WebhookNotifier{
		Store:   store,
		Secret:  secret,
		Log:     discardLog(),
		Now:     func() time.Time { return *now },
		Backoff: time.Minute,
	}
}

func TestWebhookNotifierDeliversSignedPayloadOnce(t *testing.T) {
	srv, rec := newWebhookReceiver()
	defer srv.Close()

	now := webhookNow
	store := newFakeWebhookStore(now)
	store.hooks = []coverage.Webhook{{ID: 1, URL: srv.URL, SAC: webhookSAC, MinBps: 10_000, MinTier: 1}}
	droppingCoverage(store, 9_000)
	n := testNotifier(store, "s3cret", &now)

	if err := n.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := rec.requests(); got != 1 {
		t.Fatalf("receiver got %d requests, want 1", got)
	}
	body := rec.bodies[0]
	if want := signWebhook("s3cret", body); rec.signs[0] != want {
		t.Fatalf("signature = %q, want %q", rec.signs[0], want)
	}
	var p webhookPayload
	if err := json.Unmarshal(body, &p); err != nil {
		t.Fatal(err)
	}
	if p.Event != webhookEvent || p.WebhookID != 1 || p.SAC != webhookSAC ||
		p.MinBps != 10_000 || p.Bps != 9_000 || p.Tier != "transcribed" ||
		p.Supply != "1000000" || p.Reserves != "900000" || p.MinTier != "transcribed" {
		t.Fatalf("payload = %+v", p)
	}
	if !store.hooks[0].Alerted {
		t.Fatal("webhook was not marked alerted")
	}

	// A second pass while still below the threshold must not alert again.
	if err := n.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := rec.requests(); got != 1 {
		t.Fatalf("receiver got %d requests after re-run, want 1", got)
	}
}

func TestWebhookNotifierRearmsAfterRecovery(t *testing.T) {
	srv, rec := newWebhookReceiver()
	defer srv.Close()

	now := webhookNow
	store := newFakeWebhookStore(now)
	store.hooks = []coverage.Webhook{{ID: 1, URL: srv.URL, SAC: webhookSAC, MinBps: 9_500, MinTier: 1}}
	droppingCoverage(store, 9_000)
	n := testNotifier(store, "s", &now)
	if err := n.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Coverage recovers: the webhook re-arms without a second delivery.
	droppingCoverage(store, 10_000)
	if err := n.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.hooks[0].Alerted {
		t.Fatal("webhook stayed alerted after coverage recovered")
	}
	if got := rec.requests(); got != 1 {
		t.Fatalf("receiver got %d requests, want 1", got)
	}

	// A new drop alerts again.
	droppingCoverage(store, 9_000)
	if err := n.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := rec.requests(); got != 2 {
		t.Fatalf("receiver got %d requests after a second drop, want 2", got)
	}
}

func TestWebhookNotifierRetriesWithBackoff(t *testing.T) {
	srv, rec := newWebhookReceiver(http.StatusInternalServerError, http.StatusBadGateway, http.StatusOK)
	defer srv.Close()

	now := webhookNow
	store := newFakeWebhookStore(now)
	store.hooks = []coverage.Webhook{{ID: 1, URL: srv.URL, SAC: webhookSAC, MinBps: 10_000, MinTier: 1}}
	droppingCoverage(store, 9_000)
	n := testNotifier(store, "s", &now)

	// First attempt fails and schedules the retry a minute out.
	if err := n.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Before the backoff elapses nothing is sent.
	if err := n.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := rec.requests(); got != 1 {
		t.Fatalf("receiver got %d requests before backoff, want 1", got)
	}

	now = now.Add(time.Minute)
	if err := n.Run(context.Background()); err != nil { // second attempt: 502
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)                      // backoff doubled
	if err := n.Run(context.Background()); err != nil { // third attempt: 200
		t.Fatal(err)
	}
	if got := rec.requests(); got != 3 {
		t.Fatalf("receiver got %d requests, want 3", got)
	}
	d := store.deliveries[0]
	if d.attempts != 3 || !d.delivered || d.lastStatus != http.StatusOK {
		t.Fatalf("delivery = %+v, want 3 attempts and delivered", d)
	}

	// Delivered deliveries are never sent again.
	now = now.Add(time.Hour)
	if err := n.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := rec.requests(); got != 3 {
		t.Fatalf("receiver got %d requests after delivery, want 3", got)
	}
}

func TestWebhookNotifierStopsAfterFiveAttempts(t *testing.T) {
	srv, rec := newWebhookReceiver(http.StatusInternalServerError)
	defer srv.Close()

	now := webhookNow
	store := newFakeWebhookStore(now)
	store.hooks = []coverage.Webhook{{ID: 1, URL: srv.URL, SAC: webhookSAC, MinBps: 10_000, MinTier: 1}}
	droppingCoverage(store, 9_000)
	n := testNotifier(store, "s", &now)

	for range 10 {
		if err := n.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Hour)
	}
	if got := rec.requests(); got != webhookMaxAttempts {
		t.Fatalf("receiver got %d requests, want %d", got, webhookMaxAttempts)
	}
	if got := store.deliveries[0].attempts; got != webhookMaxAttempts {
		t.Fatalf("delivery logged %d attempts, want %d", got, webhookMaxAttempts)
	}
	if store.deliveries[0].delivered {
		t.Fatal("failed delivery marked delivered")
	}
}

func TestWebhookNotifierIgnoresUnknownCoverage(t *testing.T) {
	srv, rec := newWebhookReceiver()
	defer srv.Close()

	now := webhookNow
	store := newFakeWebhookStore(now)
	store.hooks = []coverage.Webhook{{ID: 1, URL: srv.URL, SAC: webhookSAC, MinBps: 10_000, MinTier: 1}}
	n := testNotifier(store, "s", &now)

	if err := n.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := rec.requests(); got != 0 {
		t.Fatalf("receiver got %d requests with no coverage, want 0", got)
	}
	if len(store.deliveries) != 0 {
		t.Fatalf("queued %d deliveries with no coverage, want 0", len(store.deliveries))
	}
}
