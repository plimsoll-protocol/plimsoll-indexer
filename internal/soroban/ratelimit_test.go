package soroban

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// captureHandler records log records so a test can assert a back-off is logged
// at WARN level rather than reading formatted output.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }

func (h *captureHandler) warns() []slog.Record {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []slog.Record
	for _, r := range h.records {
		if r.Level == slog.LevelWarn {
			out = append(out, r)
		}
	}
	return out
}

func recordAttr(r slog.Record, key string) (string, bool) {
	var value string
	found := false
	r.Attrs(func(a slog.Attr) bool {
		if a.Key == key {
			value, found = a.Value.String(), true
			return false
		}
		return true
	})
	return value, found
}

// A 429 with Retry-After is retried after exactly the delay the node asks for,
// and the back-off is logged at WARN.
func TestRateLimitTransportHonoursRetryAfter(t *testing.T) {
	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		if requests == 1 {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = io.WriteString(w, "slow down")
			return
		}
		_, _ = io.WriteString(w, `{"jsonrpc":"2.0","result":{}}`)
	}))
	defer srv.Close()

	logs := &captureHandler{}
	client := &http.Client{Transport: NewRateLimitTransport(nil, slog.New(logs))}
	start := time.Now()
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	elapsed := time.Since(start)
	if resp.StatusCode != http.StatusOK || string(body) != `{"jsonrpc":"2.0","result":{}}` {
		t.Fatalf("status %d body %q", resp.StatusCode, body)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2 (one 429 and one retry)", requests)
	}
	if elapsed < time.Second {
		t.Errorf("retried after %s, want at least the 1s Retry-After", elapsed)
	}
	warns := logs.warns()
	if len(warns) != 1 {
		t.Fatalf("WARN records = %d, want 1", len(warns))
	}
	if warns[0].Message != "rpc node is rate limiting; backing off" {
		t.Errorf("message = %q", warns[0].Message)
	}
	if got, ok := recordAttr(warns[0], "retry_after"); !ok || got != "1" {
		t.Errorf("retry_after attribute = %q, present %v", got, ok)
	}
	if got, ok := recordAttr(warns[0], "attempt"); !ok || got != "1" {
		t.Errorf("attempt attribute = %q, present %v", got, ok)
	}
}

// Without Retry-After the delay doubles per attempt, retries are bounded, and
// the final 429 is handed back to the caller with its body intact.
func TestRateLimitTransportBacksOffExponentially(t *testing.T) {
	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, "slow down")
	}))
	defer srv.Close()

	logs := &captureHandler{}
	transport := NewRateLimitTransport(nil, slog.New(logs))
	transport.MaxRetries = 2
	transport.BaseDelay = 20 * time.Millisecond
	transport.MaxDelay = time.Second
	client := &http.Client{Transport: transport}

	start := time.Now()
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	elapsed := time.Since(start)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want the final 429", resp.StatusCode)
	}
	if string(body) != "slow down" {
		t.Errorf("body = %q, want the final response body", body)
	}
	if requests != 3 {
		t.Fatalf("requests = %d, want 3 (first attempt plus two retries)", requests)
	}
	// 20 ms then 40 ms of back-off.
	if elapsed < 60*time.Millisecond {
		t.Errorf("elapsed = %s, want at least 60ms of back-off", elapsed)
	}
	if warns := logs.warns(); len(warns) != 2 {
		t.Errorf("WARN records = %d, want one per retry", len(warns))
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name  string
		value string
		want  time.Duration
		ok    bool
	}{
		{"seconds", "120", 2 * time.Minute, true},
		{"zero", "0", 0, true},
		{"negative", "-3", 0, true},
		{"http date", now.Add(90 * time.Second).Format(http.TimeFormat), 90 * time.Second, true},
		{"http date in the past", now.Add(-time.Minute).Format(http.TimeFormat), 0, true},
		{"clamped", "604800", maxRetryAfter, true},
		{"absent", "", 0, false},
		{"garbage", "soon", 0, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseRetryAfter(tc.value, now)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("parseRetryAfter(%q) = %s, %v; want %s, %v", tc.value, got, ok, tc.want, tc.ok)
			}
		})
	}
}

// A request body without a replay function is buffered, so the retry sends the
// same bytes instead of the empty remainder of the first read.
func TestRateLimitTransportReplaysRequestBody(t *testing.T) {
	const payload = `{"jsonrpc":"2.0","id":1,"method":"getLatestLedger"}`
	var (
		mu     sync.Mutex
		bodies []string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies = append(bodies, string(body))
		attempt := len(bodies)
		mu.Unlock()
		if attempt == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL, io.NopCloser(strings.NewReader(payload)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{Transport: NewRateLimitTransport(nil, nil)}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 {
		t.Fatalf("requests = %d, want 2", len(bodies))
	}
	for i, body := range bodies {
		if body != payload {
			t.Errorf("attempt %d body = %q, want %q", i+1, body, payload)
		}
	}
}

// Only 429 is the node asking to be left alone; every other status passes
// through on the first attempt.
func TestRateLimitTransportPassesThroughOtherStatuses(t *testing.T) {
	var requests int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := &http.Client{Transport: NewRateLimitTransport(nil, nil)}
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
	if requests != 1 {
		t.Fatalf("requests = %d, want 1", requests)
	}
}

// A back-off is a wait, not a wedge: cancelling the request ends it.
func TestRateLimitTransportStopsWhenContextIsDone(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "30")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	transport := NewRateLimitTransport(nil, nil)
	transport.MaxRetries = 3
	client := &http.Client{Transport: transport}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL, nil)
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	resp, err := client.Do(req)
	if resp != nil {
		resp.Body.Close()
	}
	if err == nil {
		t.Fatal("expected the cancelled back-off to fail the request")
	} else if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context.DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("waited %s after cancellation", elapsed)
	}
}
