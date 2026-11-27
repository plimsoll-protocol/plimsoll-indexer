package horizon

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// newRetryingClient returns a client that records its backoff waits instead of
// sleeping them, so retry tests do not spend the policy's wall time.
func newRetryingClient(t *testing.T, url string) (*Client, *[]time.Duration) {
	t.Helper()
	var waits []time.Duration
	c := NewClient(url)
	c.sleep = func(_ context.Context, d time.Duration) error {
		waits = append(waits, d)
		return nil
	}
	return c, &waits
}

// A public Horizon rate-limits, and the poster posts every asset in one tick.
// A 429 must be waited out for as long as Horizon asked, then retried.
func TestGetHonoursRetryAfter(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Header().Set("Retry-After", "2")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		fmt.Fprint(w, `{"history_latest_ledger": 42}`)
	}))
	defer srv.Close()

	c, waits := newRetryingClient(t, srv.URL)
	got, err := c.LatestLedger(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Errorf("ledger %d, want 42", got)
	}
	if n := calls.Load(); n != 2 {
		t.Errorf("made %d requests, want 2", n)
	}
	if len(*waits) != 1 {
		t.Fatalf("waits %v, want exactly one", *waits)
	}
	if (*waits)[0] != 2*time.Second {
		t.Errorf("waited %s, want the Retry-After value of 2s", (*waits)[0])
	}
}

// A brief 503 is what a Horizon mid-deploy looks like; one retry is enough.
func TestGetRetriesServerErrorThenSucceeds(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, `{"history_latest_ledger": 7}`)
	}))
	defer srv.Close()

	c, waits := newRetryingClient(t, srv.URL)
	got, err := c.LatestLedger(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got != 7 {
		t.Errorf("ledger %d, want 7", got)
	}
	if n := calls.Load(); n != 2 {
		t.Errorf("made %d requests, want 2", n)
	}
	if len(*waits) != 1 {
		t.Fatalf("waits %v, want exactly one", *waits)
	}
	if w := (*waits)[0]; w < baseBackoff/2 || w > baseBackoff {
		t.Errorf("waited %s, want jittered backoff within [%s, %s]", w, baseBackoff/2, baseBackoff)
	}
}

// Repeating a request Horizon has rejected as wrong will not fix it, and a
// missing record is an answer, not a failure to retry.
func TestGetDoesNotRetryClientErrors(t *testing.T) {
	for _, status := range []int{
		http.StatusBadRequest,
		http.StatusUnauthorized,
		http.StatusForbidden,
		http.StatusNotFound,
		http.StatusUnprocessableEntity,
	} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.WriteHeader(status)
			}))
			defer srv.Close()

			c, waits := newRetryingClient(t, srv.URL)
			_, err := c.LatestLedger(context.Background())
			if err == nil {
				t.Fatal("want an error")
			}
			if n := calls.Load(); n != 1 {
				t.Errorf("made %d requests, want 1: %d is not retryable", n, status)
			}
			if len(*waits) != 0 {
				t.Errorf("waited %v, want no retry", *waits)
			}
		})
	}
}

func TestGetStopsAfterMaxAttempts(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	c, waits := newRetryingClient(t, srv.URL)
	_, err := c.LatestLedger(context.Background())
	if err == nil || !strings.Contains(err.Error(), "giving up after") {
		t.Fatalf("want a give-up error, got %v", err)
	}
	if n := calls.Load(); n != maxAttempts {
		t.Errorf("made %d requests, want the limit of %d", n, maxAttempts)
	}
	if len(*waits) != maxAttempts-1 {
		t.Errorf("waited %d times, want %d", len(*waits), maxAttempts-1)
	}
}

// A Retry-After longer than the whole budget is Horizon saying "not now", so the
// request gives up at once rather than sleeping through the tick.
func TestGetGivesUpWhenRetryAfterExceedsTheBudget(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()

	c, waits := newRetryingClient(t, srv.URL)
	_, err := c.LatestLedger(context.Background())
	if err == nil || !strings.Contains(err.Error(), "retry budget") {
		t.Fatalf("want a budget error, got %v", err)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("made %d requests, want 1", n)
	}
	if len(*waits) != 0 {
		t.Errorf("waited %v, want to give up without sleeping", *waits)
	}
}

// The poster shuts down on a signal, and a retry loop must not outlive it.
func TestGetStopsWhenTheContextIsCancelled(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	c := NewClient(srv.URL)
	c.sleep = func(ctx context.Context, _ time.Duration) error {
		cancel()
		return ctx.Err()
	}

	_, err := c.LatestLedger(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("made %d requests after cancellation, want 1", n)
	}
}

// Shared Horizon instances ask clients to identify themselves, and an operator
// seeing this indexer in their logs needs to know which build it is.
func TestGetSendsUserAgent(t *testing.T) {
	var ua string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ua = r.Header.Get("User-Agent")
		fmt.Fprint(w, `{"history_latest_ledger": 1}`)
	}))
	defer srv.Close()

	if _, err := NewClient(srv.URL).LatestLedger(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := "plimsoll-indexer/" + Version
	if ua != want {
		t.Errorf("User-Agent %q, want %q", ua, want)
	}
}

// A horizon URL pointing at something else must not be able to make this
// process read an unbounded response into memory.
func TestGetRejectsAnOversizedBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"history_latest_ledger": 1}`))
		_, _ = w.Write([]byte(strings.Repeat(" ", maxBodyBytes)))
	}))
	defer srv.Close()

	_, err := NewClient(srv.URL).LatestLedger(context.Background())
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("want an oversized-body error, got %v", err)
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		in   string
		want time.Duration
	}{
		{"", 0},
		{"5", 5 * time.Second},
		{"0", 0},
		{"-3", 0},
		{"soon", 0},
		{now.Add(30 * time.Second).Format(http.TimeFormat), 30 * time.Second},
		{now.Add(-time.Minute).Format(http.TimeFormat), 0},
	}
	for _, c := range cases {
		hdr := http.Header{}
		if c.in != "" {
			hdr.Set("Retry-After", c.in)
		}
		if got := parseRetryAfter(hdr, now); got != c.want {
			t.Errorf("Retry-After %q: got %s, want %s", c.in, got, c.want)
		}
	}
}

func TestBackoffIsCappedAndJittered(t *testing.T) {
	for attempt := 1; attempt <= 10; attempt++ {
		d, err := backoffDelay(attempt, 0)
		if err != nil {
			t.Fatalf("backoffDelay(%d): %v", attempt, err)
		}
		if d < baseBackoff/2 || d > maxBackoff {
			t.Errorf("attempt %d: backoff %s outside [%s, %s]", attempt, d, baseBackoff/2, maxBackoff)
		}
	}
	// A Retry-After is passed through, not clamped: the budget decides.
	if got, err := backoffDelay(1, time.Hour); err != nil || got != time.Hour {
		t.Errorf("Retry-After not honoured: %s (err %v)", got, err)
	}
}
