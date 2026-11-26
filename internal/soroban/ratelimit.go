package soroban

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Defaults for RateLimitTransport. A congested RPC node usually lifts its
// limit within a few seconds, so retries are cheap; the delays double from
// 500 ms to 30 s so a sustained limit backs a worker tick off instead of
// hammering the node.
const (
	defaultRateLimitRetries = 4
	defaultRateLimitBase    = 500 * time.Millisecond
	defaultRateLimitMax     = 30 * time.Second

	// drainLimit bounds how much of a 429 body is read before the connection
	// is reused. RPC error bodies are small.
	drainLimit = 1 << 16

	// replayBodyLimit is how large a request body without a replay function
	// may be and still be buffered so it can be resent.
	replayBodyLimit = 1 << 20

	// maxRetryAfter caps a Retry-After header, so a broken or hostile value
	// cannot park a worker for longer than a day. The request's own context
	// still bounds the wait.
	maxRetryAfter = 24 * time.Hour
)

// RateLimitTransport is an http.RoundTripper for a Soroban RPC node that
// retries HTTP 429 Too Many Requests responses with exponential backoff.
//
// The delay before a retry is the node's Retry-After header when it sends
// one, in either delta-seconds or HTTP-date form, and otherwise BaseDelay
// doubled once per attempt up to MaxDelay. Every back-off is logged at WARN
// level. A response that is not 429 passes straight through, as does the
// final 429 once retries are exhausted, so the RPC client still sees and
// reports the node's error.
//
// The zero value is usable: it wraps http.DefaultTransport with the defaults
// above and no logger.
type RateLimitTransport struct {
	// Base is the transport that performs the request. Nil means
	// http.DefaultTransport.
	Base http.RoundTripper
	// Log receives the WARN record written before each back-off. Nil disables
	// logging.
	Log *slog.Logger
	// MaxRetries is how many times one request is retried after a 429. Zero
	// means defaultRateLimitRetries.
	MaxRetries int
	// BaseDelay is the first back-off delay when the node sends no
	// Retry-After. Zero means defaultRateLimitBase.
	BaseDelay time.Duration
	// MaxDelay caps the doubled back-off delay. Zero means
	// defaultRateLimitMax.
	MaxDelay time.Duration
	// Now is the clock Retry-After HTTP-dates are compared against. Nil means
	// time.Now.
	Now func() time.Time
}

// NewRateLimitTransport returns a transport that retries 429 responses from
// base, logging each back-off to log.
func NewRateLimitTransport(base http.RoundTripper, log *slog.Logger) *RateLimitTransport {
	return &RateLimitTransport{Base: base, Log: log}
}

// RoundTrip implements http.RoundTripper.
func (t *RateLimitTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := t.Base
	if base == nil {
		base = http.DefaultTransport
	}
	maxRetries := t.MaxRetries
	if maxRetries <= 0 {
		maxRetries = defaultRateLimitRetries
	}
	baseDelay := t.BaseDelay
	if baseDelay <= 0 {
		baseDelay = defaultRateLimitBase
	}
	maxDelay := t.MaxDelay
	if maxDelay <= 0 {
		maxDelay = defaultRateLimitMax
	}

	// A body that cannot be re-read makes retrying unsafe: the second attempt
	// would send only the bytes the first one left behind.
	attemptReq, replayable := bufferBody(req)
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			next, err := rewind(attemptReq)
			if err != nil {
				return nil, err
			}
			attemptReq = next
		}

		resp, err := base.RoundTrip(attemptReq)
		if err != nil || resp.StatusCode != http.StatusTooManyRequests || !replayable || attempt >= maxRetries {
			return resp, err
		}

		// The body has to be read and closed before the connection can be
		// reused for the retry.
		drainAndClose(resp)

		retryAfter, hasRetryAfter := parseRetryAfter(resp.Header.Get("Retry-After"), t.now())
		wait := retryAfter
		if !hasRetryAfter {
			wait = backoffDelay(baseDelay, attempt, maxDelay)
		}
		if t.Log != nil {
			attrs := []any{
				"attempt", attempt + 1,
				"max_attempts", maxRetries + 1,
				"wait", wait,
				"url", req.URL.Redacted(),
			}
			if hasRetryAfter {
				attrs = append(attrs, "retry_after", strings.TrimSpace(resp.Header.Get("Retry-After")))
			}
			t.Log.Warn("rpc node is rate limiting; backing off", attrs...)
		}
		if err := sleep(req.Context(), wait); err != nil {
			return nil, err
		}
	}
}

func (t *RateLimitTransport) now() time.Time {
	if t.Now != nil {
		return t.Now()
	}
	return time.Now()
}

// backoffDelay returns base doubled once per attempt, capped at max.
func backoffDelay(base time.Duration, attempt int, max time.Duration) time.Duration {
	wait := base
	for i := 0; i < attempt; i++ {
		if wait >= max/2 {
			return max
		}
		wait *= 2
	}
	if wait > max {
		return max
	}
	return wait
}

// parseRetryAfter reads a Retry-After header, which is either delta-seconds or
// an HTTP-date. It reports whether the header was present and usable; an
// unusable value falls back to exponential backoff.
func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds < 0 {
			return 0, true
		}
		if seconds > int64(maxRetryAfter/time.Second) {
			return maxRetryAfter, true
		}
		return time.Duration(seconds) * time.Second, true
	}
	if at, err := http.ParseTime(value); err == nil {
		wait := at.Sub(now)
		if wait < 0 {
			wait = 0
		}
		if wait > maxRetryAfter {
			wait = maxRetryAfter
		}
		return wait, true
	}
	return 0, false
}

// sleep waits for d, returning early when ctx is done.
func sleep(ctx context.Context, d time.Duration) error {
	if d > 0 {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	return ctx.Err()
}

// bufferBody makes a request body replayable. A body that already knows how to
// produce itself is left alone; a small body without a replay function is
// buffered in memory, which is what an RPC node's JSON request always is. A
// body past replayBodyLimit is left in place and reports false, so the caller
// sends it once rather than risk a truncated retry.
func bufferBody(req *http.Request) (*http.Request, bool) {
	if req.Body == nil || req.GetBody != nil {
		return req, true
	}
	body, err := io.ReadAll(io.LimitReader(req.Body, replayBodyLimit+1))
	if err != nil {
		return req, false
	}
	rest := req.Body
	if len(body) > replayBodyLimit {
		req.Body = rejoinedBody{Reader: io.MultiReader(bytes.NewReader(body), rest), Closer: rest}
		return req, false
	}
	_ = rest.Close()
	out := req.Clone(req.Context())
	out.Body = io.NopCloser(bytes.NewReader(body))
	out.ContentLength = int64(len(body))
	out.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(body)), nil
	}
	return out, true
}

// rejoinedBody is a partially read body with its buffered prefix put back in
// front, used when a request is too large to replay.
type rejoinedBody struct {
	io.Reader
	io.Closer
}

// rewind returns a copy of req with a fresh body for the next attempt.
func rewind(req *http.Request) (*http.Request, error) {
	if req.Body == nil {
		return req, nil
	}
	if req.GetBody == nil {
		return nil, fmt.Errorf("retry %s: request body cannot be replayed", req.URL.Redacted())
	}
	body, err := req.GetBody()
	if err != nil {
		return nil, fmt.Errorf("retry %s: rewind request body: %w", req.URL.Redacted(), err)
	}
	out := req.Clone(req.Context())
	out.Body = body
	return out, nil
}

// drainAndClose reads a bounded amount of the response body so the underlying
// connection stays reusable, then closes it.
func drainAndClose(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, drainLimit))
	_ = resp.Body.Close()
}
