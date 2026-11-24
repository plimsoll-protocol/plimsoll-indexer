package worker

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
)

const (
	// webhookMaxAttempts is the total number of delivery attempts, the first
	// one included.
	webhookMaxAttempts = 5
	// webhookBaseBackoff is doubled after each failed attempt.
	webhookBaseBackoff     = 30 * time.Second
	webhookDeliveryTimeout = 10 * time.Second
	webhookBatchSize       = 20
)

// WebhookStore is the storage surface the notifier needs.
type WebhookStore interface {
	ListWebhooks(ctx context.Context) ([]coverage.Webhook, error)
	LatestSupply(ctx context.Context, sac string) (*coverage.Supply, error)
	LatestReports(ctx context.Context, sac string) (map[uint32]coverage.Report, error)
	SetWebhookAlerted(ctx context.Context, id int64, alerted bool) error
	EnqueueWebhookDelivery(ctx context.Context, webhookID int64, payload []byte) error
	DueWebhookDeliveries(ctx context.Context, limit, maxAttempts int) ([]coverage.WebhookDelivery, error)
	RecordWebhookAttempt(ctx context.Context, id int64, status int, errMsg string, delivered bool, nextAt time.Time) error
}

// WebhookNotifier raises one alert when an asset's coverage falls below a
// webhook's min_bps, and re-arms once coverage is back at or above it.
//
// Alerts are queued in webhook_deliveries and sent as JSON POSTs signed with
// HMAC-SHA256 under Secret. A delivery is retried with exponential backoff
// until it succeeds or has been attempted webhookMaxAttempts times.
type WebhookNotifier struct {
	Store  WebhookStore
	Secret string
	Log    *slog.Logger
	// Now and Backoff are overridable in tests. Backoff defaults to
	// webhookBaseBackoff.
	Now     func() time.Time
	Backoff time.Duration
	// Client defaults to an http.Client with a 10-second timeout.
	Client *http.Client
}

// Run evaluates every registration and then sends the deliveries that are due.
// Call it on a ticker.
func (n *WebhookNotifier) Run(ctx context.Context) error {
	return errors.Join(n.evaluate(ctx), n.deliver(ctx))
}

func (n *WebhookNotifier) evaluate(ctx context.Context) error {
	hooks, err := n.Store.ListWebhooks(ctx)
	if err != nil {
		return fmt.Errorf("list webhooks: %w", err)
	}
	var errs []error
	for _, h := range hooks {
		supply, err := n.Store.LatestSupply(ctx, h.SAC)
		if err != nil {
			errs = append(errs, fmt.Errorf("webhook %d supply for %s: %w", h.ID, h.SAC, err))
			continue
		}
		reports, err := n.Store.LatestReports(ctx, h.SAC)
		if err != nil {
			errs = append(errs, fmt.Errorf("webhook %d reports for %s: %w", h.ID, h.SAC, err))
			continue
		}
		// No snapshot or no report means coverage is unknown, not that it
		// dropped: an unlisted or brand-new asset must not fire an alert.
		cov := coverage.Compute(supply, reports, h.MinTier)
		if cov == nil || cov.Bps >= h.MinBps {
			if h.Alerted {
				if err := n.Store.SetWebhookAlerted(ctx, h.ID, false); err != nil {
					errs = append(errs, fmt.Errorf("re-arm webhook %d: %w", h.ID, err))
				}
			}
			continue
		}
		if h.Alerted {
			continue // already alerted for this crossing
		}
		payload, err := webhookPayloadFor(h, cov, n.now())
		if err != nil {
			errs = append(errs, fmt.Errorf("webhook %d payload: %w", h.ID, err))
			continue
		}
		if err := n.Store.EnqueueWebhookDelivery(ctx, h.ID, payload); err != nil {
			errs = append(errs, fmt.Errorf("queue webhook %d: %w", h.ID, err))
			continue
		}
		if err := n.Store.SetWebhookAlerted(ctx, h.ID, true); err != nil {
			errs = append(errs, fmt.Errorf("mark webhook %d alerted: %w", h.ID, err))
			continue
		}
		n.Log.Info("coverage below webhook threshold",
			"webhook_id", h.ID, "sac", h.SAC, "bps", cov.Bps, "min_bps", h.MinBps)
	}
	return errors.Join(errs...)
}

func (n *WebhookNotifier) deliver(ctx context.Context) error {
	due, err := n.Store.DueWebhookDeliveries(ctx, webhookBatchSize, webhookMaxAttempts)
	if err != nil {
		return fmt.Errorf("list due webhook deliveries: %w", err)
	}
	var errs []error
	for _, d := range due {
		status, errMsg, ok := n.post(ctx, d)
		nextAt := n.now().Add(n.backoff(d.Attempts + 1))
		if err := n.Store.RecordWebhookAttempt(ctx, d.ID, status, errMsg, ok, nextAt); err != nil {
			errs = append(errs, fmt.Errorf("record webhook delivery %d: %w", d.ID, err))
			continue
		}
		if ok {
			n.Log.Info("webhook delivered",
				"webhook_id", d.WebhookID, "delivery_id", d.ID, "attempt", d.Attempts+1)
			continue
		}
		n.Log.Warn("webhook delivery failed",
			"webhook_id", d.WebhookID, "delivery_id", d.ID, "attempt", d.Attempts+1,
			"status", status, "err", errMsg, "retry_at", nextAt)
	}
	return errors.Join(errs...)
}

// post sends one signed delivery. It returns the HTTP status (0 on a network
// error), the error text, and whether the receiver accepted it.
func (n *WebhookNotifier) post(ctx context.Context, d coverage.WebhookDelivery) (int, string, bool) {
	ctx, cancel := context.WithTimeout(ctx, webhookDeliveryTimeout)
	defer cancel()
	// #nosec G107,G704 -- the URL is an admin-registered webhook, validated
	// as absolute http(s) by the admin API, and the body is signed.
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.URL, bytes.NewReader(d.Payload))
	if err != nil {
		return 0, err.Error(), false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "plimsoll-indexer")
	req.Header.Set("X-Plimsoll-Signature", signWebhook(n.Secret, d.Payload))
	req.Header.Set("X-Plimsoll-Delivery", strconv.FormatInt(d.ID, 10))

	resp, err := n.client().Do(req)
	if err != nil {
		return 0, err.Error(), false
	}
	defer func() { _ = resp.Body.Close() }()
	// The body is not part of the contract; drain a little so the connection
	// can be reused, then discard it.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4*1024))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return resp.StatusCode, "", true
	}
	return resp.StatusCode, fmt.Sprintf("status %d", resp.StatusCode), false
}

func (n *WebhookNotifier) client() *http.Client {
	if n.Client != nil {
		return n.Client
	}
	return &http.Client{Timeout: webhookDeliveryTimeout}
}

func (n *WebhookNotifier) now() time.Time {
	if n.Now != nil {
		return n.Now().UTC()
	}
	return time.Now().UTC()
}

// backoff is the wait before attempt number attempt (1 is the first retry
// after the initial delivery failed): base, 2×base, 4×base, and so on.
func (n *WebhookNotifier) backoff(attempt int) time.Duration {
	base := n.Backoff
	if base <= 0 {
		base = webhookBaseBackoff
	}
	if attempt < 1 {
		attempt = 1
	}
	return base << (attempt - 1)
}

// webhookPayload is the JSON body receivers get.
type webhookPayload struct {
	Event          string    `json:"event"`
	WebhookID      int64     `json:"webhook_id"`
	SAC            string    `json:"sac"`
	MinBps         uint32    `json:"min_bps"`
	MinTier        string    `json:"min_tier"`
	Bps            uint32    `json:"bps"`
	Tier           string    `json:"tier"`
	Supply         string    `json:"supply"`
	Reserves       string    `json:"reserves"`
	SupplyPostedAt time.Time `json:"supply_posted_at"`
	ReportAsOf     time.Time `json:"report_as_of"`
	SentAt         time.Time `json:"sent_at"`
}

// webhookEvent is the value of the payload's "event" field.
const webhookEvent = "coverage_dropped"

func webhookPayloadFor(h coverage.Webhook, cov *coverage.Result, sentAt time.Time) ([]byte, error) {
	body, err := json.Marshal(webhookPayload{
		Event:          webhookEvent,
		WebhookID:      h.ID,
		SAC:            h.SAC,
		MinBps:         h.MinBps,
		MinTier:        coverage.TierName(h.MinTier),
		Bps:            cov.Bps,
		Tier:           coverage.TierName(cov.Tier),
		Supply:         cov.Supply.String(),
		Reserves:       cov.Reserves.String(),
		SupplyPostedAt: cov.SupplyPostedAt.UTC(),
		ReportAsOf:     cov.ReportAsOf.UTC(),
		SentAt:         sentAt.UTC(),
	})
	if err != nil {
		return nil, fmt.Errorf("encode payload: %w", err)
	}
	return body, nil
}

// signWebhook returns the value of the X-Plimsoll-Signature header:
// "sha256=" followed by the lower-case hex HMAC-SHA256 of body under secret.
func signWebhook(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}
