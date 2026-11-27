package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

type deliveryStore interface {
	ListWebhooks(context.Context) ([]store.Webhook, error)
	RecordWebhookDelivery(ctx context.Context, id int64, at time.Time, failure string) error
	GetWebhook(context.Context, int64) (store.Webhook, bool, error)
	CreateWebhookDLQ(context.Context, int64, []byte, string, int) error
	ListWebhookDLQ(context.Context, int64, int) ([]store.WebhookDLQ, error)
	GetWebhookDLQ(context.Context, int64, int64) (store.WebhookDLQ, bool, error)
	UpdateWebhookDLQFailure(context.Context, int64, int64, string, int) (bool, error)
	DeleteWebhookDLQ(context.Context, int64, int64) (bool, error)
}

const maxRetries = 3

type Dispatcher struct {
	Store  deliveryStore
	Codec  *SecretCodec
	Client *http.Client
	Now    func() time.Time
	Store      subscriptionStore
	Codec      *SecretCodec
	Client     *http.Client
	Now        func() time.Time
	RetryDelay time.Duration
}

type TOMLChangedPayload struct {
	Event      string    `json:"event"`
	Issuer     string    `json:"issuer"`
	HomeDomain string    `json:"home_domain"`
	OldHash    string    `json:"old_hash"`
	NewHash    string    `json:"new_hash"`
	OccurredAt time.Time `json:"occurred_at"`
}

func NewDispatcher(db deliveryStore, codec *SecretCodec) *Dispatcher {
type SupplyDropPayload struct {
	Event        string    `json:"event"`
	SAC          string    `json:"sac"`
	Previous     string    `json:"previous"`
	Current      string    `json:"current"`
	DropFraction float64   `json:"drop_fraction"`
	OccurredAt   time.Time `json:"occurred_at"`
}

func NewDispatcher(db subscriptionStore, codec *SecretCodec) *Dispatcher {
	return &Dispatcher{
		Store: db,
		Codec: codec,
		Client: &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		Now:        time.Now,
		RetryDelay: time.Second,
	}
}

func (d *Dispatcher) TOMLChanged(ctx context.Context, issuer, domain, oldHash, newHash string) error {
	webhooks, err := d.Store.ListWebhooks(ctx)
	if err != nil {
		return fmt.Errorf("list webhook subscriptions: %w", err)
	}
	now := d.Now().UTC()
	payload, err := json.Marshal(TOMLChangedPayload{
		Event:      "issuer_toml_changed",
		Issuer:     issuer,
		HomeDomain: domain,
		OldHash:    oldHash,
		NewHash:    newHash,
		OccurredAt: now,
	})
	if err != nil {
		return fmt.Errorf("encode TOML change webhook payload: %w", err)
	}

	var failures []error
	for _, w := range webhooks {
		delivered := ""
		if err := d.deliver(ctx, w, payload); err != nil {
			delivered = err.Error()
			failures = append(failures, fmt.Errorf("deliver webhook %d: %w", w.ID, err))
		}
		// Track the attempt per endpoint even when it failed, so GET
		// /v1/webhooks shows which destination is broken.
		if err := d.Store.RecordWebhookDelivery(ctx, w.ID, now, delivered); err != nil {
			failures = append(failures, fmt.Errorf("record delivery for webhook %d: %w", w.ID, err))
		}
	}
	return errors.Join(failures...)
}

func (d *Dispatcher) SupplyDrop(ctx context.Context, sac, prev, curr string, drop float64) error {
	webhooks, err := d.Store.ListWebhooks(ctx)
	if err != nil {
		return fmt.Errorf("list webhook subscriptions: %w", err)
	}
	now := d.Now().UTC()
	payload, err := json.Marshal(SupplyDropPayload{
		Event:        "supply_drop_alert",
		SAC:          sac,
		Previous:     prev,
		Current:      curr,
		DropFraction: drop,
		OccurredAt:   now,
	})
	if err != nil {
		return fmt.Errorf("encode supply drop webhook payload: %w", err)
	}

	var failures []error
	for _, w := range webhooks {
		if err := d.deliver(ctx, w, payload); err != nil {
			failures = append(failures, fmt.Errorf("deliver webhook %d: %w", w.ID, err))
		attempts, err := d.deliverWithRetries(ctx, w, payload)
		if err != nil {
			dlqErr := d.Store.CreateWebhookDLQ(ctx, w.ID, payload, err.Error(), attempts)
			if dlqErr != nil {
				err = errors.Join(err, fmt.Errorf("store failed delivery in webhook DLQ: %w", dlqErr))
			}
			failures = append(failures, fmt.Errorf("deliver webhook %d after %d attempts: %w", w.ID, attempts, err))
		}
	}
	return errors.Join(failures...)
}

func (d *Dispatcher) DeliverTest(ctx context.Context, w store.Webhook) (int, error) {
	now := d.Now().UTC()
	payload, err := json.Marshal(map[string]interface{}{ 
		"event":       "test",
		"webhook_id":  w.ID,
		"occurred_at": now,
	})
	if err != nil {
		return 0, fmt.Errorf("encode test webhook payload: %w", err)
	}
	secret, err := d.Codec.Decrypt(w.SecretCiphertext)
	if err != nil {
		return 0, err
	}
	if !VerifySecretHash(secret, w.SecretHash) {
		return 0, fmt.Errorf("webhook secret hash mismatch")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(payload))
	if err != nil {
		return 0, fmt.Errorf("create webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Plimsoll-Signature-256", Sign(payload, secret))
	resp, err := d.Client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("send webhook request: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4097))
	return resp.StatusCode, nil
// RetryWebhookDLQ immediately redelivers one stored payload, applying the same
// bounded retry policy as a fresh event. A successful delivery removes the row.
func (d *Dispatcher) RetryWebhookDLQ(ctx context.Context, webhookID, deliveryID int64) (bool, error) {
	item, found, err := d.Store.GetWebhookDLQ(ctx, webhookID, deliveryID)
	if err != nil || !found {
		return found, err
	}
	w, found, err := d.Store.GetWebhook(ctx, webhookID)
	if err != nil || !found {
		return false, err
	}

	attempts, deliveryErr := d.deliverWithRetries(ctx, w, item.Payload)
	if deliveryErr != nil {
		updated, err := d.Store.UpdateWebhookDLQFailure(ctx, webhookID, deliveryID, deliveryErr.Error(), attempts)
		if err != nil {
			return true, errors.Join(deliveryErr, fmt.Errorf("update failed webhook DLQ retry: %w", err))
		}
		if !updated {
			return false, nil
		}
		return true, fmt.Errorf("retry webhook delivery after %d attempts: %w", attempts, deliveryErr)
	}
	deleted, err := d.Store.DeleteWebhookDLQ(ctx, webhookID, deliveryID)
	if err != nil {
		return true, fmt.Errorf("remove delivered webhook DLQ entry: %w", err)
	}
	return deleted, nil
}

func (d *Dispatcher) deliverWithRetries(ctx context.Context, w store.Webhook, payload []byte) (int, error) {
	for attempt := 1; ; attempt++ {
		err := d.deliver(ctx, w, payload)
		if err == nil {
			return attempt, nil
		}
		if attempt > maxRetries {
			return attempt, err
		}
		delay := d.RetryDelay * time.Duration(1<<(attempt-1))
		if delay <= 0 {
			continue
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return attempt, errors.Join(err, ctx.Err())
		case <-timer.C:
		}
	}
}

func (d *Dispatcher) deliver(ctx context.Context, w store.Webhook, payload []byte) error {
	secret, err := d.Codec.Decrypt(w.SecretCiphertext)
	if err != nil {
		return err
	}
	if !VerifySecretHash(secret, w.SecretHash) {
		return fmt.Errorf("webhook secret hash mismatch")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.URL, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("create webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Plimsoll-Signature-256", Sign(payload, secret))
	resp, err := d.Client.Do(req)
	if err != nil {
		return fmt.Errorf("send webhook request: %w", err)
	}
	defer resp.Body.Close()
	if _, err := io.Copy(io.Discard, io.LimitReader(resp.Body, 4097)); err != nil {
		return fmt.Errorf("read webhook response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}
	return nil
}
