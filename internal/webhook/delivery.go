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
}

type Dispatcher struct {
	Store  deliveryStore
	Codec  *SecretCodec
	Client *http.Client
	Now    func() time.Time
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
	return &Dispatcher{
		Store: db,
		Codec: codec,
		Client: &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		Now: time.Now,
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
