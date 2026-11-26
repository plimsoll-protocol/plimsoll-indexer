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

type subscriptionStore interface {
	ListWebhooks(context.Context) ([]store.Webhook, error)
}

type Dispatcher struct {
	Store  subscriptionStore
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
		if err := d.deliver(ctx, w, payload); err != nil {
			failures = append(failures, fmt.Errorf("deliver webhook %d: %w", w.ID, err))
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
