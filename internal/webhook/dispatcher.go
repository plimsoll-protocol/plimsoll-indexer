package webhook

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

type Dispatcher struct {
	store             webhookStore
	codec             *SecretCodec
	slackURL          string
	discordURL        string
	httpClient        *http.Client
}

type webhookStore interface {
	ListWebhooks(ctx context.Context) ([]Webhook, error)
}

func NewDispatcher(store webhookStore, codec *SecretCodec) *Dispatcher {
	return &Dispatcher{
		store:      store,
		codec:      codec,
		httpClient: &http.Client{Timeout: 10 * time.Second},
	}
}

func (d *Dispatcher) SetSlackURL(url string) {
	d.slackURL = url
}

func (d *Dispatcher) SetDiscordURL(url string) {
	d.discordURL = url
}

func (d *Dispatcher) Dispatch(ctx context.Context, eventType string, payload any) error {
	webhooks, err := d.store.ListWebhooks(ctx)
	if err != nil {
		return fmt.Errorf("list webhooks: %w", err)
	}

	body, err := json.Marshal(map[string]any{
		"event_type": eventType,
		"payload":    payload,
		"timestamp":  time.Now().UTC().Format(time.RFC3339),
	})
	if err != nil {
		return fmt.Errorf("marshal webhook: %w", err)
	}

	for _, wh := range webhooks {
		go func(w Webhook) {
			secret, _ := d.codec.Decrypt(w.EncryptedSecret)
			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, w.URL, bytes.NewReader(body))
			if err != nil {
				return
			}
			req.Header.Set("Content-Type", "application/json")
			if secret != "" {
				req.Header.Set("X-Plimsoll-Signature", sign(secret, body))
			}
			resp, err := d.httpClient.Do(req)
			if err != nil {
				slog.Error("webhook delivery failed", "url", w.URL, "err", err)
				return
			}
			defer resp.Body.Close()
		}(wh)
	}

	if eventType == "coverage_drop" {
		d.notifyChannels(payload)
	}

	return nil
}

func (d *Dispatcher) notifyChannels(payload any) {
	if d.slackURL == "" && d.discordURL == "" {
		return
	}
	data, ok := payload.(map[string]any)
	if !ok {
		return
	}
	asset, _ := data["asset"].(string)
	bps, _ := data["bps"].(uint32)
	threshold, _ := data["threshold"].(uint32)

	msg := fmt.Sprintf("Coverage drop detected for asset *%s*: current bps %d, threshold %d", asset, bps, threshold)

	if d.slackURL != "" {
		slackPayload, _ := json.Marshal(map[string]any{
			"attachments": []any{
				map[string]any{
					"color": "danger",
					"title": "Coverage Drop Alert",
					"text":  msg,
					"fields": []any{
						map[string]any{"title": "Asset", "value": asset, "short": true},
						map[string]any{"title": "BPS", "value": fmt.Sprintf("%d", bps), "short": true},
						map[string]any{"title": "Threshold", "value": fmt.Sprintf("%d", threshold), "short": true},
					},
				},
			},
		})
		go http.Post(d.slackURL, "application/json", bytes.NewReader(slackPayload))
	}

	if d.discordURL != "" {
		discordPayload, _ := json.Marshal(map[string]any{
			"embeds": []any{
				map[string]any{
					"title": "Coverage Drop Alert",
					"description": msg,
					"color": 15158332,
					"fields": []any{
						map[string]any{"name": "Asset", "value": asset, "inline": true},
						map[string]any{"name": "BPS", "value": fmt.Sprintf("%d", bps), "inline": true},
						map[string]any{"name": "Threshold", "value": fmt.Sprintf("%d", threshold), "inline": true},
					},
				},
			},
		})
		go http.Post(d.discordURL, "application/json", bytes.NewReader(discordPayload))
	}
}
