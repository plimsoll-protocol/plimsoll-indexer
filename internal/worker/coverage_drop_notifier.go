package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// CoverageDropEvent represents a coverage drop webhook payload or notification trigger.
type CoverageDropEvent struct {
	Asset     string `json:"asset"`
	Bps       uint32 `json:"bps"`
	Threshold uint32 `json:"threshold"`
}

// CoverageDropNotifier sends formatted messages to Slack or Discord webhook URLs.
type CoverageDropNotifier struct {
	SlackURL   string
	DiscordURL string
	HTTPClient interface {
		Do(req *http.Request) (*http.Response, error)
	}
}

func (n *CoverageDropNotifier) Notify(ctx context.Context, ev CoverageDropEvent) error {
	if n == nil || (n.SlackURL == "" && n.DiscordURL == "") {
		return nil
	}

	var errs []error
	client := n.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}

	text := fmt.Sprintf("🚨 Coverage Drop Alert\nAsset: %s\nBps: %d\nThreshold: %d", ev.Asset, ev.Bps, ev.Threshold)

	if n.SlackURL != "" {
		slackPayload := map[string]any{
			"attachments": []map[string]any{
				{
					"color": "danger",
					"title": "Coverage Drop Alert",
					"fields": []map[string]any{
						{"title": "Asset", "value": ev.Asset, "short": true},
						{"title": "Bps", "value": fmt.Sprintf("%d", ev.Bps), "short": true},
						{"title": "Threshold", "value": fmt.Sprintf("%d", ev.Threshold), "short": true},
					},
				},
			},
		}
		if err := postWebhook(ctx, client, n.SlackURL, slackPayload); err != nil {
			errs = append(errs, fmt.Errorf("slack webhook: %w", err))
		}
	}

	if n.DiscordURL != "" {
		discordPayload := map[string]any{
			"embeds": []map[string]any{
				{
					"title":       "Coverage Drop Alert",
					"description": text,
					"color":       15158332,
				},
			},
		}
		if err := postWebhook(ctx, client, n.DiscordURL, discordPayload); err != nil {
			errs = append(errs, fmt.Errorf("discord webhook: %w", err))
		}
	}

	return errorsJoin(errs...)
}

func postWebhook(ctx context.Context, client interface{ Do(req *http.Request) (*http.Response, error) }, url string, payload any) error {
	bs, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bs))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("bad status %d", resp.StatusCode)
	}
	return nil
}

func errorsJoin(errs ...error) error {
	var nonNil []error
	for _, e := range errs {
		if e != nil {
			nonNil = append(nonNil, e)
		}
	}
	if len(nonNil) == 0 {
		return nil
	}
	return fmt.Errorf("%v", nonNil)
}
