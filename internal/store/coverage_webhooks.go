package store

import (
	"context"
	"fmt"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
)

// CreateCoverageWebhook stores a registration and returns it with its id and
// creation time set. URL, SAC and thresholds must already be validated.
func (s *Store) CreateCoverageWebhook(ctx context.Context, w coverage.Webhook) (coverage.Webhook, error) {
	err := s.pool.QueryRow(ctx, `
		INSERT INTO coverage_webhooks (url, sac, min_bps, min_tier)
		VALUES ($1, $2, $3, $4)
		RETURNING id, created_at`,
		w.URL, w.SAC, int32(w.MinBps), int16(w.MinTier)).Scan(&w.ID, &w.CreatedAt)
	if err != nil {
		return coverage.Webhook{}, fmt.Errorf("create coverage webhook: %w", err)
	}
	w.Alerted = false
	return w, nil
}

// DeleteCoverageWebhook removes a registration and its delivery log. It returns
// ErrNotFound when the id is unknown.
func (s *Store) DeleteCoverageWebhook(ctx context.Context, id int64) error {
	tag, err := s.pool.Exec(ctx, `DELETE FROM coverage_webhooks WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("delete coverage webhook %d: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ListCoverageWebhooks returns every registration, alerted or not: an alerted webhook
// still has to be observed so it can re-arm when coverage recovers.
func (s *Store) ListCoverageWebhooks(ctx context.Context) ([]coverage.Webhook, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, url, sac, min_bps, min_tier, alerted, created_at
		FROM coverage_webhooks ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list coverage webhooks: %w", err)
	}
	defer rows.Close()
	var out []coverage.Webhook
	for rows.Next() {
		var w coverage.Webhook
		var minBps int32
		var minTier int16
		if err := rows.Scan(&w.ID, &w.URL, &w.SAC, &minBps, &minTier, &w.Alerted, &w.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan webhook: %w", err)
		}
		w.MinBps, w.MinTier = uint32(minBps), uint32(minTier)
		out = append(out, w)
	}
	return out, rows.Err()
}

// SetCoverageWebhookAlerted records whether the webhook has already alerted for the
// current crossing.
func (s *Store) SetCoverageWebhookAlerted(ctx context.Context, id int64, alerted bool) error {
	if _, err := s.pool.Exec(ctx, `UPDATE coverage_webhooks SET alerted = $2 WHERE id = $1`, id, alerted); err != nil {
		return fmt.Errorf("set coverage webhook %d alerted=%t: %w", id, alerted, err)
	}
	return nil
}

// EnqueueCoverageWebhookDelivery queues payload for immediate delivery. The delivery
// is due as soon as it is written.
func (s *Store) EnqueueCoverageWebhookDelivery(ctx context.Context, webhookID int64, payload []byte) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO coverage_webhook_deliveries (webhook_id, payload) VALUES ($1, $2::jsonb)`,
		webhookID, string(payload))
	if err != nil {
		return fmt.Errorf("enqueue coverage webhook %d delivery: %w", webhookID, err)
	}
	return nil
}

// DueCoverageWebhookDeliveries returns undelivered deliveries that are due and have
// attempts left, oldest first.
func (s *Store) DueCoverageWebhookDeliveries(ctx context.Context, limit, maxAttempts int) ([]coverage.WebhookDelivery, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT d.id, d.webhook_id, w.url, d.payload, d.attempts
		FROM coverage_webhook_deliveries d
		JOIN coverage_webhooks w ON w.id = d.webhook_id
		WHERE d.delivered_at IS NULL
		  AND d.attempts < $1
		  AND d.next_attempt_at <= now()
		ORDER BY d.next_attempt_at, d.id
		LIMIT $2`, maxAttempts, limit)
	if err != nil {
		return nil, fmt.Errorf("list due coverage webhook deliveries: %w", err)
	}
	defer rows.Close()
	var out []coverage.WebhookDelivery
	for rows.Next() {
		var d coverage.WebhookDelivery
		if err := rows.Scan(&d.ID, &d.WebhookID, &d.URL, &d.Payload, &d.Attempts); err != nil {
			return nil, fmt.Errorf("scan webhook delivery: %w", err)
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// RecordCoverageWebhookAttempt appends one delivery attempt to the log: the response
// status (0 for a network error), the error text, whether it succeeded, and
// when the next attempt is due.
func (s *Store) RecordCoverageWebhookAttempt(ctx context.Context, id int64, status int, errMsg string, delivered bool, nextAt time.Time) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE coverage_webhook_deliveries
		SET attempts = attempts + 1,
		    last_status = NULLIF($2, 0),
		    last_error = NULLIF($3, ''),
		    delivered_at = CASE WHEN $4 THEN now() ELSE delivered_at END,
		    next_attempt_at = $5
		WHERE id = $1`,
		id, status, errMsg, delivered, nextAt)
	if err != nil {
		return fmt.Errorf("record coverage webhook delivery %d attempt: %w", id, err)
	}
	return nil
}
