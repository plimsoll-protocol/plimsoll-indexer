package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

type TimelineEvent struct {
	Type      string    `json:"type"`
	Actor     string    `json:"actor"`
	Ledger    uint32    `json:"ledger"`
	Timestamp time.Time `json:"timestamp"`
	Details   any       `json:"details,omitempty"`
}

func (db *DB) AssetTimeline(ctx context.Context, sac string, cursor string, limit int) ([]TimelineEvent, string, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}

	var minLedger uint32 = 0
	if cursor != "" {
		dec, err := base64.StdEncoding.DecodeString(cursor)
		if err == nil {
			var c uint32
			if err := json.Unmarshal(dec, &c); err == nil {
				minLedger = c
			}
		}
	}

	query := `
		SELECT event_type, actor, ledger, timestamp, details
		FROM raw_events
		WHERE (details->>'sac' = $1 OR details->>'asset' = $1) AND ledger >= $2
		ORDER BY ledger ASC, id ASC
		LIMIT $3
	`

	rows, err := db.QueryContext(ctx, query, sac, minLedger, limit+1)
	if err != nil {
		return nil, "", fmt.Errorf("query timeline: %w", err)
	}
	defer rows.Close()

	var events []TimelineEvent
	for rows.Next() {
		var ev TimelineEvent
		var detailsJSON []byte
		var rawType string
		if err := rows.Scan(&rawType, &ev.Actor, &ev.Ledger, &ev.Timestamp, &detailsJSON);
		err != nil {
			return nil, "", fmt.Errorf("scan timeline: %w", err)
		}
		ev.Type = rawType
		var d any
		if len(detailsJSON) > 0 {
			_ = json.Unmarshal(detailsJSON, &d)
		}
		ev.Details = d
		events = append(events, ev)
	}

	var nextCursor string
	if len(events) > limit {
		events = events[:limit]
		nextEnc, _ := json.Marshal(events[len(events)-1].Ledger)
		nextCursor = base64.StdEncoding.EncodeToString(nextEnc)
	}

	return events, nextCursor, nil
}
