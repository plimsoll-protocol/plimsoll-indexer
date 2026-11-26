package store

import (
	"context"
	"fmt"
	"time"
)

type Reporter struct {
	Reporter   string    `json:"reporter"`
	Tier       int       `json:"tier"`
	LastPostAt time.Time `json:"last_post_at"`
}

func (s *Store) GetAssetReporters(ctx context.Context, sac string) ([]Reporter, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT reporter, tier, MAX(posted_at) as last_post_at
		FROM reports
		WHERE sac = $1
		GROUP BY reporter, tier
		ORDER BY tier DESC, last_post_at DESC
	`, sac)
	if err != nil {
		return nil, fmt.Errorf("get asset reporters: %w", err)
	}
	defer rows.Close()

	var out []Reporter
	for rows.Next() {
		var r Reporter
		if err := rows.Scan(&r.Reporter, &r.Tier, &r.LastPostAt); err != nil {
			return nil, fmt.Errorf("scan reporter: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
