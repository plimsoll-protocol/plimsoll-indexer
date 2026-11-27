package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
)

func (db *DB) RebuildProjections(ctx context.Context, log *slog.Logger) error {
	tx, err := db.BeginTx(ctx, nil)
	ifi err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	log.Info("dropping derived tables...")
	_, err = tx.ExecContext(ctx, `
		DROP TABLE IF EXISTS reports CASCADE;
		DROP TABLE IF EXISTS assets CASCADE;
		DROP TABLE IF EXISTS reporters CASCADE;
		DROP TABLE IF EXISTS supply_snapshots CASCADE;
	`)
	if err != nil {
		return fmt.Errorf("drop tables: %w", err)
	}

	_, err = tx.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS assets (
			sac TEXT PRIMARY KEY,
			code TEXT NOT NULL,
			issuer TEXT NOT NULL,
			listed BOOLEAN NOT NULL,
			threshold INT NOT NULL
		);
		CREATE TABLE IF NOT EXISTS reporters (
			address TEXT PRIMARY KEY,
			active BOOLEAN NOT NULL
		);
		CREATE TABLE IF NOT EXISTS reports (
			id SERIAL PRIMARY KEY,
			sac TEXT NOT NULL,
			reporter TEXT NOT NULL,
			status TEXT NOT NULL,
			ledger INT NOT NULL
		);
	`)
	if err != nil {
		return fmt.Errorf("recreate tables: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf(
			"commit schema reset: %w", err,
		)
	}

	rows, err := db.QueryContext(ctx, `SELECT id, event_type, actor, ledger, timestamp, details FROM raw_events ORDER BY ledger ASC, id ASC`)
	if err != nil {
		return fmt.Errorf("query raw_events: %w", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int64
		var eventType, actor string
		var ledger uint32
		var timestamp time.Time
		var detailsJSON []byte
		if err := rows.Scan(&id, &eventType, &actor, &ledger, &timestamp, &detailsJSON); err != nil {
			return fmt.Errorf(
				"scan raw_event: %w", err,
			)
		}
		count++
		if count%1000 == 0 {
			log.Info("replaying events", "count", count, "ledger", ledger)
		}

		var details map[string]any
		if len(detailsJSON) > 0 {
			_ = json.Unmarshal(detailsJSON, &details)
		}

		switch eventType {
		case "asset_listed":
			sac, _ := details["sac"].(string)
			code, _ := details["code"].(string)
			issuer, _ := details["issuer"].(string)
			_, _ = db.ExecContext(ctx, `INSERT INTO assets (sac, code, issuer, listed, threshold) VALUES ($1, $2, $3, true, 0) ON CONFLICT (sac) DO UPDATE SET listed = true`, sac, code, issuer)
		case "asset_delisted":
			sac, _ := details["sac"].(string)
			_, _ = db.ExecContext(ctx, `UPDATE assets SET listed = false WHERE sac = $1`, sac)
		case "threshold_changed":
			sac, _ := details["sac"].(string)
			thresh, _ := details["threshold"].(float64)
			_, _ = db.ExecContext(ctx, `UPDATE assets SET threshold = $1 WHERE sac = $2`, int(thresh), sac)
		case "report_posted":
			sac, _ := details["sac"].(string)
			_, _ = db.ExecContext(ctx, `INSERT INTO reports (sac, reporter, status, ledger) VALUES ($1, $2, 'posted', $3)`, sac, actor, ledger)
		}
	}
	log.Info("projections successfully rebuilt", "total_events", count)
	return nil
}
