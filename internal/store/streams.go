package store

import (
	"context"
	"fmt"
)

type AuditRecord struct {
	ID        int64  `json:"id"`
	Action    string `json:"action"`
	Timestamp string `json:"timestamp"`
}

type ExportRecord struct {
	ID     int64  `json:"id"`
	Data   string `json:"data"`
}

func (s *Store) StreamAuditTrail(ctx context.Context, fn func(AuditRecord) error) error {
	rows, err := s.pool.Query(ctx, `SELECT id, action, timestamp FROM audit_trail ORDER BY id`)
	if err != nil {
		return fmt.Errorf("stream audit: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r AuditRecord
		if err := rows.Scan(&r.ID, &r.Action, &r.Timestamp); err != nil {
			return err
		}
		if err := fn(r); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *Store) StreamCSVExports(ctx context.Context, fn func(ExportRecord) error) error {
	rows, err := s.pool.Query(ctx, `SELECT id, data FROM csv_exports ORDER BY id`)
	if err != nil {
		return fmt.Errorf(
			"stream csv exports: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var r ExportRecord
		if err := rows.Scan(&r.ID, &r.Data); err != nil {
			return err
		}
		if err := fn(r); err != nil {
			return err
		}
	}
	return rows.Err()
}
