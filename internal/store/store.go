// Package store persists indexed Plimsoll state in Postgres.
package store

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

//go:embed schema.sql
var schema string

var ErrNotFound = errors.New("not found")

type Store struct {
	pool *pgxpool.Pool
}

func Open(ctx context.Context, url string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	// Timestamps leave the API in UTC regardless of the server's zone.
	cfg.ConnConfig.RuntimeParams["timezone"] = "UTC"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	if _, err := pool.Exec(ctx, schema); err != nil {
		pool.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// ---- ingest cursor --------------------------------------------------------

// Progress is how far ingestion has reached.
type Progress struct {
	// Cursor and ScannedLedger advance on every page, empty pages included.
	Cursor        string
	ScannedLedger uint32
	// EventLedger and EventAt describe the last event actually written. A scan
	// that keeps advancing while these stand still means pages are arriving but
	// nothing in them is being stored.
	EventLedger uint32
	EventAt     time.Time
}

// Progress returns how far ingestion for name has reached.
func (s *Store) Progress(ctx context.Context, name string) (Progress, error) {
	var p Progress
	var scanned, event int64
	var at *time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT cursor, ledger, event_ledger, event_at FROM ingest_cursor WHERE name = $1`, name,
	).Scan(&p.Cursor, &scanned, &event, &at)
	if errors.Is(err, pgx.ErrNoRows) {
		return Progress{}, nil
	}
	p.ScannedLedger, p.EventLedger = uint32(scanned), uint32(event)
	if at != nil {
		p.EventAt = *at
	}
	return p, err
}

// ApplyEvents writes decoded events and advances the cursor in one transaction.
func (s *Store) ApplyEvents(ctx context.Context, name, cursor string, ledger uint32, events []any) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, ev := range events {
		if err := applyEvent(ctx, tx, ev); err != nil {
			return err
		}
	}
	// GREATEST keeps the recorded event position monotonic, so a page that
	// carries nothing cannot walk it backwards.
	var eventLedger int64
	var eventAt *time.Time
	for _, ev := range events {
		m, ok := ev.(soroban.WithMeta)
		if !ok {
			continue
		}
		if meta := m.EventMeta(); int64(meta.Ledger) > eventLedger {
			eventLedger = int64(meta.Ledger)
			at := meta.ClosedAt
			eventAt = &at
		}
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO ingest_cursor (name, cursor, ledger, event_ledger, event_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (name) DO UPDATE
		SET cursor = EXCLUDED.cursor,
		    ledger = EXCLUDED.ledger,
		    event_ledger = GREATEST(ingest_cursor.event_ledger, EXCLUDED.event_ledger),
		    event_at = CASE
		        WHEN EXCLUDED.event_ledger >= ingest_cursor.event_ledger THEN EXCLUDED.event_at
		        ELSE ingest_cursor.event_at
		    END,
		    updated_at = now()`,
		name, cursor, int64(ledger), eventLedger, eventAt)
	if err != nil {
		return fmt.Errorf("save cursor: %w", err)
	}
	return tx.Commit(ctx)
}

func applyEvent(ctx context.Context, tx pgx.Tx, ev any) error {
	var err error
	switch e := ev.(type) {
	case soroban.AssetListed:
		_, err = tx.Exec(ctx, `
			INSERT INTO assets (sac, code, issuer, listed_ledger, listed_at, event_id)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (sac) DO NOTHING`,
			e.SAC, e.Code, e.Issuer, int64(e.Ledger), e.ClosedAt, e.ID)
	case soroban.SupplyPosted:
		_, err = tx.Exec(ctx, `
			INSERT INTO supply_snapshots
			  (event_id, sac, amount, at_ledger, breakdown_hash, poster, posted_ledger, posted_at, tx_hash, source)
			VALUES ($1, $2, $3::numeric, $4, $5, $6, $7, $8, $9, 'event')
			ON CONFLICT (event_id) DO NOTHING`,
			e.ID, e.SAC, e.Amount.String(), int64(e.AtLedger), e.BreakdownHash, e.Poster,
			int64(e.Ledger), e.ClosedAt, e.TxHash)
	case soroban.ReservePosted:
		_, err = tx.Exec(ctx, `
			INSERT INTO reserve_reports
			  (event_id, sac, tier, amount, as_of, reporter, doc_hash, doc_uri, posted_ledger, posted_at, tx_hash, source)
			VALUES ($1, $2, $3, $4::numeric, $5, $6, $7, $8, $9, $10, $11, 'event')
			ON CONFLICT (event_id) DO NOTHING`,
			e.ID, e.SAC, int16(e.Tier), e.Amount.String(), e.AsOf, e.Reporter, e.DocHash, e.DocURI,
			int64(e.Ledger), e.ClosedAt, e.TxHash)
	case soroban.ReporterSet:
		_, err = tx.Exec(ctx, `
			INSERT INTO reporters (address, role, name, active, updated_ledger, updated_at)
			VALUES ($1, $2, $3, true, $4, $5)
			ON CONFLICT (address) DO UPDATE
			SET role = EXCLUDED.role, name = EXCLUDED.name, active = true,
			    updated_ledger = EXCLUDED.updated_ledger, updated_at = EXCLUDED.updated_at`,
			e.Reporter, int16(e.Role), e.Name, int64(e.Ledger), e.ClosedAt)
	case soroban.ReporterRevoked:
		_, err = tx.Exec(ctx, `
			UPDATE reporters SET active = false, updated_ledger = $2, updated_at = $3
			WHERE address = $1`,
			e.Reporter, int64(e.Ledger), e.ClosedAt)
	}
	if err != nil {
		return fmt.Errorf("apply %T: %w", ev, err)
	}
	return nil
}

// ---- reads ----------------------------------------------------------------

const assetCols = `sac, code, issuer, listed_ledger, listed_at, toml, toml_checked_at, COALESCE(toml_error, '')`

func scanAsset(row pgx.Row) (coverage.Asset, error) {
	var a coverage.Asset
	var listed int64
	err := row.Scan(&a.SAC, &a.Code, &a.Issuer, &listed, &a.ListedAt, &a.Toml, &a.TomlCheckedAt, &a.TomlError)
	a.ListedLedger = uint32(listed)
	return a, err
}

func (s *Store) ListAssets(ctx context.Context) ([]coverage.Asset, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+assetCols+` FROM assets ORDER BY code, issuer`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []coverage.Asset
	for rows.Next() {
		a, err := scanAsset(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) GetAsset(ctx context.Context, sac string) (coverage.Asset, error) {
	a, err := scanAsset(s.pool.QueryRow(ctx, `SELECT `+assetCols+` FROM assets WHERE sac = $1`, sac))
	if errors.Is(err, pgx.ErrNoRows) {
		return a, ErrNotFound
	}
	return a, err
}

func (s *Store) SupplyHistory(ctx context.Context, sac string, limit int) ([]coverage.Supply, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT amount::text, at_ledger, posted_at, breakdown_hash, poster, tx_hash, source
		FROM supply_snapshots WHERE sac = $1
		ORDER BY at_ledger DESC LIMIT $2`, sac, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []coverage.Supply
	for rows.Next() {
		var sp coverage.Supply
		var amount string
		var ledger int64
		if err := rows.Scan(&amount, &ledger, &sp.PostedAt, &sp.BreakdownHash, &sp.Poster, &sp.TxHash, &sp.Source); err != nil {
			return nil, err
		}
		sp.Amount = mustBig(amount)
		sp.Ledger = uint32(ledger)
		out = append(out, sp)
	}
	return out, rows.Err()
}

// LatestSupply returns nil when nothing has been posted.
func (s *Store) LatestSupply(ctx context.Context, sac string) (*coverage.Supply, error) {
	h, err := s.SupplyHistory(ctx, sac, 1)
	if err != nil || len(h) == 0 {
		return nil, err
	}
	return &h[0], nil
}

func (s *Store) ReportHistory(ctx context.Context, sac string, limit int) ([]coverage.Report, error) {
	return s.queryReports(ctx, `
		SELECT tier, amount::text, as_of, posted_at, reporter, doc_hash, doc_uri, tx_hash, source
		FROM reserve_reports WHERE sac = $1
		ORDER BY posted_ledger DESC, as_of DESC LIMIT $2`, sac, limit)
}

// LatestReports returns the newest report (by as_of) for each tier.
func (s *Store) LatestReports(ctx context.Context, sac string) (map[uint32]coverage.Report, error) {
	list, err := s.queryReports(ctx, `
		SELECT DISTINCT ON (tier) tier, amount::text, as_of, posted_at, reporter, doc_hash, doc_uri, tx_hash, source
		FROM reserve_reports WHERE sac = $1
		ORDER BY tier, as_of DESC`, sac)
	if err != nil {
		return nil, err
	}
	out := make(map[uint32]coverage.Report, len(list))
	for _, r := range list {
		out[r.Tier] = r
	}
	return out, nil
}

func (s *Store) queryReports(ctx context.Context, sql string, args ...any) ([]coverage.Report, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []coverage.Report
	for rows.Next() {
		var r coverage.Report
		var tier int16
		var amount string
		if err := rows.Scan(&tier, &amount, &r.AsOf, &r.PostedAt, &r.Reporter, &r.DocHash, &r.DocURI, &r.TxHash, &r.Source); err != nil {
			return nil, err
		}
		r.Tier = uint32(tier)
		r.Amount = mustBig(amount)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) Reporters(ctx context.Context) ([]coverage.Reporter, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT address, role, name, updated_at FROM reporters
		WHERE active ORDER BY role, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []coverage.Reporter
	for rows.Next() {
		var r coverage.Reporter
		var role int16
		if err := rows.Scan(&r.Address, &role, &r.Name, &r.UpdatedAt); err != nil {
			return nil, err
		}
		r.Role = uint32(role)
		out = append(out, r)
	}
	return out, rows.Err()
}

// ---- breakdowns and toml --------------------------------------------------

func (s *Store) SaveBreakdown(ctx context.Context, hash, sac string, body []byte) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO breakdowns (hash, sac, body) VALUES ($1, $2, $3)
		ON CONFLICT (hash) DO NOTHING`, hash, sac, string(body))
	return err
}

func (s *Store) Breakdown(ctx context.Context, hash string) ([]byte, error) {
	var body string
	err := s.pool.QueryRow(ctx, `SELECT body FROM breakdowns WHERE hash = $1`, hash).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return []byte(body), err
}

// SaveToml stores the stellar.toml lookup result (info JSON or an error).
func (s *Store) SaveToml(ctx context.Context, sac string, info []byte, lookupErr string, at time.Time) error {
	var infoArg any
	if info != nil {
		infoArg = string(info)
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE assets SET toml = COALESCE($2::jsonb, toml), toml_error = NULLIF($3, ''), toml_checked_at = $4
		WHERE sac = $1`, sac, infoArg, lookupErr, at)
	return err
}

func mustBig(s string) *big.Int {
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return new(big.Int)
	}
	return n
}

// ---- reconciliation -------------------------------------------------------

// SaveChainSupply records a snapshot read back from contract state, and reports
// whether it was new. Events are the normal source of these rows and carry the
// transaction that produced them; this fills what an ingestion gap lost, so the
// row is marked 'chain' with an empty tx_hash and is only written when it is
// ahead of everything stored for the asset.
func (s *Store) SaveChainSupply(ctx context.Context, sac string, sp coverage.Supply) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO supply_snapshots
		  (event_id, sac, amount, at_ledger, breakdown_hash, poster, posted_ledger, posted_at, tx_hash, source)
		SELECT $1, $2, $3::numeric, $4, $5, $6, $4, $7, '', 'chain'
		WHERE NOT EXISTS (
		    SELECT 1 FROM supply_snapshots WHERE sac = $2 AND at_ledger >= $4
		)
		ON CONFLICT (event_id) DO NOTHING`,
		chainSupplyID(sac, sp.Ledger), sac, sp.Amount.String(), int64(sp.Ledger),
		sp.BreakdownHash, sp.Poster, sp.PostedAt)
	if err != nil {
		return false, fmt.Errorf("save chain supply: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// SaveChainReport records a report read back from contract state, and reports
// whether it was new. See SaveChainSupply.
func (s *Store) SaveChainReport(ctx context.Context, sac string, r coverage.Report) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO reserve_reports
		  (event_id, sac, tier, amount, as_of, reporter, doc_hash, doc_uri, posted_ledger, posted_at, tx_hash, source)
		SELECT $1, $2, $3, $4::numeric, $5, $6, $7, $8, 0, $9, '', 'chain'
		WHERE NOT EXISTS (
		    SELECT 1 FROM reserve_reports WHERE sac = $2 AND tier = $3 AND as_of >= $5
		)
		ON CONFLICT (event_id) DO NOTHING`,
		chainReportID(sac, r.Tier, r.AsOf), sac, int16(r.Tier), r.Amount.String(), r.AsOf,
		r.Reporter, r.DocHash, r.DocURI, r.PostedAt)
	if err != nil {
		return false, fmt.Errorf("save chain report: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

func chainSupplyID(sac string, ledger uint32) string {
	return fmt.Sprintf("chain:%s:supply:%d", sac, ledger)
}

func chainReportID(sac string, tier uint32, asOf time.Time) string {
	return fmt.Sprintf("chain:%s:report:%d:%d", sac, tier, asOf.Unix())
}
