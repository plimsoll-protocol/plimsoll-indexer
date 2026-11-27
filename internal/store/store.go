// Package store persists indexed Plimsoll state in Postgres.
package store

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/metrics"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

var ErrNotFound = errors.New("not found")
var ErrForbidden = errors.New("forbidden")

// ErrLockNotAcquired is returned by TryPostLock when another poster instance
// already holds the advisory lock.
var ErrLockNotAcquired = errors.New("poster lock already held by another instance")

// MaxWebhooksPerKey is how many webhook endpoints one API key may register.
const MaxWebhooksPerKey = 10

// ErrWebhookLimit is returned by CreateWebhook when the key already holds
// MaxWebhooksPerKey endpoints.
var ErrWebhookLimit = errors.New("webhook endpoint limit reached")

type Store struct {
	pool *pgxpool.Pool

	// Publisher receives events newly written by ApplyEvents, after the
	// transaction that stored them commits, so live API streams see data that
	// is already readable through the REST endpoints. Nil publishes nothing.
	Publisher Publisher
}

// Publisher is the fan-out surface ingestion needs. It must not block: the
// ingest loop calls it once per page.
type Publisher interface {
	Publish(events []any)
}

type Webhook struct {
	ID int64

	// OwnerKeyHash is the SHA-256 fingerprint of the key that registered this
	// endpoint, never the key itself. It is empty for endpoints registered
	// before endpoints were keyed; those stay visible to every key and are
	// deletable by the admin token only.
	OwnerKeyHash     string
	URL              string
	SecretHash       string
	SecretCiphertext []byte
	CreatedAt        time.Time

	// Delivery tracking, written after every attempt by the dispatcher.
	LastDeliveryAt     *time.Time
	LastDeliveryStatus string
	LastDeliveryError  string
	Deliveries         int64
	Failures           int64
}

type WebhookDLQ struct {
	ID        int64
	WebhookID int64
	Payload   []byte
	LastError string
	Attempts  int
	FailedAt  time.Time
}

type CoverageData struct {
	Supply  *coverage.Supply
	Reports map[uint32]coverage.Report
}

// WebhookDelivery is one attempted delivery to a webhook destination.
// DeliveryLatencyMS is measured from sending the request to receiving the
// complete response, so slow upstreams are visible next to their failures.
type WebhookDelivery struct {
	ID                int64
	WebhookID         int64
	Event             string
	StatusCode        int
	Error             string
	DeliveryLatencyMS int64
	AttemptedAt       time.Time
}

// Open connects to the database and brings its schema up to date by applying
// any pending migrations (see migrate.go).
func Open(ctx context.Context, url string) (*Store, error) {
	return OpenWithPassword(ctx, url, "")
}

// OpenWithPassword is Open with the connection password replaced by password
// when it is non-empty. It is how a database password fetched from a secrets
// backend is used without writing it into DATABASE_URL.
func OpenWithPassword(ctx context.Context, url, password string) (*Store, error) {
	cfg, err := poolConfig(url, password)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	if err := migrate(ctx, pool, migrationsFS()); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate schema: %w", err)
	}
	return &Store{pool: pool}, nil
}

// poolConfig parses DATABASE_URL and applies the secrets-backend password, if
// any, before the pool connects.
func poolConfig(url, password string) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if password != "" {
		cfg.ConnConfig.Password = password
	}
	// Timestamps leave the API in UTC regardless of the server's zone.
	cfg.ConnConfig.RuntimeParams["timezone"] = "UTC"
	return cfg, nil
func measureQuery(op string) func() {
	start := time.Now()
	return func() {
		metrics.DBQueryDuration.WithLabelValues(op).Observe(time.Since(start).Seconds())
	}
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error {
	defer measureQuery("ping")()
	return s.pool.Ping(ctx)
}

// ---- ingest cursor --------------------------------------------------------

// LegacyIngestCursorName is the pre-streaming cursor retained for upgrades.
const LegacyIngestCursorName = "plimsoll-events"

// ContractIngestCursorName returns the checkpoint name for one event stream.
func ContractIngestCursorName(contractID string) string {
	return LegacyIngestCursorName + ":" + contractID
}

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

// ReporterLatestReport is the most recent report date for an active reporter.
type ReporterLatestReport struct {
	Reporter string
	AsOf     time.Time
}

// Progress returns how far ingestion for name has reached.
func (s *Store) Progress(ctx context.Context, name string) (Progress, error) {
	defer measureQuery("progress")()
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

// ApplyEvents writes one page of events and advances the cursor in one
// transaction: raw holds every event in the page, decoded or not, and events
// the decoded ones. A page delivered twice stores each raw event once.
func (s *Store) ApplyEvents(ctx context.Context, name, cursor string, ledger uint32, raw []soroban.RawEvent, events []any) error {
// ApplyEvents writes decoded events and advances the cursor in one transaction.
func (s *Store) ApplyEvents(ctx context.Context, name, cursor string, ledger uint32, events []any) error {
	defer measureQuery("apply_events")()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	for _, r := range raw {
		if err := saveRawEvent(ctx, tx, r); err != nil {
			return err
		}
	}
	// Only events this transaction actually wrote are published: a replayed
	// page after a reconnect must not re-send events subscribers already saw.
	written := make([]any, 0, len(events))
	for _, ev := range events {
		inserted, err := applyEvent(ctx, tx, ev)
		if err != nil {
			return err
		}
		if inserted {
			written = append(written, ev)
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
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	if s.Publisher != nil && len(written) > 0 {
		s.Publisher.Publish(written)
	}
	return nil
}

func saveRawEvent(ctx context.Context, tx pgx.Tx, r soroban.RawEvent) error {
	var closedAt *time.Time
	if !r.ClosedAt.IsZero() {
		closedAt = &r.ClosedAt
	}
	topics := r.TopicXDR
	if topics == nil {
		topics = []string{}
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO raw_events (id, contract, ledger, closed_at, tx_hash, topics_xdr, value_xdr)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (id) DO NOTHING`,
		r.ID, r.Contract, int64(r.Ledger), closedAt, r.TxHash, topics, r.ValueXDR)
	if err != nil {
		return fmt.Errorf("save raw event %s: %w", r.ID, err)
	}
	return nil
}

// ReplayStats counts what ReplayRawEvents read and applied.
type ReplayStats struct {
	// Events is every raw event read.
	Events int
	// Decoded is the events the current decoder turned into typed rows; the
	// rest are events it does not track.
	Decoded int
}

// ReplayRawEvents decodes every stored raw event again with the current
// decoder, in event order, and applies the typed rows, batchSize events per
// transaction. Applying is idempotent, so a replay over tables that already
// hold the rows leaves them as they are and adds the ones a newer decoder
// recognises. The ingest cursor is not touched.
//
// An event the decoder rejects stops the replay with its id; the batches
// before it stay applied.
func (s *Store) ReplayRawEvents(ctx context.Context, batchSize int) (ReplayStats, error) {
	if batchSize <= 0 {
		batchSize = 500
	}
	var stats ReplayStats
	after := ""
	for {
		batch, err := s.rawEventsAfter(ctx, after, batchSize)
		if err != nil {
			return stats, err
		}
		if len(batch) == 0 {
			return stats, nil
		}
		decoded := make([]any, 0, len(batch))
		for _, r := range batch {
			ev, err := soroban.Decode(r.Info())
			if err != nil {
				return stats, fmt.Errorf("replay: %w", err)
			}
			if ev != nil {
				decoded = append(decoded, ev)
			}
		}
		if err := s.applyDecoded(ctx, decoded); err != nil {
			return stats, fmt.Errorf("replay after %s: %w", after, err)
		}
		stats.Events += len(batch)
		stats.Decoded += len(decoded)
		after = batch[len(batch)-1].ID
	}
}

// rawEventsAfter returns up to limit raw events with ids after the given one,
// in id order. RPC event ids are zero-padded, so id order is chain order.
func (s *Store) rawEventsAfter(ctx context.Context, after string, limit int) ([]soroban.RawEvent, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, contract, ledger, closed_at, tx_hash, topics_xdr, value_xdr
		FROM raw_events WHERE id > $1 ORDER BY id LIMIT $2`, after, limit)
	if err != nil {
		return nil, fmt.Errorf("read raw events: %w", err)
	}
	defer rows.Close()
	var out []soroban.RawEvent
	for rows.Next() {
		var r soroban.RawEvent
		var ledger int64
		var closedAt *time.Time
		if err := rows.Scan(&r.ID, &r.Contract, &ledger, &closedAt, &r.TxHash, &r.TopicXDR, &r.ValueXDR); err != nil {
			return nil, fmt.Errorf("read raw events: %w", err)
		}
		r.Ledger = uint32(ledger)
		if closedAt != nil {
			r.ClosedAt = closedAt.UTC()
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) applyDecoded(ctx context.Context, events []any) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, ev := range events {
		if err := applyEvent(ctx, tx, ev); err != nil {
			return err
	// Keep the materialised latest coverage in step with the rows this batch
	// just wrote. The refresh is inside the transaction, so readers see the
	// old view until the new rows are committed, never a half-applied batch.
	if len(events) > 0 {
		if _, err := tx.Exec(ctx, `REFRESH MATERIALIZED VIEW latest_coverage`); err != nil {
			return fmt.Errorf("refresh latest_coverage: %w", err)
		}
	}
	return tx.Commit(ctx)
}

// RefreshLatestCoverage rebuilds the materialised view directly. Ingestion
// refreshes it itself after each batch; this is for writers outside the event
// path, such as reconciliation.
func (s *Store) RefreshLatestCoverage(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `REFRESH MATERIALIZED VIEW latest_coverage`); err != nil {
		return fmt.Errorf("refresh latest_coverage: %w", err)
	}
	return nil
}

func applyEvent(ctx context.Context, tx pgx.Tx, ev any) error {
// applyEvent writes one decoded event and reports whether it was new, so a
// re-ingested page is not fanned out a second time.
func applyEvent(ctx context.Context, tx pgx.Tx, ev any) (bool, error) {
	var tag pgconn.CommandTag
	var err error
	switch e := ev.(type) {
	case soroban.AssetListed:
		tag, err = tx.Exec(ctx, `
			INSERT INTO assets (sac, code, issuer, listed_ledger, listed_at, event_id)
			VALUES ($1, $2, $3, $4, $5, $6)
			ON CONFLICT (sac) DO NOTHING`,
			e.SAC, e.Code, e.Issuer, int64(e.Ledger), e.ClosedAt, e.ID)
	case soroban.SupplyPosted:
		tag, err = tx.Exec(ctx, `
			INSERT INTO supply_snapshots
			  (event_id, sac, amount, at_ledger, breakdown_hash, poster, posted_ledger, posted_at, tx_hash, source)
			VALUES ($1, $2, $3::numeric, $4, $5, $6, $7, $8, $9, 'event')
			ON CONFLICT (event_id) DO NOTHING`,
			e.ID, e.SAC, e.Amount.String(), int64(e.AtLedger), e.BreakdownHash, e.Poster,
			int64(e.Ledger), e.ClosedAt, e.TxHash)
		if err == nil {
			err = confirmBreakdown(ctx, tx, e.SAC, e.BreakdownHash)
		}
	case soroban.ReservePosted:
		tag, err = tx.Exec(ctx, `
			INSERT INTO reserve_reports
			  (event_id, sac, tier, amount, as_of, reporter, doc_hash, doc_uri, posted_ledger, posted_at, tx_hash, source)
			VALUES ($1, $2, $3, $4::numeric, $5, $6, $7, $8, $9, $10, $11, 'event')
			ON CONFLICT (event_id) DO NOTHING`,
			e.ID, e.SAC, int16(e.Tier), e.Amount.String(), e.AsOf, e.Reporter, e.DocHash, e.DocURI,
			int64(e.Ledger), e.ClosedAt, e.TxHash)
	case soroban.ReporterSet:
		// The ledger check keeps a replayed historical event from overwriting
		// a newer registration or revocation. Normal ingestion is ordered, so
		// it always passes.
		_, err = tx.Exec(ctx, `
			INSERT INTO reporters (address, role, name, display_name, active, updated_ledger, updated_at)
			VALUES ($1, $2, $3, NULL, true, $4, $5)
		tag, err = tx.Exec(ctx, `
			INSERT INTO reporters (address, role, name, active, updated_ledger, updated_at)
			VALUES ($1, $2, $3, true, $4, $5)
			ON CONFLICT (address) DO UPDATE
			SET role = EXCLUDED.role, name = EXCLUDED.name, active = true,
			    updated_ledger = EXCLUDED.updated_ledger, updated_at = EXCLUDED.updated_at
			WHERE reporters.updated_ledger <= EXCLUDED.updated_ledger`,
			e.Reporter, int16(e.Role), e.Name, int64(e.Ledger), e.ClosedAt)
	case soroban.ReporterMetadataUpdated:
		// An inactive placeholder lets reconciliation recover registration details
		// when the corresponding reporter_set event has aged out of RPC retention.
		_, err = tx.Exec(ctx, `
			INSERT INTO reporters
			  (address, role, name, display_name, website_uri, contact_hash, active, updated_ledger, updated_at)
			VALUES ($1, 0, '', $2, $3, $4, false, $5, $6)
			ON CONFLICT (address) DO UPDATE
			SET display_name = EXCLUDED.display_name,
			    website_uri = EXCLUDED.website_uri,
			    contact_hash = EXCLUDED.contact_hash,
			    updated_ledger = EXCLUDED.updated_ledger,
			    updated_at = EXCLUDED.updated_at`,
			e.Reporter, e.DisplayName, e.WebsiteURI, e.ContactHash, int64(e.Ledger), e.ClosedAt)
	case soroban.ReporterRevoked:
		tag, err = tx.Exec(ctx, `
			UPDATE reporters SET active = false, updated_ledger = $2, updated_at = $3
			WHERE address = $1 AND updated_ledger <= $2`,
			e.Reporter, int64(e.Ledger), e.ClosedAt)
	case soroban.ThresholdsUpdated:
		// ThresholdsUpdated is acknowledged idempotently; no-op if no schema table yet
		return nil
			INSERT INTO reporters (address, role, name, active, updated_ledger, updated_at, updated_event_id)
			VALUES ($1, $2, $3, true, $4, $5, $6)
			ON CONFLICT (address) DO UPDATE
			SET role = EXCLUDED.role, name = EXCLUDED.name, active = true,
			    updated_ledger = EXCLUDED.updated_ledger, updated_at = EXCLUDED.updated_at,
			    updated_event_id = EXCLUDED.updated_event_id
			WHERE (EXCLUDED.updated_ledger, EXCLUDED.updated_event_id) >=
			      (reporters.updated_ledger, reporters.updated_event_id)`,
			e.Reporter, int16(e.Role), e.Name, int64(e.Ledger), e.ClosedAt, e.ID)
	case soroban.ReporterRevoked:
		_, err = tx.Exec(ctx, `
			UPDATE reporters SET active = false, updated_ledger = $2, updated_at = $3,
			    updated_event_id = $4
			WHERE address = $1
			  AND (updated_ledger, updated_event_id) <= ($2, $4)`,
			e.Reporter, int64(e.Ledger), e.ClosedAt, e.ID)
	}
	if err != nil {
		return false, fmt.Errorf("apply %T: %w", ev, err)
	}
	return tag.RowsAffected() > 0, nil
}

// ---- event replay ---------------------------------------------------------

// replayLimit bounds one EventsSince call. The stream asks for a batch and
// calls again until it has caught up.
const replayLimit = 1000

// EventsSince returns the events stored after eventID in chain order, oldest
// first. It backs Last-Event-ID resume: a reconnecting stream replays what it
// missed from the same rows the REST endpoints read.
//
// Rows the reconciler recovered from contract state are skipped: they carry
// synthetic ids instead of the id the chain assigned the event, so they never
// landed as an event a subscriber could have seen. An empty eventID returns
// nothing; an unknown one returns whatever sorts after it, which for a cursor
// older than the stored history is the whole history.
func (s *Store) EventsSince(ctx context.Context, eventID string, limit int) ([]any, error) {
	if eventID == "" {
		return nil, nil
	}
	if limit <= 0 || limit > replayLimit {
		limit = replayLimit
	}

	type replayable struct {
		id string
		ev any
	}
	found := make([]replayable, 0, limit)

	rows, err := s.pool.Query(ctx, `
		SELECT event_id, sac, code, issuer, listed_ledger, listed_at
		FROM assets WHERE event_id > $1
		ORDER BY event_id COLLATE "C" LIMIT $2`, eventID, limit)
	if err != nil {
		return nil, fmt.Errorf("query asset events: %w", err)
	}
	for rows.Next() {
		var e soroban.AssetListed
		var ledger int64
		if err := rows.Scan(&e.ID, &e.SAC, &e.Code, &e.Issuer, &ledger, &e.ClosedAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan asset event: %w", err)
		}
		e.Ledger = uint32(ledger)
		// Live frames carry the RPC's UTC timestamps; replayed ones must match
		// them byte-for-byte, so drop whatever zone pgx decoded.
		e.ClosedAt = e.ClosedAt.UTC()
		found = append(found, replayable{id: e.ID, ev: e})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read asset events: %w", err)
	}

	rows, err = s.pool.Query(ctx, `
		SELECT event_id, sac, amount::text, at_ledger, breakdown_hash, poster,
		       posted_ledger, posted_at, tx_hash
		FROM supply_snapshots
		WHERE event_id > $1 AND source = 'event'
		ORDER BY event_id COLLATE "C" LIMIT $2`, eventID, limit)
	if err != nil {
		return nil, fmt.Errorf("query supply events: %w", err)
	}
	for rows.Next() {
		var e soroban.SupplyPosted
		var amount string
		var atLedger, postedLedger int64
		if err := rows.Scan(&e.ID, &e.SAC, &amount, &atLedger, &e.BreakdownHash, &e.Poster,
			&postedLedger, &e.ClosedAt, &e.TxHash); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan supply event: %w", err)
		}
		e.Amount = mustBig(amount)
		e.AtLedger = uint32(atLedger)
		e.Ledger = uint32(postedLedger)
		e.ClosedAt = e.ClosedAt.UTC()
		found = append(found, replayable{id: e.ID, ev: e})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read supply events: %w", err)
	}

	rows, err = s.pool.Query(ctx, `
		SELECT event_id, sac, tier, amount::text, as_of, reporter, doc_hash, doc_uri,
		       posted_ledger, posted_at, tx_hash
		FROM reserve_reports
		WHERE event_id > $1 AND source = 'event'
		ORDER BY event_id COLLATE "C" LIMIT $2`, eventID, limit)
	if err != nil {
		return nil, fmt.Errorf("query reserve events: %w", err)
	}
	for rows.Next() {
		var e soroban.ReservePosted
		var tier int16
		var amount string
		var postedLedger int64
		if err := rows.Scan(&e.ID, &e.SAC, &tier, &amount, &e.AsOf, &e.Reporter, &e.DocHash,
			&e.DocURI, &postedLedger, &e.ClosedAt, &e.TxHash); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan reserve event: %w", err)
		}
		e.Tier = uint32(tier)
		e.Amount = mustBig(amount)
		e.Ledger = uint32(postedLedger)
		e.ClosedAt = e.ClosedAt.UTC()
		e.AsOf = e.AsOf.UTC()
		found = append(found, replayable{id: e.ID, ev: e})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read reserve events: %w", err)
	}

	// Merge the per-table pages. Event ids are zero-padded TOIDs, so byte
	// order is chain order; sorting the merged set keeps one table's page from
	// being sent before another's.
	sort.Slice(found, func(i, j int) bool { return found[i].id < found[j].id })
	if len(found) > limit {
		found = found[:limit]
	}
	out := make([]any, len(found))
	for i, r := range found {
		out[i] = r.ev
	}
	return out, nil
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
	defer measureQuery("list_assets")()
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

// ListAssetsPage returns rows ordered by (code, issuer). The API requests
// one extra row to determine whether another page exists.
func (s *Store) ListAssetsPage(ctx context.Context, limit int, afterCode, afterIssuer string) ([]coverage.Asset, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+assetCols+`
		FROM assets
		WHERE ($1 = '' AND $2 = '')
		   OR code > $1
		   OR (code = $1 AND issuer > $2)
		ORDER BY code, issuer
		LIMIT $3`, afterCode, afterIssuer, limit)
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
	defer measureQuery("get_asset")()
	a, err := scanAsset(s.pool.QueryRow(ctx, `SELECT `+assetCols+` FROM assets WHERE sac = $1`, sac))
	if errors.Is(err, pgx.ErrNoRows) {
		return a, ErrNotFound
	}
	return a, err
}

func (s *Store) SupplyHistory(ctx context.Context, sac string, limit int, before *uint32) ([]coverage.Supply, *uint32, error) {
	var beforeArg any
	if before != nil {
		v := int64(*before)
		beforeArg = &v
	}
	rows, err := s.pool.Query(ctx, `
		SELECT amount::text, at_ledger, posted_at, breakdown_hash, poster, tx_hash, source
		FROM supply_snapshots WHERE sac = $1
		  AND ($2::bigint IS NULL OR at_ledger < $2)
		ORDER BY at_ledger DESC, event_id DESC
		LIMIT $3`, sac, beforeArg, limit+1)
func (s *Store) SupplyHistory(ctx context.Context, sac string, limit int) ([]coverage.Supply, error) {
	return s.querySupplies(ctx, `
		SELECT amount::text, at_ledger, posted_at, breakdown_hash, poster, tx_hash, source
		FROM supply_snapshots WHERE sac = $1
		ORDER BY at_ledger DESC LIMIT $2`, sac, limit)
}

// SeriesSupply returns the snapshots a coverage series needs between from and
// to: the latest snapshot posted at or before from, plus every snapshot posted
// after from up to to, oldest first.
func (s *Store) SeriesSupply(ctx context.Context, sac string, from, to time.Time) ([]coverage.Supply, error) {
	return s.querySupplies(ctx, `
		WITH needed AS (
		    SELECT event_id FROM supply_snapshots
		    WHERE sac = $1 AND posted_at > $2 AND posted_at <= $3
		    UNION
		    SELECT event_id FROM (
		        SELECT event_id FROM supply_snapshots
		        WHERE sac = $1 AND posted_at <= $2
		        ORDER BY posted_at DESC, at_ledger DESC
		        LIMIT 1
		    ) latest_before
		)
		SELECT amount::text, at_ledger, posted_at, breakdown_hash, poster, tx_hash, source
		FROM supply_snapshots WHERE event_id IN (SELECT event_id FROM needed)
		ORDER BY posted_at, at_ledger`, sac, from, to)
}

func (s *Store) querySupplies(ctx context.Context, sql string, args ...any) ([]coverage.Supply, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var out []coverage.Supply
	for rows.Next() {
		var sp coverage.Supply
		var amount string
		var ledger int64
		if err := rows.Scan(&amount, &ledger, &sp.PostedAt, &sp.BreakdownHash, &sp.Poster, &sp.TxHash, &sp.Source); err != nil {
			return nil, nil, err
		}
		n, err := parseAmount(amount)
		if err != nil {
			return nil, fmt.Errorf("supply snapshot for %s at ledger %d: %w", sac, ledger, err)
		}
		sp.Amount = n
		sp.Ledger = uint32(ledger)
		out = append(out, sp)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	var nextBefore *uint32
	if len(out) == limit+1 {
		out = out[:limit]
		v := out[limit-1].Ledger
		nextBefore = &v
	}
	return out, nextBefore, nil
}

// LatestSupply returns nil when nothing has been posted.
func (s *Store) LatestSupply(ctx context.Context, sac string) (*coverage.Supply, error) {
	h, _, err := s.SupplyHistory(ctx, sac, 1, nil)
	defer measureQuery("latest_supply")()
	var sp coverage.Supply
	var amount string
	var ledger int64
	err := s.pool.QueryRow(ctx, `
		SELECT supply_amount::text, supply_ledger, supply_posted_at,
		       breakdown_hash, poster, supply_tx_hash, supply_source
		FROM latest_coverage
		WHERE sac = $1 AND supply_amount IS NOT NULL
		LIMIT 1`, sac).
		Scan(&amount, &ledger, &sp.PostedAt, &sp.BreakdownHash, &sp.Poster, &sp.TxHash, &sp.Source)
	if err == nil {
		sp.Amount = mustBig(amount)
		sp.Ledger = uint32(ledger)
		return &sp, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	// An asset with a snapshot but no asset_listed row (for example data
	// written before listing) is not in the view; fall back to the table so
	// the API keeps the same answer it had before the view existed.
	h, err := s.SupplyHistory(ctx, sac, 1)
	if err != nil || len(h) == 0 {
		return nil, err
	}
	return &h[0], nil
}

// LatestSupplies returns the newest supply snapshot for each requested SAC.
func (s *Store) LatestSupplies(ctx context.Context, sacs []string) (map[string]*coverage.Supply, error) {
	out := make(map[string]*coverage.Supply)
	if len(sacs) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT ON (sac)
		       sac, amount::text, at_ledger, posted_at, breakdown_hash, poster, tx_hash, source
		FROM supply_snapshots
		WHERE sac = ANY($1)
		ORDER BY sac, at_ledger DESC`, sacs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var sac string
		var sp coverage.Supply
		var amount string
		var ledger int64
		if err := rows.Scan(&sac, &amount, &ledger, &sp.PostedAt, &sp.BreakdownHash, &sp.Poster, &sp.TxHash, &sp.Source); err != nil {
			return nil, err
		}
		n, err := parseAmount(amount)
		if err != nil {
			return nil, fmt.Errorf("supply snapshot for %s at ledger %d: %w", sac, ledger, err)
		}
		sp.Amount = n
		sp.Ledger = uint32(ledger)
		out[sac] = &sp
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// LatestReportsBySAC returns the newest report for each tier of each requested SAC.
func (s *Store) LatestReportsBySAC(ctx context.Context, sacs []string) (map[string]map[uint32]coverage.Report, error) {
	out := make(map[string]map[uint32]coverage.Report)
	if len(sacs) == 0 {
		return out, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT ON (sac, tier)
		       sac, tier, amount::text, as_of, posted_at, reporter, doc_hash, doc_uri, tx_hash, source,
		       COALESCE(doc_check, '')
		FROM reserve_reports
		WHERE sac = ANY($1)
		ORDER BY sac, tier, as_of DESC`, sacs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var sac string
		var r coverage.Report
		var tier int16
		var amount string
		if err := rows.Scan(&sac, &tier, &amount, &r.AsOf, &r.PostedAt, &r.Reporter, &r.DocHash, &r.DocURI, &r.TxHash, &r.Source, &r.DocCheck); err != nil {
			return nil, err
		}
		r.Tier = uint32(tier)
		n, err := parseAmount(amount)
		if err != nil {
			return nil, fmt.Errorf("reserve report for %s at tier %d: %w", sac, tier, err)
		}
		r.Amount = n
		if out[sac] == nil {
			out[sac] = make(map[uint32]coverage.Report)
		}
		out[sac][r.Tier] = r
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// reportHistorySQL returns one asset's reports newest-posted first. The
// reserve_reports_sac_posted_ledger index (migration 0004) supplies that order,
// so LIMIT stops the scan early instead of sorting the whole history.
const reportHistorySQL = `
	SELECT tier, amount::text, as_of, posted_at, reporter, doc_hash, doc_uri, tx_hash, source,
	       COALESCE(doc_check, '')
	FROM reserve_reports WHERE sac = $1
	ORDER BY posted_ledger DESC, as_of DESC LIMIT $2`

func (s *Store) ReportHistory(ctx context.Context, sac string, limit int) ([]coverage.Report, error) {
	return s.queryReports(ctx, reportHistorySQL, sac, limit)
}

// latestReportsSQL asks for one report per tier, each the newest by as_of.
//
// It is written as one bounded lookup per tier rather than
// `DISTINCT ON (tier)` over the asset's reports: the composite index
// reserve_reports_sac_tier_asof (migration 0001) answers each lookup with a
// single index probe, so the cost is O(log n) in the asset's report count
// instead of reading and sorting every report it has ever posted.
const latestReportsSQL = `
	SELECT r.tier, r.amount::text, r.as_of, r.posted_at, r.reporter, r.doc_hash, r.doc_uri, r.tx_hash, r.source,
	       COALESCE(r.doc_check, '')
	FROM (VALUES (1::smallint), (2::smallint), (3::smallint)) AS wanted(tier)
	CROSS JOIN LATERAL (
		SELECT tier, amount, as_of, posted_at, reporter, doc_hash, doc_uri, tx_hash, source, doc_check
		FROM reserve_reports
		WHERE sac = $1 AND tier = wanted.tier
		ORDER BY as_of DESC
		LIMIT 1
	) AS r`

type ReportExportCursor struct {
	Ledger  int64
	EventID string
}

type ReportExportRow struct {
	Ledger    uint32
	Timestamp time.Time
	Reporter  string
	Bps       *uint32
	Tier      uint32
	DocURI    string
	Cursor    ReportExportCursor
}

// ReportExportPage returns one bounded page of report history in posting order.
// The optional date bounds are inclusive dates represented as [from, to).
func (s *Store) ReportExportPage(ctx context.Context, sac string, from, to *time.Time, after *ReportExportCursor, limit int) ([]ReportExportRow, error) {
	var afterLedger *int64
	var afterEventID *string
	if after != nil {
		afterLedger = &after.Ledger
		afterEventID = &after.EventID
	}

	rows, err := s.pool.Query(ctx, `
		SELECT r.posted_ledger, r.event_id, r.posted_at, r.reporter, r.tier, r.doc_uri,
		       r.amount::text, supply.amount::text
		FROM reserve_reports r
		LEFT JOIN LATERAL (
		    SELECT amount FROM supply_snapshots
		    WHERE sac = r.sac AND posted_at <= r.posted_at
		    ORDER BY posted_at DESC, posted_ledger DESC, event_id DESC
		    LIMIT 1
		) supply ON true
		WHERE r.sac = $1
		  AND ($2::timestamptz IS NULL OR r.posted_at >= $2)
		  AND ($3::timestamptz IS NULL OR r.posted_at < $3)
		  AND ($4::bigint IS NULL OR (r.posted_ledger, r.event_id) > ($4, $5))
		ORDER BY r.posted_ledger, r.event_id
		LIMIT $6`, sac, from, to, afterLedger, afterEventID, limit)
	if err != nil {
		return nil, fmt.Errorf("query report export page: %w", err)
	}
	defer rows.Close()

	var out []ReportExportRow
	for rows.Next() {
		var row ReportExportRow
		var ledger int64
		var tier int16
		var amount string
		var supplyAmount pgtype.Text
		if err := rows.Scan(&ledger, &row.Cursor.EventID, &row.Timestamp, &row.Reporter, &tier, &row.DocURI, &amount, &supplyAmount); err != nil {
			return nil, fmt.Errorf("scan report export row: %w", err)
		}
		row.Ledger = uint32(ledger)
		row.Tier = uint32(tier)
		row.Cursor.Ledger = ledger
		if supplyAmount.Valid {
			reportAmount, err := parseAmount(amount)
			if err != nil {
				return nil, fmt.Errorf("report export amount for %s at ledger %d: %w", sac, ledger, err)
			}
			supplyAmountInt, err := parseAmount(supplyAmount.String)
			if err != nil {
				return nil, fmt.Errorf("report export supply for %s at ledger %d: %w", sac, ledger, err)
			}
			bps := coverage.BPS(reportAmount, supplyAmountInt)
			row.Bps = &bps
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read report export page: %w", err)
	}
	return out, nil
}

// SeriesReports returns the reports a coverage series needs between from and
// to: for each tier the best report (by as_of) posted at or before from, plus
// every report posted after from up to to, oldest first.
func (s *Store) SeriesReports(ctx context.Context, sac string, from, to time.Time) ([]coverage.Report, error) {
	return s.queryReports(ctx, `
		WITH needed AS (
		    SELECT event_id FROM reserve_reports
		    WHERE sac = $1 AND posted_at > $2 AND posted_at <= $3
		    UNION
		    SELECT event_id FROM (
		        SELECT DISTINCT ON (tier) event_id FROM reserve_reports
		        WHERE sac = $1 AND posted_at <= $2
		        ORDER BY tier, as_of DESC, posted_at DESC
		    ) latest_before
		)
		SELECT tier, amount::text, as_of, posted_at, reporter, doc_hash, doc_uri, tx_hash, source,
		       COALESCE(doc_check, '')
		FROM reserve_reports WHERE event_id IN (SELECT event_id FROM needed)
		ORDER BY posted_at, as_of`, sac, from, to)
}

// LatestReports returns the newest report (by as_of) for each tier.
func (s *Store) LatestReports(ctx context.Context, sac string) (map[uint32]coverage.Report, error) {
	defer measureQuery("latest_reports")()
	list, err := s.queryReports(ctx, `
		SELECT tier, report_amount::text, as_of, report_posted_at, reporter,
		       doc_hash, doc_uri, report_tx_hash, report_source,
		       COALESCE(doc_check, '')
		FROM latest_coverage
		WHERE sac = $1 AND tier > 0
		ORDER BY tier`, sac)
	list, err := s.queryReports(ctx, latestReportsSQL, sac)
	if err != nil {
		return nil, err
	}
	// An unlisted asset is absent from the view even if reports exist for it.
	// Fall back to the table so behaviour matches the pre-view API.
	if len(list) == 0 {
		var exists bool
		if err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM latest_coverage WHERE sac = $1)`, sac).Scan(&exists); err != nil {
			return nil, err
		}
		if !exists {
			list, err = s.queryReports(ctx, `
				SELECT DISTINCT ON (tier) tier, amount::text, as_of, posted_at, reporter, doc_hash, doc_uri, tx_hash, source,
				                           COALESCE(doc_check, '')
				FROM reserve_reports WHERE sac = $1
				ORDER BY tier, as_of DESC`, sac)
			if err != nil {
				return nil, err
			}
		}
	}
	out := make(map[uint32]coverage.Report, len(list))
	for _, r := range list {
		out[r.Tier] = r
	}
	return out, nil
}

// LatestReporterReports returns the newest report by as_of date for each
// active reporter that has submitted at least one report.
func (s *Store) LatestReporterReports(ctx context.Context) ([]ReporterLatestReport, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT ON (rr.reporter) rr.reporter, rr.as_of
		FROM reserve_reports rr
		JOIN reporters r ON r.address = rr.reporter
		WHERE r.active
		ORDER BY rr.reporter, rr.as_of DESC`)
	if err != nil {
		return nil, fmt.Errorf("query latest reporter reports: %w", err)
	}
	defer rows.Close()
	var out []ReporterLatestReport
	for rows.Next() {
		var report ReporterLatestReport
		if err := rows.Scan(&report.Reporter, &report.AsOf); err != nil {
			return nil, fmt.Errorf("scan latest reporter report: %w", err)
		}
		out = append(out, report)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate latest reporter reports: %w", err)
// BatchCoverage returns the latest supply and reserve reports for each SAC
// using one query.
func (s *Store) BatchCoverage(ctx context.Context, sacs []string) (map[string]CoverageData, error) {
	out := make(map[string]CoverageData, len(sacs))
	for _, sac := range sacs {
		if _, ok := out[sac]; !ok {
			out[sac] = CoverageData{Reports: make(map[uint32]coverage.Report)}
		}
	}

	rows, err := s.pool.Query(ctx, `
		WITH requested AS (
			SELECT sac, ord
			FROM unnest($1::text[]) WITH ORDINALITY AS input(sac, ord)
		),
		latest_supply AS (
			SELECT DISTINCT ON (sac) sac, amount::text AS amount, at_ledger, posted_at,
			       breakdown_hash, poster, tx_hash, source
			FROM supply_snapshots
			WHERE sac = ANY($1::text[])
			ORDER BY sac, at_ledger DESC
		),
		latest_reports AS (
			SELECT DISTINCT ON (sac, tier) sac, tier, amount::text AS amount, as_of,
			       posted_at, reporter, doc_hash, doc_uri, tx_hash, source,
			       COALESCE(doc_check, '') AS doc_check
			FROM reserve_reports
			WHERE sac = ANY($1::text[])
			ORDER BY sac, tier, as_of DESC
		)
		SELECT requested.sac,
		       COALESCE(latest_supply.amount, ''),
		       COALESCE(latest_supply.at_ledger, 0),
		       COALESCE(latest_supply.posted_at, 'epoch'::timestamptz),
		       COALESCE(latest_supply.breakdown_hash, ''),
		       COALESCE(latest_supply.poster, ''),
		       COALESCE(latest_supply.tx_hash, ''),
		       COALESCE(latest_supply.source, ''),
		       COALESCE(latest_reports.tier, 0),
		       COALESCE(latest_reports.amount, ''),
		       COALESCE(latest_reports.as_of, 'epoch'::timestamptz),
		       COALESCE(latest_reports.posted_at, 'epoch'::timestamptz),
		       COALESCE(latest_reports.reporter, ''),
		       COALESCE(latest_reports.doc_hash, ''),
		       COALESCE(latest_reports.doc_uri, ''),
		       COALESCE(latest_reports.tx_hash, ''),
		       COALESCE(latest_reports.source, ''),
		       COALESCE(latest_reports.doc_check, '')
		FROM requested
		LEFT JOIN latest_supply USING (sac)
		LEFT JOIN latest_reports USING (sac)
		ORDER BY requested.ord, latest_reports.tier`, sacs)
	if err != nil {
		return nil, fmt.Errorf("query batch coverage: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var sac, supplyAmount, breakdownHash, poster, supplyTxHash, supplySource string
		var supplyLedger int64
		var supplyPostedAt time.Time
		var tier int16
		var reportAmount, reporter, docHash, docURI, reportTxHash, reportSource, docCheck string
		var reportAsOf, reportPostedAt time.Time
		if err := rows.Scan(
			&sac, &supplyAmount, &supplyLedger, &supplyPostedAt, &breakdownHash, &poster, &supplyTxHash, &supplySource,
			&tier, &reportAmount, &reportAsOf, &reportPostedAt, &reporter, &docHash, &docURI, &reportTxHash, &reportSource, &docCheck,
		); err != nil {
			return nil, fmt.Errorf("scan batch coverage: %w", err)
		}

		data := out[sac]
		if supplyAmount != "" {
			data.Supply = &coverage.Supply{
				Amount:        mustBig(supplyAmount),
				Ledger:        uint32(supplyLedger),
				PostedAt:      supplyPostedAt,
				BreakdownHash: breakdownHash,
				Poster:        poster,
				TxHash:        supplyTxHash,
				Source:        supplySource,
			}
		}
		if tier != 0 {
			data.Reports[uint32(tier)] = coverage.Report{
				Tier:     uint32(tier),
				Amount:   mustBig(reportAmount),
				AsOf:     reportAsOf,
				PostedAt: reportPostedAt,
				Reporter: reporter,
				DocHash:  docHash,
				DocURI:   docURI,
				TxHash:   reportTxHash,
				Source:   reportSource,
				DocCheck: docCheck,
			}
		}
		out[sac] = data
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate batch coverage: %w", err)
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
		if err := rows.Scan(&tier, &amount, &r.AsOf, &r.PostedAt, &r.Reporter, &r.DocHash, &r.DocURI, &r.TxHash, &r.Source, &r.DocCheck); err != nil {
			return nil, err
		}
		n, err := parseAmount(amount)
		if err != nil {
			return nil, fmt.Errorf("reserve report tier %d as of %s: %w", tier, r.AsOf.Format(time.RFC3339), err)
		}
		r.Tier = uint32(tier)
		r.Amount = n
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *Store) Reporters(ctx context.Context) ([]coverage.Reporter, error) {
	defer measureQuery("reporters")()
	rows, err := s.pool.Query(ctx, `
		SELECT address, role, name, COALESCE(display_name, name), website_uri, contact_hash, updated_at FROM reporters
		WHERE active ORDER BY role, name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []coverage.Reporter
	for rows.Next() {
		var r coverage.Reporter
		var role int16
		var websiteURI, contactHash *string
		if err := rows.Scan(&r.Address, &role, &r.Name, &r.DisplayName, &websiteURI, &contactHash, &r.UpdatedAt); err != nil {
			return nil, err
		}
		r.Role = uint32(role)
		r.WebsiteURI, r.ContactHash = websiteURI, contactHash
		out = append(out, r)
	}
	return out, rows.Err()
}

// Disputes returns one asset's disputes newest-opened first. An empty status
// returns both open and resolved disputes. The caller asks for limit+1 rows to
// learn whether another page exists.
func (s *Store) Disputes(ctx context.Context, sac, status string, limit, offset int) ([]coverage.Dispute, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT sac, reporter, disputer, status, resolution, report_as_of,
		       hold_until_ledger, opened_ledger, opened_at, resolved_ledger, resolved_at, tx_hash
		FROM disputes
		WHERE sac = $1 AND ($2 = '' OR status = $2)
		ORDER BY opened_at DESC, id DESC
		LIMIT $3 OFFSET $4`, sac, status, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []coverage.Dispute
	for rows.Next() {
		var d coverage.Dispute
		var holdUntil, openedLedger int64
		var resolvedLedger *int64
		var resolvedAt *time.Time
		if err := rows.Scan(&d.SAC, &d.Reporter, &d.Disputer, &d.Status, &d.Resolution,
			&d.ReportAsOf, &holdUntil, &openedLedger, &d.OpenedAt,
			&resolvedLedger, &resolvedAt, &d.TxHash); err != nil {
			return nil, err
		}
		d.HoldUntilLedger = uint32(holdUntil)
		d.OpenedLedger = uint32(openedLedger)
		if resolvedLedger != nil {
			ledger := uint32(*resolvedLedger)
			d.ResolvedLedger = &ledger
		}
		d.ResolvedAt = resolvedAt
		out = append(out, d)
	}
	return out, rows.Err()
// ---- aggregate stats -------------------------------------------------------

// Stats is the summary served at /v1/stats.
type Stats struct {
	TotalAssets     int64
	CoveredAssets   int64
	TotalReporters  int64
	ActiveReporters int64
	ReportsToday    int64
	// LastEventAt is when the last contract event was written; nil before the
	// first one lands.
	LastEventAt *time.Time
}

// Stats reads every aggregate in one round trip. An asset counts as covered
// when coverage.Compute would return a result for it, i.e. it has a supply
// snapshot and at least one reserve report.
func (s *Store) Stats(ctx context.Context, dayStart time.Time) (Stats, error) {
	var out Stats
	err := s.pool.QueryRow(ctx, `
		SELECT
		    (SELECT count(*) FROM assets),
		    (SELECT count(*) FROM assets a
		     WHERE EXISTS (SELECT 1 FROM supply_snapshots sp WHERE sp.sac = a.sac)
		       AND EXISTS (SELECT 1 FROM reserve_reports rr WHERE rr.sac = a.sac)),
		    (SELECT count(*) FROM reporters),
		    (SELECT count(*) FROM reporters WHERE active),
		    (SELECT count(*) FROM reserve_reports WHERE posted_at >= $1),
		    (SELECT max(event_at) FROM ingest_cursor)`, dayStart,
	).Scan(&out.TotalAssets, &out.CoveredAssets, &out.TotalReporters,
		&out.ActiveReporters, &out.ReportsToday, &out.LastEventAt)
	if err != nil {
		return Stats{}, fmt.Errorf("read stats: %w", err)
	}
	return out, nil
}

// ---- breakdowns and toml --------------------------------------------------

// SaveBreakdown stores a breakdown the poster is about to post. It is not
// served until it is confirmed (see ConfirmBreakdown). Saving an unconfirmed
// breakdown again, for a retried post, restarts its grace period.
func (s *Store) SaveBreakdown(ctx context.Context, hash, sac string, body []byte) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO breakdowns (hash, sac, body) VALUES ($1, $2, $3)
		ON CONFLICT (hash) DO UPDATE SET created_at = now()
		WHERE breakdowns.confirmed_at IS NULL`, hash, sac, string(body))
	return err
}

// Breakdown returns the body of a confirmed breakdown, byte for byte. A
// breakdown whose post never reached the chain is ErrNotFound.
func (s *Store) Breakdown(ctx context.Context, hash string) ([]byte, error) {
	defer measureQuery("breakdown")()
	var body string
	err := s.pool.QueryRow(ctx,
		`SELECT body FROM breakdowns WHERE hash = $1 AND confirmed_at IS NOT NULL`, hash).Scan(&body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return []byte(body), err
}

// HasBreakdown reports whether the body for hash is stored, confirmed or not.
func (s *Store) HasBreakdown(ctx context.Context, hash string) (bool, error) {
	var ok bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM breakdowns WHERE hash = $1)`, hash).Scan(&ok)
	if err != nil {
		return false, fmt.Errorf("look up breakdown: %w", err)
	}
	return ok, nil
}

// ConfirmBreakdown marks the breakdown for hash as backing an on-chain
// snapshot: the poster's post_supply succeeded, or the coverage ledger holds
// the hash. Confirmed breakdowns are served and never deleted.
func (s *Store) ConfirmBreakdown(ctx context.Context, hash string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE breakdowns SET confirmed_at = now() WHERE hash = $1 AND confirmed_at IS NULL`, hash)
	if err != nil {
		return fmt.Errorf("confirm breakdown: %w", err)
	}
	return nil
}

// confirmBreakdown confirms the breakdown a stored snapshot points at.
func confirmBreakdown(ctx context.Context, db interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}, sac, hash string) error {
	_, err := db.Exec(ctx, `
		UPDATE breakdowns SET confirmed_at = now()
		WHERE hash = $1 AND sac = $2 AND confirmed_at IS NULL`, hash, sac)
	if err != nil {
		return fmt.Errorf("confirm breakdown: %w", err)
	}
	return nil
}

// DeleteUnconfirmedBreakdowns deletes the breakdowns saved more than
// olderThan ago that nothing has confirmed, and returns how many it deleted.
// These are posts that never reached the chain: the transaction failed, or
// the post was abandoned after the breakdown was saved. A breakdown a
// snapshot row points at is kept even if its confirmation was missed.
func (s *Store) DeleteUnconfirmedBreakdowns(ctx context.Context, olderThan time.Duration) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM breakdowns b
		WHERE b.confirmed_at IS NULL
		  AND b.created_at < now() - make_interval(secs => $1)
		  AND NOT EXISTS (
		      SELECT 1 FROM supply_snapshots s WHERE s.sac = b.sac AND s.breakdown_hash = b.hash
		  )`, olderThan.Seconds())
	if err != nil {
		return 0, fmt.Errorf("delete unconfirmed breakdowns: %w", err)
	}
	return tag.RowsAffected(), nil
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

// SaveIssuerTomlSnapshot records a new content hash only when it differs from
// the issuer's latest successful stellar.toml fetch.
func (s *Store) SaveIssuerTomlSnapshot(ctx context.Context, issuer, domain, hash string, at time.Time) (string, bool, error) {
	return s.saveIssuerTomlSnapshot(ctx, issuer, domain, hash, nil, at)
}

// SaveIssuerTomlSnapshotWithContent records the fetched stellar.toml content
// and SHA-256 hash, inserting a new snapshot row only when the hash changes and
// refreshing checked_at (and content) when unchanged.
func (s *Store) SaveIssuerTomlSnapshotWithContent(ctx context.Context, issuer, domain, hash, content string, at time.Time) (string, bool, error) {
	return s.saveIssuerTomlSnapshot(ctx, issuer, domain, hash, &content, at)
}

func (s *Store) saveIssuerTomlSnapshot(ctx context.Context, issuer, domain, hash string, content *string, at time.Time) (string, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", false, fmt.Errorf("begin issuer TOML snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, issuer); err != nil {
		return "", false, fmt.Errorf("lock issuer TOML snapshot: %w", err)
	}
	var latestID int64
	var oldHash string
	err = tx.QueryRow(ctx, `
		SELECT id, sha256_hash FROM issuer_toml_snapshots
		WHERE issuer = $1 ORDER BY id DESC LIMIT 1`, issuer).Scan(&latestID, &oldHash)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", false, fmt.Errorf("read previous issuer TOML hash: %w", err)
	}
	changed := err == nil && oldHash != hash
	if errors.Is(err, pgx.ErrNoRows) || changed {
		if _, err := tx.Exec(ctx, `
			INSERT INTO issuer_toml_snapshots (issuer, home_domain, sha256_hash, content, checked_at)
			VALUES ($1, $2, $3, $4, $5)`, issuer, domain, hash, content, at.UTC()); err != nil {
			return "", false, fmt.Errorf("insert issuer TOML snapshot: %w", err)
		}
	} else {
		if _, err := tx.Exec(ctx, `
			UPDATE issuer_toml_snapshots
			SET home_domain = $2,
			    checked_at = $3,
			    content = COALESCE($4, content)
			WHERE id = $1`, latestID, domain, at.UTC(), content); err != nil {
			return "", false, fmt.Errorf("refresh issuer TOML snapshot: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return "", false, fmt.Errorf("commit issuer TOML snapshot: %w", err)
	}
	return oldHash, changed, nil
}

const webhookCols = `id, owner_key_hash, url, secret_hash, secret_ciphertext, created_at,
	last_delivery_at, COALESCE(last_delivery_status, ''), COALESCE(last_delivery_error, ''),
	deliveries, failures`

func scanWebhook(row pgx.Row) (Webhook, error) {
// LatestIssuerToml returns the most recently fetched stellar.toml snapshot
// with cached content for issuer, or ErrNotFound when the issuer has not been
// synced yet.
func (s *Store) LatestIssuerToml(ctx context.Context, issuer string) (coverage.IssuerTomlSnapshot, error) {
	var snap coverage.IssuerTomlSnapshot
	err := s.pool.QueryRow(ctx, `
		SELECT issuer, home_domain, sha256_hash, content, checked_at
		FROM issuer_toml_snapshots
		WHERE issuer = $1 AND content IS NOT NULL
		ORDER BY id DESC
		LIMIT 1`, issuer,
	).Scan(&snap.Issuer, &snap.HomeDomain, &snap.SHA256Hash, &snap.Content, &snap.FetchedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return snap, ErrNotFound
	}
	if err != nil {
		return snap, fmt.Errorf("latest issuer TOML: %w", err)
	}
	snap.FetchedAt = snap.FetchedAt.UTC()
	return snap, nil
}

func (s *Store) CreateWebhook(ctx context.Context, url, secretHash string, ciphertext []byte) (Webhook, error) {
	var w Webhook
	err := row.Scan(&w.ID, &w.OwnerKeyHash, &w.URL, &w.SecretHash, &w.SecretCiphertext, &w.CreatedAt,
		&w.LastDeliveryAt, &w.LastDeliveryStatus, &w.LastDeliveryError, &w.Deliveries, &w.Failures)
	return w, err
}

// CreateWebhook registers one endpoint for the key with this fingerprint. It
// fails with ErrWebhookLimit once the key holds MaxWebhooksPerKey endpoints.
func (s *Store) CreateWebhook(ctx context.Context, url, secretHash string, ciphertext []byte, ownerKeyHash string) (Webhook, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Webhook{}, fmt.Errorf("begin create webhook: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Serialise registrations for this key, so two concurrent requests cannot
	// both see room below the limit and both insert.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, "webhook:"+ownerKeyHash); err != nil {
		return Webhook{}, fmt.Errorf("lock webhook registrations: %w", err)
	}
	var count int
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM webhooks WHERE owner_key_hash = $1`, ownerKeyHash).Scan(&count); err != nil {
		return Webhook{}, fmt.Errorf("count webhooks: %w", err)
	}
	if count >= MaxWebhooksPerKey {
		return Webhook{}, ErrWebhookLimit
	}
	w, err := scanWebhook(tx.QueryRow(ctx, `
		INSERT INTO webhooks (owner_key_hash, url, secret_hash, secret_ciphertext)
		VALUES ($1, $2, $3, $4)
		RETURNING `+webhookCols,
		ownerKeyHash, url, secretHash, ciphertext))
	if err != nil {
		return Webhook{}, fmt.Errorf("create webhook: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return Webhook{}, fmt.Errorf("commit webhook: %w", err)
	}
	return w, nil
}

// ListWebhooks returns every endpoint, for the dispatcher: events fan out to
// all of them regardless of the key that registered them.
func (s *Store) ListWebhooks(ctx context.Context) ([]Webhook, error) {
	return s.queryWebhooks(ctx, `SELECT `+webhookCols+` FROM webhooks ORDER BY id`)
}

// ListWebhooksForKey returns the endpoints the key may manage. Endpoints from
// before endpoints were keyed (empty owner_key_hash) are included, so an
// operator can still see them from any key.
func (s *Store) ListWebhooksForKey(ctx context.Context, ownerKeyHash string) ([]Webhook, error) {
	return s.queryWebhooks(ctx,
		`SELECT `+webhookCols+` FROM webhooks WHERE owner_key_hash = $1 OR owner_key_hash = '' ORDER BY id`, ownerKeyHash)
}

func (s *Store) queryWebhooks(ctx context.Context, sql string, args ...any) ([]Webhook, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("list webhooks: %w", err)
	}
	defer rows.Close()
	var out []Webhook
	for rows.Next() {
		w, err := scanWebhook(rows)
		if err != nil {
			return nil, fmt.Errorf("scan webhook: %w", err)
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// DeleteWebhook removes one endpoint. A key cannot delete another key's
// endpoint, and endpoints from before keying (empty owner_key_hash) are
// deletable by the admin token only.
func (s *Store) DeleteWebhook(ctx context.Context, id int64, ownerKeyHash string, allowLegacyAdmin bool) error {
	tx, err := s.pool.Begin(ctx)
func (s *Store) GetWebhook(ctx context.Context, id int64) (Webhook, bool, error) {
	var w Webhook
	err := s.pool.QueryRow(ctx, `
		SELECT id, url, secret_hash, secret_ciphertext, created_at
		FROM webhooks WHERE id = $1`, id,
	).Scan(&w.ID, &w.URL, &w.SecretHash, &w.SecretCiphertext, &w.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Webhook{}, false, nil
	}
	if err != nil {
		return Webhook{}, false, fmt.Errorf("get webhook: %w", err)
	}
	return w, true, nil
}

func (s *Store) DeleteWebhook(ctx context.Context, id int64) (bool, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM webhooks WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("begin delete webhook: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var storedOwner string
	err = tx.QueryRow(ctx, `SELECT owner_key_hash FROM webhooks WHERE id = $1 FOR UPDATE`, id).Scan(&storedOwner)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("read webhook owner: %w", err)
	}
	// Rows created before owner tracking have no key hash and are admin-only.
	if storedOwner != ownerKeyHash && (storedOwner != "" || !allowLegacyAdmin) {
		return ErrForbidden
	}
	if _, err := tx.Exec(ctx, `DELETE FROM webhooks WHERE id = $1`, id); err != nil {
		return fmt.Errorf("delete webhook: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit delete webhook: %w", err)
	}
	return nil
}

// RecordWebhookDelivery records one delivery attempt against its endpoint.
// failure is empty for a successful delivery and the error text otherwise.
func (s *Store) RecordWebhookDelivery(ctx context.Context, id int64, at time.Time, failure string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE webhooks SET
		    deliveries = deliveries + 1,
		    failures = failures + CASE WHEN $3 = '' THEN 0 ELSE 1 END,
		    last_delivery_at = $2,
		    last_delivery_status = CASE WHEN $3 = '' THEN 'delivered' ELSE 'failed' END,
		    last_delivery_error = NULLIF($3, '')
		WHERE id = $1`, id, at, failure)
	if err != nil {
		return fmt.Errorf("record webhook delivery: %w", err)
	}
	return nil
}

// ---- replay ---------------------------------------------------------------

// ReplaySummary counts the events a replay processed, by type.
//
// Rows are keyed by event id, so the counts are what the replay handled, not
// how many rows changed: running the same range twice reports the same numbers
// and leaves the same state.
type ReplaySummary struct {
	Assets    int
	Supplies  int
	Reports   int
	Reporters int
	Revoked   int
}

// Total is the number of events in the summary.
func (s ReplaySummary) Total() int {
	return s.Assets + s.Supplies + s.Reports + s.Reporters + s.Revoked
}

// Add accumulates another summary.
func (s *ReplaySummary) Add(other ReplaySummary) {
	s.Assets += other.Assets
	s.Supplies += other.Supplies
	s.Reports += other.Reports
	s.Reporters += other.Reporters
	s.Revoked += other.Revoked
}

// SummarizeReplay counts the events a replay would apply, without touching the
// database. It is what a dry run reports.
func SummarizeReplay(events []any) ReplaySummary {
	var summary ReplaySummary
	for _, ev := range events {
		switch ev.(type) {
		case soroban.AssetListed:
			summary.Assets++
		case soroban.SupplyPosted:
			summary.Supplies++
		case soroban.ReservePosted:
			summary.Reports++
		case soroban.ReporterSet:
			summary.Reporters++
		case soroban.ReporterRevoked:
			summary.Revoked++
		}
	}
	return summary
}

// ApplyReplayEvents upserts decoded events in one transaction without moving
// the ingest cursor, so a replay can run beside the normal ingest loop. Rows
// are keyed by event id and reporter changes are only applied when they are
// not older than what is stored, so replaying the same range twice produces
// the same state and cannot overwrite newer data with older.
func (s *Store) ApplyReplayEvents(ctx context.Context, events []any) (ReplaySummary, error) {
	summary := SummarizeReplay(events)
	if len(events) == 0 {
		return summary, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return summary, fmt.Errorf("begin replay: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	for _, ev := range events {
		if err := applyEvent(ctx, tx, ev); err != nil {
			return summary, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return summary, fmt.Errorf("commit replay: %w", err)
	}
	return summary, nil
}

// CreateAPIKey stores a bcrypt hash and returns the selector used to locate it.
func (s *Store) CreateAPIKey(ctx context.Context, keyHash string) (int64, error) {
	var id int64
	err := s.pool.QueryRow(ctx,
		`INSERT INTO api_keys (key_hash) VALUES ($1) RETURNING id`, keyHash,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("create API key: %w", err)
	}
	return id, nil
}

// ValidateAPIKey checks the secret portion of an id.secret API key.
func (s *Store) ValidateAPIKey(ctx context.Context, key string) (bool, error) {
	id, secret, ok := strings.Cut(key, ".")
	if !ok || id == "" || secret == "" || strings.Contains(secret, ".") || len(secret) > 72 {
		return false, nil
	}
	var keyID int64
	keyID, err := strconv.ParseInt(id, 10, 64)
	if err != nil || keyID < 1 {
		return false, nil
	}
	var hash string
	if err := s.pool.QueryRow(ctx, `SELECT key_hash FROM api_keys WHERE id = $1`, keyID).Scan(&hash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("read API key: %w", err)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte(secret)); err != nil {
		if errors.Is(err, bcrypt.ErrMismatchedHashAndPassword) {
			return false, nil
		}
		return false, fmt.Errorf("compare API key: %w", err)
	}
	return true, nil
}

// parseAmount reads a NUMERIC(40, 0) amount scanned as text. A value that is
// not a base-10 integer (NUMERIC also holds 'NaN' and, from Postgres 14,
// 'Infinity') is an error, never zero: a zero would be served as zero supply
// or zero reserves, which the API reports as a real reading.
func parseAmount(s string) (*big.Int, error) {
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("amount %q is not an integer", s)
	}
	return n, nil
}

func (s *Store) CreateWebhookDLQ(ctx context.Context, webhookID int64, payload []byte, lastError string, attempts int) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO webhook_dlq (webhook_id, payload, last_error, attempts)
		VALUES ($1, $2, $3, $4)`, webhookID, payload, lastError, attempts)
	if err != nil {
		return fmt.Errorf("create webhook DLQ entry: %w", err)
	}
	return nil
}

func (s *Store) ListWebhookDLQ(ctx context.Context, webhookID int64, limit int) ([]WebhookDLQ, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, webhook_id, payload, last_error, attempts, failed_at
		FROM webhook_dlq WHERE webhook_id = $1
		ORDER BY id DESC LIMIT $2`, webhookID, limit)
	if err != nil {
		return nil, fmt.Errorf("list webhook DLQ: %w", err)
	}
	defer rows.Close()
	var out []WebhookDLQ
	for rows.Next() {
		var item WebhookDLQ
		if err := rows.Scan(&item.ID, &item.WebhookID, &item.Payload, &item.LastError, &item.Attempts, &item.FailedAt); err != nil {
			return nil, fmt.Errorf("scan webhook DLQ entry: %w", err)
		}
		out = append(out, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list webhook DLQ rows: %w", err)
	}
	return out, nil
}

func (s *Store) GetWebhookDLQ(ctx context.Context, webhookID, id int64) (WebhookDLQ, bool, error) {
	var item WebhookDLQ
	err := s.pool.QueryRow(ctx, `
		SELECT id, webhook_id, payload, last_error, attempts, failed_at
		FROM webhook_dlq WHERE webhook_id = $1 AND id = $2`, webhookID, id,
	).Scan(&item.ID, &item.WebhookID, &item.Payload, &item.LastError, &item.Attempts, &item.FailedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return WebhookDLQ{}, false, nil
	}
	if err != nil {
		return WebhookDLQ{}, false, fmt.Errorf("get webhook DLQ entry: %w", err)
	}
	return item, true, nil
}

func (s *Store) UpdateWebhookDLQFailure(ctx context.Context, webhookID, id int64, lastError string, attempts int) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE webhook_dlq
		SET last_error = $3, attempts = attempts + $4, failed_at = now()
		WHERE webhook_id = $1 AND id = $2`, webhookID, id, lastError, attempts)
	if err != nil {
		return false, fmt.Errorf("update webhook DLQ entry: %w", err)
	}
	return tag.RowsAffected() != 0, nil
}

func (s *Store) DeleteWebhookDLQ(ctx context.Context, webhookID, id int64) (bool, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM webhook_dlq WHERE webhook_id = $1 AND id = $2`, webhookID, id)
	if err != nil {
		return false, fmt.Errorf("delete webhook DLQ entry: %w", err)
	}
	return tag.RowsAffected() != 0, nil
}

// RecordWebhookDelivery keeps one delivery attempt, successful or failed, so
// its latency can be compared with the endpoint's error.
func (s *Store) RecordWebhookDelivery(ctx context.Context, d WebhookDelivery) error {
	var errText any
	if d.Error != "" {
		errText = d.Error
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO webhook_deliveries
		  (webhook_id, event, status_code, error, delivery_latency_ms, attempted_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		d.WebhookID, d.Event, d.StatusCode, errText, d.DeliveryLatencyMS, d.AttemptedAt)
	if err != nil {
		return fmt.Errorf("record webhook delivery: %w", err)
	}
	return nil
}

// ListWebhookDLQ returns failed delivery attempts for one webhook, newest
// first. They are the dead letters an operator can inspect and retry.
func (s *Store) ListWebhookDLQ(ctx context.Context, webhookID int64, limit int) ([]WebhookDelivery, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, webhook_id, event, status_code, COALESCE(error, ''),
		       delivery_latency_ms, attempted_at
		FROM webhook_deliveries
		WHERE webhook_id = $1 AND error IS NOT NULL
		ORDER BY id DESC LIMIT $2`, webhookID, limit)
	if err != nil {
		return nil, fmt.Errorf("list webhook DLQ: %w", err)
	}
	defer rows.Close()
	var out []WebhookDelivery
	for rows.Next() {
		var d WebhookDelivery
		if err := rows.Scan(&d.ID, &d.WebhookID, &d.Event, &d.StatusCode, &d.Error,
			&d.DeliveryLatencyMS, &d.AttemptedAt); err != nil {
			return nil, fmt.Errorf("scan webhook DLQ entry: %w", err)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list webhook DLQ rows: %w", err)
	}
	return out, nil
}

func mustBig(s string) *big.Int {
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return new(big.Int)
	}
	return n
}

// ---- advisory lock --------------------------------------------------------

// posterLockKey is the Postgres advisory lock key for the supply poster.
// Fixed value; must not collide with any other advisory lock in the database.
const posterLockKey = int64(0x706c696d736f6c6c) // "plimsoll" in hex ASCII

// TryPostLock acquires a Postgres session-level advisory lock that guarantees
// only one poster instance posts at a time. It returns an unlock function that
// must be called when the lock is no longer needed. If another instance already
// holds the lock it returns ErrLockNotAcquired; the caller should skip posting
// for this tick and try again next time.
func (s *Store) TryPostLock(ctx context.Context) (func(), error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire connection for poster lock: %w", err)
	}
	var acquired bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, posterLockKey).Scan(&acquired); err != nil {
		conn.Release()
		return nil, fmt.Errorf("pg_try_advisory_lock: %w", err)
	}
	if !acquired {
		conn.Release()
		return nil, ErrLockNotAcquired
	}
	unlock := func() {
		unlockCtx := context.Background()
		_, _ = conn.Exec(unlockCtx, `SELECT pg_advisory_unlock($1)`, posterLockKey)
		conn.Release()
	}
	return unlock, nil
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
	// The ledger holds this hash, so its breakdown backs a real snapshot.
	if err := confirmBreakdown(ctx, s.pool, sac, sp.BreakdownHash); err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
	added := tag.RowsAffected() > 0
	if added {
		if err := s.RefreshLatestCoverage(ctx); err != nil {
			return false, err
		}
	}
	return added, nil
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
	added := tag.RowsAffected() > 0
	if added {
		if err := s.RefreshLatestCoverage(ctx); err != nil {
			return false, err
		}
	}
	return added, nil
}

func chainSupplyID(sac string, ledger uint32) string {
	return fmt.Sprintf("chain:%s:supply:%d", sac, ledger)
}

func chainReportID(sac string, tier uint32, asOf time.Time) string {
	return fmt.Sprintf("chain:%s:report:%d:%d", sac, tier, asOf.Unix())
}

// ---- retention -------------------------------------------------------------

// PruneResult counts the rows a retention pass removed.
type PruneResult struct {
	SupplySnapshots int64
	ReserveReports  int64
	Breakdowns      int64
}

// Total is the number of rows deleted across every table.
func (r PruneResult) Total() int64 {
	return r.SupplySnapshots + r.ReserveReports + r.Breakdowns
}

// Prune deletes snapshots, reports and breakdowns posted before cutoff. A
// breakdown is kept while any surviving snapshot still refers to it, because
// /v1/breakdowns/{hash} must keep serving the bytes behind a live hash.
func (s *Store) Prune(ctx context.Context, cutoff time.Time) (PruneResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return PruneResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var out PruneResult
	tag, err := tx.Exec(ctx, `DELETE FROM supply_snapshots WHERE posted_at < $1`, cutoff)
	if err != nil {
		return out, fmt.Errorf("prune supply snapshots: %w", err)
	}
	out.SupplySnapshots = tag.RowsAffected()

	tag, err = tx.Exec(ctx, `DELETE FROM reserve_reports WHERE posted_at < $1`, cutoff)
	if err != nil {
		return out, fmt.Errorf("prune reserve reports: %w", err)
	}
	out.ReserveReports = tag.RowsAffected()

	tag, err = tx.Exec(ctx, `
		DELETE FROM breakdowns b
		WHERE b.created_at < $1
		  AND NOT EXISTS (SELECT 1 FROM supply_snapshots s WHERE s.breakdown_hash = b.hash)`, cutoff)
	if err != nil {
		return out, fmt.Errorf("prune breakdowns: %w", err)
	}
	out.Breakdowns = tag.RowsAffected()

	if _, err := tx.Exec(ctx, `REFRESH MATERIALIZED VIEW latest_coverage`); err != nil {
		return out, fmt.Errorf("refresh latest_coverage after prune: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return out, fmt.Errorf("commit prune: %w", err)
	}
	return out, nil
}

// retentionTables is the allowlist Vacuum accepts. Table names are never taken
// from user input, and this keeps it that way.
var retentionTables = map[string]bool{
	"supply_snapshots": true,
	"reserve_reports":  true,
	"breakdowns":       true,
}

// Vacuum reclaims space in the tables a prune touched. VACUUM cannot run in a
// transaction, so this is deliberately a separate call after Prune commits.
func (s *Store) Vacuum(ctx context.Context, tables ...string) error {
	for _, table := range tables {
		if !retentionTables[table] {
			return fmt.Errorf("refusing to vacuum unknown table %q", table)
		}
		if _, err := s.pool.Exec(ctx, `VACUUM (ANALYZE) `+pgx.Identifier{table}.Sanitize()); err != nil {
			return fmt.Errorf("vacuum %s: %w", table, err)
		}
	}
	return nil
}

// ---- reindex ---------------------------------------------------------------

// ReindexCounts is how many rows a reindex from a ledger would remove.
type ReindexCounts struct {
	Assets          int64
	SupplySnapshots int64
	ReserveReports  int64
}

// Total is the number of rows across every table.
func (c ReindexCounts) Total() int64 {
	return c.Assets + c.SupplySnapshots + c.ReserveReports
}

type rowQuerier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func countReindexRows(ctx context.Context, q rowQuerier, from uint32) (ReindexCounts, error) {
	var c ReindexCounts
	if err := q.QueryRow(ctx, `SELECT count(*) FROM assets WHERE listed_ledger >= $1`, int64(from)).Scan(&c.Assets); err != nil {
		return c, fmt.Errorf("count assets: %w", err)
	}
	if err := q.QueryRow(ctx, `
		SELECT count(*) FROM supply_snapshots
		WHERE at_ledger >= $1 OR posted_ledger >= $1`, int64(from)).Scan(&c.SupplySnapshots); err != nil {
		return c, fmt.Errorf("count supply snapshots: %w", err)
	}
	if err := q.QueryRow(ctx, `SELECT count(*) FROM reserve_reports WHERE posted_ledger >= $1`, int64(from)).Scan(&c.ReserveReports); err != nil {
		return c, fmt.Errorf("count reserve reports: %w", err)
	}
	return c, nil
}

// CountEventsFromLedger reports what DeleteEventsFromLedger would remove,
// without changing anything. It is what --dry-run prints.
func (s *Store) CountEventsFromLedger(ctx context.Context, from uint32) (ReindexCounts, error) {
	return countReindexRows(ctx, s.pool, from)
}

// DeleteEventsFromLedger discards rows derived from events at or after from:
// asset listings, supply snapshots and reserve reports. Reporters are current
// state rather than an event log, so they are left for the replay to update
// idempotently. The ingest cursor is reset separately with ResetIngestCursor.
func (s *Store) DeleteEventsFromLedger(ctx context.Context, from uint32) (ReindexCounts, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return ReindexCounts{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var out ReindexCounts
	tag, err := tx.Exec(ctx, `
		DELETE FROM supply_snapshots
		WHERE at_ledger >= $1 OR posted_ledger >= $1`, int64(from))
	if err != nil {
		return out, fmt.Errorf("delete supply snapshots: %w", err)
	}
	out.SupplySnapshots = tag.RowsAffected()

	tag, err = tx.Exec(ctx, `DELETE FROM reserve_reports WHERE posted_ledger >= $1`, int64(from))
	if err != nil {
		return out, fmt.Errorf("delete reserve reports: %w", err)
	}
	out.ReserveReports = tag.RowsAffected()

	tag, err = tx.Exec(ctx, `DELETE FROM assets WHERE listed_ledger >= $1`, int64(from))
	if err != nil {
		return out, fmt.Errorf("delete assets: %w", err)
	}
	out.Assets = tag.RowsAffected()

	if _, err := tx.Exec(ctx, `REFRESH MATERIALIZED VIEW latest_coverage`); err != nil {
		return out, fmt.Errorf("refresh latest_coverage after reindex delete: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return out, fmt.Errorf("commit reindex delete: %w", err)
	}
	return out, nil
}

// ResetIngestCursor removes the saved cursor so the next ingest starts from
// the configured StartLedger instead of resuming after the discarded range.
func (s *Store) ResetIngestCursor(ctx context.Context, name string) error {
	if _, err := s.pool.Exec(ctx, `DELETE FROM ingest_cursor WHERE name = $1`, name); err != nil {
		return fmt.Errorf("reset ingest cursor: %w", err)
	}
	return nil
}

// ---- reporter reconciliation -----------------------------------------------

// AllReporterAddresses returns every reporter address in the reporters table,
// active or inactive, so the reconciler can check each one against the chain.
func (s *Store) AllReporterAddresses(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT address FROM reporters ORDER BY address`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			return nil, err
		}
		out = append(out, addr)
	}
	return out, rows.Err()
}

// ReportReporterAddresses returns the reporter addresses that appear in
// reserve_reports but not in the reporters table. These are reporters whose
// reporter_set event was missed during an ingestion gap.
func (s *Store) ReportReporterAddresses(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT reporter FROM reserve_reports
		WHERE reporter NOT IN (SELECT address FROM reporters)
		ORDER BY reporter`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var addr string
		if err := rows.Scan(&addr); err != nil {
			return nil, err
		}
		out = append(out, addr)
	}
	return out, rows.Err()
}

// SaveChainReporter upserts a reporter read back from the registry contract.
// It returns true when any stored registry field changed.
// updated_ledger is set to 0 because the reconciler reads current state without
// knowing which specific ledger set it.
func (s *Store) SaveChainReporter(ctx context.Context, r coverage.Reporter) (bool, error) {
	var displayName *string
	if r.DisplayName != "" {
		displayName = &r.DisplayName
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO reporters
		  (address, role, name, display_name, website_uri, contact_hash, active, updated_ledger, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, true, 0, now())
		ON CONFLICT (address) DO UPDATE
		SET role = EXCLUDED.role, name = EXCLUDED.name,
		    display_name = EXCLUDED.display_name, website_uri = EXCLUDED.website_uri,
		    contact_hash = EXCLUDED.contact_hash, active = true,
		    updated_at = now()
		WHERE reporters.active = false
		   OR reporters.role <> EXCLUDED.role
		   OR reporters.name <> EXCLUDED.name
		   OR reporters.display_name IS DISTINCT FROM EXCLUDED.display_name
		   OR reporters.website_uri IS DISTINCT FROM EXCLUDED.website_uri
		   OR reporters.contact_hash IS DISTINCT FROM EXCLUDED.contact_hash`,
		r.Address, int16(r.Role), r.Name, displayName, r.WebsiteURI, r.ContactHash)
	if err != nil {
		return false, fmt.Errorf("save chain reporter: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// MarkReporterInactive marks a reporter as revoked. Returns true when the
// row existed and was changed (i.e., the reporter was previously active).
func (s *Store) MarkReporterInactive(ctx context.Context, address string) (bool, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE reporters SET active = false, updated_at = now()
		WHERE address = $1 AND active = true`,
		address)
	if err != nil {
		return false, fmt.Errorf("mark reporter inactive: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

// ---- doc hash checker -------------------------------------------------------

// ListPendingDocChecks returns reports that have a doc_uri but have not yet
// been verified, up to limit rows.
func (s *Store) ListPendingDocChecks(ctx context.Context, limit int) ([]coverage.DocCheckTask, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT event_id, doc_uri, doc_hash FROM reserve_reports
		WHERE doc_check IS NULL AND doc_uri != ''
		ORDER BY posted_ledger DESC
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []coverage.DocCheckTask
	for rows.Next() {
		var t coverage.DocCheckTask
		if err := rows.Scan(&t.EventID, &t.DocURI, &t.DocHash); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// SaveDocCheck records the result of a document hash verification. result must
// be one of "match", "mismatch", or "unreachable".
func (s *Store) SaveDocCheck(ctx context.Context, eventID, result string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE reserve_reports SET doc_check = $2 WHERE event_id = $1`,
		eventID, result)
	return err
}

// ---- vaults -----------------------------------------------------------------

// GetVault returns the indexed vault metadata for sac, or ErrNotFound if no
// vault has been indexed for that asset.
func (s *Store) GetVault(ctx context.Context, sac string) (coverage.Vault, error) {
	var v coverage.Vault
	var balance string
	var threshold int64
	err := s.pool.QueryRow(ctx, `
		SELECT sac, vault_address, reserve_balance::text, threshold_bps, updated_at
		FROM vaults WHERE sac = $1`, sac,
	).Scan(&v.SAC, &v.VaultAddress, &balance, &threshold, &v.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, ErrNotFound
	}
	if err != nil {
		return v, fmt.Errorf("get vault: %w", err)
	}
	v.ReserveBalance = mustBig(balance)
	v.ThresholdBps = uint32(threshold)
	v.UpdatedAt = v.UpdatedAt.UTC()
	return v, nil
}

// SaveVault inserts or updates indexed vault metadata for an asset.
func (s *Store) SaveVault(ctx context.Context, v coverage.Vault) error {
	balance := "0"
	if v.ReserveBalance != nil {
		balance = v.ReserveBalance.String()
	}
	updatedAt := v.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	_, err := s.pool.Exec(ctx, `
		INSERT INTO vaults (sac, vault_address, reserve_balance, threshold_bps, updated_at)
		VALUES ($1, $2, $3::numeric, $4, $5)
		ON CONFLICT (sac) DO UPDATE
		SET vault_address = EXCLUDED.vault_address,
		    reserve_balance = EXCLUDED.reserve_balance,
		    threshold_bps = EXCLUDED.threshold_bps,
		    updated_at = EXCLUDED.updated_at`,
		v.SAC, v.VaultAddress, balance, int64(v.ThresholdBps), updatedAt.UTC())
	if err != nil {
		return fmt.Errorf("save vault: %w", err)
	}
	return nil
}
