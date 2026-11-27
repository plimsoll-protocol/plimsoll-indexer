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
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

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

// ApplyEvents writes one page of events and advances the cursor in one
// transaction: raw holds every event in the page, decoded or not, and events
// the decoded ones. A page delivered twice stores each raw event once.
func (s *Store) ApplyEvents(ctx context.Context, name, cursor string, ledger uint32, raw []soroban.RawEvent, events []any) error {
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
		}
	}
	return tx.Commit(ctx)
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
		n, err := parseAmount(amount)
		if err != nil {
			return nil, fmt.Errorf("supply snapshot for %s at ledger %d: %w", sac, ledger, err)
		}
		sp.Amount = n
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

// LatestReports returns the newest report (by as_of) for each tier.
func (s *Store) LatestReports(ctx context.Context, sac string) (map[uint32]coverage.Report, error) {
	list, err := s.queryReports(ctx, latestReportsSQL, sac)
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
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", false, fmt.Errorf("begin issuer TOML snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, issuer); err != nil {
		return "", false, fmt.Errorf("lock issuer TOML snapshot: %w", err)
	}
	var oldHash string
	err = tx.QueryRow(ctx, `
		SELECT sha256_hash FROM issuer_toml_snapshots
		WHERE issuer = $1 ORDER BY id DESC LIMIT 1`, issuer).Scan(&oldHash)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return "", false, fmt.Errorf("read previous issuer TOML hash: %w", err)
	}
	changed := err == nil && oldHash != hash
	if err == pgx.ErrNoRows || changed {
		if _, err := tx.Exec(ctx, `
			INSERT INTO issuer_toml_snapshots (issuer, home_domain, sha256_hash, checked_at)
			VALUES ($1, $2, $3, $4)`, issuer, domain, hash, at); err != nil {
			return "", false, fmt.Errorf("insert issuer TOML snapshot: %w", err)
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
