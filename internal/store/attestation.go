package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// Attestation is an asset's current attestation-of-reserve document: its hash,
// where it was listed, when that hash was first seen and, once the issuer has
// replaced an earlier document, when the current one appeared.
type Attestation struct {
	URL         string
	Hash        string
	FirstSeenAt time.Time
	// ChangedAt is FirstSeenAt of the current hash when an earlier, different
	// hash is on record; nil for the first document ever seen.
	ChangedAt *time.Time
}

// RecordAttestation records that sac's stellar.toml listed url holding the
// document with the given SHA-256 at the given time. Repeated observations of
// the same hash keep their first_seen_at and only move last_seen_at, so the
// table is a history of document versions, not of checks.
func (s *Store) RecordAttestation(ctx context.Context, sac, url, hash string, at time.Time) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO attestation_documents (sac, url, hash, first_seen_at, last_seen_at)
		VALUES ($1, $2, $3, $4, $4)
		ON CONFLICT (sac, hash) DO UPDATE
		SET url = EXCLUDED.url, last_seen_at = EXCLUDED.last_seen_at`,
		sac, url, hash, at)
	if err != nil {
		return fmt.Errorf("record attestation for %s: %w", sac, err)
	}
	return nil
}

// Attestation returns the asset's current document. It returns ErrNotFound
// when the issuer has never listed a reachable document.
func (s *Store) Attestation(ctx context.Context, sac string) (Attestation, error) {
	var a Attestation
	err := s.pool.QueryRow(ctx, `
		WITH current AS (
		    SELECT url, hash, first_seen_at
		    FROM attestation_documents
		    WHERE sac = $1
		    ORDER BY last_seen_at DESC, first_seen_at DESC
		    LIMIT 1
		)
		SELECT c.url, c.hash, c.first_seen_at,
		       CASE WHEN (SELECT count(*) FROM attestation_documents WHERE sac = $1) > 1
		            THEN c.first_seen_at END
		FROM current c`, sac).Scan(&a.URL, &a.Hash, &a.FirstSeenAt, &a.ChangedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Attestation{}, ErrNotFound
	}
	if err != nil {
		return Attestation{}, fmt.Errorf("load attestation for %s: %w", sac, err)
	}
	return a, nil
}
