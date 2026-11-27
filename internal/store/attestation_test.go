package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

// TestRecordAttestationKeepsHistory proves that repeated checks of the same
// document do not create history, and that a changed document records the
// time the new hash appeared.
func TestRecordAttestationKeepsHistory(t *testing.T) {
	s, url := openStore(t)
	ctx := context.Background()
	const sac = "CSAC"
	first := time.Date(2026, 10, 1, 9, 0, 0, 0, time.UTC)
	second := first.Add(24 * time.Hour)
	third := second.Add(24 * time.Hour)

	if err := s.RecordAttestation(ctx, sac, "https://issuer.example/a.pdf", "aaaa", first); err != nil {
		t.Fatal(err)
	}
	a, err := s.Attestation(ctx, sac)
	if err != nil {
		t.Fatal(err)
	}
	if a.Hash != "aaaa" || a.URL != "https://issuer.example/a.pdf" || !a.FirstSeenAt.Equal(first) {
		t.Fatalf("first document: %+v", a)
	}
	if a.ChangedAt != nil {
		t.Fatalf("ChangedAt = %v for the first document, want nil", a.ChangedAt)
	}

	// The same document seen again keeps its first-seen time.
	if err := s.RecordAttestation(ctx, sac, "https://issuer.example/a.pdf", "aaaa", second); err != nil {
		t.Fatal(err)
	}
	a, err = s.Attestation(ctx, sac)
	if err != nil {
		t.Fatal(err)
	}
	if !a.FirstSeenAt.Equal(first) || a.ChangedAt != nil {
		t.Fatalf("after re-check: %+v, want first_seen_at %v and no change", a, first)
	}

	// A new document becomes current and its first-seen time is the change.
	if err := s.RecordAttestation(ctx, sac, "https://issuer.example/b.pdf", "bbbb", third); err != nil {
		t.Fatal(err)
	}
	a, err = s.Attestation(ctx, sac)
	if err != nil {
		t.Fatal(err)
	}
	if a.Hash != "bbbb" || a.URL != "https://issuer.example/b.pdf" || !a.FirstSeenAt.Equal(third) {
		t.Fatalf("current document: %+v", a)
	}
	if a.ChangedAt == nil || !a.ChangedAt.Equal(third) {
		t.Fatalf("ChangedAt = %v, want %v", a.ChangedAt, third)
	}

	// The history keeps the replaced document.
	var rows int
	if err := testPool(t, url).QueryRow(ctx,
		`SELECT count(*) FROM attestation_documents WHERE sac = $1`, sac).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("attestation history has %d rows, want 2", rows)
	}
}

func TestAttestationNotFound(t *testing.T) {
	s, _ := openStore(t)
	_, err := s.Attestation(context.Background(), "CUNKNOWN")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}
