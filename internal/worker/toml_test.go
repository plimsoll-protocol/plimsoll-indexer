package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/stellartoml"
)

type tomlTestHorizon struct{}

func (tomlTestHorizon) HomeDomain(context.Context, string) (string, error) {
	return "issuer.example", nil
}

type tomlTestStore struct {
	assets  []coverage.Asset
	hashes  map[string]string
	changed int
}

func (s *tomlTestStore) ListAssets(context.Context) ([]coverage.Asset, error) {
	return s.assets, nil
}

func (s *tomlTestStore) SaveToml(context.Context, string, []byte, string, time.Time) error {
	return nil
}

func (s *tomlTestStore) SaveIssuerTomlSnapshot(_ context.Context, issuer, _, hash string, _ time.Time) (string, bool, error) {
	old := s.hashes[issuer]
	if old == hash {
		return old, false, nil
	}
	s.hashes[issuer] = hash
	s.changed++
	return old, old != "", nil
}

type tomlTestNotifier struct {
	changes [][4]string
}

func (n *tomlTestNotifier) TOMLChanged(_ context.Context, issuer, domain, oldHash, newHash string) error {
	n.changes = append(n.changes, [4]string{issuer, domain, oldHash, newHash})
	return nil
}

func TestTomlSyncDetectsAndDeliversContentChange(t *testing.T) {
	document := []byte("[DOCUMENTATION]\nORG_NAME = \"First\"\n")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(document)
	}))
	defer server.Close()
	fetcher := stellartoml.NewFetcher()
	fetcher.HTTP = server.Client()
	fetcher.URLFor = func(string) string { return server.URL }

	db := &tomlTestStore{
		assets: []coverage.Asset{{SAC: "CSAC", Code: "PUSD", Issuer: "GISSUER"}},
		hashes: make(map[string]string),
	}
	notifier := &tomlTestNotifier{}
	syncer := &TomlSync{
		Horizon:  tomlTestHorizon{},
		Fetcher:  fetcher,
		Store:    db,
		Webhooks: notifier,
		MaxAge:   -time.Second,
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	if err := syncer.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(notifier.changes) != 0 {
		t.Fatalf("initial document unexpectedly generated a change: %+v", notifier.changes)
	}
	oldSum := sha256.Sum256(document)

	document = []byte("[DOCUMENTATION]\nORG_NAME = \"Changed\"\n")
	if err := syncer.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	newSum := sha256.Sum256(document)
	if len(notifier.changes) != 1 {
		t.Fatalf("got %d change notifications, want 1", len(notifier.changes))
	}
	change := notifier.changes[0]
	if change[0] != "GISSUER" || change[1] != "issuer.example" ||
		change[2] != hex.EncodeToString(oldSum[:]) || change[3] != hex.EncodeToString(newSum[:]) {
		t.Fatalf("notification = %q, want issuer/domain/old/new hashes", change)
	}
	if db.changed != 2 {
		t.Fatalf("stored %d changed snapshots, want initial and changed snapshots", db.changed)
	}
}
