package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/horizon"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/stellartoml"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

// TomlSync records what each issuer's stellar.toml says about its asset,
// including the SEP-1 attestation_of_reserve link.
type TomlSync struct {
	Horizon *horizon.Client
	Fetcher *stellartoml.Fetcher
	Store   *store.Store
	// Attestation fetches and hashes the issuer's attestation_of_reserve
	// document. Nil disables the check.
	Attestation *stellartoml.DocumentFetcher
	// MaxAge is how long a lookup result is kept before it is refreshed.
	MaxAge time.Duration
	Log    *slog.Logger
}

func (t *TomlSync) Run(ctx context.Context) error {
	assets, err := t.Store.ListAssets(ctx)
	if err != nil {
		return fmt.Errorf("list assets: %w", err)
	}
	now := time.Now().UTC()
	for _, a := range assets {
		if a.TomlCheckedAt != nil && now.Sub(*a.TomlCheckedAt) < t.MaxAge {
			continue
		}
		info, lookupErr := t.lookup(ctx, a.Code, a.Issuer)
		var body []byte
		msg := ""
		if lookupErr != nil {
			msg = lookupErr.Error()
		}
		// Keep whatever we learned (e.g. home_domain) even on partial failure.
		if info.HomeDomain != "" || lookupErr == nil {
			body, _ = json.Marshal(info)
		}
		if err := t.Store.SaveToml(ctx, a.SAC, body, msg, now); err != nil {
			return fmt.Errorf("save toml for %s: %w", a.Code, err)
		}
		if lookupErr != nil {
			t.Log.Debug("stellar.toml lookup failed", "code", a.Code, "issuer", a.Issuer, "err", lookupErr)
			continue
		}
		t.checkAttestation(ctx, a, info, now)
	}
	return nil
}

// checkAttestation records the hash of the document the issuer lists at
// attestation_of_reserve. A fetch failure is logged and retried on the next
// pass; it does not fail the sync or discard the stellar.toml that was read.
func (t *TomlSync) checkAttestation(ctx context.Context, a coverage.Asset, info stellartoml.Info, now time.Time) {
	if t.Attestation == nil || info.Currency == nil || info.Currency.AttestationOfReserve == "" {
		return
	}
	uri := info.Currency.AttestationOfReserve
	hash, err := t.Attestation.Fetch(ctx, uri)
	if err != nil {
		t.Log.Debug("attestation document fetch failed", "code", a.Code, "issuer", a.Issuer, "uri", uri, "err", err)
		return
	}
	if err := t.Store.RecordAttestation(ctx, a.SAC, uri, hash, now); err != nil {
		t.Log.Error("record attestation document", "code", a.Code, "sac", a.SAC, "err", err)
	}
}

func (t *TomlSync) lookup(ctx context.Context, code, issuer string) (stellartoml.Info, error) {
	domain, err := t.Horizon.HomeDomain(ctx, issuer)
	if err != nil {
		return stellartoml.Info{}, err
	}
	return t.Fetcher.Fetch(ctx, domain, code, issuer)
}
