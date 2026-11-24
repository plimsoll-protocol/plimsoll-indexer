package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/stellartoml"
)

type tomlHorizon interface {
	HomeDomain(context.Context, string) (string, error)
}

type tomlFetcher interface {
	FetchSnapshot(context.Context, string, string, string) (stellartoml.Info, string, error)
}

type tomlStore interface {
	ListAssets(context.Context) ([]coverage.Asset, error)
	SaveToml(context.Context, string, []byte, string, time.Time) error
	SaveIssuerTomlSnapshot(context.Context, string, string, string, time.Time) (string, bool, error)
}

type tomlChangeNotifier interface {
	TOMLChanged(context.Context, string, string, string, string) error
}

// TomlSync records what each issuer's stellar.toml says about its asset,
// including the SEP-1 attestation_of_reserve link.
type TomlSync struct {
	Horizon  tomlHorizon
	Fetcher  tomlFetcher
	Store    tomlStore
	Webhooks tomlChangeNotifier
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
		info, hash, lookupErr := t.lookup(ctx, a.Code, a.Issuer)
		if hash != "" {
			oldHash, changed, err := t.Store.SaveIssuerTomlSnapshot(ctx, a.Issuer, info.HomeDomain, hash, now)
			if err != nil {
				return fmt.Errorf("save TOML hash for issuer %s: %w", a.Issuer, err)
			}
			if changed {
				t.Log.Info("issuer stellar.toml changed",
					"issuer", a.Issuer,
					"home_domain", info.HomeDomain,
					"old_hash", oldHash,
					"new_hash", hash)
				if t.Webhooks != nil {
					if err := t.Webhooks.TOMLChanged(ctx, a.Issuer, info.HomeDomain, oldHash, hash); err != nil {
						t.Log.Error("issuer stellar.toml change webhook delivery failed",
							"issuer", a.Issuer,
							"home_domain", info.HomeDomain,
							"old_hash", oldHash,
							"new_hash", hash,
							"err", err)
					}
				}
			}
		}
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
		}
	}
	return nil
}

func (t *TomlSync) lookup(ctx context.Context, code, issuer string) (stellartoml.Info, string, error) {
	domain, err := t.Horizon.HomeDomain(ctx, issuer)
	if err != nil {
		return stellartoml.Info{}, "", err
	}
	return t.Fetcher.FetchSnapshot(ctx, domain, code, issuer)
}
