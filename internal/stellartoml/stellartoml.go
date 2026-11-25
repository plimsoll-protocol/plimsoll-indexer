// Package stellartoml reads an issuer's SEP-1 stellar.toml and extracts what
// the issuer says about one of its currencies, in particular the
// attestation_of_reserve link.
package stellartoml

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/BurntSushi/toml"
)

// maxTomlBytes matches the SEP-1 limit of 100KB.
const maxTomlBytes = 100 * 1024

const maxRedirects = 5

// Currency is the subset of a [[CURRENCIES]] entry Plimsoll shows.
type Currency struct {
	Code                   string `toml:"code" json:"code"`
	Issuer                 string `toml:"issuer" json:"issuer"`
	Name                   string `toml:"name" json:"name,omitempty"`
	Desc                   string `toml:"desc" json:"desc,omitempty"`
	IsAssetAnchored        bool   `toml:"is_asset_anchored" json:"is_asset_anchored"`
	AnchorAsset            string `toml:"anchor_asset" json:"anchor_asset,omitempty"`
	AttestationOfReserve   string `toml:"attestation_of_reserve" json:"attestation_of_reserve,omitempty"`
	RedemptionInstructions string `toml:"redemption_instructions" json:"redemption_instructions,omitempty"`
}

// Info is what Plimsoll stores per asset.
type Info struct {
	HomeDomain string    `json:"home_domain"`
	OrgName    string    `json:"org_name,omitempty"`
	OrgURL     string    `json:"org_url,omitempty"`
	Currency   *Currency `json:"currency,omitempty"`
}

type document struct {
	Documentation struct {
		OrgName string `toml:"ORG_NAME"`
		OrgURL  string `toml:"ORG_URL"`
	} `toml:"DOCUMENTATION"`
	Currencies []Currency `toml:"CURRENCIES"`
}

type Fetcher struct {
	HTTP *http.Client
	// URLFor builds the stellar.toml URL for a domain; overridable in tests.
	URLFor func(domain string) string
}

func NewFetcher() *Fetcher {
	return &Fetcher{
		HTTP: &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(_ *http.Request, via []*http.Request) error {
				if len(via) > maxRedirects {
					return fmt.Errorf("stopped after %d redirects", maxRedirects)
				}
				return nil
			},
		},
		URLFor: func(domain string) string {
			return "https://" + domain + "/.well-known/stellar.toml"
		},
	}
}

// Fetch downloads and parses the stellar.toml for domain, and returns the
// entry matching code and issuer (Currency is nil if there is none).
func (f *Fetcher) Fetch(ctx context.Context, domain, code, issuer string) (Info, error) {
	info, _, err := f.FetchSnapshot(ctx, domain, code, issuer)
	return info, err
}

// FetchSnapshot fetches and parses a stellar.toml, returning its SHA-256 over
// the exact response bytes. The hash is returned even if TOML parsing fails.
func (f *Fetcher) FetchSnapshot(ctx context.Context, domain, code, issuer string) (Info, string, error) {
	info := Info{HomeDomain: domain}
	if domain == "" {
		return info, "", errors.New("issuer has no home_domain")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.URLFor(domain), nil)
	if err != nil {
		return info, "", err
	}
	resp, err := f.HTTP.Do(req)
	if err != nil {
		return info, "", fmt.Errorf("fetch stellar.toml: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return info, "", fmt.Errorf("fetch stellar.toml: status %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxTomlBytes+1))
	if err != nil {
		return info, "", fmt.Errorf("read stellar.toml: %w", err)
	}
	if len(body) > maxTomlBytes {
		return info, "", errors.New("stellar.toml exceeds 100KB")
	}
	hash := sha256.Sum256(body)
	info, err = Parse(domain, body, code, issuer)
	return info, hex.EncodeToString(hash[:]), err
}

// Parse extracts Info from a stellar.toml body.
func Parse(domain string, body []byte, code, issuer string) (Info, error) {
	info := Info{HomeDomain: domain}
	var doc document
	if _, err := toml.Decode(string(body), &doc); err != nil {
		return info, fmt.Errorf("parse stellar.toml: %w", err)
	}
	info.OrgName = doc.Documentation.OrgName
	info.OrgURL = doc.Documentation.OrgURL
	for i := range doc.Currencies {
		c := doc.Currencies[i]
		if strings.EqualFold(c.Code, code) && c.Issuer == issuer {
			info.Currency = &c
			break
		}
	}
	return info, nil
}
