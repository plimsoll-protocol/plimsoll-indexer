package stellartoml

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const sample = `
VERSION = "2.7.0"

[DOCUMENTATION]
ORG_NAME = "Example Money Ltd"
ORG_URL = "https://example.money"

[[CURRENCIES]]
code = "EXM"
issuer = "GOTHER"

[[CURRENCIES]]
code = "USDX"
issuer = "GISSUER"
name = "Example Dollar"
is_asset_anchored = true
anchor_asset = "USD"
attestation_of_reserve = "https://example.money/reserves/2026-09.pdf"
redemption_instructions = "Redeem at example.money"
`

func TestParseFindsMatchingCurrency(t *testing.T) {
	info, err := Parse("example.money", []byte(sample), "USDX", "GISSUER")
	if err != nil {
		t.Fatal(err)
	}
	if info.OrgName != "Example Money Ltd" || info.OrgURL != "https://example.money" {
		t.Errorf("org: %+v", info)
	}
	if info.Currency == nil {
		t.Fatal("currency not found")
	}
	c := info.Currency
	if c.AttestationOfReserve != "https://example.money/reserves/2026-09.pdf" {
		t.Errorf("attestation %q", c.AttestationOfReserve)
	}
	if !c.IsAssetAnchored || c.AnchorAsset != "USD" {
		t.Errorf("anchor: %+v", c)
	}
}

func TestParseNoMatch(t *testing.T) {
	info, err := Parse("example.money", []byte(sample), "USDX", "GSOMEONEELSE")
	if err != nil {
		t.Fatal(err)
	}
	if info.Currency != nil {
		t.Fatal("matched the wrong issuer")
	}
}

func TestParseInvalid(t *testing.T) {
	if _, err := Parse("x", []byte("not = = toml"), "A", "B"); err == nil {
		t.Fatal("expected parse error")
	}
}

func TestFetchEnforcesSizeLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("#", maxTomlBytes+10)))
	}))
	defer srv.Close()
	f := NewFetcher()
	f.URLFor = func(string) string { return srv.URL }
	if _, err := f.Fetch(context.Background(), "big.example", "A", "B"); err == nil {
		t.Fatal("expected size error")
	}
}

func TestFetchRequiresDomain(t *testing.T) {
	if _, err := NewFetcher().Fetch(context.Background(), "", "A", "B"); err == nil {
		t.Fatal("expected error for empty domain")
	}
}

func TestFetchOK(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(sample))
	}))
	defer srv.Close()
	f := NewFetcher()
	f.URLFor = func(string) string { return srv.URL }
	info, err := f.Fetch(context.Background(), "example.money", "USDX", "GISSUER")
	if err != nil || info.Currency == nil {
		t.Fatalf("got %+v, %v", info, err)
	}
}

func TestFetchFollowsUpToFiveRedirects(t *testing.T) {
	for _, redirects := range []int{5, 6} {
		t.Run(fmt.Sprintf("redirects_%d", redirects), func(t *testing.T) {
			requests := 0
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				var hop int
				if _, err := fmt.Sscanf(r.URL.Path, "/%d", &hop); err != nil {
					t.Errorf("parse redirect path %q: %v", r.URL.Path, err)
					return
				}
				if hop < redirects {
					http.Redirect(w, r, fmt.Sprintf("/%d", hop+1), http.StatusFound)
					return
				}
				_, _ = w.Write([]byte(sample))
			}))
			defer srv.Close()

			f := NewFetcher()
			f.URLFor = func(string) string { return srv.URL + "/0" }
			info, err := f.Fetch(context.Background(), "example.money", "USDX", "GISSUER")
			if redirects == maxRedirects {
				if err != nil || info.Currency == nil {
					t.Fatalf("Fetch after %d redirects: info=%+v err=%v", redirects, info, err)
				}
				if requests != redirects+1 {
					t.Fatalf("got %d requests, want %d", requests, redirects+1)
				}
			} else {
				if err == nil {
					t.Fatalf("Fetch after %d redirects succeeded, want limit error", redirects)
				}
				if requests != maxRedirects+1 {
					t.Fatalf("got %d requests before rejection, want %d", requests, maxRedirects+1)
				}
			}
		})
	}
}

func TestFetchSnapshotHashesExactResponseBytes(t *testing.T) {
	body := []byte(sample)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	f := NewFetcher()
	f.URLFor = func(string) string { return srv.URL }

	info, gotHash, err := f.FetchSnapshot(context.Background(), "example.money", "USDX", "GISSUER")
	if err != nil || info.Currency == nil {
		t.Fatalf("FetchSnapshot: info=%+v err=%v", info, err)
	}
	sum := sha256.Sum256(body)
	if gotHash != hex.EncodeToString(sum[:]) {
		t.Fatalf("hash %q, want SHA-256 of exact response bytes %q", gotHash, hex.EncodeToString(sum[:]))
	}
}
