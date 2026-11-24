package stellartoml

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"
)

const (
	// attestationMaxBytes caps the downloaded document at 10 MB. Larger
	// documents are rejected rather than truncated: a truncated prefix would
	// hash to a value no one could reproduce.
	attestationMaxBytes = 10 * 1024 * 1024
	// attestationTimeout is the whole-request budget for one document.
	attestationTimeout = 20 * time.Second
	// attestationMaxRedirects is how many redirects are followed. Every hop
	// goes through the same private-address guard.
	attestationMaxRedirects = 3
)

// DocumentFetcher downloads the attestation_of_reserve document an issuer
// lists in its stellar.toml and returns its SHA-256.
//
// Every fetch is constrained:
//   - http and https only;
//   - no connections to private, loopback or link-local addresses (SSRF guard);
//   - 20-second total timeout;
//   - at most 10 MB, larger documents are an error;
//   - at most 3 redirects, each still http(s) and behind the same guard.
type DocumentFetcher struct {
	client *http.Client
}

// NewDocumentFetcher returns a DocumentFetcher with a constrained client.
func NewDocumentFetcher() *DocumentFetcher {
	return &DocumentFetcher{client: buildDocumentClient()}
}

// Fetch downloads uri and returns the lower-case hex SHA-256 of its body.
func (f *DocumentFetcher) Fetch(ctx context.Context, uri string) (string, error) {
	if err := checkDocumentURL(uri); err != nil {
		return "", err
	}
	// #nosec G107,G704 -- the scheme and host are validated above and the
	// client's dialer refuses private addresses; body size and time are capped.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
	if err != nil {
		return "", fmt.Errorf("attestation document %s: %w", uri, err)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch attestation document %s: %w", uri, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("fetch attestation document %s: status %d", uri, resp.StatusCode)
	}
	h := sha256.New()
	n, err := io.Copy(h, io.LimitReader(resp.Body, attestationMaxBytes+1))
	if err != nil {
		return "", fmt.Errorf("read attestation document %s: %w", uri, err)
	}
	if n > attestationMaxBytes {
		return "", fmt.Errorf("attestation document %s exceeds 10MB", uri)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// checkDocumentURL accepts only absolute http(s) URLs with a host.
func checkDocumentURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("attestation document url %q: %w", raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("attestation document url %q: scheme must be http or https", raw)
	}
	if u.Host == "" {
		return fmt.Errorf("attestation document url %q: missing host", raw)
	}
	return nil
}

// documentPrivateRanges are the IP blocks an issuer document must not reach.
var documentPrivateRanges = func() []net.IPNet {
	blocks := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"127.0.0.0/8",
		"169.254.0.0/16", // link-local IPv4
		"::1/128",
		"fc00::/7",  // unique local IPv6
		"fe80::/10", // link-local IPv6
		"0.0.0.0/8", // "this" network
	}
	var out []net.IPNet
	for _, b := range blocks {
		_, ipnet, err := net.ParseCIDR(b)
		if err != nil {
			panic("bad private range: " + b)
		}
		out = append(out, *ipnet)
	}
	return out
}()

// isDocumentPrivate reports whether addr is in a private or loopback range.
func isDocumentPrivate(addr net.Addr) bool {
	var ip net.IP
	switch a := addr.(type) {
	case *net.TCPAddr:
		ip = a.IP
	default:
		host, _, _ := net.SplitHostPort(addr.String())
		ip = net.ParseIP(host)
	}
	if ip == nil {
		return true // fail closed: unparse-able address is treated as private
	}
	for _, r := range documentPrivateRanges {
		if r.Contains(ip) {
			return true
		}
	}
	return false
}

// buildDocumentClient returns a client that refuses private addresses on every
// connection, including after a redirect.
func buildDocumentClient() *http.Client {
	dialer := &net.Dialer{Timeout: attestationTimeout}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			if isDocumentPrivate(conn.RemoteAddr()) {
				_ = conn.Close()
				return nil, fmt.Errorf("refusing connection to private address %s", conn.RemoteAddr())
			}
			return conn, nil
		},
		DisableKeepAlives: true,
	}
	return &http.Client{
		Timeout:   attestationTimeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= attestationMaxRedirects {
				return http.ErrUseLastResponse
			}
			if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
				return errors.New("redirect to non-http(s) refused")
			}
			return nil
		},
	}
}
