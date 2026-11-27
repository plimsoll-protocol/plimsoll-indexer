package stellartoml

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// TestDocumentFetcherHashesContent proves a reachable document is hashed over
// exactly the bytes served.
func TestDocumentFetcherHashesContent(t *testing.T) {
	body := []byte(`{"reserves":"10200000000","as_of":"2026-10-01"}`)
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	f := &DocumentFetcher{client: srv.Client()}
	got, err := f.Fetch(context.Background(), srv.URL+"/reserves.json")
	if err != nil {
		t.Fatal(err)
	}
	if want := sha256Hex(body); got != want {
		t.Fatalf("hash = %s, want %s", got, want)
	}
}

// TestDocumentFetcherRejectsOversize documents the 10 MB cap: a body one byte
// over is an error rather than a hash of its prefix.
func TestDocumentFetcherRejectsOversize(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(make([]byte, attestationMaxBytes+1))
	}))
	defer srv.Close()

	f := &DocumentFetcher{client: srv.Client()}
	_, err := f.Fetch(context.Background(), srv.URL+"/big.pdf")
	if err == nil || !strings.Contains(err.Error(), "exceeds 10MB") {
		t.Fatalf("err = %v, want an exceeds-10MB error", err)
	}
}

// TestDocumentFetcherRejectsNonHTTPScheme proves anything but http(s) is
// refused before a connection is attempted.
func TestDocumentFetcherRejectsNonHTTPScheme(t *testing.T) {
	f := NewDocumentFetcher()
	for _, uri := range []string{
		"ftp://example.com/reserves.pdf",
		"file:///etc/passwd",
		"gopher://example.com/reserves",
		"https:///no-host",
	} {
		if _, err := f.Fetch(context.Background(), uri); err == nil {
			t.Errorf("Fetch(%q) = nil error, want refusal", uri)
		}
	}
}

// TestDocumentFetcherRefusesPrivateAddress is the SSRF guard: with the
// production client, a document served from loopback is refused before any
// body is read, even though http is an allowed scheme.
func TestDocumentFetcherRefusesPrivateAddress(t *testing.T) {
	var reached bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		reached = true
		_, _ = w.Write([]byte("secret internal document"))
	}))
	defer srv.Close()

	f := NewDocumentFetcher()
	_, err := f.Fetch(context.Background(), srv.URL+"/reserves.json")
	if err == nil || !strings.Contains(err.Error(), "private address") {
		t.Fatalf("err = %v, want a private-address refusal", err)
	}
	if reached {
		t.Fatal("request reached the loopback server; SSRF guard did not fire")
	}
}

// TestIsDocumentPrivate covers the range table, in particular that public
// addresses are still allowed.
func TestIsDocumentPrivate(t *testing.T) {
	cases := []struct {
		ip   string
		want bool
	}{
		{"10.0.0.1", true},
		{"172.16.0.1", true},
		{"172.31.255.255", true},
		{"172.32.0.0", false},
		{"192.168.1.1", true},
		{"127.0.0.1", true},
		{"169.254.0.1", true},
		{"::1", true},
		{"fe80::1", true},
		{"8.8.8.8", false},
		{"1.1.1.1", false},
		{"2001:db8::1", false},
	}
	for _, tc := range cases {
		addr := &documentAddr{ip: tc.ip}
		if got := isDocumentPrivate(addr); got != tc.want {
			t.Errorf("isDocumentPrivate(%s) = %v, want %v", tc.ip, got, tc.want)
		}
	}
}

type documentAddr struct{ ip string }

func (a *documentAddr) Network() string { return "tcp" }
func (a *documentAddr) String() string {
	if strings.Contains(a.ip, ":") {
		return "[" + a.ip + "]:443"
	}
	return a.ip + ":443"
}
