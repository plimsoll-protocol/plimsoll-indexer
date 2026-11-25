package api

import (
	"compress/gzip"
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/coverage"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

const compressionSAC = "CCHPT4TEJDPZQDSUVW3NP6A35ROGV4WEFZKT45SKWYCFZSUB7R5HIM7S"

type compressionReader struct {
	assets []coverage.Asset
}

func (r compressionReader) Ping(context.Context) error { return nil }
func (r compressionReader) ListAssets(context.Context) ([]coverage.Asset, error) {
	return r.assets, nil
}
func (compressionReader) GetAsset(context.Context, string) (coverage.Asset, error) {
	return coverage.Asset{}, store.ErrNotFound
}
func (compressionReader) LatestSupply(context.Context, string) (*coverage.Supply, error) {
	return nil, nil
}
func (compressionReader) SupplyHistory(context.Context, string, int) ([]coverage.Supply, error) {
	return nil, nil
}
func (compressionReader) LatestReports(context.Context, string) (map[uint32]coverage.Report, error) {
	return map[uint32]coverage.Report{}, nil
}
func (compressionReader) ReportHistory(context.Context, string, int) ([]coverage.Report, error) {
	return nil, nil
}
func (compressionReader) Reporters(context.Context) ([]coverage.Reporter, error) {
	return nil, nil
}
func (compressionReader) Breakdown(context.Context, string) ([]byte, error) {
	return nil, store.ErrNotFound
}
func (compressionReader) Progress(context.Context, string) (store.Progress, error) {
	return store.Progress{}, nil
}

func TestCompressionMiddleware(t *testing.T) {
	largeStore := compressionReader{assets: []coverage.Asset{{SAC: compressionSAC, Code: strings.Repeat("P", 1200), Issuer: "GISS"}}}
	s := &Server{
		Store: largeStore,
		Log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	request := func(path, accepted string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, srv.URL+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Accept-Encoding", accepted)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	t.Run("gzip large JSON response", func(t *testing.T) {
		resp := request("/v1/assets", "gzip")
		defer resp.Body.Close()
		if resp.Header.Get("Content-Encoding") != "gzip" {
			t.Fatalf("Content-Encoding = %q, want gzip", resp.Header.Get("Content-Encoding"))
		}
		reader, err := gzip.NewReader(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		if err := reader.Close(); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), strings.Repeat("P", 1200)) {
			t.Fatal("decompressed response is missing asset data")
		}
	})

	t.Run("zstd large JSON response", func(t *testing.T) {
		resp := request("/v1/assets", "zstd")
		defer resp.Body.Close()
		if resp.Header.Get("Content-Encoding") != "zstd" {
			t.Fatalf("Content-Encoding = %q, want zstd", resp.Header.Get("Content-Encoding"))
		}
		reader, err := zstd.NewReader(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		reader.Close()
		if !strings.Contains(string(body), strings.Repeat("P", 1200)) {
			t.Fatal("decompressed response is missing asset data")
		}
	})

	t.Run("small JSON response remains uncompressed", func(t *testing.T) {
		resp := request("/v1/network", "gzip")
		defer resp.Body.Close()
		if resp.Header.Get("Content-Encoding") != "" {
			t.Fatalf("small response has Content-Encoding %q", resp.Header.Get("Content-Encoding"))
		}
	})
}

func TestCompressionThresholdAndPrecompressedResponses(t *testing.T) {
	tests := []struct {
		name            string
		size            int
		contentType     string
		contentEncoding string
		wantEncoding    string
	}{
		{name: "below threshold", size: minimumCompressionSize - 1, contentType: "application/json"},
		{name: "threshold JSON", size: minimumCompressionSize, contentType: "application/json", wantEncoding: "gzip"},
		{name: "threshold NDJSON", size: minimumCompressionSize, contentType: "application/x-ndjson", wantEncoding: "gzip"},
		{name: "already encoded", size: minimumCompressionSize, contentType: "application/json", contentEncoding: "br"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler := (&Server{}).compress(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", test.contentType)
				if test.contentEncoding != "" {
					w.Header().Set("Content-Encoding", test.contentEncoding)
				}
				_, _ = io.WriteString(w, strings.Repeat("x", test.size))
			}))
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			req.Header.Set("Accept-Encoding", "gzip")
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, req)
			resp := recorder.Result()
			defer resp.Body.Close()
			if got := resp.Header.Get("Content-Encoding"); got != test.wantEncoding && got != test.contentEncoding {
				t.Fatalf("Content-Encoding = %q, want %q", got, test.wantEncoding)
			}
			if test.wantEncoding == "gzip" {
				reader, err := gzip.NewReader(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				body, err := io.ReadAll(reader)
				if err != nil {
					t.Fatal(err)
				}
				if len(body) != test.size {
					t.Fatalf("decompressed body length = %d, want %d", len(body), test.size)
				}
			} else if body, err := io.ReadAll(resp.Body); err != nil || len(body) != test.size {
				t.Fatalf("body length = %d, want %d (err %v)", len(body), test.size, err)
			}
		})
	}
}

func TestCompressionBuffersFlushedNDJSONUntilThreshold(t *testing.T) {
	handler := (&Server{}).compress(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/x-ndjson")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatal("response writer does not support flush")
		}
		_, _ = io.WriteString(w, strings.Repeat("x", 600))
		flusher.Flush()
		_, _ = io.WriteString(w, strings.Repeat("y", 600))
		flusher.Flush()
	}))
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Accept-Encoding", "gzip")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	resp := recorder.Result()
	defer resp.Body.Close()
	if got := resp.Header.Get("Content-Encoding"); got != "gzip" {
		t.Fatalf("Content-Encoding = %q, want gzip", got)
	}
	reader, err := gzip.NewReader(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if len(body) != 1200 || body[599] != 'x' || body[600] != 'y' {
		t.Fatalf("decompressed NDJSON body has incorrect content or length: %d bytes", len(body))
	}
}
