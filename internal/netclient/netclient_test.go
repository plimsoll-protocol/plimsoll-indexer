package netclient

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// testClient returns a client that trusts the test server's certificate.
func testClient(t *testing.T, srv *httptest.Server) *http.Client {
	t.Helper()
	client := New(5*time.Second, time.Minute)
	tr := client.Transport.(*http.Transport)
	tr.TLSClientConfig = srv.Client().Transport.(*http.Transport).TLSClientConfig
	return client
}

// Horizon and RPC are HTTPS endpoints that offer HTTP/2, so the shared
// transport must negotiate it rather than fall back to HTTP/1.1.
func TestClientNegotiatesHTTP2(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.Proto)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	resp, err := testClient(t, srv).Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.ProtoMajor != 2 {
		t.Fatalf("negotiated %s, want HTTP/2", resp.Proto)
	}
}

// Sequential calls reuse one connection instead of making a TLS handshake per
// call, which is the point of the keep-alive pool.
func TestClientReusesConnections(t *testing.T) {
	var opened int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	srv.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			atomic.AddInt32(&opened, 1)
		}
	}
	srv.StartTLS()
	defer srv.Close()

	client := testClient(t, srv)
	for range 3 {
		resp, err := client.Get(srv.URL)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	if got := atomic.LoadInt32(&opened); got != 1 {
		t.Fatalf("opened %d connections for three requests, want 1", got)
	}
}

func TestTransportKeepAliveIsConfigurable(t *testing.T) {
	if got := Transport(0).IdleConnTimeout; got != DefaultKeepAlive {
		t.Errorf("Transport(0).IdleConnTimeout = %v, want %v", got, DefaultKeepAlive)
	}
	tr := Transport(42 * time.Second)
	if tr.IdleConnTimeout != 42*time.Second {
		t.Errorf("IdleConnTimeout = %v, want 42s", tr.IdleConnTimeout)
	}
	if !tr.ForceAttemptHTTP2 {
		t.Error("ForceAttemptHTTP2 is off, so a custom dialer would disable HTTP/2")
	}
	if tr.MaxIdleConnsPerHost < 2 {
		t.Errorf("MaxIdleConnsPerHost = %d, want a pool larger than one", tr.MaxIdleConnsPerHost)
	}
}
