// Package netclient builds the HTTP client the indexer uses for its own
// outbound Horizon and Soroban RPC calls.
//
// Both are JSON APIs called from every worker tick, so reusing connections
// matters: a fresh TLS handshake per call adds latency for no benefit. The
// transport therefore keeps a pool of persistent connections, negotiates
// HTTP/2 when the server offers it, and keeps idle connections open for a
// configurable time (HTTP_KEEPALIVE_TIMEOUT).
package netclient

import (
	"net"
	"net/http"
	"time"
)

// DefaultKeepAlive matches net/http's default idle connection timeout, so an
// unset HTTP_KEEPALIVE_TIMEOUT keeps the standard behaviour.
const DefaultKeepAlive = 90 * time.Second

// New returns a client with an HTTP/2-capable transport that reuses
// connections. timeout is the overall request timeout; keepAlive is how long
// an idle connection is kept open for reuse. A keepAlive of zero or less uses
// DefaultKeepAlive.
func New(timeout, keepAlive time.Duration) *http.Client {
	return &http.Client{Transport: Transport(keepAlive), Timeout: timeout}
}

// Transport returns the shared transport settings. ForceAttemptHTTP2 is set
// explicitly because it is otherwise disabled as soon as DialContext or
// TLSClientConfig is customised, which this transport does.
func Transport(keepAlive time.Duration) *http.Transport {
	if keepAlive <= 0 {
		keepAlive = DefaultKeepAlive
	}
	dialer := &net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DialContext = dialer.DialContext
	tr.ForceAttemptHTTP2 = true
	tr.MaxIdleConns = 100
	tr.MaxIdleConnsPerHost = 16
	tr.IdleConnTimeout = keepAlive
	return tr
}
