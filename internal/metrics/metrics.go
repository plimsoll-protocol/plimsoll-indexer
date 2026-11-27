// Package metrics defines Prometheus metrics for the Plimsoll indexer.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// IngestLagLedgers tracks the difference between the latest ledger known to RPC
// and the last ledger scanned by the event ingester.
var IngestLagLedgers = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "plimsoll",
	Subsystem: "ingest",
	Name:      "lag_ledgers",
	Help:      "Lag in ledgers between the latest network ledger and the scanned ledger.",
})

// ReportsPerMinute measures the rolling number of reserve reports ingested per minute.
var ReportsPerMinute = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "plimsoll",
	Subsystem: "reports",
	Name:      "per_minute",
	Help:      "Current rate of reserve reports ingested per minute.",
})

// ActiveAssets tracks the current number of unique assets listed on the Plimsoll coverage ledger.
var ActiveAssets = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "plimsoll",
	Subsystem: "assets",
	Name:      "active",
	Help:      "Current count of active assets listed and indexed.",
})

// DBQueryDuration measures database query execution latency in seconds,
// partitioned by operation name.
var DBQueryDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "plimsoll",
	Subsystem: "db",
	Name:      "query_duration_seconds",
	Help:      "Database query latency histogram in seconds partitioned by operation.",
	Buckets:   prometheus.DefBuckets,
}, []string{"operation"})

// ReportsIngestedTotal counts the cumulative number of reserve reports
// ingested from on-chain contract events.
var ReportsIngestedTotal = prometheus.NewCounter(prometheus.CounterOpts{
	Namespace: "plimsoll",
	Subsystem: "reports",
	Name:      "ingested_total",
	Help:      "Total number of reserve reports ingested.",
})

// EventsIngestedTotal counts the total number of contract events successfully
// ingested from RPC, labeled by event type.
var EventsIngestedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "plimsoll",
	Subsystem: "events",
	Name:      "ingested_total",
	Help:      "Total number of contract events ingested, labeled by event type.",
}, []string{"type"})

// IngestLastEventAgeSeconds measures the time elapsed in seconds since the
// most recent contract event was ingested.
var IngestLastEventAgeSeconds = prometheus.NewGauge(prometheus.GaugeOpts{
	Namespace: "plimsoll",
	Subsystem: "ingest",
	Name:      "last_event_age_seconds",
	Help:      "Seconds elapsed since the last contract event was ingested.",
})

// HTTPRequestDuration measures the latency of HTTP requests served by the API
// in seconds, partitioned by route path and status code.
var HTTPRequestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "plimsoll",
	Subsystem: "http",
	Name:      "request_duration_seconds",
	Help:      "HTTP request latency histogram in seconds partitioned by path and status.",
	Buckets:   prometheus.DefBuckets,
}, []string{"path", "status"})

func init() {
	register(
		IngestLagLedgers,
		ReportsPerMinute,
		ActiveAssets,
		DBQueryDuration,
		ReportsIngestedTotal,
		EventsIngestedTotal,
		IngestLastEventAgeSeconds,
		HTTPRequestDuration,
	)
}

func register(cs ...prometheus.Collector) {
	for _, c := range cs {
		if err := prometheus.Register(c); err != nil {
			if _, ok := err.(prometheus.AlreadyRegisteredError); ok {
				continue
			}
			panic(err)
		}
	}
}
// Package metrics exposes Prometheus instrumentation for the indexer.
package metrics

import (
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry       *prometheus.Registry
	ingestedLedger prometheus.Gauge
	ingestLag      prometheus.Gauge
	postAttempts   prometheus.Counter
	postSucceeded  prometheus.Counter
	postFailed     prometheus.Counter
	requests       *prometheus.CounterVec
	requestLatency *prometheus.HistogramVec
	lastPosts      *lastPostCollector
}

func New() *Metrics {
	m := &Metrics{
		registry: prometheus.NewRegistry(),
		ingestedLedger: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "plimsoll_ingested_ledger",
			Help: "Latest ledger reached by the event ingester.",
		}),
		ingestLag: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "plimsoll_ingest_lag_ledgers",
			Help: "Number of ledgers between the latest known ledger and the ingester position.",
		}),
		postAttempts: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "plimsoll_supply_posts_attempted_total",
			Help: "Supply post transactions attempted.",
		}),
		postSucceeded: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "plimsoll_supply_posts_succeeded_total",
			Help: "Supply post transactions that succeeded.",
		}),
		postFailed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "plimsoll_supply_posts_failed_total",
			Help: "Supply post transactions that failed.",
		}),
		requests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "plimsoll_http_requests_total",
			Help: "HTTP requests served by the indexer.",
		}, []string{"method", "path", "status"}),
		requestLatency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "plimsoll_http_request_duration_seconds",
			Help:    "HTTP request latency in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "path"}),
		lastPosts: newLastPostCollector(),
	}
	m.registry.MustRegister(
		m.ingestedLedger,
		m.ingestLag,
		m.postAttempts,
		m.postSucceeded,
		m.postFailed,
		m.requests,
		m.requestLatency,
		m.lastPosts,
	)
	return m
}

func (m *Metrics) SetIngestProgress(ingested, latest uint32) {
	m.ingestedLedger.Set(float64(ingested))
	lag := uint32(0)
	if latest > ingested {
		lag = latest - ingested
	}
	m.ingestLag.Set(float64(lag))
}

func (m *Metrics) SupplyPostAttempted() { m.postAttempts.Inc() }
func (m *Metrics) SupplyPostSucceeded() { m.postSucceeded.Inc() }
func (m *Metrics) SupplyPostFailed()    { m.postFailed.Inc() }

func (m *Metrics) RecordLastSupplyPost(assetSAC string, at time.Time) {
	if assetSAC != "" && !at.IsZero() {
		m.lastPosts.set(assetSAC, at)
	}
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(recorder, r)
		status := recorder.status
		if status == 0 {
			status = http.StatusOK
		}
		path := r.Pattern
		if path == "" {
			path = "unmatched"
		}
		m.requests.WithLabelValues(r.Method, path, strconv.Itoa(status)).Inc()
		m.requestLatency.WithLabelValues(r.Method, path).Observe(time.Since(start).Seconds())
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (w *statusRecorder) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusRecorder) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(body)
}

func (w *statusRecorder) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *statusRecorder) Unwrap() http.ResponseWriter { return w.ResponseWriter }

type lastPostCollector struct {
	desc *prometheus.Desc
	mu   sync.RWMutex
	last map[string]time.Time
}

func newLastPostCollector() *lastPostCollector {
	return &lastPostCollector{
		desc: prometheus.NewDesc(
			"plimsoll_supply_post_last_age_seconds",
			"Seconds since the last successful supply post for the asset.",
			[]string{"asset_sac"},
			nil,
		),
		last: make(map[string]time.Time),
	}
}

func (c *lastPostCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.desc
}

func (c *lastPostCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now := time.Now()
	for sac, postedAt := range c.last {
		age := now.Sub(postedAt).Seconds()
		if age < 0 {
			age = 0
		}
		ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue, age, sac)
	}
}

func (c *lastPostCollector) set(sac string, at time.Time) {
	c.mu.Lock()
	c.last[sac] = at
	c.mu.Unlock()
}
