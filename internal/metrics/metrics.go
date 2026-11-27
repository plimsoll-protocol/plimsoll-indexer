<<<<<<< HEAD
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
=======
// Package metrics publishes the indexer's gauges in the Prometheus text
// exposition format.
//
// It is deliberately small and dependency free: the indexer ships as one binary
// and the figures it publishes, such as the ledger each RPC endpoint reports,
// are gauges set once per job tick. Anything more would pull a client library
// into the binary for one endpoint.
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Registry holds the gauges one process publishes.
type Registry struct {
	mu       sync.RWMutex
	families map[string]*family
}

// family is one metric name with its help text and its samples.
type family struct {
	name    string
	help    string
	samples map[string]float64 // rendered label set -> value
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{families: map[string]*family{}}
}

// Gauge is one metric a caller sets. It is safe for concurrent use.
type Gauge struct {
	registry *Registry
	name     string
	labels   []string
}

// NewGauge registers a gauge. labelNames are the label keys every sample
// carries, in the order Set expects their values.
func (r *Registry) NewGauge(name, help string, labelNames ...string) *Gauge {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.families[name]; !ok {
		r.families[name] = &family{name: name, help: help, samples: map[string]float64{}}
	}
	return &Gauge{registry: r, name: name, labels: labelNames}
}

// Set records value for the sample with these label values.
func (g *Gauge) Set(value float64, labelValues ...string) {
	key := renderLabels(g.labels, labelValues)
	g.registry.mu.Lock()
	defer g.registry.mu.Unlock()
	if f := g.registry.families[g.name]; f != nil {
		f.samples[key] = value
	}
}

// WritePrometheus writes every registered family in the text exposition format,
// families and samples in name order, so two scrapes of the same state are
// byte-identical.
func (r *Registry) WritePrometheus(w io.Writer) error {
	r.mu.RLock()
	families := make([]*family, 0, len(r.families))
	for _, f := range r.families {
		samples := make(map[string]float64, len(f.samples))
		for key, value := range f.samples {
			samples[key] = value
		}
		families = append(families, &family{name: f.name, help: f.help, samples: samples})
	}
	r.mu.RUnlock()

	sort.Slice(families, func(i, j int) bool { return families[i].name < families[j].name })
	for _, f := range families {
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s gauge\n", f.name, f.help, f.name); err != nil {
			return err
		}
		keys := make([]string, 0, len(f.samples))
		for key := range f.samples {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if _, err := fmt.Fprintf(w, "%s%s %s\n", f.name, key, formatValue(f.samples[key])); err != nil {
				return err
			}
		}
	}
	return nil
}

// Handler serves the registry in the text exposition format.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_ = r.WritePrometheus(w)
	})
}

// renderLabels renders one sample's label set, escaping values as the exposition
// format requires.
func renderLabels(names, values []string) string {
	if len(names) == 0 {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, name := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(name)
		b.WriteString(`="`)
		if i < len(values) {
			b.WriteString(escapeLabelValue(values[i]))
		}
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

func escapeLabelValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	return strings.ReplaceAll(value, `"`, `\"`)
}

// formatValue prints whole numbers without an exponent, which is how ledger
// numbers and basis points should read in a scrape.
func formatValue(value float64) string {
	if value == math.Trunc(value) && math.Abs(value) < 1e15 {
		return strconv.FormatInt(int64(value), 10)
	}
	return strconv.FormatFloat(value, 'g', -1, 64)
>>>>>>> pr-215
}
