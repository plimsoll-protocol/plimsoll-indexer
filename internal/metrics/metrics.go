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
