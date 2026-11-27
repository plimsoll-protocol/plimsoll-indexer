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
}
