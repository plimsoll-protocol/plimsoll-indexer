// Package changelog serves the hand-maintained list of breaking HTTP API
// changes from CHANGELOG.yaml.
package changelog

import (
	_ "embed"
	"fmt"
	"regexp"
	"time"

	"gopkg.in/yaml.v3"
)

//go:embed CHANGELOG.yaml
var raw []byte

// Change is one breaking change in a release.
type Change struct {
	// Kind names the sort of change: removed_field, renamed_endpoint,
	// new_required_param, and so on.
	Kind string `yaml:"kind" json:"kind"`
	// Detail tells a client what to change.
	Detail string `yaml:"detail" json:"detail"`
}

// Entry is one released version and the breaking changes it carried.
type Entry struct {
	Version string `yaml:"version" json:"version"`
	// Date is the release date, YYYY-MM-DD.
	Date    string   `yaml:"date" json:"date"`
	Changes []Change `yaml:"changes" json:"changes"`
}

var versionPattern = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+$`)

// Load parses the embedded changelog. It fails on a malformed file rather than
// serving a changelog clients cannot rely on.
func Load() ([]Entry, error) {
	return Parse(raw)
}

// Parse parses and validates a changelog document.
func Parse(data []byte) ([]Entry, error) {
	var entries []Entry
	if err := yaml.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("parse changelog: %w", err)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("parse changelog: no versions")
	}
	seen := make(map[string]bool, len(entries))
	var previous time.Time
	for i, e := range entries {
		if !versionPattern.MatchString(e.Version) {
			return nil, fmt.Errorf("parse changelog: entry %d: version %q is not MAJOR.MINOR.PATCH", i, e.Version)
		}
		if seen[e.Version] {
			return nil, fmt.Errorf("parse changelog: version %s appears twice", e.Version)
		}
		seen[e.Version] = true
		date, err := time.Parse(time.DateOnly, e.Date)
		if err != nil {
			return nil, fmt.Errorf("parse changelog: version %s: date must be YYYY-MM-DD: %w", e.Version, err)
		}
		// Newest first: clients read the top entry as the current release.
		if i > 0 && !date.Before(previous) {
			return nil, fmt.Errorf("parse changelog: version %s is not older than the version above it", e.Version)
		}
		previous = date
		for j, c := range e.Changes {
			if c.Kind == "" || c.Detail == "" {
				return nil, fmt.Errorf("parse changelog: version %s: change %d needs a kind and a detail", e.Version, j)
			}
		}
	}
	return entries, nil
}
