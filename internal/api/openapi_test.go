package api

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestGeneratedOpenAPIIsUpToDate is the golden-file check: openapi.yaml is
// generated from the route table and openapi.components.yaml, and this fails
// when the committed file no longer matches. Regenerate it with `make generate`.
func TestGeneratedOpenAPIIsUpToDate(t *testing.T) {
	got, want := string(openAPISpec), string(GeneratedOpenAPI())
	if got == want {
		return
	}
	t.Fatalf("internal/api/openapi.yaml is out of date with the routes; run `make generate` (first difference at byte %d)", firstDifference(got, want))
}

func firstDifference(a, b string) int {
	limit := min(len(a), len(b))
	for i := 0; i < limit; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return limit
}

// The generated document must be YAML a client can parse, and must describe
// exactly the served routes.
func TestGeneratedOpenAPIHasEveryRoute(t *testing.T) {
	var doc struct {
		Paths map[string]map[string]any `yaml:"paths"`
	}
	if err := yaml.Unmarshal(GeneratedOpenAPI(), &doc); err != nil {
		t.Fatalf("generated OpenAPI is not valid YAML: %v", err)
	}
	seen := map[string]bool{}
	for _, r := range routes() {
		key := r.Method + " " + r.Pattern
		if seen[key] {
			t.Fatalf("route %s is registered twice", key)
		}
		seen[key] = true
		if strings.TrimSpace(r.Doc) == "" {
			t.Fatalf("route %s has no OpenAPI documentation", key)
		}
		if r.Handle == nil {
			t.Fatalf("route %s has no handler", key)
		}
		operations, ok := doc.Paths[r.Pattern]
		if !ok {
			t.Fatalf("generated spec has no path %s", r.Pattern)
		}
		if _, ok := operations[strings.ToLower(r.Method)]; !ok {
			t.Fatalf("generated spec has no %s %s operation", r.Method, r.Pattern)
		}
	}
}
