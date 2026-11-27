package api

import (
	_ "embed"
	"fmt"
	"strings"
)

//go:embed openapi.yaml
var openAPISpec []byte

// openAPIComponents is the part of the document that is written by hand: the
// reusable schemas, parameters, responses and the auth scheme. Everything that
// describes an endpoint comes from routes() instead.
//
//go:embed openapi.components.yaml
var openAPIComponents []byte

// openAPIHeader is the part of the document that depends on neither routes nor
// schemas.
const openAPIHeader = `openapi: 3.1.0
info:
  title: Plimsoll Indexer API
  description: OpenAPI 3.1.0 specification for Plimsoll read API over HTTP.
  version: 1.0.0
  license:
    name: Apache 2.0
    identifier: Apache-2.0
servers:
  - url: https://plimsoll-indexer.onrender.com
    description: Hosted testnet API
  - url: http://localhost:8080
    description: Local development API
security: []
`

// GeneratedOpenAPI builds the OpenAPI document: a fixed header, one path per
// route in routes(), and the hand-maintained components.
//
// internal/api/openapi.yaml is this function's output. Run `make generate`
// after changing a route or a component; never edit the generated file by hand.
// TestGeneratedOpenAPIIsUpToDate fails when the committed file is stale.
func GeneratedOpenAPI() []byte {
	var b strings.Builder
	b.WriteString(openAPIHeader)
	b.WriteString("paths:\n")
	// One path entry per pattern: methods served on the same pattern are
	// operations of the same YAML mapping key.
	var patterns []string
	byPattern := map[string][]route{}
	for _, r := range routes() {
		if _, seen := byPattern[r.Pattern]; !seen {
			patterns = append(patterns, r.Pattern)
		}
		byPattern[r.Pattern] = append(byPattern[r.Pattern], r)
	}
	for _, pattern := range patterns {
		fmt.Fprintf(&b, "  %s:\n", pattern)
		for _, r := range byPattern[pattern] {
			fmt.Fprintf(&b, "    %s:\n", strings.ToLower(r.Method))
			writeIndented(&b, r.Doc, 6)
		}
	}
	b.WriteString("components:\n")
	writeIndented(&b, string(openAPIComponents), 2)
	return []byte(b.String())
}

// writeIndented copies YAML text to b with every non-empty line indented by
// spaces, so the caller can nest it.
func writeIndented(b *strings.Builder, yaml string, spaces int) {
	pad := strings.Repeat(" ", spaces)
	for _, line := range strings.Split(strings.Trim(yaml, "\n"), "\n") {
		if line == "" {
			b.WriteString("\n")
			continue
		}
		b.WriteString(pad)
		b.WriteString(line)
		b.WriteString("\n")
	}
}
