// Package docs contains generated OpenAPI specifications and documentation.
package docs

import _ "embed"

// OpenAPISpec is the embedded OpenAPI 3.0 specification in YAML format.
//
//go:embed openapi.yaml
var OpenAPISpec []byte
