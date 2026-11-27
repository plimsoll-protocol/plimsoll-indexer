// Command openapi writes the generated OpenAPI document.
//
// It is run by `make generate` from the repository root. The output is
// committed and embedded into the binary, so a stale file is caught both by
// `make generate` plus git diff in CI and by TestGeneratedOpenAPIIsUpToDate.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/api"
)

func main() {
	out := flag.String("out", "internal/api/openapi.yaml", "file to write the OpenAPI document to")
	flag.Parse()

	//nolint:gosec // the generated spec is a committed source file, not a secret; 0644 matches the repository
	if err := os.WriteFile(*out, api.GeneratedOpenAPI(), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, "openapi:", err)
		os.Exit(1)
	}
}
