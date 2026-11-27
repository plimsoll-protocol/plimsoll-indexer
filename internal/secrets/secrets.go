// Package secrets resolves credentials from an optional secrets backend.
//
// A Provider returns secret values by name. Values are never logged: errors
// name the provider and the key only, and callers put the returned value
// straight into the field that uses it.
package secrets

import (
	"context"
	"fmt"
	"strings"
)

// Supported providers.
const (
	// Env reads the plain environment variables the indexer has always used.
	Env = "env"
	// SSM reads AWS Systems Manager Parameter Store parameters.
	SSM = "ssm"
	// GCP reads GCP Secret Manager secret versions.
	GCP = "gcp"
)

// Provider fetches secret values.
type Provider interface {
	// Get returns the value for key. key is a provider-specific identifier:
	// an environment variable name for env, a parameter name for SSM and a
	// secret name or resource for GCP.
	Get(ctx context.Context, key string) (string, error)
	// Name is the provider name used in logs and errors.
	Name() string
	// Close releases anything the provider holds.
	Close() error
}

// New returns the provider selected by name. An empty name selects Env, so an
// instance that does not configure a backend keeps reading environment
// variables.
func New(ctx context.Context, name string) (Provider, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", Env:
		return envProvider{}, nil
	case SSM:
		return newSSM(ctx)
	case GCP:
		return newGCP(ctx)
	default:
		return nil, fmt.Errorf("unknown secrets provider %q: want %s, %s or %s", name, Env, SSM, GCP)
	}
}
