package secrets

import (
	"context"
	"fmt"
	"os"
)

// envProvider reads a secret from the environment variable named by key.
type envProvider struct{}

func (envProvider) Name() string { return Env }
func (envProvider) Close() error { return nil }

func (envProvider) Get(_ context.Context, key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("secret name is empty")
	}
	v := os.Getenv(key)
	if v == "" {
		return "", fmt.Errorf("environment variable %q is not set", key)
	}
	return v, nil
}
