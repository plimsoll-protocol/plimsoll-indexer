package secrets

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	gax "github.com/googleapis/gax-go/v2"
)

// gcpAPI is the part of the Secret Manager client this package uses. It is an
// interface so the provider can be tested without GCP.
type gcpAPI interface {
	AccessSecretVersion(ctx context.Context, req *secretmanagerpb.AccessSecretVersionRequest, opts ...gax.CallOption) (*secretmanagerpb.AccessSecretVersionResponse, error)
	Close() error
}

// gcpProvider reads the latest version of a secret from GCP Secret Manager,
// authenticating with application default credentials.
type gcpProvider struct {
	client  gcpAPI
	project string
}

func newGCP(ctx context.Context) (Provider, error) {
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return nil, fmt.Errorf("create GCP Secret Manager client: %w", err)
	}
	return &gcpProvider{client: client, project: gcpProject()}, nil
}

// gcpProject is the project bare secret names resolve against.
func gcpProject() string {
	for _, k := range []string{"GOOGLE_CLOUD_PROJECT", "GCLOUD_PROJECT"} {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

func (p *gcpProvider) Name() string { return GCP }
func (p *gcpProvider) Close() error { return p.client.Close() }

func (p *gcpProvider) Get(ctx context.Context, key string) (string, error) {
	resource, err := gcpSecretVersion(key, p.project)
	if err != nil {
		return "", err
	}
	resp, err := p.client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: resource})
	if err != nil {
		return "", fmt.Errorf("access GCP secret %q: %w", resource, err)
	}
	data := resp.GetPayload().GetData()
	if len(data) == 0 {
		return "", fmt.Errorf("GCP secret %q is empty", resource)
	}
	return string(data), nil
}

// gcpSecretVersion turns a configured secret name into a Secret Manager
// resource. It accepts a full resource with a version, a resource without one,
// or a bare secret id resolved against project.
func gcpSecretVersion(name, project string) (string, error) {
	name = strings.TrimSpace(name)
	switch {
	case name == "":
		return "", errors.New("GCP secret name is empty")
	case strings.Contains(name, "/versions/"):
		return name, nil
	case strings.HasPrefix(name, "projects/"):
		return name + "/versions/latest", nil
	case project == "":
		return "", fmt.Errorf("GOOGLE_CLOUD_PROJECT is required when the GCP secret name %q is not a projects/... resource", name)
	default:
		return fmt.Sprintf("projects/%s/secrets/%s/versions/latest", project, name), nil
	}
}
