package secrets

import (
	"context"
	"errors"
	"strings"
	"testing"

	secretmanagerpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
	gax "github.com/googleapis/gax-go/v2"
)

func TestNewSelectsEnvByDefault(t *testing.T) {
	for _, name := range []string{"", "env", "ENV", " env "} {
		p, err := New(context.Background(), name)
		if err != nil {
			t.Fatalf("New(%q): %v", name, err)
		}
		if p.Name() != Env {
			t.Errorf("New(%q).Name() = %q, want %q", name, p.Name(), Env)
		}
		if err := p.Close(); err != nil {
			t.Errorf("New(%q).Close(): %v", name, err)
		}
	}
}

func TestNewRejectsUnknownProvider(t *testing.T) {
	_, err := New(context.Background(), "vault")
	if err == nil {
		t.Fatal("New(vault) = nil error, want error")
	}
	if !strings.Contains(err.Error(), "vault") || !strings.Contains(err.Error(), "env, ssm or gcp") {
		t.Errorf("error = %q, want it to name the value and the valid providers", err)
	}
}

func TestEnvProvider(t *testing.T) {
	t.Setenv("PLIMSOLL_TEST_SECRET", "s3cret-value")
	p := envProvider{}

	got, err := p.Get(context.Background(), "PLIMSOLL_TEST_SECRET")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "s3cret-value" {
		t.Errorf("Get = %q, want the environment value", got)
	}

	if _, err := p.Get(context.Background(), "PLIMSOLL_TEST_SECRET_UNSET"); err == nil {
		t.Fatal("Get on an unset variable = nil error, want error")
	} else if !strings.Contains(err.Error(), "PLIMSOLL_TEST_SECRET_UNSET") {
		t.Errorf("error = %q, want it to name the variable", err)
	}

	if _, err := p.Get(context.Background(), ""); err == nil {
		t.Fatal("Get with an empty name = nil error, want error")
	}
}

type fakeSSM struct {
	name    string
	decrypt bool
	value   string
	err     error
}

func (f *fakeSSM) GetParameter(_ context.Context, params *ssm.GetParameterInput, _ ...func(*ssm.Options)) (*ssm.GetParameterOutput, error) {
	f.name = aws.ToString(params.Name)
	f.decrypt = aws.ToBool(params.WithDecryption)
	if f.err != nil {
		return nil, f.err
	}
	return &ssm.GetParameterOutput{Parameter: &types.Parameter{Value: aws.String(f.value)}}, nil
}

func TestSSMProviderReadsSecureString(t *testing.T) {
	fake := &fakeSSM{value: "poster-secret-value"}
	p := &ssmProvider{client: fake}

	got, err := p.Get(context.Background(), "/plimsoll/testnet/poster")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "poster-secret-value" {
		t.Errorf("Get = %q, want the parameter value", got)
	}
	if fake.name != "/plimsoll/testnet/poster" {
		t.Errorf("parameter name = %q", fake.name)
	}
	if !fake.decrypt {
		t.Error("GetParameter was called without WithDecryption, so a SecureString would not decrypt")
	}
}

func TestSSMProviderErrors(t *testing.T) {
	p := &ssmProvider{client: &fakeSSM{err: errors.New("AccessDeniedException")}}
	if _, err := p.Get(context.Background(), "/plimsoll/poster"); err == nil {
		t.Fatal("Get on a failing client = nil error, want error")
	} else if !strings.Contains(err.Error(), "/plimsoll/poster") {
		t.Errorf("error = %q, want it to name the parameter", err)
	}

	p = &ssmProvider{client: &fakeSSM{value: ""}}
	if _, err := p.Get(context.Background(), "/plimsoll/poster"); err == nil {
		t.Fatal("Get on an empty parameter = nil error, want error")
	} else if !strings.Contains(err.Error(), "empty") {
		t.Errorf("error = %q, want it to report an empty parameter", err)
	}

	p = &ssmProvider{client: &fakeSSM{}}
	if _, err := p.Get(context.Background(), ""); err == nil {
		t.Fatal("Get with an empty name = nil error, want error")
	}
}

type fakeGCP struct {
	resource string
	value    []byte
	err      error
	closed   bool
}

func (f *fakeGCP) AccessSecretVersion(_ context.Context, req *secretmanagerpb.AccessSecretVersionRequest, _ ...gax.CallOption) (*secretmanagerpb.AccessSecretVersionResponse, error) {
	f.resource = req.GetName()
	if f.err != nil {
		return nil, f.err
	}
	return &secretmanagerpb.AccessSecretVersionResponse{
		Payload: &secretmanagerpb.SecretPayload{Data: f.value},
	}, nil
}

func (f *fakeGCP) Close() error {
	f.closed = true
	return nil
}

func TestGCPSecretVersion(t *testing.T) {
	cases := []struct {
		name, project, want, wantErr string
	}{
		{"poster-secret", "my-project", "projects/my-project/secrets/poster-secret/versions/latest", ""},
		{"projects/p/secrets/poster-secret", "", "projects/p/secrets/poster-secret/versions/latest", ""},
		{"projects/p/secrets/poster-secret/versions/7", "", "projects/p/secrets/poster-secret/versions/7", ""},
		{"poster-secret", "", "", "GOOGLE_CLOUD_PROJECT"},
		{"", "my-project", "", "empty"},
	}
	for _, c := range cases {
		got, err := gcpSecretVersion(c.name, c.project)
		if c.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Errorf("gcpSecretVersion(%q, %q) error = %v, want it to mention %q", c.name, c.project, err, c.wantErr)
			}
			continue
		}
		if err != nil {
			t.Errorf("gcpSecretVersion(%q, %q): %v", c.name, c.project, err)
			continue
		}
		if got != c.want {
			t.Errorf("gcpSecretVersion(%q, %q) = %q, want %q", c.name, c.project, got, c.want)
		}
	}
}

func TestGCPProviderReadsLatestVersion(t *testing.T) {
	fake := &fakeGCP{value: []byte("poster-secret-value")}
	p := &gcpProvider{client: fake, project: "my-project"}

	got, err := p.Get(context.Background(), "poster-secret")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != "poster-secret-value" {
		t.Errorf("Get = %q, want the secret payload", got)
	}
	if fake.resource != "projects/my-project/secrets/poster-secret/versions/latest" {
		t.Errorf("resource = %q", fake.resource)
	}
	if err := p.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
	if !fake.closed {
		t.Error("Close did not close the client")
	}
}

func TestGCPProviderErrors(t *testing.T) {
	p := &gcpProvider{client: &fakeGCP{err: errors.New("PermissionDenied")}, project: "p"}
	if _, err := p.Get(context.Background(), "poster"); err == nil {
		t.Fatal("Get on a failing client = nil error, want error")
	} else if !strings.Contains(err.Error(), "projects/p/secrets/poster/versions/latest") {
		t.Errorf("error = %q, want it to name the resource", err)
	}

	p = &gcpProvider{client: &fakeGCP{value: nil}, project: "p"}
	if _, err := p.Get(context.Background(), "poster"); err == nil {
		t.Fatal("Get on an empty secret = nil error, want error")
	} else if !strings.Contains(err.Error(), "empty") {
		t.Errorf("error = %q, want it to report an empty secret", err)
	}

	p = &gcpProvider{client: &fakeGCP{}, project: ""}
	if _, err := p.Get(context.Background(), "poster"); err == nil {
		t.Fatal("Get with a bare name and no project = nil error, want error")
	}
}
