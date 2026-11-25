package secrets

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
)

// ssmAPI is the part of the SSM client this package uses. It is an interface so
// the provider can be tested without AWS.
type ssmAPI interface {
	GetParameter(ctx context.Context, params *ssm.GetParameterInput, optFns ...func(*ssm.Options)) (*ssm.GetParameterOutput, error)
}

// ssmProvider reads parameters from AWS Systems Manager Parameter Store. The
// AWS SDK resolves credentials and the region from the usual environment,
// shared config or instance role; AWS_REGION is required when none of those
// provides one.
type ssmProvider struct {
	client ssmAPI
}

func newSSM(ctx context.Context) (Provider, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("load AWS configuration: %w", err)
	}
	if cfg.Region == "" {
		return nil, fmt.Errorf("AWS region is not configured: set AWS_REGION (or AWS_DEFAULT_REGION)")
	}
	return &ssmProvider{client: ssm.NewFromConfig(cfg)}, nil
}

func (p *ssmProvider) Name() string { return SSM }
func (p *ssmProvider) Close() error { return nil }

func (p *ssmProvider) Get(ctx context.Context, key string) (string, error) {
	if key == "" {
		return "", fmt.Errorf("SSM parameter name is empty")
	}
	out, err := p.client.GetParameter(ctx, &ssm.GetParameterInput{
		Name:           aws.String(key),
		WithDecryption: aws.Bool(true), // SecureString parameters hold the value.
	})
	if err != nil {
		return "", fmt.Errorf("read SSM parameter %q: %w", key, err)
	}
	if out.Parameter == nil || out.Parameter.Value == nil || *out.Parameter.Value == "" {
		return "", fmt.Errorf("SSM parameter %q is empty", key)
	}
	return *out.Parameter.Value, nil
}
