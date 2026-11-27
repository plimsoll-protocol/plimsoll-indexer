// Package config loads indexer settings from environment variables.
package config

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/secrets"
	"unicode"
	"github.com/stellar/go-stellar-sdk/strkey"
)

const (
	TestnetRPC        = "https://soroban-testnet.stellar.org"
	TestnetHorizon    = "https://horizon-testnet.stellar.org"
	TestnetPassphrase = "Test SDF Network ; September 2015"

	MainnetHorizon    = "https://horizon.stellar.org"
	MainnetPassphrase = "Public Global Stellar Network ; September 2015"

	StandaloneRPC        = "http://localhost:8000/rpc"
	StandaloneHorizon    = "http://localhost:8000"
	StandalonePassphrase = "Standalone Network ; February 2017"
)

// Preset is a named set of network defaults. Each field is only a default:
// STELLAR_RPC_URL, HORIZON_URL and STELLAR_NETWORK_PASSPHRASE override it.
type Preset struct {
	RPCURL     string
	HorizonURL string
	Passphrase string
}

// Presets are selected with the NETWORK variable. Mainnet has no RPC default
// because SDF does not run a public Soroban RPC for it: STELLAR_RPC_URL must
// be set to a provider's endpoint.
var Presets = map[string]Preset{
	"testnet":    {RPCURL: TestnetRPC, HorizonURL: TestnetHorizon, Passphrase: TestnetPassphrase},
	"mainnet":    {RPCURL: "", HorizonURL: MainnetHorizon, Passphrase: MainnetPassphrase},
	"standalone": {RPCURL: StandaloneRPC, HorizonURL: StandaloneHorizon, Passphrase: StandalonePassphrase},
}

type Config struct {
	// Network is the preset name the defaults below came from.
	Network     string
	DatabaseURL string
	Port        string
	RPCURL      string
	RPCURLs     []string
	HorizonURL  string
	// HorizonCheckURL is a second, independent Horizon the poster compares the
	// supply against before signing it on chain. One Horizon is one point of
	// trust for the only figure this indexer asserts itself. Empty disables the
	// comparison.
	HorizonCheckURL      string
	NetworkPassphrase    string
	CoverageLedgerID     string
	ReporterRegistryID   string
	StartLedger          uint32
	IngestWorkers        int
	PosterSecret         string
	WebhookAdminToken    string
	WebhookAPIKeys       []string
	WebhookEncryptionKey string
	SlackWebhookURL      string
	DiscordWebhookURL    string
	WebhookSigningSecret string
	// SlackWebhookURL and DiscordWebhookURL receive coverage drop alerts.
	SlackWebhookURL   string
	DiscordWebhookURL string
	WebhookInterval   time.Duration
	IngestInterval    time.Duration
	PostInterval      time.Duration
	// ReconcileInterval is how often contract state is read back to repair
	// what event ingestion missed. Each pass is a handful of simulations per
	// asset, so it can be infrequent.
	ReconcileInterval time.Duration
	RepostAfter       time.Duration
	TomlInterval      time.Duration
	CORSOrigins       []string
	LogLevel          string
	// KeepAwakeURL is requested every KeepAwakeInterval so a host that sleeps
	// idle services (Render's free tier) keeps this one, and its poster, up.
	KeepAwakeURL      string
	KeepAwakeInterval time.Duration

	// ReporterInterval is how often the registry contract is polled on its own
	// cursor. It is deliberately shorter than IngestInterval so a reporter
	// registration reaches /v1/reporters within about one ledger (roughly 5s)
	// instead of waiting for the next reconcile pass.
	ReporterInterval time.Duration

	// HTTPKeepAlive is how long an idle connection to Horizon or Soroban RPC
	// is kept open for reuse. Both are called several times a tick, so
	// connection reuse avoids a TLS handshake per call.
	HTTPKeepAlive time.Duration

	// MaxFeeStroops is the absolute ceiling the invoker will sign per
	// transaction. Zero means no ceiling.
	MaxFeeStroops int64
	// InclusionFee is the per-operation base fee bid in stroops. Zero means
	// MinBaseFee (100 stroops).
	InclusionFee int64
	// ResourceFeeMarginPct is the percentage added to the simulated resource
	// fee as headroom. Zero means no margin.
	ResourceFeeMarginPct int64
	// PosterLowBalanceStroops is the XLM balance (in stroops) below which the
	// poster logs a warning on every tick. Zero disables the check.
	PosterLowBalanceStroops int64
	// DecimalCheckEnabled warns when a new supply snapshot differs from the
	// previous on-chain one by more than six orders of magnitude, which is the
	// shape of a decimal-point mistake rather than a real supply change.
	DecimalCheckEnabled bool
	// ReporterStaleThreshold is how old a reporter's latest report may be
	// before ingestion logs a warning. Zero disables the check.
	ReporterStaleThreshold time.Duration
}

// Load reads the environment. Missing required values are reported together.
// A local .env.local overlay is applied first when present, unless
// GO_ENV=production.
func Load() (Config, error) {
	var errs []error
	if err := loadEnvLocal(); err != nil {
		errs = append(errs, err)
	}
	network := strings.ToLower(envOr("NETWORK", "testnet"))
	preset, ok := Presets[network]
	if !ok {
		errs = append(errs, fmt.Errorf("NETWORK %q is not one of testnet, mainnet, standalone", network))
		preset = Presets["testnet"]
	}

	posterSecret, err := loadPosterSecret()
	if err != nil {
		errs = append(errs, err)
	var rpcURLs []string
	if raw := envOr("RPC_URLS", os.Getenv("STELLAR_RPC_URLS")); raw != "" {
		for _, u := range strings.Split(raw, ",") {
			if u = strings.TrimSpace(u); u != "" {
				rpcURLs = append(rpcURLs, u)
			}
		}
	}
	if len(rpcURLs) == 0 {
		single := envOr("STELLAR_RPC_URL", envOr("RPC_URL", preset.RPCURL))
		if single != "" {
			rpcURLs = []string{single}
		}
	}

	primaryRPC := ""
	if len(rpcURLs) > 0 {
		primaryRPC = rpcURLs[0]
	}

	c := Config{
		Network:              network,
		DatabaseURL:          os.Getenv("DATABASE_URL"),
		Port:                 envOr("PORT", "8080"),
		RPCURL:               primaryRPC,
		RPCURLs:              rpcURLs,
		HorizonURL:           strings.TrimRight(envOr("HORIZON_URL", preset.HorizonURL), "/"),
		HorizonCheckURL:      strings.TrimRight(os.Getenv("HORIZON_CHECK_URL"), "/"),
		NetworkPassphrase:    envOr("STELLAR_NETWORK_PASSPHRASE", preset.Passphrase),
		CoverageLedgerID:     os.Getenv("COVERAGE_LEDGER_ID"),
		ReporterRegistryID:   os.Getenv("REPORTER_REGISTRY_ID"),
		SecretsProvider:      strings.ToLower(strings.TrimSpace(envOr("SECRETS_PROVIDER", secrets.Env))),
		PosterSecretName:     strings.TrimSpace(os.Getenv("SUPPLY_POSTER_SECRET_NAME")),
		DatabasePasswordName: strings.TrimSpace(os.Getenv("DATABASE_PASSWORD_SECRET_NAME")),
		PosterSecret:         os.Getenv("SUPPLY_POSTER_SECRET"),
		WebhookAdminToken:    os.Getenv("WEBHOOK_ADMIN_TOKEN"),
		WebhookEncryptionKey: os.Getenv("WEBHOOK_ENCRYPTION_KEY"),
		SlackWebhookURL:      os.Getenv("SLACK_WEBHOOK_URL"),
		DiscordWebhookURL:    os.Getenv("DISCORD_WEBHOOK_URL"),
		WebhookSigningSecret: os.Getenv("WEBHOOK_SIGNING_SECRET"),
		LogLevel:             envOr("LOG_LEVEL", "info"),
		IngestWorkers:        2,
		KeepAwakeURL:         keepAwakeURL(os.Getenv("KEEP_AWAKE_URL"), os.Getenv("RENDER_EXTERNAL_URL")),
		DecimalCheckEnabled:  true,
	}
	if v := os.Getenv("DECIMAL_CHECK_ENABLED"); v != "" {
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			errs = append(errs, fmt.Errorf("DECIMAL_CHECK_ENABLED: %w", err))
		} else {
			c.DecimalCheckEnabled = enabled
		}
	}
	if rawKeys := os.Getenv("WEBHOOK_API_KEYS"); rawKeys != "" {
		keys, err := parseWebhookAPIKeys(rawKeys, c.WebhookAdminToken)
		if err != nil {
			errs = append(errs, err)
		} else {
			c.WebhookAPIKeys = keys
		}
	}

	switch c.SecretsProvider {
	case secrets.Env, secrets.SSM, secrets.GCP:
	default:
		errs = append(errs, fmt.Errorf("SECRETS_PROVIDER %q is not one of env, ssm, gcp", c.SecretsProvider))
		Network:            network,
		DatabaseURL:        os.Getenv("DATABASE_URL"),
		Port:               envOr("PORT", "8080"),
		RPCURL:             envOr("STELLAR_RPC_URL", preset.RPCURL),
		HorizonURL:         strings.TrimRight(envOr("HORIZON_URL", preset.HorizonURL), "/"),
		HorizonCheckURL:    strings.TrimRight(os.Getenv("HORIZON_CHECK_URL"), "/"),
		NetworkPassphrase:  envOr("STELLAR_NETWORK_PASSPHRASE", preset.Passphrase),
		CoverageLedgerID:   os.Getenv("COVERAGE_LEDGER_ID"),
		ReporterRegistryID: os.Getenv("REPORTER_REGISTRY_ID"),
		PosterSecret:       posterSecret,
		LogLevel:           envOr("LOG_LEVEL", "info"),
		KeepAwakeURL:       keepAwakeURL(os.Getenv("KEEP_AWAKE_URL"), os.Getenv("RENDER_EXTERNAL_URL")),
	}

	if len(c.RPCURLs) == 0 || c.RPCURL == "" {
		errs = append(errs, fmt.Errorf("STELLAR_RPC_URL is required for NETWORK=%s", network))
	}
	if c.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}
	if c.CoverageLedgerID == "" {
		errs = append(errs, errors.New("COVERAGE_LEDGER_ID is required"))
	}
	if c.ReporterRegistryID == "" {
		errs = append(errs, errors.New("REPORTER_REGISTRY_ID is required"))
	}
	// A key without the admin token could never be used, and a token with no
	// key registers nothing, so refuse both half-configurations.
	if c.WebhookEncryptionKey != "" && c.WebhookAdminToken == "" {
		errs = append(errs, errors.New("WEBHOOK_ADMIN_TOKEN and WEBHOOK_ENCRYPTION_KEY must be configured together"))
	}
	if len(c.WebhookAPIKeys) > 0 && c.WebhookAdminToken == "" {
		errs = append(errs, errors.New("WEBHOOK_API_KEYS requires WEBHOOK_ADMIN_TOKEN and WEBHOOK_ENCRYPTION_KEY"))
	}
	if c.WebhookSigningSecret != "" && c.WebhookAdminToken == "" {
		errs = append(errs, errors.New("WEBHOOK_ADMIN_TOKEN and WEBHOOK_SIGNING_SECRET must be configured together"))
	}
	if c.WebhookAdminToken != "" && c.WebhookEncryptionKey == "" && c.WebhookSigningSecret == "" {
		errs = append(errs, errors.New("WEBHOOK_ADMIN_TOKEN is set but neither WEBHOOK_ENCRYPTION_KEY nor WEBHOOK_SIGNING_SECRET is configured"))
	}
	if c.WebhookEncryptionKey != "" {
		key, err := base64.StdEncoding.DecodeString(c.WebhookEncryptionKey)
		if err != nil || len(key) != 32 {
			errs = append(errs, errors.New("WEBHOOK_ENCRYPTION_KEY must be base64-encoded 32-byte key"))
		}
	}

	start, err := strconv.ParseUint(envOr("START_LEDGER", "0"), 10, 32)
	if err != nil {
		errs = append(errs, fmt.Errorf("START_LEDGER: %w", err))
	}
	c.StartLedger = uint32(start)

	workers, err := strconv.Atoi(envOr("INGEST_WORKERS", "2"))
	if err != nil || workers < 1 {
		errs = append(errs, errors.New("INGEST_WORKERS must be a positive integer"))
	} else {
		c.IngestWorkers = workers
	}

	durations := []struct {
		name string
		def  string
		dst  *time.Duration
	}{
		{"INGEST_INTERVAL", "10s", &c.IngestInterval},
		{"POST_INTERVAL", "15m", &c.PostInterval},
		{"RECONCILE_INTERVAL", "15m", &c.ReconcileInterval},
		{"REPOST_AFTER", "6h", &c.RepostAfter},
		{"TOML_INTERVAL", "6h", &c.TomlInterval},
		{"WEBHOOK_INTERVAL", "1m", &c.WebhookInterval},
		{"KEEP_AWAKE_INTERVAL", "10m", &c.KeepAwakeInterval},
		{"REPORTER_INTERVAL", "5s", &c.ReporterInterval},
		{"HTTP_KEEPALIVE_TIMEOUT", "90s", &c.HTTPKeepAlive},
	}
	// minInterval is the floor for intervals whose tick hits an external
	// service (RPC, Horizon, or an HTTP endpoint). Zero or negative intervals
	// would panic in time.NewTicker, so every interval must be positive.
	const minInterval = time.Second

	externalIntervals := map[string]bool{
		"INGEST_INTERVAL":     true,
		"POST_INTERVAL":       true,
		"RECONCILE_INTERVAL":  true,
		"TOML_INTERVAL":       true,
		"KEEP_AWAKE_INTERVAL": true,
		{"REPORTER_STALE_THRESHOLD", "720h", &c.ReporterStaleThreshold},
	}
	for _, d := range durations {
		v, err := time.ParseDuration(envOr(d.name, d.def))
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", d.name, err))
			continue
		}
		if v <= 0 {
			errs = append(errs, fmt.Errorf("%s must be positive, got %s", d.name, v))
			continue
		}
		if externalIntervals[d.name] && v < minInterval {
			errs = append(errs, fmt.Errorf("%s must be at least %s, got %s", d.name, minInterval, v))
			continue
		}
		*d.dst = v
	}
	if c.ReporterStaleThreshold < 0 {
		errs = append(errs, errors.New("REPORTER_STALE_THRESHOLD must not be negative"))
	}

	if c.CoverageLedgerID != "" && !strkey.IsValidContractAddress(c.CoverageLedgerID) {
		errs = append(errs, fmt.Errorf("COVERAGE_LEDGER_ID %q is not a contract address (must start with C)", c.CoverageLedgerID))
	}
	if c.ReporterRegistryID != "" && !strkey.IsValidContractAddress(c.ReporterRegistryID) {
		errs = append(errs, fmt.Errorf("REPORTER_REGISTRY_ID %q is not a contract address (must start with C)", c.ReporterRegistryID))
	}

	for _, u := range []struct {
		name, val string
	}{
		{"STELLAR_RPC_URL", c.RPCURL},
		{"HORIZON_URL", c.HorizonURL},
		{"HORIZON_CHECK_URL", c.HorizonCheckURL},
	} {
		if u.val == "" {
			continue
		}
		parsed, err := url.ParseRequestURI(u.val)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			errs = append(errs, fmt.Errorf("%s must be an absolute http(s) URL, got %q", u.name, u.val))
		}
	}

	for _, o := range strings.Split(envOr("CORS_ORIGINS", "*"), ",") {
		if o = strings.TrimSpace(o); o != "" {
			c.CORSOrigins = append(c.CORSOrigins, o)
		}
	}

	int64s := []struct {
		name string
		dst  *int64
	}{
		{"MAX_FEE_STROOPS", &c.MaxFeeStroops},
		{"INCLUSION_FEE", &c.InclusionFee},
		{"RESOURCE_FEE_MARGIN_PCT", &c.ResourceFeeMarginPct},
		{"POSTER_LOW_BALANCE_STROOPS", &c.PosterLowBalanceStroops},
	}
	for _, f := range int64s {
		if v := os.Getenv(f.name); v != "" {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", f.name, err))
				continue
			}
			*f.dst = n
		}
	}

	return c, errors.Join(errs...)
}

func parseWebhookAPIKeys(raw, adminToken string) ([]string, error) {
	seen := map[string]struct{}{adminToken: {}}
	var keys []string
	for _, key := range strings.Split(raw, ",") {
		key = strings.TrimSpace(key)
		if key == "" {
			return nil, errors.New("WEBHOOK_API_KEYS must contain unique, non-empty comma-separated keys")
		}
		if _, ok := seen[key]; ok {
			return nil, errors.New("WEBHOOK_API_KEYS must contain unique, non-empty comma-separated keys")
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return keys, nil
// loadPosterSecret returns the poster secret from SUPPLY_POSTER_SECRET or,
// preferably, from the file named by SUPPLY_POSTER_SECRET_FILE. Setting both
// is an error. Trailing whitespace is trimmed because secret files written by
// Docker, Kubernetes or echo usually end in a newline.
func loadPosterSecret() (string, error) {
	env := os.Getenv("SUPPLY_POSTER_SECRET")
	path := os.Getenv("SUPPLY_POSTER_SECRET_FILE")
	if env != "" && path != "" {
		return "", errors.New("SUPPLY_POSTER_SECRET and SUPPLY_POSTER_SECRET_FILE are both set; set only one")
	}
	if path == "" {
		return env, nil
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("SUPPLY_POSTER_SECRET_FILE: %w", err)
	}
	return strings.TrimRightFunc(string(b), unicode.IsSpace), nil
}

// keepAwakeURL picks the URL to ping: KEEP_AWAKE_URL if set ("none" turns
// it off), otherwise Render's RENDER_EXTERNAL_URL plus /healthz, otherwise
// nothing.
func keepAwakeURL(explicit, renderExternal string) string {
	switch {
	case explicit == "none":
		return ""
	case explicit != "":
		return explicit
	case renderExternal != "":
		return strings.TrimRight(renderExternal, "/") + "/healthz"
	default:
		return ""
	}
}

// ResolveSecrets fills PosterSecret and DatabasePassword from p. A credential
// with no *_SECRET_NAME keeps the value from the environment, so either one can
// stay in env vars while the other comes from the backend.
func (c *Config) ResolveSecrets(ctx context.Context, p secrets.Provider) error {
	if p == nil {
		return errors.New("resolve secrets: nil provider")
	}
	var errs []error
	if c.PosterSecretName != "" {
		v, err := p.Get(ctx, c.PosterSecretName)
		if err != nil {
			errs = append(errs, fmt.Errorf("SUPPLY_POSTER_SECRET_NAME %q: %w", c.PosterSecretName, err))
		} else {
			c.PosterSecret = v
		}
	}
	if c.DatabasePasswordName != "" {
		v, err := p.Get(ctx, c.DatabasePasswordName)
		if err != nil {
			errs = append(errs, fmt.Errorf("DATABASE_PASSWORD_SECRET_NAME %q: %w", c.DatabasePasswordName, err))
		} else {
			c.DatabasePassword = v
		}
	}
	return errors.Join(errs...)
}

// LogValue implements slog.LogValuer so that logging a Config records which
// credentials were supplied, never their values.
func (c Config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("network", c.Network),
		slog.String("secrets_provider", c.SecretsProvider),
		slog.Bool("poster_secret_set", c.PosterSecret != ""),
		slog.Bool("database_password_set", c.DatabasePassword != ""),
	)
}

// PostingEnabled reports whether this instance should post supply snapshots.
func (c Config) PostingEnabled() bool { return c.PosterSecret != "" }

// String implements fmt.Stringer so formatting a Config never reveals the
// poster secret.
func (c Config) String() string { return fmt.Sprintf("%+v", configWithoutSecret(c.redact())) }

// Format implements fmt.Formatter so every fmt verb, including %#v, redacts
// the poster secret instead of bypassing String.
func (c Config) Format(s fmt.State, verb rune) {
	_, _ = fmt.Fprintf(s, fmt.FormatString(s, verb), configWithoutSecret(c.redact()))
}

// configWithoutSecret is a distinct type so fmt does not call Config's own
// String and Format methods on the redacted copy.
type configWithoutSecret Config

// redact replaces a non-empty secret with a placeholder.
func (c Config) redact() Config {
	if c.PosterSecret != "" {
		c.PosterSecret = "[REDACTED]"
	}
	return c
}

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}
