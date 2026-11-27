// Package config loads indexer settings from environment variables.
package config

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
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
	HorizonURL  string
	// HorizonCheckURL is a second, independent Horizon the poster compares the
	// supply against before signing it on chain. One Horizon is one point of
	// trust for the only figure this indexer asserts itself. Empty disables the
	// comparison.
	HorizonCheckURL    string
	NetworkPassphrase  string
	CoverageLedgerID   string
	ReporterRegistryID string
	StartLedger        uint32
	IngestWorkers      int
	PosterSecret       string
	// WebhookAdminToken protects webhook administration. It is shared by
	// issuer TOML change subscriptions and coverage alerts.
	WebhookAdminToken string
	WebhookAPIKeys    []string
	// WebhookEncryptionKey encrypts the per-subscription secrets of issuer
	// TOML change webhooks at rest; WebhookSigningSecret keys the HMAC-SHA256
	// signature on coverage alert deliveries. Each enables its own feature and
	// needs WebhookAdminToken.
	WebhookEncryptionKey string
	WebhookSigningSecret string
	WebhookInterval      time.Duration
	SlackWebhookURL      string
	DiscordWebhookURL    string
	IngestInterval       time.Duration
	PostInterval         time.Duration
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

	c := Config{
		Network:              network,
		DatabaseURL:          os.Getenv("DATABASE_URL"),
		Port:                 envOr("PORT", "8080"),
		RPCURL:               envOr("STELLAR_RPC_URL", preset.RPCURL),
		HorizonURL:           strings.TrimRight(envOr("HORIZON_URL", preset.HorizonURL), "/"),
		HorizonCheckURL:      strings.TrimRight(os.Getenv("HORIZON_CHECK_URL"), "/"),
		NetworkPassphrase:    envOr("STELLAR_NETWORK_PASSPHRASE", preset.Passphrase),
		CoverageLedgerID:     os.Getenv("COVERAGE_LEDGER_ID"),
		ReporterRegistryID:   os.Getenv("REPORTER_REGISTRY_ID"),
		PosterSecret:         os.Getenv("SUPPLY_POSTER_SECRET"),
		WebhookAdminToken:    os.Getenv("WEBHOOK_ADMIN_TOKEN"),
		WebhookEncryptionKey: os.Getenv("WEBHOOK_ENCRYPTION_KEY"),
		SlackWebhookURL:      os.Getenv("SLACK_WEBHOOK_URL"),
		DiscordWebhookURL:    os.Getenv("DISCORD_WEBHOOK_URL"),
		WebhookSigningSecret: os.Getenv("WEBHOOK_SIGNING_SECRET"),
		LogLevel:             envOr("LOG_LEVEL", "info"),
		IngestWorkers:        2,
		KeepAwakeURL:         keepAwakeURL(os.Getenv("KEEP_AWAKE_URL"), os.Getenv("RENDER_EXTERNAL_URL")),
	}
	if rawKeys := os.Getenv("WEBHOOK_API_KEYS"); rawKeys != "" {
		keys, err := parseWebhookAPIKeys(rawKeys, c.WebhookAdminToken)
		if err != nil {
			errs = append(errs, err)
		} else {
			c.WebhookAPIKeys = keys
		}
	}

	if c.RPCURL == "" {
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
	}
	for _, d := range durations {
		v, err := time.ParseDuration(envOr(d.name, d.def))
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", d.name, err))
			continue
		}
		*d.dst = v
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

// PostingEnabled reports whether this instance should post supply snapshots.
func (c Config) PostingEnabled() bool { return c.PosterSecret != "" }

func envOr(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}
