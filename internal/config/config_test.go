package config

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/secrets"
)

func TestEnvExampleDocumentsLoadVariables(t *testing.T) {
	configSource, err := os.ReadFile("config.go")
	if err != nil {
		t.Fatalf("read config.go: %v", err)
	}
	envExample, err := os.ReadFile("../../.env.example")
	if err != nil {
		t.Fatalf("read .env.example: %v", err)
	}

	codeNames := regexp.MustCompile(`"([A-Z][A-Z0-9_]*)"`)
	documentedNames := regexp.MustCompile(`(?m)^([A-Z][A-Z0-9_]*)=`)

	documented := make(map[string]bool)
	for _, match := range documentedNames.FindAllSubmatch(envExample, -1) {
		documented[string(match[1])] = true
	}

	var missing []string
	seen := make(map[string]bool)
	for _, match := range codeNames.FindAllSubmatch(configSource, -1) {
		name := string(match[1])
		if !seen[name] && !documented[name] {
			missing = append(missing, name)
		}
		seen[name] = true
	}
	sort.Strings(missing)

	if len(missing) > 0 {
		t.Errorf("variables read by config.Load are missing from .env.example: %s", strings.Join(missing, ", "))
	}
}

func TestKeepAwakeURL(t *testing.T) {
	cases := []struct {
		explicit, render, want string
	}{
		{"", "", ""},
		{"", "https://plimsoll-indexer.onrender.com", "https://plimsoll-indexer.onrender.com/healthz"},
		{"", "https://x.onrender.com/", "https://x.onrender.com/healthz"},
		{"https://example.org/ping", "https://x.onrender.com", "https://example.org/ping"},
		{"none", "https://x.onrender.com", ""},
	}
	for _, c := range cases {
		if got := keepAwakeURL(c.explicit, c.render); got != c.want {
			t.Errorf("keepAwakeURL(%q, %q) = %q, want %q", c.explicit, c.render, got, c.want)
		}
	}
}

func setBase(t *testing.T) {
	t.Helper()
	for _, k := range []string{"NETWORK", "STELLAR_RPC_URL", "HORIZON_URL", "STELLAR_NETWORK_PASSPHRASE", "WEBHOOK_ADMIN_TOKEN", "WEBHOOK_API_KEYS", "WEBHOOK_ENCRYPTION_KEY", "WEBHOOK_SIGNING_SECRET", "SECRETS_PROVIDER", "SUPPLY_POSTER_SECRET", "SUPPLY_POSTER_SECRET_NAME", "DATABASE_PASSWORD_SECRET_NAME"} {
		t.Setenv(k, "")
	}
	t.Setenv("DATABASE_URL", "postgres://x")
	t.Setenv("COVERAGE_LEDGER_ID", "C1")
	t.Setenv("REPORTER_REGISTRY_ID", "C2")
}

func TestWebhookConfigurationMustBeComplete(t *testing.T) {
	setBase(t)
	t.Setenv("WEBHOOK_ADMIN_TOKEN", "admin")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "neither WEBHOOK_ENCRYPTION_KEY nor WEBHOOK_SIGNING_SECRET") {
		t.Fatalf("want an unusable admin token error, got %v", err)
	}

	setBase(t)
	t.Setenv("WEBHOOK_ENCRYPTION_KEY", "not-base64")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "must be configured together") {
		t.Fatalf("want paired webhook configuration error, got %v", err)
	}

	setBase(t)
	t.Setenv("WEBHOOK_ADMIN_TOKEN", "admin")
	t.Setenv("WEBHOOK_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	if _, err := Load(); err != nil {
		t.Fatalf("valid webhook configuration: %v", err)
	}

	t.Setenv("WEBHOOK_ENCRYPTION_KEY", "short")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "32-byte key") {
		t.Fatalf("want invalid key error, got %v", err)
	}
}

func TestWebhookAPIKeys(t *testing.T) {
	setBase(t)
	t.Setenv("WEBHOOK_ADMIN_TOKEN", "admin")
	t.Setenv("WEBHOOK_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	t.Setenv("WEBHOOK_API_KEYS", " client-one ,client-two")
	got, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(got.WebhookAPIKeys) != 2 || got.WebhookAPIKeys[0] != "client-one" || got.WebhookAPIKeys[1] != "client-two" {
		t.Fatalf("WebhookAPIKeys = %q", got.WebhookAPIKeys)
	}

	for _, raw := range []string{"client-one,,client-two", "same,same", "admin"} {
		t.Setenv("WEBHOOK_API_KEYS", raw)
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), "WEBHOOK_API_KEYS") {
			t.Errorf("WEBHOOK_API_KEYS=%q: want validation error, got %v", raw, err)
		}
	}

	setBase(t)
	t.Setenv("WEBHOOK_API_KEYS", "client-one")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "requires WEBHOOK_ADMIN_TOKEN") {
		t.Fatalf("API keys without webhook management config: %v", err)
	}
}

func TestNetworkPresets(t *testing.T) {
	cases := []struct {
		network, rpc, horizon, pass string
	}{
		{"", TestnetRPC, TestnetHorizon, TestnetPassphrase},
		{"testnet", TestnetRPC, TestnetHorizon, TestnetPassphrase},
		{"TestNet", TestnetRPC, TestnetHorizon, TestnetPassphrase},
		{"standalone", StandaloneRPC, StandaloneHorizon, StandalonePassphrase},
	}
	for _, c := range cases {
		setBase(t)
		t.Setenv("NETWORK", c.network)
		got, err := Load()
		if err != nil {
			t.Fatalf("NETWORK=%q: %v", c.network, err)
		}
		if got.RPCURL != c.rpc || got.HorizonURL != c.horizon || got.NetworkPassphrase != c.pass {
			t.Errorf("NETWORK=%q: got %q %q %q", c.network, got.RPCURL, got.HorizonURL, got.NetworkPassphrase)
		}
	}
}

func TestNetworkPresetOverrides(t *testing.T) {
	setBase(t)
	t.Setenv("NETWORK", "standalone")
	t.Setenv("STELLAR_RPC_URL", "http://rpc:9000/")
	t.Setenv("HORIZON_URL", "http://horizon:9001/")
	t.Setenv("STELLAR_NETWORK_PASSPHRASE", "custom")
	got, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.RPCURL != "http://rpc:9000/" || got.HorizonURL != "http://horizon:9001" || got.NetworkPassphrase != "custom" {
		t.Errorf("overrides ignored: %+v", got)
	}
	if got.Network != "standalone" {
		t.Errorf("Network = %q", got.Network)
	}
}

func TestMainnetNeedsRPC(t *testing.T) {
	setBase(t)
	t.Setenv("NETWORK", "mainnet")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "STELLAR_RPC_URL is required") {
		t.Fatalf("want missing RPC error, got %v", err)
	}
	t.Setenv("STELLAR_RPC_URL", "https://rpc.example.org")
	got, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.HorizonURL != MainnetHorizon || got.NetworkPassphrase != MainnetPassphrase {
		t.Errorf("mainnet defaults: %+v", got)
	}
}

func TestUnknownNetwork(t *testing.T) {
	setBase(t)
	t.Setenv("NETWORK", "futurenet")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "NETWORK") {
		t.Fatalf("want unknown network error, got %v", err)
	}
}

// TestLoadDefaults verifies that all optional fields carry their documented
// defaults when only the required variables are set.
func TestLoadDefaults(t *testing.T) {
	setBase(t)
	got, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Port != "8080" {
		t.Errorf("Port = %q, want 8080", got.Port)
	}
	if got.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want info", got.LogLevel)
	}
	if len(got.CORSOrigins) != 1 || got.CORSOrigins[0] != "*" {
		t.Errorf("CORSOrigins = %v, want [*]", got.CORSOrigins)
	}
	if got.IngestInterval.String() != "10s" {
		t.Errorf("IngestInterval = %v, want 10s", got.IngestInterval)
	}
	if got.PostInterval.String() != "15m0s" {
		t.Errorf("PostInterval = %v, want 15m0s", got.PostInterval)
	}
	if got.ReconcileInterval.String() != "15m0s" {
		t.Errorf("ReconcileInterval = %v, want 15m0s", got.ReconcileInterval)
	}
	if got.RepostAfter.String() != "6h0m0s" {
		t.Errorf("RepostAfter = %v, want 6h0m0s", got.RepostAfter)
	}
	if got.TomlInterval.String() != "6h0m0s" {
		t.Errorf("TomlInterval = %v, want 6h0m0s", got.TomlInterval)
	}
	if got.KeepAwakeInterval.String() != "10m0s" {
		t.Errorf("KeepAwakeInterval = %v, want 10m0s", got.KeepAwakeInterval)
	}
	if got.ReporterInterval.String() != "5s" {
		t.Errorf("ReporterInterval = %v, want 5s", got.ReporterInterval)
	}
	if got.HTTPKeepAlive != 90*time.Second {
		t.Errorf("HTTPKeepAlive = %v, want 90s", got.HTTPKeepAlive)
	}
	if got.IngestWorkers != 2 {
		t.Errorf("IngestWorkers = %d, want 2", got.IngestWorkers)
	}
}

func TestLoadIngestWorkers(t *testing.T) {
	for _, value := range []string{"0", "-1", "many"} {
		t.Run(value, func(t *testing.T) {
			setBase(t)
			t.Setenv("INGEST_WORKERS", value)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), "INGEST_WORKERS") {
				t.Fatalf("want INGEST_WORKERS validation error, got %v", err)
			}
		})
	}
	setBase(t)
	t.Setenv("INGEST_WORKERS", "1")
	got, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.IngestWorkers != 1 {
		t.Errorf("IngestWorkers = %d, want 1", got.IngestWorkers)
	}
}

// TestLoadRequiredMissing checks that each required variable produces an error
// when absent, and that all missing variables are reported together.
func TestLoadRequiredMissing(t *testing.T) {
	required := []struct {
		key     string
		wantMsg string
	}{
		{"DATABASE_URL", "DATABASE_URL is required"},
		{"COVERAGE_LEDGER_ID", "COVERAGE_LEDGER_ID is required"},
		{"REPORTER_REGISTRY_ID", "REPORTER_REGISTRY_ID is required"},
	}

	// Each variable alone.
	for _, r := range required {
		t.Run(r.key, func(t *testing.T) {
			setBase(t)
			t.Setenv(r.key, "")
			_, err := Load()
			if err == nil {
				t.Fatalf("Load() = nil error, want error containing %q", r.wantMsg)
			}
			if !strings.Contains(err.Error(), r.wantMsg) {
				t.Errorf("error = %q, want it to contain %q", err.Error(), r.wantMsg)
			}
		})
	}

	// All required variables absent at once — all messages must appear together.
	t.Run("all_missing", func(t *testing.T) {
		setBase(t)
		t.Setenv("DATABASE_URL", "")
		t.Setenv("COVERAGE_LEDGER_ID", "")
		t.Setenv("REPORTER_REGISTRY_ID", "")
		_, err := Load()
		if err == nil {
			t.Fatal("Load() = nil error, want combined error")
		}
		for _, r := range required {
			if !strings.Contains(err.Error(), r.wantMsg) {
				t.Errorf("combined error missing %q: %v", r.wantMsg, err)
			}
		}
	})
}

// TestLoadBadDurations checks that each malformed duration variable is reported.
func TestLoadBadDurations(t *testing.T) {
	durationVars := []string{
		"INGEST_INTERVAL",
		"POST_INTERVAL",
		"RECONCILE_INTERVAL",
		"REPOST_AFTER",
		"TOML_INTERVAL",
		"WEBHOOK_INTERVAL",
		"KEEP_AWAKE_INTERVAL",
		"REPORTER_INTERVAL",
		"HTTP_KEEPALIVE_TIMEOUT",
	}
	for _, v := range durationVars {
		t.Run(v, func(t *testing.T) {
			setBase(t)
			t.Setenv(v, "notaduration")
			_, err := Load()
			if err == nil {
				t.Fatalf("Load() = nil error with bad %s", v)
			}
			if !strings.Contains(err.Error(), v) {
				t.Errorf("error = %q, want it to mention %q", err.Error(), v)
			}
		})
	}
}

// TestWebhookConfig checks that the admin token and signing secret are only
// accepted together, and that the notifier interval has a default.
func TestWebhookConfig(t *testing.T) {
	setBase(t)
	t.Setenv("WEBHOOK_ADMIN_TOKEN", "token")
	t.Setenv("WEBHOOK_SIGNING_SECRET", "")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "neither WEBHOOK_ENCRYPTION_KEY nor WEBHOOK_SIGNING_SECRET") {
		t.Fatalf("want a half-configured webhook error, got %v", err)
	}

	t.Setenv("WEBHOOK_ADMIN_TOKEN", "")
	t.Setenv("WEBHOOK_SIGNING_SECRET", "secret")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "WEBHOOK_ADMIN_TOKEN and WEBHOOK_SIGNING_SECRET") {
		t.Fatalf("want a half-configured webhook error, got %v", err)
	}

	t.Setenv("WEBHOOK_ADMIN_TOKEN", "token")
	t.Setenv("WEBHOOK_SIGNING_SECRET", "secret")
	got, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.WebhookAdminToken != "token" || got.WebhookSigningSecret != "secret" {
		t.Errorf("webhook config: %+v", got)
	}
	// A signing secret alone is a complete coverage-alert setup: it does not
	// need the encryption key that TOML change subscriptions use.
	if got.WebhookEncryptionKey != "" {
		t.Errorf("WebhookEncryptionKey = %q, want empty", got.WebhookEncryptionKey)
	}
	if got.WebhookInterval.String() != "1m0s" {
		t.Errorf("WebhookInterval = %v, want 1m0s", got.WebhookInterval)
	}
}

// TestLoadCORSParsing checks that CORS_ORIGINS is split on commas and trimmed.
func TestLoadCORSParsing(t *testing.T) {
	cases := []struct {
		value string
		want  []string
	}{
		{"*", []string{"*"}},
		{"https://app.example.com", []string{"https://app.example.com"}},
		{"https://a.example.com,https://b.example.com", []string{"https://a.example.com", "https://b.example.com"}},
		{"  https://a.example.com ,  https://b.example.com  ", []string{"https://a.example.com", "https://b.example.com"}},
	}
	for _, c := range cases {
		t.Run(c.value, func(t *testing.T) {
			setBase(t)
			t.Setenv("CORS_ORIGINS", c.value)
			got, err := Load()
			if err != nil {
				t.Fatalf("Load() error: %v", err)
			}
			if len(got.CORSOrigins) != len(c.want) {
				t.Fatalf("CORSOrigins = %v, want %v", got.CORSOrigins, c.want)
			}
			for i, o := range got.CORSOrigins {
				if o != c.want[i] {
					t.Errorf("CORSOrigins[%d] = %q, want %q", i, o, c.want[i])
				}
			}
		})
	}
}

// TestKeepAwakeURLIntegration tests all four KEEP_AWAKE_URL cases through Load.
func TestKeepAwakeURLIntegration(t *testing.T) {
	cases := []struct {
		name           string
		keepAwake      string
		renderExternal string
		want           string
	}{
		{
			name: "neither set",
		},
		{
			name:           "render_external_url_derived",
			renderExternal: "https://plimsoll-indexer.onrender.com",
			want:           "https://plimsoll-indexer.onrender.com/healthz",
		},
		{
			name:           "explicit_overrides_render",
			keepAwake:      "https://example.org/ping",
			renderExternal: "https://x.onrender.com",
			want:           "https://example.org/ping",
		},
		{
			name:           "none_disables",
			keepAwake:      "none",
			renderExternal: "https://x.onrender.com",
			want:           "",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			setBase(t)
			t.Setenv("KEEP_AWAKE_URL", c.keepAwake)
			t.Setenv("RENDER_EXTERNAL_URL", c.renderExternal)
			got, err := Load()
			if err != nil {
				t.Fatalf("Load() error: %v", err)
			}
			if got.KeepAwakeURL != c.want {
				t.Errorf("KeepAwakeURL = %q, want %q", got.KeepAwakeURL, c.want)
			}
		})
	}
}

func TestDecimalCheckEnabled(t *testing.T) {
	setBase(t)
	got, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !got.DecimalCheckEnabled {
		t.Error("DecimalCheckEnabled = false by default, want true")
	}

	setBase(t)
	t.Setenv("DECIMAL_CHECK_ENABLED", "false")
	got, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.DecimalCheckEnabled {
		t.Error("DecimalCheckEnabled = true with DECIMAL_CHECK_ENABLED=false")
	}

	setBase(t)
	t.Setenv("DECIMAL_CHECK_ENABLED", "sometimes")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "DECIMAL_CHECK_ENABLED") {
		t.Fatalf("want an invalid boolean error, got %v", err)
	}
}

// TestPostingEnabled verifies the PostingEnabled predicate.
func TestPostingEnabled(t *testing.T) {
	setBase(t)
	got, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.PostingEnabled() {
		t.Error("PostingEnabled() = true with no SUPPLY_POSTER_SECRET, want false")
	}

	t.Setenv("SUPPLY_POSTER_SECRET", "SCZANGBA5TTPJQS62SL7TURZT5FSZMR3TCZDWNBEBGCMQK3JLYLUVRMJ")
	got, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if !got.PostingEnabled() {
		t.Error("PostingEnabled() = false with SUPPLY_POSTER_SECRET set, want true")
	}
}

// TestSecretsProviderSelection checks that SECRETS_PROVIDER accepts the three
// documented backends, defaults to env, and rejects anything else.
func TestSecretsProviderSelection(t *testing.T) {
	cases := []struct {
		value, want string
	}{
		{"", secrets.Env},
		{"env", secrets.Env},
		{"SSM", secrets.SSM},
		{" gcp ", secrets.GCP},
	}
	for _, c := range cases {
		setBase(t)
		t.Setenv("SECRETS_PROVIDER", c.value)
		got, err := Load()
		if err != nil {
			t.Fatalf("SECRETS_PROVIDER=%q: %v", c.value, err)
		}
		if got.SecretsProvider != c.want {
			t.Errorf("SECRETS_PROVIDER=%q: SecretsProvider = %q, want %q", c.value, got.SecretsProvider, c.want)
		}
	}

	setBase(t)
	t.Setenv("SECRETS_PROVIDER", "vault")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "SECRETS_PROVIDER") {
		t.Fatalf("want unknown provider error, got %v", err)
	}
}

// stubProvider is a secrets.Provider backed by a map.
type stubProvider struct {
	values map[string]string
	err    error
	asked  []string
}

func (s *stubProvider) Get(_ context.Context, key string) (string, error) {
	s.asked = append(s.asked, key)
	if s.err != nil {
		return "", s.err
	}
	v, ok := s.values[key]
	if !ok {
		return "", fmt.Errorf("no such secret %q", key)
	}
	return v, nil
}

func (s *stubProvider) Name() string { return "stub" }
func (s *stubProvider) Close() error { return nil }

// TestResolveSecrets checks that named credentials replace the environment
// values and that an unnamed one keeps its environment fallback.
func TestResolveSecrets(t *testing.T) {
	p := &stubProvider{values: map[string]string{
		"/plimsoll/poster": "poster-from-backend",
		"/plimsoll/db":     "db-from-backend",
	}}
	cfg := Config{
		SecretsProvider:      secrets.SSM,
		PosterSecret:         "poster-from-env",
		PosterSecretName:     "/plimsoll/poster",
		DatabasePasswordName: "/plimsoll/db",
	}
	if err := cfg.ResolveSecrets(context.Background(), p); err != nil {
		t.Fatalf("ResolveSecrets: %v", err)
	}
	if cfg.PosterSecret != "poster-from-backend" {
		t.Errorf("PosterSecret = %q, want the backend value", cfg.PosterSecret)
	}
	if cfg.DatabasePassword != "db-from-backend" {
		t.Errorf("DatabasePassword = %q, want the backend value", cfg.DatabasePassword)
	}
}

func TestResolveSecretsKeepsEnvFallback(t *testing.T) {
	p := &stubProvider{}
	cfg := Config{SecretsProvider: secrets.SSM, PosterSecret: "poster-from-env"}
	if err := cfg.ResolveSecrets(context.Background(), p); err != nil {
		t.Fatalf("ResolveSecrets: %v", err)
	}
	if cfg.PosterSecret != "poster-from-env" {
		t.Errorf("PosterSecret = %q, want the environment fallback", cfg.PosterSecret)
	}
	if cfg.DatabasePassword != "" {
		t.Errorf("DatabasePassword = %q, want it unset", cfg.DatabasePassword)
	}
	if len(p.asked) != 0 {
		t.Errorf("asked for %v, want no lookups without a *_SECRET_NAME", p.asked)
	}
}

// TestResolveSecretsReportsKeysNotValues checks that a backend failure names the
// configuration variable and the secret key without changing either credential.
func TestResolveSecretsReportsKeysNotValues(t *testing.T) {
	p := &stubProvider{err: errors.New("access denied")}
	cfg := Config{
		PosterSecret:         "poster-from-env",
		PosterSecretName:     "/plimsoll/poster",
		DatabasePasswordName: "/plimsoll/db",
	}
	err := cfg.ResolveSecrets(context.Background(), p)
	if err == nil {
		t.Fatal("ResolveSecrets = nil error, want error")
	}
	for _, want := range []string{"SUPPLY_POSTER_SECRET_NAME", "DATABASE_PASSWORD_SECRET_NAME", "/plimsoll/poster", "/plimsoll/db"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to mention %q", err, want)
		}
	}
	if cfg.PosterSecret != "poster-from-env" {
		t.Errorf("PosterSecret = %q, want the failed lookup to leave it unchanged", cfg.PosterSecret)
	}
}

func TestResolveSecretsRejectsNilProvider(t *testing.T) {
	cfg := Config{PosterSecretName: "/plimsoll/poster"}
	if err := cfg.ResolveSecrets(context.Background(), nil); err == nil {
		t.Fatal("ResolveSecrets(nil) = nil error, want error")
	}
}

// TestConfigLogValueRedactsCredentials checks that logging a Config records
// whether credentials are set but never their values.
func TestConfigLogValueRedactsCredentials(t *testing.T) {
	cfg := Config{
		Network:              "testnet",
		SecretsProvider:      secrets.SSM,
		DatabaseURL:          "postgres://plimsoll:db-password-value@db:5432/plimsoll",
		PosterSecret:         "poster-secret-value",
		DatabasePassword:     "db-password-value",
		WebhookAdminToken:    "admin-token-value",
		WebhookEncryptionKey: "encryption-key-value",
	}
	var buf bytes.Buffer
	slog.New(slog.NewJSONHandler(&buf, nil)).Info("config", "config", cfg)
	out := buf.String()
	for _, leaked := range []string{"poster-secret-value", "db-password-value", "admin-token-value", "encryption-key-value", "postgres://"} {
		if strings.Contains(out, leaked) {
			t.Errorf("log output %q leaks %q", out, leaked)
		}
	}
	for _, want := range []string{`"poster_secret_set":true`, `"database_password_set":true`, `"secrets_provider":"ssm"`} {
		if !strings.Contains(out, want) {
			t.Errorf("log output %q missing %q", out, want)
		}
	}
}
