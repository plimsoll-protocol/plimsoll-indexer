package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseEnvLine(t *testing.T) {
	cases := []struct {
		line, key, value string
		ok               bool
	}{
		{"", "", "", false},
		{"   ", "", "", false},
		{"# comment", "", "", false},
		{"  # indented comment", "", "", false},
		{"FOO=bar", "FOO", "bar", true},
		{"FOO=", "FOO", "", true},
		{"FOO = bar ", "FOO", "bar", true},
		{"export FOO=bar", "FOO", "bar", true},
		{"export\tFOO=bar", "FOO", "bar", true},
		{`FOO="bar baz"`, "FOO", "bar baz", true},
		{`FOO="line\nbreak\ttab"`, "FOO", "line\nbreak\ttab", true},
		{`FOO="say \"hi\""`, "FOO", `say "hi"`, true},
		{`FOO="C:\\temp"`, "FOO", `C:\temp`, true},
		{"FOO='bar baz'", "FOO", "bar baz", true},
		{`FOO='keep \n literal'`, "FOO", `keep \n literal`, true},
		{"FOO=bar # trailing comment", "FOO", "bar", true},
		{"FOO=bar#not-a-comment", "FOO", "bar#not-a-comment", true},
		{`FOO="bar" # trailing comment`, "FOO", "bar", true},
		{`FOO='bar'# trailing comment`, "FOO", "bar", true},
		{"1FOO=bar", "", "", false},
		{"FOO-BAR=baz", "", "", false},
		{"NOEQUALS", "", "", false},
		{`FOO="unterminated`, "", "", false},
		{"FOO='unterminated", "", "", false},
		{`FOO="bar" baz`, "", "", false},
	}
	for _, c := range cases {
		key, value, ok, err := parseEnvLine(c.line)
		if ok != c.ok {
			t.Errorf("parseEnvLine(%q) ok = %v, want %v (err %v)", c.line, ok, c.ok, err)
			continue
		}
		if !ok {
			if c.key != "" || c.value != "" {
				t.Errorf("parseEnvLine(%q) = %q, %q, want reject", c.line, key, value)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseEnvLine(%q): unexpected error: %v", c.line, err)
			continue
		}
		if key != c.key || value != c.value {
			t.Errorf("parseEnvLine(%q) = %q, %q, want %q, %q", c.line, key, value, c.key, c.value)
		}
	}
}

func TestLoadEnvFileMissingIsNoOp(t *testing.T) {
	if err := loadEnvFile(filepath.Join(t.TempDir(), ".env.local")); err != nil {
		t.Fatalf("missing file: %v", err)
	}
}

func TestLoadEnvFileSetsAndOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env.local")
	content := strings.Join([]string{
		"# local overrides",
		"PLIMSOLL_TEST_NEW=added",
		"PLIMSOLL_TEST_EXISTING=from-file",
		`PLIMSOLL_TEST_QUOTED="two words"`,
		"PLIMSOLL_TEST_DUP=first",
		"PLIMSOLL_TEST_DUP=second",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PLIMSOLL_TEST_EXISTING", "from-env")
	t.Setenv("PLIMSOLL_TEST_NEW", "")
	t.Setenv("PLIMSOLL_TEST_DUP", "")

	if err := loadEnvFile(path); err != nil {
		t.Fatalf("loadEnvFile: %v", err)
	}
	for key, want := range map[string]string{
		"PLIMSOLL_TEST_NEW":      "added",
		"PLIMSOLL_TEST_EXISTING": "from-file",
		"PLIMSOLL_TEST_QUOTED":   "two words",
		"PLIMSOLL_TEST_DUP":      "second",
	} {
		if got := os.Getenv(key); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
}

// TestLoadEnvFileMalformedChangesNothing verifies a parse error is reported
// with its line number and that no earlier assignment is applied.
func TestLoadEnvFileMalformedChangesNothing(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env.local")
	content := "PLIMSOLL_TEST_KEPT=changed\nnot a valid line\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PLIMSOLL_TEST_KEPT", "original")

	err := loadEnvFile(path)
	if err == nil {
		t.Fatal("loadEnvFile = nil error, want parse error")
	}
	if !strings.Contains(err.Error(), path+":2:") {
		t.Errorf("error = %q, want it to name %s:2", err, path)
	}
	if got := os.Getenv("PLIMSOLL_TEST_KEPT"); got != "original" {
		t.Errorf("PLIMSOLL_TEST_KEPT = %q, want original (file applied partially)", got)
	}
}

func TestLoadEnvLocalSkippedInProduction(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	path := filepath.Join(dir, envLocalPath)
	if err := os.WriteFile(path, []byte("PLIMSOLL_TEST_PROD=loaded\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PLIMSOLL_TEST_PROD", "from-env")

	t.Setenv("GO_ENV", "production")
	if err := loadEnvLocal(); err != nil {
		t.Fatalf("loadEnvLocal: %v", err)
	}
	if got := os.Getenv("PLIMSOLL_TEST_PROD"); got != "from-env" {
		t.Errorf("production loaded %s: PLIMSOLL_TEST_PROD = %q, want from-env", envLocalPath, got)
	}

	t.Setenv("GO_ENV", "")
	if err := loadEnvLocal(); err != nil {
		t.Fatalf("loadEnvLocal: %v", err)
	}
	if got := os.Getenv("PLIMSOLL_TEST_PROD"); got != "loaded" {
		t.Errorf("PLIMSOLL_TEST_PROD = %q, want loaded", got)
	}
}

// TestLoadUsesEnvLocal checks the overlay is applied through Load and wins
// over variables already in the environment.
func TestLoadUsesEnvLocal(t *testing.T) {
	t.Chdir(t.TempDir())
	setBase(t)
	t.Setenv("GO_ENV", "")
	t.Setenv("DATABASE_URL", "postgres://from-env")

	content := "DATABASE_URL=postgres://from-local\nLOG_LEVEL=debug\n"
	if err := os.WriteFile(envLocalPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.DatabaseURL != "postgres://from-local" {
		t.Errorf("DatabaseURL = %q, want overlay value", got.DatabaseURL)
	}
	if got.LogLevel != "debug" {
		t.Errorf("LogLevel = %q, want debug", got.LogLevel)
	}
}
