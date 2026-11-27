package config

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const (
	// envLocalPath is relative to the working directory, which for local runs
	// is the repository root.
	envLocalPath = ".env.local"
	// maxEnvFileBytes bounds how much of a stray .env.local is read. Real
	// files are a few kilobytes; anything larger is a mistake.
	maxEnvFileBytes = 1 << 20
)

// loadEnvLocal overlays .env.local on the process environment. The file is a
// development convenience: it lets a developer override values sourced from
// .env without editing the committed .env.example. It is ignored entirely when
// GO_ENV=production so a stray file cannot change a deployment.
func loadEnvLocal() error {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("GO_ENV")), "production") {
		return nil
	}
	return loadEnvFile(envLocalPath)
}

// loadEnvFile reads KEY=VALUE lines from path and sets them in the process
// environment. A missing file is not an error. The whole file is parsed before
// any variable is set, so a malformed line changes nothing.
func loadEnvFile(path string) error {
	// Open through a filesystem rooted at the file's directory: the read is
	// confined to that directory and a path cannot reach outside it.
	dir, name := filepath.Split(path)
	if dir == "" {
		dir = "."
	}
	f, err := os.DirFS(dir).Open(name)
	// The path is the package's own constant (.env.local), never user input.
	f, err := os.Open(path) //nolint:gosec // fixed local path, not tainted input
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	if info, err := f.Stat(); err == nil && info.Size() > maxEnvFileBytes {
		return fmt.Errorf("%s: file larger than %d bytes", path, maxEnvFileBytes)
	}

	type entry struct{ key, value string }
	var entries []entry

	r := bufio.NewScanner(io.LimitReader(f, maxEnvFileBytes))
	for n := 1; r.Scan(); n++ {
		key, value, ok, err := parseEnvLine(r.Text())
		if err != nil {
			return fmt.Errorf("%s:%d: %w", path, n, err)
		}
		if ok {
			entries = append(entries, entry{key, value})
		}
	}
	if err := r.Err(); err != nil {
		return fmt.Errorf("read %s: %w", path, err)
	}

	for _, e := range entries {
		if err := os.Setenv(e.key, e.value); err != nil {
			return fmt.Errorf("set %s from %s: %w", e.key, path, err)
		}
	}
	return nil
}

// parseEnvLine parses one line of a dotenv file. ok is false for blank lines
// and comments. Values may be single-quoted (literal), double-quoted (with
// \n, \r, \t, \" and \\ escapes) or unquoted; a '#' preceded by whitespace
// starts a comment outside quotes.
func parseEnvLine(line string) (key, value string, ok bool, err error) {
	s := strings.TrimSpace(line)
	if s == "" || strings.HasPrefix(s, "#") {
		return "", "", false, nil
	}
	if rest, found := strings.CutPrefix(s, "export"); found {
		if rest == "" || (rest[0] != ' ' && rest[0] != '\t') {
			return "", "", false, errors.New("expected KEY=VALUE")
		}
		s = strings.TrimSpace(rest)
	}

	eq := strings.IndexByte(s, '=')
	if eq <= 0 {
		return "", "", false, errors.New("expected KEY=VALUE")
	}
	key = strings.TrimSpace(s[:eq])
	if !validEnvKey(key) {
		return "", "", false, fmt.Errorf("invalid key %q", key)
	}
	value, err = parseEnvValue(strings.TrimSpace(s[eq+1:]))
	if err != nil {
		return "", "", false, err
	}
	return key, value, true, nil
}

func validEnvKey(key string) bool {
	for i := 0; i < len(key); i++ {
		c := key[i]
		switch {
		case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c == '_':
		case i > 0 && c >= '0' && c <= '9':
		default:
			return false
		}
	}
	return key != ""
}

func parseEnvValue(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	switch raw[0] {
	case '\'':
		end := strings.IndexByte(raw[1:], '\'')
		if end < 0 {
			return "", errors.New("unterminated single-quoted value")
		}
		if err := expectCommentOrEnd(raw[end+2:]); err != nil {
			return "", err
		}
		return raw[1 : end+1], nil
	case '"':
		var b strings.Builder
		for i := 1; i < len(raw); i++ {
			switch c := raw[i]; c {
			case '\\':
				if i+1 >= len(raw) {
					return "", errors.New("unterminated double-quoted value")
				}
				i++
				switch raw[i] {
				case 'n':
					b.WriteByte('\n')
				case 'r':
					b.WriteByte('\r')
				case 't':
					b.WriteByte('\t')
				case '"', '\\':
					b.WriteByte(raw[i])
				default:
					// Keep the backslash so Windows paths and the like survive.
					b.WriteByte('\\')
					b.WriteByte(raw[i])
				}
			case '"':
				if err := expectCommentOrEnd(raw[i+1:]); err != nil {
					return "", err
				}
				return b.String(), nil
			default:
				b.WriteByte(c)
			}
		}
		return "", errors.New("unterminated double-quoted value")
	}

	for i := 0; i < len(raw); i++ {
		if raw[i] == '#' && i > 0 && (raw[i-1] == ' ' || raw[i-1] == '\t') {
			return strings.TrimSpace(raw[:i]), nil
		}
	}
	return raw, nil
}

// expectCommentOrEnd rejects trailing text after a closing quote, allowing
// only whitespace and a comment.
func expectCommentOrEnd(rest string) error {
	rest = strings.TrimSpace(rest)
	if rest == "" || strings.HasPrefix(rest, "#") {
		return nil
	}
	return errors.New("unexpected text after quoted value")
}
