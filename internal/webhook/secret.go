package webhook

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"strings"
)

// SecretCodec encrypts webhook secrets at rest so delivery can use the
// original key while the database also retains a one-way hash.
type SecretCodec struct {
	aead cipher.AEAD
}

func NewSecretCodec(encodedKey string) (*SecretCodec, error) {
	key, err := base64.StdEncoding.DecodeString(encodedKey)
	if err != nil {
		return nil, fmt.Errorf("decode webhook encryption key: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("webhook encryption key must decode to 32 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create webhook encryption cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create webhook encryption AEAD: %w", err)
	}
	return &SecretCodec{aead: aead}, nil
}

func (c *SecretCodec) Encrypt(secret string) ([]byte, error) {
	nonce := make([]byte, c.aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate webhook secret nonce: %w", err)
	}
	return c.aead.Seal(nonce, nonce, []byte(secret), nil), nil
}

func (c *SecretCodec) Decrypt(ciphertext []byte) (string, error) {
	if len(ciphertext) < c.aead.NonceSize() {
		return "", fmt.Errorf("webhook secret ciphertext is too short")
	}
	nonce := ciphertext[:c.aead.NonceSize()]
	secret, err := c.aead.Open(nil, nonce, ciphertext[c.aead.NonceSize():], nil)
	if err != nil {
		return "", fmt.Errorf("decrypt webhook secret: %w", err)
	}
	return string(secret), nil
}

func GenerateSecret() (string, error) {
	secret := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, secret); err != nil {
		return "", fmt.Errorf("generate webhook secret: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(secret), nil
}

func SecretHash(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

func VerifySecretHash(secret, expected string) bool {
	got := SecretHash(secret)
	return len(got) == len(expected) && subtle.ConstantTimeCompare([]byte(got), []byte(expected)) == 1
}

// Sign returns the documented header value for the exact payload bytes.
func Sign(payload []byte, secret string) string {
	mac := hmacSHA256([]byte(secret), payload)
	return "sha256=" + hex.EncodeToString(mac)
}

func hmacSHA256(key, message []byte) []byte {
	h := hmac.New(sha256.New, key)
	_, _ = h.Write(message)
	return h.Sum(nil)
}

// ValidateURL accepts HTTPS destinations only. Destinations are explicitly
// configured by an authenticated administrator, and delivery uses a timeout
// with redirects disabled.
func ValidateURL(raw string) error {
	u, err := url.ParseRequestURI(raw)
	if err != nil {
		return fmt.Errorf("invalid webhook URL: %w", err)
	}
	if u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" || strings.Contains(raw, "#") {
		return fmt.Errorf("webhook URL must be HTTPS and must not contain credentials or a fragment")
	}
	if strings.TrimSpace(raw) != raw {
		return fmt.Errorf("webhook URL must not contain surrounding whitespace")
	}
	return nil
}
