package webhook

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

type testSubscriptionStore struct {
	webhooks []store.Webhook
}

func (s testSubscriptionStore) ListWebhooks(context.Context) ([]store.Webhook, error) {
	return s.webhooks, nil
}

func testCodec(t *testing.T) *SecretCodec {
	t.Helper()
	codec, err := NewSecretCodec(base64.StdEncoding.EncodeToString(make([]byte, 32)))
	if err != nil {
		t.Fatal(err)
	}
	return codec
}

func TestDispatcherSignatureMatchesDocumentedReference(t *testing.T) {
	const secret = "receiver-secret"
	var gotBody []byte
	var gotSignature string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotBody, _ = io.ReadAll(r.Body)
		gotSignature = r.Header.Get("X-Plimsoll-Signature-256")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	codec := testCodec(t)
	ciphertext, err := codec.Encrypt(secret)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher := NewDispatcher(testSubscriptionStore{webhooks: []store.Webhook{{
		ID: 1, URL: server.URL, SecretHash: SecretHash(secret), SecretCiphertext: ciphertext,
	}}}, codec)
	dispatcher.Client = &http.Client{Timeout: time.Second}
	dispatcher.Now = func() time.Time { return time.Date(2026, 11, 24, 15, 0, 0, 0, time.UTC) }

	if err := dispatcher.TOMLChanged(context.Background(), "GISSUER", "issuer.example", "old-hash", "new-hash"); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(gotSignature, "sha256=") {
		t.Fatalf("signature header = %q", gotSignature)
	}

	// This is the same HMAC-SHA256 reference calculation shown in docs/webhooks.md.
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(gotBody)
	expected := mac.Sum(nil)
	received, err := hex.DecodeString(strings.TrimPrefix(gotSignature, "sha256="))
	if err != nil || !hmac.Equal(received, expected) {
		t.Fatalf("signature %q does not verify against exact request body (decode error %v)", gotSignature, err)
	}

	var payload TOMLChangedPayload
	if err := json.Unmarshal(gotBody, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Event != "issuer_toml_changed" || payload.OldHash != "old-hash" || payload.NewHash != "new-hash" {
		t.Fatalf("payload = %+v", payload)
	}
}

func TestSecretCodecEncryptsAndAuthenticates(t *testing.T) {
	codec := testCodec(t)
	const secret = "webhook-secret-value"
	ciphertext, err := codec.Encrypt(secret)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(ciphertext), secret) {
		t.Fatal("ciphertext contains plaintext secret")
	}
	got, err := codec.Decrypt(ciphertext)
	if err != nil || got != secret {
		t.Fatalf("Decrypt() = %q, %v", got, err)
	}
	ciphertext[len(ciphertext)-1] ^= 0xff
	if _, err := codec.Decrypt(ciphertext); err == nil {
		t.Fatal("tampered ciphertext was accepted")
	}
}

func TestValidateWebhookURL(t *testing.T) {
	for _, invalid := range []string{
		"http://example.com/hook",
		"https://user:pass@example.com/hook",
		"https://example.com/hook#fragment",
		" https://example.com/hook",
	} {
		if err := ValidateURL(invalid); err == nil {
			t.Errorf("ValidateURL(%q) unexpectedly succeeded", invalid)
		}
	}
	if err := ValidateURL("https://example.com/plimsoll"); err != nil {
		t.Fatalf("valid HTTPS URL: %v", err)
	}
}
