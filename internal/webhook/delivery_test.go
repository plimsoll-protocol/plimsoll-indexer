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

type recordedDelivery struct {
	id      int64
	at      time.Time
	failure string
}

type testSubscriptionStore struct {
	webhooks   []store.Webhook
	deliveries []recordedDelivery
}

type testSubscriptionStore struct {
	webhooks []store.Webhook
	dlq      []store.WebhookDLQ
	nextID   int64
}

func (s *testSubscriptionStore) ListWebhooks(context.Context) ([]store.Webhook, error) {
	return s.webhooks, nil
}

func (s *testSubscriptionStore) RecordWebhookDelivery(_ context.Context, id int64, at time.Time, failure string) error {
	s.deliveries = append(s.deliveries, recordedDelivery{id: id, at: at, failure: failure})
	return nil
}

func (s *testSubscriptionStore) GetWebhook(_ context.Context, id int64) (store.Webhook, bool, error) {
	for _, w := range s.webhooks {
		if w.ID == id {
			return w, true, nil
		}
	}
	return store.Webhook{}, false, nil
}

func (s *testSubscriptionStore) CreateWebhookDLQ(_ context.Context, webhookID int64, payload []byte, lastError string, attempts int) error {
	s.nextID++
	s.dlq = append(s.dlq, store.WebhookDLQ{
		ID: s.nextID, WebhookID: webhookID, Payload: append([]byte(nil), payload...),
		LastError: lastError, Attempts: attempts,
	})
	return nil
}

func (s *testSubscriptionStore) ListWebhookDLQ(_ context.Context, webhookID int64, limit int) ([]store.WebhookDLQ, error) {
	var out []store.WebhookDLQ
	for _, item := range s.dlq {
		if item.WebhookID == webhookID && len(out) < limit {
			out = append(out, item)
		}
	}
	return out, nil
}

func (s *testSubscriptionStore) GetWebhookDLQ(_ context.Context, webhookID, id int64) (store.WebhookDLQ, bool, error) {
	for _, item := range s.dlq {
		if item.WebhookID == webhookID && item.ID == id {
			return item, true, nil
		}
	}
	return store.WebhookDLQ{}, false, nil
}

func (s *testSubscriptionStore) UpdateWebhookDLQFailure(_ context.Context, webhookID, id int64, lastError string, attempts int) (bool, error) {
	for i := range s.dlq {
		if s.dlq[i].WebhookID == webhookID && s.dlq[i].ID == id {
			s.dlq[i].LastError = lastError
			s.dlq[i].Attempts += attempts
			return true, nil
		}
	}
	return false, nil
}

func (s *testSubscriptionStore) DeleteWebhookDLQ(_ context.Context, webhookID, id int64) (bool, error) {
	for i := range s.dlq {
		if s.dlq[i].WebhookID == webhookID && s.dlq[i].ID == id {
			s.dlq = append(s.dlq[:i], s.dlq[i+1:]...)
			return true, nil
		}
	}
	return false, nil
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
	subs := &testSubscriptionStore{webhooks: []store.Webhook{{
	dispatcher := NewDispatcher(&testSubscriptionStore{webhooks: []store.Webhook{{
		ID: 1, URL: server.URL, SecretHash: SecretHash(secret), SecretCiphertext: ciphertext,
	}}}
	dispatcher := NewDispatcher(subs, codec)
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
	if len(subs.deliveries) != 1 || subs.deliveries[0].id != 1 || subs.deliveries[0].failure != "" {
		t.Fatalf("recorded deliveries = %+v, want one success for endpoint 1", subs.deliveries)
	}
}

// Every registered endpoint receives the same event, and each delivery is
// recorded against its own endpoint so one failing receiver does not hide the
// others.
func TestDispatcherFansOutAndTracksEachEndpoint(t *testing.T) {
	const secret = "receiver-secret"
	delivered := 0
	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		delivered++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer ok.Close()
	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer bad.Close()

	codec := testCodec(t)
	ciphertext, err := codec.Encrypt(secret)
	if err != nil {
		t.Fatal(err)
	}
	subs := &testSubscriptionStore{webhooks: []store.Webhook{
		{ID: 1, URL: ok.URL, SecretHash: SecretHash(secret), SecretCiphertext: ciphertext},
		{ID: 2, URL: bad.URL, SecretHash: SecretHash(secret), SecretCiphertext: ciphertext},
	}}
	dispatcher := NewDispatcher(subs, codec)
	dispatcher.Client = &http.Client{Timeout: time.Second}

	err = dispatcher.TOMLChanged(context.Background(), "GISSUER", "issuer.example", "old", "new")
	if err == nil || !strings.Contains(err.Error(), "deliver webhook 2") {
		t.Fatalf("want the failing endpoint reported, got %v", err)
	}
	if delivered != 1 {
		t.Errorf("the healthy endpoint received %d deliveries, want 1", delivered)
	}
	if len(subs.deliveries) != 2 {
		t.Fatalf("recorded %d deliveries, want 2", len(subs.deliveries))
	}
	if subs.deliveries[0].id != 1 || subs.deliveries[0].failure != "" {
		t.Errorf("endpoint 1 delivery = %+v, want a success", subs.deliveries[0])
	}
	if subs.deliveries[1].id != 2 || !strings.Contains(subs.deliveries[1].failure, "status 500") {
		t.Errorf("endpoint 2 delivery = %+v, want the status failure", subs.deliveries[1])
	}
}

func TestKeyIDIsStableAndDoesNotContainTheToken(t *testing.T) {
	const token = "admin-token-value"
	id := KeyID(token)
	if id == "" || id == token || strings.Contains(id, token) {
		t.Fatalf("KeyID(%q) = %q, want an opaque fingerprint", token, id)
	}
	if KeyID(token) != id || KeyID("another-token") == id {
		t.Error("KeyID must be stable for one token and different for another")
	}
}

func TestDispatcherRetriesAndDeadLettersFailedDelivery(t *testing.T) {
	const secret = "receiver-secret"
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	codec := testCodec(t)
	ciphertext, err := codec.Encrypt(secret)
	if err != nil {
		t.Fatal(err)
	}
	storage := &testSubscriptionStore{webhooks: []store.Webhook{{
		ID: 7, URL: server.URL, SecretHash: SecretHash(secret), SecretCiphertext: ciphertext,
	}}}
	dispatcher := NewDispatcher(storage, codec)
	dispatcher.RetryDelay = 0
	dispatcher.Client = &http.Client{Timeout: time.Second}

	if err := dispatcher.TOMLChanged(context.Background(), "GISSUER", "issuer.example", "old", "new"); err == nil {
		t.Fatal("expected failed webhook delivery")
	}
	if calls != maxRetries+1 {
		t.Fatalf("delivery attempts = %d, want initial attempt plus %d retries", calls, maxRetries)
	}
	if len(storage.dlq) != 1 {
		t.Fatalf("DLQ contains %d entries, want 1", len(storage.dlq))
	}
	if storage.dlq[0].Attempts != maxRetries+1 || storage.dlq[0].LastError == "" {
		t.Fatalf("DLQ entry = %+v", storage.dlq[0])
	}
}

func TestRetryWebhookDLQRemovesSuccessfullyDeliveredEntry(t *testing.T) {
	const secret = "receiver-secret"
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	codec := testCodec(t)
	ciphertext, err := codec.Encrypt(secret)
	if err != nil {
		t.Fatal(err)
	}
	storage := &testSubscriptionStore{
		webhooks: []store.Webhook{{
			ID: 7, URL: server.URL, SecretHash: SecretHash(secret), SecretCiphertext: ciphertext,
		}},
		dlq: []store.WebhookDLQ{{ID: 8, WebhookID: 7, Payload: []byte(`{"event":"test"}`), Attempts: 4}},
	}
	dispatcher := NewDispatcher(storage, codec)
	dispatcher.RetryDelay = 0
	dispatcher.Client = &http.Client{Timeout: time.Second}

	found, err := dispatcher.RetryWebhookDLQ(context.Background(), 7, 8)
	if err != nil || !found {
		t.Fatalf("RetryWebhookDLQ() = %v, %v", found, err)
	}
	if calls != 1 || len(storage.dlq) != 0 {
		t.Fatalf("calls=%d, remaining DLQ=%d; want 1 and 0", calls, len(storage.dlq))
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
