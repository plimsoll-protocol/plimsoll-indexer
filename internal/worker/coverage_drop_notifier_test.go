package worker

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest
	"strings"
	"testing"
)

func TestCoverageDropNotifierDisabledWhenNoneSet(t *testing.T) {
	n := &CoverageDropNotifier{}
	err := n.Notify(context.Background(), CoverageDropEvent{Asset: "USDC:ISSUER", Bps: 500, Threshold: 1000})
	if err != nil {
		t.Fatalf("unexpected error when disabled: %v", err)
	}
}

func TestCoverageDropNotifierPostsToSlackAndDiscord(t *testing.T) {
	var slackCalled, discordCalled bool
	slackServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slackCalled = true
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), "USDC:ISSUER") || !strings.Contains(string(body), "500") || !strings.Contains(string(body), "1000") {
			t.Errorf("slack body missing fields: %s", string(body))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer slackServer.Close()

	discordServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		discordCalled = true
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), "USDC:ISSUER") || !strings.Contains(string(body), "500") || !strings.Contains(string(body), "1000") {
			t.Errorf("discord body missing fields: %s", string(body))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer discordServer.Close()

	n := &CoverageDropNotifier{
		SlackURL:   slackServer.URL,
		DiscordURL: discordServer.URL,
	}

	err := n.Notify(context.Background(), CoverageDropEvent{Asset: "USDC:ISSUER", Bps: 500, Threshold: 1000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !slackCalled {
		t.Error("slack webhook was not called")
	}
	if !discordCalled {
		t.Error("discord webhook was not called")
	}
}
