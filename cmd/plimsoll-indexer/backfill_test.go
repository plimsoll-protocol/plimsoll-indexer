package main

import (
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/network"
)

func TestDefaultArchiveURLs(t *testing.T) {
	testnet, err := defaultArchiveURLs(network.TestNetworkPassphrase)
	if err != nil || len(testnet) == 0 {
		t.Fatalf("testnet archives = %v, %v", testnet, err)
	}
	mainnet, err := defaultArchiveURLs(network.PublicNetworkPassphrase)
	if err != nil || len(mainnet) == 0 {
		t.Fatalf("mainnet archives = %v, %v", mainnet, err)
	}
	if _, err := defaultArchiveURLs("custom"); err == nil {
		t.Fatal("custom passphrase without explicit archive should fail")
	}
}

func TestValidateArchiveURLs(t *testing.T) {
	if err := validateArchiveURLs([]string{"https://history.example/archive/"}); err != nil {
		t.Fatalf("valid archive: %v", err)
	}
	for _, archive := range []string{
		"http://history.example/archive/",
		"https://user:password@history.example/archive/",
		"not a url",
	} {
		if err := validateArchiveURLs([]string{archive}); err == nil || !strings.Contains(err.Error(), "HTTPS URL") {
			t.Errorf("validateArchiveURLs(%q) = %v, want HTTPS URL error", archive, err)
		}
	}
}
