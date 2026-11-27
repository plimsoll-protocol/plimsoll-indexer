package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

func runKeygenCommand(args []string) int {
	fs := flag.NewFlagSet("keygen", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return 1
	}
	if fs.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "keygen: unexpected arguments")
		fs.Usage()
		return 1
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		fmt.Fprintln(os.Stderr, "keygen: DATABASE_URL is required")
		return 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := store.Open(ctx, databaseURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "keygen: open database: %v\n", err)
		return 1
	}
	defer db.Close()

	raw := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, raw); err != nil {
		fmt.Fprintf(os.Stderr, "keygen: generate key: %v\n", err)
		return 1
	}
	secret := base64.RawURLEncoding.EncodeToString(raw)
	hash, err := bcrypt.GenerateFromPassword([]byte(secret), bcrypt.DefaultCost)
	if err != nil {
		fmt.Fprintf(os.Stderr, "keygen: hash key: %v\n", err)
		return 1
	}
	id, err := db.CreateAPIKey(ctx, string(hash))
	if err != nil {
		fmt.Fprintf(os.Stderr, "keygen: store key: %v\n", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "Save this API key now; it cannot be retrieved later.\n")
	fmt.Printf("%d.%s\n", id, secret)
	return 0
}
