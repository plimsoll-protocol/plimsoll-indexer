package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/config"
	"github.com/plimsoll-protocol/plimsoll-indexer/internal/store"
)

func runMigrateCommand(args []string) int {
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	dbURLFlag := fs.String("database-url", "", "PostgreSQL database URL (defaults to DATABASE_URL env)")
	if err := fs.Parse(args); err != nil {
		return 1
	}

	dbURL := *dbURLFlag
	if dbURL == "" {
		cfg, err := config.Load()
		if err == nil {
			dbURL = cfg.DatabaseURL
		} else {
			dbURL = os.Getenv("DATABASE_URL")
		}
	}
	if dbURL == "" {
		fmt.Fprintln(os.Stderr, "migrate: DATABASE_URL is not set")
		return 1
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if err := store.RunMigrations(ctx, dbURL); err != nil {
		fmt.Fprintf(os.Stderr, "migrate: %v\n", err)
		return 1
	}

	fmt.Println("migrate: schema is up to date")
	return 0
}
