package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Versioned schema migrations.
//
// Each file in migrations/ is named NNNN_description.sql, where NNNN is its
// version: 0001, 0002, and so on, with no gaps. Files with the same version
// form an ordered migration group. Open applies pending files in version/name
// order and records their names in schema_migrations. A merged migration is
// never edited: a later schema change is a new file.

//go:embed migrations/*.sql
var embeddedMigrations embed.FS

// migrationLockKey is the advisory lock that serialises migrations between
// instances starting at the same time. It must differ from posterLockKey.
const migrationLockKey = int64(0x706c696d2d6d6967) // "plim-mig" in hex ASCII

var migrationFileName = regexp.MustCompile(`^([0-9]{4})_([a-z0-9_]+)\.sql$`)

type migration struct {
	Version int
	Name    string
	SQL     string
}

// loadMigrations reads the migration files at the root of fsys and returns
// them in version order. Migrations sharing a version are applied by name in
// one transaction; this supports independently prepared branches that used the
// same next version without rewriting migration history.
func loadMigrations(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}
	var out []migration
	for _, e := range entries {
		m := migrationFileName.FindStringSubmatch(e.Name())
		if e.IsDir() || m == nil {
			return nil, fmt.Errorf("migration %q is not named NNNN_description.sql", e.Name())
		}
		version, err := strconv.Atoi(m[1])
		if err != nil {
			return nil, fmt.Errorf("migration %q: %w", e.Name(), err)
		}
		if version == 0 {
			return nil, fmt.Errorf("migration %q: versions start at 0001", e.Name())
		}
		body, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", e.Name(), err)
		}
		out = append(out, migration{Version: version, Name: m[2], SQL: string(body)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Version == out[j].Version {
			return out[i].Name < out[j].Name
		}
		return out[i].Version < out[j].Version
	})
	for i, expected := 0, 1; i < len(out); expected++ {
		version := out[i].Version
		if version != expected {
			return nil, fmt.Errorf("migration %04d is missing", expected)
		}
		for i < len(out) && out[i].Version == version {
			i++
		}
	}
	return out, nil
}

// migrate applies the pending migrations in fsys inside one transaction.
//
// The transaction first takes migrationLockKey, so an instance starting while
// another migrates waits for it to commit and then finds nothing left to do.
// Either every pending migration is applied and recorded or, on any error,
// none is. Statements that refuse to run in a transaction (CREATE INDEX
// CONCURRENTLY, for example) cannot be used in a migration.
func migrate(ctx context.Context, pool *pgxpool.Pool, fsys fs.FS) error {
	migrations, err := loadMigrations(fsys)
	if err != nil {
		return err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migrations: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, migrationLockKey); err != nil {
		return fmt.Errorf("lock migrations: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
		    version     INTEGER PRIMARY KEY,
		    name        TEXT NOT NULL,
		    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	appliedByVersion := map[int]map[string]bool{}
	rows, err := tx.Query(ctx, `SELECT version, name FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	for rows.Next() {
		var v int
		var names string
		if err := rows.Scan(&v, &names); err != nil {
			rows.Close()
			return fmt.Errorf("read schema_migrations: %w", err)
		}
		if appliedByVersion[v] == nil {
			appliedByVersion[v] = make(map[string]bool)
		}
		for _, name := range strings.Split(names, ",") {
			if name == "" {
				continue
			}
			appliedByVersion[v][name] = true
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}

	for i := 0; i < len(migrations); {
		j := i + 1
		for j < len(migrations) && migrations[j].Version == migrations[i].Version {
			j++
		}
		version := migrations[i].Version
		for _, m := range migrations[i:j] {
			if appliedByVersion[version][m.Name] {
				continue
			}
			if _, err := tx.Exec(ctx, m.SQL); err != nil {
				return fmt.Errorf("apply migration %04d_%s: %w", m.Version, m.Name, err)
			}
		}

		names := make(map[string]bool)
		for name := range appliedByVersion[version] {
			names[name] = true
		}
		for _, m := range migrations[i:j] {
			names[m.Name] = true
		}
		orderedNames := make([]string, 0, len(names))
		for name := range names {
			orderedNames = append(orderedNames, name)
		}
		sort.Strings(orderedNames)
		if _, exists := appliedByVersion[version]; exists {
			if _, err := tx.Exec(ctx,
				`UPDATE schema_migrations SET name = $2 WHERE version = $1`,
				version, strings.Join(orderedNames, ",")); err != nil {
				return fmt.Errorf("record migrations for version %04d: %w", version, err)
			}
		} else if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`,
			version, strings.Join(orderedNames, ",")); err != nil {
			return fmt.Errorf("record migrations for version %04d: %w", version, err)
		}
		appliedByVersion[version] = names
		i = j
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migrations: %w", err)
	}
	return nil
}

// migrationsFS is the migrations directory compiled into the binary.
func migrationsFS() fs.FS {
	sub, err := fs.Sub(embeddedMigrations, "migrations")
	if err != nil {
		// fs.Sub only fails for an invalid path, and "migrations" is a constant.
		panic(err)
	}
	return sub
}
