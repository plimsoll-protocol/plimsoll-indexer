package store

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"regexp"
	"sort"
	"strconv"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Versioned schema migrations.
//
// Each file in migrations/ is one migration named NNNN_description.sql, where
// NNNN is its version: 0001, 0002, and so on, with no gaps. Two branches that
// land a schema change in the same release can end up sharing a version; those
// run in name order, and the name is what identifies them, because a database
// can only have recorded one row per number under the old scheme. Open applies
// every migration a database has not recorded yet, in version order, and
// records each one in schema_migrations. A merged migration is never edited: a
// later schema change is a new file.

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
// them in version order. Misnamed files, duplicate versions and gaps are
// errors, so a typo cannot silently skip a migration.
func loadMigrations(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, ".")
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}
	var out []migration
	seen := map[string]bool{}
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
		if seen[m[2]] {
			return nil, fmt.Errorf("two migrations are named %q", m[2])
		}
		seen[m[2]] = true
		body, err := fs.ReadFile(fsys, e.Name())
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", e.Name(), err)
		}
		out = append(out, migration{Version: version, Name: m[2], SQL: string(body)})
	}
	// Versions must cover 1..N without gaps. Equal versions are allowed and run
	// in name order: they are changes that landed together from separate
	// branches, and their names are what tell them apart in schema_migrations.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Version != out[j].Version {
			return out[i].Version < out[j].Version
		}
		return out[i].Name < out[j].Name
	})
	if len(out) > 0 && out[0].Version != 1 {
		return nil, fmt.Errorf("migration 0001 is missing")
	}
	for i := 1; i < len(out); i++ {
		prev, cur := out[i-1].Version, out[i].Version
		if cur != prev && cur != prev+1 {
			return nil, fmt.Errorf("migration %04d is missing", prev+1)
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
		    name        TEXT PRIMARY KEY,
		    version     INTEGER NOT NULL,
		    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}
	// A database migrated before versions could repeat recorded them by version
	// alone. Its rows already carry the name, so re-key the table in place on
	// name: every applied migration stays applied, and two migrations that share
	// a version can both be recorded.
	if _, err := tx.Exec(ctx, `
		DO $$
		BEGIN
		    IF EXISTS (
		        SELECT 1 FROM pg_constraint
		        WHERE conrelid = 'schema_migrations'::regclass
		          AND contype = 'p'
		          AND pg_get_constraintdef(oid) = 'PRIMARY KEY (version)'
		    ) THEN
		        ALTER TABLE schema_migrations DROP CONSTRAINT schema_migrations_pkey;
		        ALTER TABLE schema_migrations ADD CONSTRAINT schema_migrations_name_key UNIQUE (name);
		    END IF;
		END $$`); err != nil {
		return fmt.Errorf("re-key schema_migrations: %w", err)
	}

	applied := map[string]bool{}
	rows, err := tx.Query(ctx, `SELECT name FROM schema_migrations`)
	if err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return fmt.Errorf("read schema_migrations: %w", err)
		}
		applied[name] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}

	for _, m := range migrations {
		if applied[m.Name] {
			continue
		}
		if _, err := tx.Exec(ctx, m.SQL); err != nil {
			return fmt.Errorf("apply migration %04d_%s: %w", m.Version, m.Name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO schema_migrations (name, version) VALUES ($1, $2)
			 ON CONFLICT (name) DO UPDATE SET version = EXCLUDED.version`, m.Name, m.Version); err != nil {
			return fmt.Errorf("record migration %04d_%s: %w", m.Version, m.Name, err)
		}
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
