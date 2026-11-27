package store

import (
	"context"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
)

// migrationsUpTo returns the embedded migrations with version <= max, as a
// filesystem migrate can consume.
func migrationsUpTo(t *testing.T, max int) fstest.MapFS {
	t.Helper()
	ms, err := loadMigrations(migrationsFS())
	if err != nil {
		t.Fatal(err)
	}
	out := fstest.MapFS{}
	for _, m := range ms {
		if m.Version > max {
			continue
		}
		name := fmt.Sprintf("%04d_%s.sql", m.Version, m.Name)
		body, err := fs.ReadFile(migrationsFS(), name)
		if err != nil {
			t.Fatal(err)
		}
		out[name] = &fstest.MapFile{Data: body}
	}
	return out
}

// Every migration applies on a fresh database both in one run and one version
// at a time (as a database upgraded release by release sees them), and both
// paths end at the same schema. This catches migration-ordering bugs: a
// migration that depends on something only a later one creates fails here.
func TestMigrationsApplyStepByStepOnFreshDatabase(t *testing.T) {
	ctx := context.Background()
	versions := embeddedVersions(t)
	if len(versions) == 0 {
		t.Fatal("no embedded migrations")
	}

	allAtOnce := newDatabase(t)
	if err := migrate(ctx, testPool(t, allAtOnce), migrationsFS()); err != nil {
		t.Fatalf("apply all migrations on a fresh database: %v", err)
	}
	if got := appliedVersions(t, allAtOnce); !slices.Equal(got, versions) {
		t.Fatalf("fresh database recorded %v, want %v", got, versions)
	}

	stepwise := newDatabase(t)
	p := testPool(t, stepwise)
	for i, v := range versions {
		if err := migrate(ctx, p, migrationsUpTo(t, v)); err != nil {
			t.Fatalf("apply migrations up to %04d: %v", v, err)
		}
		if got, want := appliedVersions(t, stepwise), versions[:i+1]; !slices.Equal(got, want) {
			t.Fatalf("after %04d recorded %v, want %v", v, got, want)
		}
	}

	if d := schemaDiff(schemaOf(t, allAtOnce), schemaOf(t, stepwise)); len(d) > 0 {
		t.Fatalf("step-by-step schema differs from one-shot schema:\n%s", strings.Join(d, "\n"))
	}
}
