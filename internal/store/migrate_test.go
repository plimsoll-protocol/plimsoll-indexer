package store

import (
	"context"
	"fmt"
	"io/fs"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestLoadMigrations(t *testing.T) {
	ms, err := loadMigrations(fstest.MapFS{
		"0002_second.sql": {Data: []byte("SELECT 2")},
		"0001_first.sql":  {Data: []byte("SELECT 1")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 2 || ms[0].Version != 1 || ms[0].Name != "first" || ms[1].Version != 2 || ms[1].SQL != "SELECT 2" {
		t.Fatalf("migrations = %+v", ms)
	}

	// The embedded migrations themselves must load.
	if _, err := loadMigrations(migrationsFS()); err != nil {
		t.Fatalf("embedded migrations: %v", err)
	}
}

// TestLoadMigrationsAllowsVersionReuse pins the rule that a version number may
// name more than one migration, ordered by description: releases merged on the
// same day each added a 0005.
func TestLoadMigrationsAllowsVersionReuse(t *testing.T) {
	sql := &fstest.MapFile{Data: []byte("SELECT 1")}
	ms, err := loadMigrations(fstest.MapFS{"0001_b.sql": sql, "0001_a.sql": sql})
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 2 || ms[0].Name != "a" || ms[1].Name != "b" {
		t.Fatalf("migrations = %+v", ms)
	}
	if ms[0].key() != "0001_a" || ms[1].key() != "0001_b" {
		t.Fatalf("keys = %q, %q", ms[0].key(), ms[1].key())
	}
}

func TestLoadMigrationsRejectsMistakes(t *testing.T) {
	sql := &fstest.MapFile{Data: []byte("SELECT 1")}
	for name, tc := range map[string]struct {
		files fstest.MapFS
		want  string
	}{
		"gap":          {fstest.MapFS{"0001_a.sql": sql, "0003_c.sql": sql}, "migration 0002 is missing"},
		"no first":     {fstest.MapFS{"0002_b.sql": sql}, "migration 0001 is missing"},
		"no number":    {fstest.MapFS{"initial.sql": sql}, `"initial.sql" is not named`},
		"short number": {fstest.MapFS{"1_initial.sql": sql}, `"1_initial.sql" is not named`},
		"not sql":      {fstest.MapFS{"0001_a.txt": sql}, `"0001_a.txt" is not named`},
		"upper case":   {fstest.MapFS{"0001_Initial.sql": sql}, `"0001_Initial.sql" is not named`},
		"subdirectory": {fstest.MapFS{"0001_a.sql/x": sql}, `"0001_a.sql" is not named`},
		"stray file":   {fstest.MapFS{"0001_a.sql": sql, "README.md": sql}, `"README.md" is not named`},
		"version zero": {fstest.MapFS{"0000_a.sql": sql, "0001_b.sql": sql}, `"0000_a.sql": versions start at 0001`},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := loadMigrations(tc.files)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestLoadMigrationsGroupsSharedVersionsByName(t *testing.T) {
	ms, err := loadMigrations(fstest.MapFS{
		"0001_second.sql": {Data: []byte("SELECT 2")},
		"0001_first.sql":  {Data: []byte("SELECT 1")},
		"0002_next.sql":   {Data: []byte("SELECT 3")},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 3 || ms[0].Name != "first" || ms[1].Name != "second" ||
		ms[0].Version != 1 || ms[1].Version != 1 || ms[2].Version != 2 {
		t.Fatalf("migrations = %+v", ms)
	}
}

// legacySchema is the schema an indexer older than versioned migrations left
// behind: schema.sql applied directly, with no schema_migrations table.
func legacySchema(t *testing.T) string {
	t.Helper()
	b, err := fs.ReadFile(migrationsFS(), "0001_initial.sql")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// withoutLaterColumns drops the ALTER TABLE ... ADD COLUMN statements, leaving
// the shape of a database created before those columns existed.
func withoutLaterColumns(schema string) string {
	return regexp.MustCompile(`(?m)^ALTER TABLE .*ADD COLUMN IF NOT EXISTS.*$`).ReplaceAllString(schema, "")
}

// schemaOf describes every table, column, index and constraint in the public
// schema, one line each, sorted, so two databases can be compared.
func schemaOf(t *testing.T, url string) []string {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	var out []string
	for _, q := range []string{
		`SELECT 'column ' || table_name || '.' || column_name || ' ' || data_type
		        || ' nullable=' || is_nullable || ' default=' || COALESCE(column_default, '-')
		 FROM information_schema.columns WHERE table_schema = 'public'`,
		`SELECT 'index ' || indexdef FROM pg_indexes WHERE schemaname = 'public'`,
		`SELECT 'constraint ' || conrelid::regclass || ' ' || pg_get_constraintdef(oid)
		 FROM pg_constraint WHERE connamespace = 'public'::regnamespace`,
	} {
		rows, err := conn.Query(ctx, q+` ORDER BY 1`)
		if err != nil {
			t.Fatal(err)
		}
		lines, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, lines...)
	}
	return out
}

// embeddedVersions is every migration version compiled into the binary, in
// order: what a database is at once Open has run.
func embeddedVersions(t *testing.T) []int {
	t.Helper()
	ms, err := loadMigrations(migrationsFS())
	if err != nil {
		t.Fatal(err)
	}
	var out []int
	for i, m := range ms {
		if i == 0 || m.Version != ms[i-1].Version {
			out = append(out, m.Version)
		}
	}
	return out
}

func appliedVersions(t *testing.T, url string) []int {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	rows, err := conn.Query(ctx, `SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		t.Fatal(err)
	}
	v, err := pgx.CollectRows(rows, pgx.RowTo[int])
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// embeddedVersions is every migration compiled into the binary, in order.
func embeddedVersions(t *testing.T) []int {
	t.Helper()
	ms, err := loadMigrations(migrationsFS())
	if err != nil {
		t.Fatal(err)
	}
	out := make([]int, 0, len(ms))
	for _, m := range ms {
		out = append(out, m.Version)
	}
	return out
}

func schemaDiff(a, b []string) []string {
	in := map[string]int{}
	for _, l := range a {
		in[l]++
	}
	for _, l := range b {
		in[l]--
	}
	var out []string
	for l, n := range in {
		if n != 0 {
			out = append(out, fmt.Sprintf("%+d %s", n, l))
		}
	}
	return out
}

// A fresh database and one an older indexer created end at the same schema,
// and the existing one keeps its rows.
func TestFreshAndExistingDatabasesEndAtTheSameSchema(t *testing.T) {
	_, fresh := openStore(t)
	want := schemaOf(t, fresh)
	if len(want) == 0 {
		t.Fatal("fresh database has no schema")
	}
	if got, want := appliedVersions(t, fresh), embeddedVersions(t); !slices.Equal(got, want) {
		t.Fatalf("fresh database recorded %v, want %v", got, want)
	wantVersions := embeddedVersions(t)
	if got := appliedVersions(t, fresh); !slices.Equal(got, wantVersions) {
		t.Fatalf("fresh database recorded %v, want %v", got, wantVersions)
	if got := appliedVersions(t, fresh); len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("fresh database recorded %v, want [1 2 3]", got)
	if got := appliedVersions(t, fresh); len(got) != 4 || got[0] != 1 || got[1] != 2 || got[2] != 3 || got[3] != 4 {
		t.Fatalf("fresh database recorded %v, want [1 2 3 4]", got)
	}

	for name, schema := range map[string]string{
		"created by the last schema.sql":            legacySchema(t),
		"created before the later ALTER statements": withoutLaterColumns(legacySchema(t)),
	} {
		t.Run(name, func(t *testing.T) {
			url := newDatabase(t)
			execSQL(t, url, schema)
			execSQL(t, url, `INSERT INTO assets (sac, code, issuer, listed_ledger, listed_at, event_id)
				VALUES ('CSAC', 'PUSD', 'GISS', 7, now(), 'ev-1')`)
			execSQL(t, url, `INSERT INTO breakdowns (hash, sac, body) VALUES ('h1', 'CSAC', '{"total":"5"}')`)
			execSQL(t, url, `INSERT INTO reporters (address, role, name, active, updated_ledger, updated_at)
				VALUES ('GREPORTER', 1, 'Acme', true, 7, now())`)
			// The snapshot that h1 backs, so the breakdown is confirmed and served
			// after the upgrade (the breakdown_confirmation migration).
			execSQL(t, url, `INSERT INTO supply_snapshots
				(event_id, sac, amount, at_ledger, breakdown_hash, poster, posted_ledger, posted_at, tx_hash)
				VALUES ('ev-h1', 'CSAC', 5, 7, 'h1', 'GPOSTER', 8, now(), 'tx-h1')`)

			s, err := Open(context.Background(), url)
			if err != nil {
				t.Fatalf("open existing database: %v", err)
			}
			defer s.Close()

			if d := schemaDiff(want, schemaOf(t, url)); len(d) > 0 {
				t.Fatalf("schema differs from a fresh database:\n%s", strings.Join(d, "\n"))
			}
			if got, want := appliedVersions(t, url), embeddedVersions(t); !slices.Equal(got, want) {
				t.Fatalf("recorded %v, want %v", got, want)
			if got := appliedVersions(t, url); !slices.Equal(got, wantVersions) {
				t.Fatalf("recorded %v, want %v", got, wantVersions)
			if got := appliedVersions(t, url); len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
				t.Fatalf("recorded %v, want [1 2 3]", got)
			if got := appliedVersions(t, url); len(got) != 4 || got[0] != 1 || got[1] != 2 || got[2] != 3 || got[3] != 4 {
				t.Fatalf("recorded %v, want [1 2 3 4]", got)
			}
			a, err := s.GetAsset(context.Background(), "CSAC")
			if err != nil || a.Code != "PUSD" {
				t.Fatalf("existing asset after migration: %+v, %v", a, err)
			}
			if body, err := s.Breakdown(context.Background(), "h1"); err != nil || string(body) != `{"total":"5"}` {
				t.Fatalf("existing breakdown after migration: %q, %v", body, err)
			}
			reporters, err := s.Reporters(context.Background())
			if err != nil || len(reporters) != 1 || reporters[0].DisplayName != "Acme" {
				t.Fatalf("existing reporter after migration: %+v, %v", reporters, err)
			}
		})
	}
}

func TestReopeningAppliesNothingTwice(t *testing.T) {
	s, url := openStore(t)
	s.Close()
	s2, err := Open(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	s2.Close()
	if got, want := appliedVersions(t, url), embeddedVersions(t); !slices.Equal(got, want) {
		t.Fatalf("recorded %v, want %v", got, want)
	want := embeddedVersions(t)
	if got := appliedVersions(t, url); !slices.Equal(got, want) {
		t.Fatalf("recorded %v, want %v", got, want)
	if got := appliedVersions(t, url); len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("recorded %v, want [1 2 3]", got)
	if got := appliedVersions(t, url); len(got) != 4 || got[0] != 1 || got[1] != 2 || got[2] != 3 || got[3] != 4 {
		t.Fatalf("recorded %v, want [1 2 3 4]", got)
	}
}

// Instances starting together serialise on the advisory lock. Without it, two
// concurrent CREATE TABLE IF NOT EXISTS can both try to create the same table
// and one fails.
func TestConcurrentStartsDoNotRace(t *testing.T) {
	url := newDatabase(t)
	const instances = 8
	var wg sync.WaitGroup
	errs := make(chan error, instances)
	start := make(chan struct{})
	for range instances {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			s, err := Open(context.Background(), url)
			if err != nil {
				errs <- err
				return
			}
			s.Close()
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent open: %v", err)
	}
	if got, want := appliedVersions(t, url), embeddedVersions(t); !slices.Equal(got, want) {
		t.Fatalf("recorded %v, want %v", got, want)
	want := embeddedVersions(t)
	if got := appliedVersions(t, url); !slices.Equal(got, want) {
		t.Fatalf("recorded %v, want %v", got, want)
	if got := appliedVersions(t, url); len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("recorded %v, want [1 2 3]", got)
	if got := appliedVersions(t, url); len(got) != 4 || got[0] != 1 || got[1] != 2 || got[2] != 3 || got[3] != 4 {
		t.Fatalf("recorded %v, want [1 2 3 4]", got)
	}
}

func tableExists(t *testing.T, p *pgxpool.Pool, name string) bool {
	t.Helper()
	var ok bool
	if err := p.QueryRow(context.Background(), `SELECT to_regclass($1) IS NOT NULL`, name).Scan(&ok); err != nil {
		t.Fatal(err)
	}
	return ok
}

// Pending migrations apply in order; a failing one leaves the database as it
// was, with nothing from that run applied or recorded.
func TestMigrateAppliesInOrderAndRollsBackOnFailure(t *testing.T) {
	ctx := context.Background()
	url := newDatabase(t)
	p := testPool(t, url)

	one := fstest.MapFS{"0001_numbers.sql": {Data: []byte(`CREATE TABLE numbers (n INT)`)}}
	if err := migrate(ctx, p, one); err != nil {
		t.Fatal(err)
	}

	// 0002 depends on 0001's table and 0003 on 0002's column, so a wrong
	// order fails. 0003 also fails on its last statement.
	broken := fstest.MapFS{
		"0001_numbers.sql": one["0001_numbers.sql"],
		"0002_more.sql":    {Data: []byte(`ALTER TABLE numbers ADD COLUMN m INT; CREATE TABLE extra (x INT)`)},
		"0003_bad.sql":     {Data: []byte(`INSERT INTO numbers (n, m) VALUES (1, 2); SELECT no_such_column FROM numbers`)},
	}
	err := migrate(ctx, p, broken)
	if err == nil || !strings.Contains(err.Error(), "apply migration 0003_bad") {
		t.Fatalf("err = %v, want 0003_bad to fail", err)
	}
	if got := appliedVersions(t, url); len(got) != 1 || got[0] != 1 {
		t.Fatalf("after a failed run recorded %v, want [1]", got)
	}
	if tableExists(t, p, "extra") {
		t.Fatal("0002 was left applied after 0003 failed")
	}

	fixed := fstest.MapFS{
		"0001_numbers.sql": broken["0001_numbers.sql"],
		"0002_more.sql":    broken["0002_more.sql"],
		"0003_bad.sql":     {Data: []byte(`INSERT INTO numbers (n, m) VALUES (1, 2)`)},
	}
	if err := migrate(ctx, p, fixed); err != nil {
		t.Fatal(err)
	}
	if got := appliedVersions(t, url); len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Fatalf("recorded %v, want [1 2 3]", got)
	}
	var m int
	if err := p.QueryRow(ctx, `SELECT m FROM numbers WHERE n = 1`).Scan(&m); err != nil || m != 2 {
		t.Fatalf("0003's row: m=%d, %v", m, err)
	}
	// A second run finds nothing pending; re-running 0003 would duplicate the row.
	if err := migrate(ctx, p, fixed); err != nil {
		t.Fatal(err)
	}
	var rows int
	if err := p.QueryRow(ctx, `SELECT count(*) FROM numbers`).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("numbers has %d rows after a second run, want 1 (%v)", rows, err)
	}
}

func TestMigrateAppliesNewMigrationWithPreviouslyAppliedVersion(t *testing.T) {
	ctx := context.Background()
	url := newDatabase(t)
	p := testPool(t, url)
	first := fstest.MapFS{
		"0001_first.sql": {Data: []byte(`CREATE TABLE first_migration (n INT)`)},
	}
	if err := migrate(ctx, p, first); err != nil {
		t.Fatal(err)
	}

	combined := fstest.MapFS{
		"0001_first.sql":  first["0001_first.sql"],
		"0001_second.sql": {Data: []byte(`CREATE TABLE second_migration (n INT)`)},
	}
	if err := migrate(ctx, p, combined); err != nil {
		t.Fatal(err)
	}
	if !tableExists(t, p, "first_migration") || !tableExists(t, p, "second_migration") {
		t.Fatal("shared-version migrations were not both applied")
	}
	var name string
	if err := p.QueryRow(ctx, `SELECT name FROM schema_migrations WHERE version = 1`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "first,second" {
		t.Fatalf("recorded migration names = %q, want %q", name, "first,second")
	}
	if err := migrate(ctx, p, combined); err != nil {
		t.Fatalf("reapply migrations: %v", err)
	}
}
