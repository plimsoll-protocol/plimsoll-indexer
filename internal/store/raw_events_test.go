package store

import (
	"context"
	"math/big"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

const (
	rawTestLedgerID = "CC2QQ7R4FLPHZP7KA5AO4IASXXICTG6N4GQXCXITTEBNYQEYTYXKMN4D"
	rawTestSAC      = "CCHPT4TEJDPZQDSUVW3NP6A35ROGV4WEFZKT45SKWYCFZSUB7R5HIM7S"
	rawTestAccount  = "GDACI55IMI5DDZWMZ2EAXWK57AV75VE4626YYBAHQJYDV7QFPNNFK6HJ"
)

func rawXDR(t *testing.T, v xdr.ScVal) string {
	t.Helper()
	s, err := xdr.MarshalBase64(v)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func rawSym(s string) xdr.ScVal {
	sym := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
}

func rawStr(s string) xdr.ScVal {
	str := xdr.ScString(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvString, Str: &str}
}

func rawU64(v uint64) xdr.ScVal {
	u := xdr.Uint64(v)
	return xdr.ScVal{Type: xdr.ScValTypeScvU64, U64: &u}
}

func rawOK(t *testing.T) func(xdr.ScVal, error) xdr.ScVal {
	return func(v xdr.ScVal, err error) xdr.ScVal {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

// rawMap builds a contract event's data map; keys must be given sorted, as the
// host orders them.
func rawMap(kv ...any) xdr.ScVal {
	m := xdr.ScMap{}
	for i := 0; i < len(kv); i += 2 {
		m = append(m, xdr.ScMapEntry{Key: rawSym(kv[i].(string)), Val: kv[i+1].(xdr.ScVal)})
	}
	mp := &m
	return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &mp}
}

// rawPage is one getEvents page from the two contracts: every event type the
// indexer decodes, plus admin_transferred, which it does not track.
func rawPage(t *testing.T) []protocol.EventInfo {
	t.Helper()
	ok := rawOK(t)
	sac := ok(soroban.AddressVal(rawTestSAC))
	account := ok(soroban.AddressVal(rawTestAccount))
	hash := ok(soroban.BytesN32Val(strings.Repeat("ab", 32)))
	ev := func(n int, ledger int32, topics []xdr.ScVal, value xdr.ScVal) protocol.EventInfo {
		enc := make([]string, len(topics))
		for i, tp := range topics {
			enc[i] = rawXDR(t, tp)
		}
		return protocol.EventInfo{
			EventType:       protocol.EventTypeContract,
			ID:              "00000000" + string(rune('0'+n)) + "0000000000-0000000001",
			Ledger:          ledger,
			LedgerClosedAt:  "2026-11-20T10:0" + string(rune('0'+n)) + ":00Z",
			ContractID:      rawTestLedgerID,
			TransactionHash: strings.Repeat(string(rune('a'+n)), 64),
			TopicXDR:        enc,
			ValueXDR:        rawXDR(t, value),
		}
	}
	return []protocol.EventInfo{
		ev(1, 100, []xdr.ScVal{rawSym(soroban.EventAssetListed), sac},
			rawMap("code", rawStr("PUSD"), "issuer", account)),
		ev(2, 101, []xdr.ScVal{rawSym(soroban.EventReporterSet), account},
			rawMap("name", rawStr("Acme Audit"), "role", soroban.U32Val(3))),
		ev(3, 102, []xdr.ScVal{rawSym(soroban.EventSupplyPosted), sac},
			rawMap("amount", ok(soroban.I128Val(big.NewInt(9_999_000000000))), "breakdown_hash", hash,
				"ledger", soroban.U32Val(101), "poster", account)),
		ev(4, 103, []xdr.ScVal{rawSym(soroban.EventReservePosted), sac, soroban.U32Val(2)},
			rawMap("amount", ok(soroban.I128Val(big.NewInt(10_200_000000000))), "as_of", rawU64(1_795_000_000),
				"doc_hash", hash, "doc_uri", rawStr("https://example.com/attestation.pdf"), "reporter", account)),
		ev(5, 104, []xdr.ScVal{rawSym("admin_transferred")}, rawMap("new_admin", account)),
		ev(6, 105, []xdr.ScVal{rawSym(soroban.EventReporterRevoked), account}, xdr.ScVal{Type: xdr.ScValTypeScvVoid}),
	}
}

// rawSplit does what ingestion does with a page: every event raw, and the
// typed rows for the ones the decoder tracks.
func rawSplit(t *testing.T, page []protocol.EventInfo) ([]soroban.RawEvent, []any) {
	t.Helper()
	var raw []soroban.RawEvent
	var decoded []any
	for _, e := range page {
		ev, err := soroban.Decode(e)
		if err != nil {
			t.Fatal(err)
		}
		raw = append(raw, soroban.NewRawEvent(e))
		if ev != nil {
			decoded = append(decoded, ev)
		}
	}
	return raw, decoded
}

// typedRows is every row of the tables built from decoded events, as text.
func typedRows(t *testing.T, url string) map[string][]string {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	out := map[string][]string{}
	for _, table := range []string{"assets", "supply_snapshots", "reserve_reports", "reporters"} {
		rows, err := conn.Query(ctx, `SELECT r::text FROM `+table+` r ORDER BY 1`)
		if err != nil {
			t.Fatal(err)
		}
		out[table], err = pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			t.Fatal(err)
		}
	}
	return out
}

func rawCount(t *testing.T, url string) int {
	t.Helper()
	conn, err := pgx.Connect(context.Background(), url)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	var n int
	if err := conn.QueryRow(context.Background(), `SELECT count(*) FROM raw_events`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestEveryEventIsStoredRawExactlyOnce(t *testing.T) {
	ctx := context.Background()
	s, url := openStore(t)
	page := rawPage(t)
	raw, decoded := rawSplit(t, page)
	if len(decoded) != 5 {
		t.Fatalf("fixture decodes to %d events, want 5", len(decoded))
	}

	if err := s.ApplyEvents(ctx, "plimsoll-events", "c1", 105, raw, decoded); err != nil {
		t.Fatal(err)
	}
	// The same page again, as after a crash between the write and the next
	// poll: nothing is stored twice.
	if err := s.ApplyEvents(ctx, "plimsoll-events", "c1", 105, raw, decoded); err != nil {
		t.Fatal(err)
	}
	if n := rawCount(t, url); n != len(page) {
		t.Fatalf("raw_events has %d rows, want %d", n, len(page))
	}

	got, err := s.rawEventsAfter(ctx, "", 100)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, raw) {
		t.Fatalf("raw events read back differ:\n got %+v\nwant %+v", got, raw)
	}
	// The untracked event is kept byte for byte, so a later decoder sees what
	// RPC sent.
	if got[4].Info().TopicXDR[0] != page[4].TopicXDR[0] || got[4].ValueXDR != page[4].ValueXDR {
		t.Fatalf("admin_transferred stored as %+v", got[4])
	}

	rows := typedRows(t, url)
	for table, want := range map[string]int{"assets": 1, "supply_snapshots": 1, "reserve_reports": 1, "reporters": 1} {
		if len(rows[table]) != want {
			t.Errorf("%s has %d rows, want %d: %v", table, len(rows[table]), want, rows[table])
		}
	}
}

// A database that has only the raw events gets the same typed tables from a
// replay as ingestion built, and replaying again changes nothing.
func TestReplayRebuildsTheTypedTablesOnAnEmptyDatabase(t *testing.T) {
	ctx := context.Background()
	raw, decoded := rawSplit(t, rawPage(t))

	ingested, ingestedURL := openStore(t)
	if err := ingested.ApplyEvents(ctx, "plimsoll-events", "c1", 105, raw, decoded); err != nil {
		t.Fatal(err)
	}
	want := typedRows(t, ingestedURL)

	empty, emptyURL := openStore(t)
	if err := empty.ApplyEvents(ctx, "plimsoll-events", "c1", 105, raw, nil); err != nil {
		t.Fatal(err)
	}
	for table, rows := range typedRows(t, emptyURL) {
		if len(rows) != 0 {
			t.Fatalf("%s is not empty before the replay: %v", table, rows)
		}
	}

	// A batch of 2 makes the replay page through the six events.
	stats, err := empty.ReplayRawEvents(ctx, 2)
	if err != nil {
		t.Fatal(err)
	}
	if stats != (ReplayStats{Events: 6, Decoded: 5}) {
		t.Fatalf("stats = %+v, want 6 events, 5 decoded", stats)
	}
	if got := typedRows(t, emptyURL); !reflect.DeepEqual(got, want) {
		t.Fatalf("replayed tables differ from ingested ones:\n got %v\nwant %v", got, want)
	}

	if _, err := empty.ReplayRawEvents(ctx, 500); err != nil {
		t.Fatal(err)
	}
	if got := typedRows(t, emptyURL); !reflect.DeepEqual(got, want) {
		t.Fatalf("a second replay changed the tables:\n got %v\nwant %v", got, want)
	}
}

// An event the decoder rejects stops the replay and names the event.
func TestReplayStopsAtAnEventItCannotDecode(t *testing.T) {
	ctx := context.Background()
	s, _ := openStore(t)
	raw, _ := rawSplit(t, rawPage(t))
	bad := raw[0]
	bad.ID = "0000000099-0000000001"
	bad.TopicXDR = []string{"not base64 xdr"}
	if err := s.ApplyEvents(ctx, "plimsoll-events", "c1", 105, append(raw, bad), nil); err != nil {
		t.Fatal(err)
	}

	stats, err := s.ReplayRawEvents(ctx, 500)
	if err == nil || !strings.Contains(err.Error(), bad.ID) {
		t.Fatalf("err = %v, want the replay to stop at %s", err, bad.ID)
	}
	if stats.Events != 0 {
		t.Fatalf("stats = %+v; the failing batch must not be counted", stats)
	}
}
