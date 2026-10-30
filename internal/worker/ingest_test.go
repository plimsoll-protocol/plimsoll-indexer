package worker

import (
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

// A getEvents page in the shape testnet RPC serves today: no
// inSuccessfulContractCall field at all. Unmarshalling leaves that bool false,
// so filtering on it drops every event and ingestion stops without raising an
// error anywhere.
const rpcPageWithoutSuccessFlag = `[
  {
    "type": "contract",
    "ledger": 5453220,
    "ledgerClosedAt": "2026-10-29T16:02:20Z",
    "contractId": "CC2QQ7R4FLPHZP7KA5AO4IASXXICTG6N4GQXCXITTEBNYQEYTYXKMN4D",
    "id": "0023421401557905408-0000000000",
    "operationIndex": 0,
    "transactionIndex": 3,
    "txHash": "ec9f24cf8f1a805ef9fa98c298df69c29732079af49df66f34a3e9a63f20d711",
    "topic": [%q, %q],
    "value": %q
  }
]`

func symVal(t *testing.T, s string) xdr.ScVal {
	t.Helper()
	sym := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
}

func mapVal(t *testing.T, fields map[string]xdr.ScVal) xdr.ScVal {
	t.Helper()
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	m := make(xdr.ScMap, 0, len(keys))
	for _, k := range keys {
		m = append(m, xdr.ScMapEntry{Key: symVal(t, k), Val: fields[k]})
	}
	mp := &m
	return xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &mp}
}

func b64(t *testing.T, v xdr.ScVal) string {
	t.Helper()
	s, err := xdr.MarshalBase64(v)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func must(t *testing.T) func(xdr.ScVal, error) xdr.ScVal {
	t.Helper()
	return func(v xdr.ScVal, err error) xdr.ScVal {
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
}

func supplyPostedPage(t *testing.T) []protocol.EventInfo {
	t.Helper()
	sac := must(t)(soroban.AddressVal("CCHPT4TEJDPZQDSUVW3NP6A35ROGV4WEFZKT45SKWYCFZSUB7R5HIM7S"))
	value := mapVal(t, map[string]xdr.ScVal{
		"amount":         must(t)(soroban.I128Val(big.NewInt(9_999_000000000))),
		"breakdown_hash": must(t)(soroban.BytesN32Val(strings.Repeat("ab", 32))),
		"ledger":         soroban.U32Val(5453219),
		"poster":         must(t)(soroban.AddressVal("GDACI55IMI5DDZWMZ2EAXWK57AV75VE4626YYBAHQJYDV7QFPNNFK6HJ")),
	})
	body := fmt.Sprintf(rpcPageWithoutSuccessFlag,
		b64(t, symVal(t, soroban.EventSupplyPosted)), b64(t, sac), b64(t, value))

	var page []protocol.EventInfo
	if err := json.Unmarshal([]byte(body), &page); err != nil {
		t.Fatal(err)
	}
	if page[0].InSuccessfulContractCall {
		t.Fatal("fixture must leave inSuccessfulContractCall false")
	}
	return page
}

func TestDecodePageKeepsEventsWithoutSuccessFlag(t *testing.T) {
	decoded, err := decodePage(supplyPostedPage(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 1 {
		t.Fatalf("want 1 decoded event, got %d", len(decoded))
	}
	sp, ok := decoded[0].(soroban.SupplyPosted)
	if !ok {
		t.Fatalf("got %T", decoded[0])
	}
	if sp.AtLedger != 5453219 || sp.Amount.Int64() != 9_999_000000000 {
		t.Errorf("%+v", sp)
	}
}

func TestDecodePageDropsUntrackedEvents(t *testing.T) {
	page := supplyPostedPage(t)
	page[0].TopicXDR[0] = b64(t, symVal(t, "something_else"))
	decoded, err := decodePage(page)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded) != 0 {
		t.Fatalf("want untracked event dropped, got %d", len(decoded))
	}
}
