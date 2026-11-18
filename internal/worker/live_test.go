package worker

import (
	"context"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stellar/go-stellar-sdk/clients/rpcclient"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

// Reading contract state by simulation from an account that does not exist is
// the premise the reconciler rests on, and no unit test can confirm RPC allows
// it. Skipped unless PLIMSOLL_LIVE is set, so CI stays offline:
//
//	PLIMSOLL_LIVE=1 go test ./internal/worker/ -run TestLive -v
func TestLiveReadWithoutAnAccount(t *testing.T) {
	if os.Getenv("PLIMSOLL_LIVE") == "" {
		t.Skip("set PLIMSOLL_LIVE=1 to read from testnet")
	}
	rpc := rpcclient.NewClient("https://soroban-testnet.stellar.org", &http.Client{Timeout: 30 * time.Second})
	defer rpc.Close()
	sac, err := soroban.AddressVal("CCHPT4TEJDPZQDSUVW3NP6A35ROGV4WEFZKT45SKWYCFZSUB7R5HIM7S")
	if err != nil {
		t.Fatal(err)
	}
	r := &soroban.Reader{RPC: rpc}
	v, err := r.Read(context.Background(), "CC2QQ7R4FLPHZP7KA5AO4IASXXICTG6N4GQXCXITTEBNYQEYTYXKMN4D", "get_supply", []xdr.ScVal{sac})
	if err != nil {
		t.Fatal(err)
	}
	st, err := soroban.DecodeSupplyState(v)
	if err != nil {
		t.Fatal(err)
	}
	if st == nil {
		t.Fatal("no snapshot posted on testnet for PUSD")
	}
	t.Logf("live: ledger=%d amount=%s posted=%s hash=%s", st.Ledger, st.Amount, st.Timestamp, st.BreakdownHash)
}
