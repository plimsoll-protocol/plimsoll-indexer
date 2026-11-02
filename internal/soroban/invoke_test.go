package soroban

import (
	"context"
	"strings"
	"testing"

	"github.com/stellar/go-stellar-sdk/keypair"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"
)

const testPassphrase = "Test SDF Network ; September 2015"

// sentTx is one transaction the fake was asked to send, decoded far enough to
// say what kind of operation it carried.
type sentTx struct {
	restore bool
	fee     int64
}

// fakeRPC answers an invocation with a scripted sequence of simulations. Each
// call to SimulateTransaction takes the next entry, so a preamble followed by a
// result is how an archived entry then a working call is expressed.
type fakeRPC struct {
	t    *testing.T
	sims []protocol.SimulateTransactionResponse
	// seq is what LoadAccount reports, raised on every send so a reused
	// sequence number would be visible.
	seq       int64
	simCalls  int
	loadCalls int
	sent      []sentTx
	pollFail  bool
}

func (f *fakeRPC) LoadAccount(_ context.Context, address string) (txnbuild.Account, error) {
	f.loadCalls++
	acct := txnbuild.NewSimpleAccount(address, f.seq)
	return &acct, nil
}

func (f *fakeRPC) SimulateTransaction(_ context.Context, _ protocol.SimulateTransactionRequest) (protocol.SimulateTransactionResponse, error) {
	if f.simCalls >= len(f.sims) {
		f.t.Fatalf("unexpected simulate call %d; only %d scripted", f.simCalls+1, len(f.sims))
	}
	out := f.sims[f.simCalls]
	f.simCalls++
	return out, nil
}

func (f *fakeRPC) SendTransaction(_ context.Context, req protocol.SendTransactionRequest) (protocol.SendTransactionResponse, error) {
	var env xdr.TransactionEnvelope
	if err := xdr.SafeUnmarshalBase64(req.Transaction, &env); err != nil {
		f.t.Fatalf("sent transaction does not decode: %v", err)
	}
	ops := env.Operations()
	if len(ops) != 1 {
		f.t.Fatalf("want 1 operation, got %d", len(ops))
	}
	if len(env.Signatures()) == 0 {
		f.t.Fatal("transaction was sent unsigned")
	}
	f.sent = append(f.sent, sentTx{
		restore: ops[0].Body.Type == xdr.OperationTypeRestoreFootprint,
		fee:     int64(env.Fee()),
	})
	f.seq++
	return protocol.SendTransactionResponse{Status: "PENDING", Hash: hashFor(len(f.sent))}, nil
}

func (f *fakeRPC) PollTransaction(_ context.Context, txHash string) (protocol.GetTransactionResponse, error) {
	if f.pollFail {
		return protocol.GetTransactionResponse{Status: protocol.TransactionStatusFailed}, nil
	}
	return protocol.GetTransactionResponse{
		Status: protocol.TransactionStatusSuccess,
		Ledger: 5_000_000 + uint32(len(txHash)),
	}, nil
}

func hashFor(n int) string {
	return strings.Repeat("a", 63) + string(rune('0'+n))
}

// sorobanData is the base64 SorobanTransactionData both a simulation result and
// a restore preamble carry.
func sorobanData(t *testing.T, fee int64) string {
	t.Helper()
	s, err := xdr.MarshalBase64(xdr.SorobanTransactionData{ResourceFee: xdr.Int64(fee)})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// Fees the fixtures use. The embedded SorobanData.ResourceFee differs from the
// advertised MinResourceFee so that a fee assertion can only pass if the right
// one of the two simulations was the source: txnbuild charges
// BaseFee*operations plus SorobanData.ResourceFee, and the invoker passes
// MinBaseFee+MinResourceFee as the base fee.
const (
	callResourceFee    = 900
	callMinResourceFee = 1234
	callFee            = txnbuild.MinBaseFee + callMinResourceFee + callResourceFee

	restoreResourceFee    = 5000
	restoreMinResourceFee = 7777
	restoreFee            = txnbuild.MinBaseFee + restoreMinResourceFee + restoreResourceFee
)

// okSim is a simulation that returns a result and needs no restore.
func okSim(t *testing.T) protocol.SimulateTransactionResponse {
	t.Helper()
	return protocol.SimulateTransactionResponse{
		TransactionDataXDR: sorobanData(t, callResourceFee),
		MinResourceFee:     callMinResourceFee,
		Results:            []protocol.SimulateHostFunctionResult{{}},
	}
}

// archivedSim is what RPC returns when the entries the call touches have been
// archived: no result, and a preamble describing the restore to send first.
func archivedSim(t *testing.T) protocol.SimulateTransactionResponse {
	t.Helper()
	return protocol.SimulateTransactionResponse{
		RestorePreamble: &protocol.RestorePreamble{
			TransactionDataXDR: sorobanData(t, restoreResourceFee),
			MinResourceFee:     restoreMinResourceFee,
		},
	}
}

func newInvoker(f *fakeRPC) *Invoker {
	return &Invoker{RPC: f, Signer: keypair.MustRandom(), Passphrase: testPassphrase}
}

func TestInvokeRestoresArchivedEntriesThenRetries(t *testing.T) {
	f := &fakeRPC{t: t, sims: []protocol.SimulateTransactionResponse{archivedSim(t), okSim(t)}, seq: 41}
	res, err := newInvoker(f).Invoke(context.Background(), cAddr, "post_supply", nil)
	if err != nil {
		t.Fatalf("invoke: %v", err)
	}

	// A restore, then the call it was blocking.
	if len(f.sent) != 2 {
		t.Fatalf("want 2 transactions sent, got %d", len(f.sent))
	}
	if !f.sent[0].restore {
		t.Error("first transaction should have been a RestoreFootprint")
	}
	if f.sent[1].restore {
		t.Error("second transaction should have been the contract call, not another restore")
	}
	// The restore is priced from the preamble, the call from its own
	// simulation. Paying the call's fee for the restore would underpay it.
	if f.sent[0].fee != restoreFee {
		t.Errorf("restore fee = %d, want %d from the preamble", f.sent[0].fee, restoreFee)
	}
	if f.sent[1].fee != callFee {
		t.Errorf("call fee = %d, want %d from its own simulation", f.sent[1].fee, callFee)
	}
	// The restore consumed a sequence number, so the retry has to have read it
	// again rather than reusing the one from the first attempt.
	if f.loadCalls != 3 {
		t.Errorf("LoadAccount called %d times, want 3 (attempt, restore, retry)", f.loadCalls)
	}
	if res.TxHash != hashFor(2) {
		t.Errorf("result reports %s, want the retried call's hash %s", res.TxHash, hashFor(2))
	}
}

func TestInvokeSendsNoRestoreWhenNothingIsArchived(t *testing.T) {
	f := &fakeRPC{t: t, sims: []protocol.SimulateTransactionResponse{okSim(t)}, seq: 7}
	if _, err := newInvoker(f).Invoke(context.Background(), cAddr, "post_supply", nil); err != nil {
		t.Fatalf("invoke: %v", err)
	}
	if len(f.sent) != 1 || f.sent[0].restore {
		t.Fatalf("want exactly one non-restore transaction, got %+v", f.sent)
	}
	if f.simCalls != 1 {
		t.Errorf("simulated %d times, want 1", f.simCalls)
	}
}

// Retrying for ever would spend fees in a loop on something a restore cannot
// fix, so a second preamble is reported rather than chased.
func TestInvokeGivesUpWhenEntriesAreStillArchived(t *testing.T) {
	f := &fakeRPC{t: t, sims: []protocol.SimulateTransactionResponse{archivedSim(t), archivedSim(t)}, seq: 1}
	_, err := newInvoker(f).Invoke(context.Background(), cAddr, "post_supply", nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "still archived after a restore") {
		t.Fatalf("error does not say why it gave up: %v", err)
	}
	if len(f.sent) != 1 || !f.sent[0].restore {
		t.Fatalf("want only the one restore to have been sent, got %+v", f.sent)
	}
}

// A restore that is accepted but then fails on chain must not be followed by
// the call, which would fail the same way and pay for the privilege.
func TestInvokeStopsWhenTheRestoreFails(t *testing.T) {
	f := &fakeRPC{t: t, sims: []protocol.SimulateTransactionResponse{archivedSim(t)}, seq: 1, pollFail: true}
	_, err := newInvoker(f).Invoke(context.Background(), cAddr, "post_supply", nil)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "restore archived entries") {
		t.Fatalf("error does not name the restore: %v", err)
	}
	if len(f.sent) != 1 {
		t.Fatalf("want only the failed restore sent, got %d transactions", len(f.sent))
	}
}

// A simulation error is the contract refusing the call and has nothing to do
// with archiving; it is reported as-is.
func TestInvokeReportsSimulationErrors(t *testing.T) {
	f := &fakeRPC{t: t, sims: []protocol.SimulateTransactionResponse{{Error: "HostError: Error(Contract, #8)"}}, seq: 1}
	_, err := newInvoker(f).Invoke(context.Background(), cAddr, "post_supply", nil)
	if err == nil || !strings.Contains(err.Error(), "Error(Contract, #8)") {
		t.Fatalf("want the contract error passed through, got %v", err)
	}
	if len(f.sent) != 0 {
		t.Fatalf("nothing should have been sent, got %d transactions", len(f.sent))
	}
}

// Reader holds no key, so a read must reach the contract without one: it is
// what lets reconciliation run on an instance with nothing to sign with.
func TestReaderNeedsNoKey(t *testing.T) {
	ret := b64(t, sym("ok"))
	f := &fakeRPC{t: t, sims: []protocol.SimulateTransactionResponse{{
		Results: []protocol.SimulateHostFunctionResult{{ReturnValueXDR: &ret}},
	}}}
	v, err := (&Reader{RPC: f}).Read(context.Background(), cAddr, "get_supply", nil)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got, err := AsSymbol(v); err != nil || got != "ok" {
		t.Fatalf("read returned %q (%v), want \"ok\"", got, err)
	}
	if len(f.sent) != 0 {
		t.Error("a read sent a transaction")
	}
}
