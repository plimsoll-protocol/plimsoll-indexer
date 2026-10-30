package soroban

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/stellar/go-stellar-sdk/clients/rpcclient"
	"github.com/stellar/go-stellar-sdk/keypair"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/txnbuild"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// Invoker submits contract calls signed by a single account. The account is
// both the transaction source and the address whose auth the contract
// requires, so simulation returns source-account credentials and no separate
// auth-entry signing is needed.
type Invoker struct {
	RPC        *rpcclient.Client
	Signer     *keypair.Full
	Passphrase string
}

// Result of a successful invocation.
type Result struct {
	TxHash string
	Ledger uint32
}

// Address is the account that signs and sources this invoker's calls.
func (inv *Invoker) Address() string { return inv.Signer.Address() }

// Read simulates fn on contractID with the signer as the source account.
func (inv *Invoker) Read(ctx context.Context, contractID, fn string, args []xdr.ScVal) (xdr.ScVal, error) {
	return simulateRead(ctx, inv.RPC, inv.Signer.Address(), contractID, fn, args)
}

// placeholderSource sources read-only simulations. Simulation does not load,
// charge or sequence the source account, so no real account is needed; this is
// the all-zero account id, which cannot exist and so cannot be mistaken for
// anyone's.
const placeholderSource = "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"

// Reader calls contract functions read-only, by simulation. It holds no key,
// so an instance with nothing to sign with can still read contract state.
type Reader struct {
	RPC *rpcclient.Client
}

// Read simulates fn on contractID and returns its result. Nothing is signed,
// submitted or paid for.
func (r *Reader) Read(ctx context.Context, contractID, fn string, args []xdr.ScVal) (xdr.ScVal, error) {
	return simulateRead(ctx, r.RPC, placeholderSource, contractID, fn, args)
}

func simulateRead(ctx context.Context, rpc *rpcclient.Client, source, contractID, fn string, args []xdr.ScVal) (xdr.ScVal, error) {
	var out xdr.ScVal
	contractAddr, err := ScAddress(contractID)
	if err != nil {
		return out, fmt.Errorf("contract id: %w", err)
	}
	op := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: contractAddr,
				FunctionName:    xdr.ScSymbol(fn),
				Args:            xdr.ScVec(args),
			},
		},
		SourceAccount: source,
	}
	// Simulation never consumes the sequence number, so a placeholder is fine
	// and saves a round trip to load the account.
	tx, err := build(source, 0, txnbuild.MinBaseFee, op)
	if err != nil {
		return out, err
	}
	b64, err := tx.Base64()
	if err != nil {
		return out, err
	}
	sim, err := rpc.SimulateTransaction(ctx, protocol.SimulateTransactionRequest{Transaction: b64})
	if err != nil {
		return out, fmt.Errorf("simulate: %w", err)
	}
	if sim.Error != "" {
		return out, fmt.Errorf("simulate %s: %s", fn, sim.Error)
	}
	if len(sim.Results) != 1 {
		return out, fmt.Errorf("simulate %s: want 1 result, got %d", fn, len(sim.Results))
	}
	ret := sim.Results[0].ReturnValueXDR
	if ret == nil {
		return out, fmt.Errorf("simulate %s: no return value", fn)
	}
	if err := xdr.SafeUnmarshalBase64(*ret, &out); err != nil {
		return out, fmt.Errorf("decode %s result: %w", fn, err)
	}
	return out, nil
}

// Invoke calls fn on contractID with args and waits for the result.
func (inv *Invoker) Invoke(ctx context.Context, contractID, fn string, args []xdr.ScVal) (Result, error) {
	contractAddr, err := ScAddress(contractID)
	if err != nil {
		return Result{}, fmt.Errorf("contract id: %w", err)
	}
	hostFn := xdr.HostFunction{
		Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
		InvokeContract: &xdr.InvokeContractArgs{
			ContractAddress: contractAddr,
			FunctionName:    xdr.ScSymbol(fn),
			Args:            xdr.ScVec(args),
		},
	}

	acct, err := inv.RPC.LoadAccount(ctx, inv.Signer.Address())
	if err != nil {
		return Result{}, fmt.Errorf("load account: %w", err)
	}
	seq, err := acct.GetSequenceNumber()
	if err != nil {
		return Result{}, fmt.Errorf("sequence: %w", err)
	}

	// 1. Simulate to learn the footprint, resource fee and auth entries.
	simOp := &txnbuild.InvokeHostFunction{HostFunction: hostFn, SourceAccount: inv.Signer.Address()}
	simTx, err := build(inv.Signer.Address(), seq, txnbuild.MinBaseFee, simOp)
	if err != nil {
		return Result{}, err
	}
	simB64, err := simTx.Base64()
	if err != nil {
		return Result{}, err
	}
	sim, err := inv.RPC.SimulateTransaction(ctx, protocol.SimulateTransactionRequest{Transaction: simB64})
	if err != nil {
		return Result{}, fmt.Errorf("simulate: %w", err)
	}
	if sim.Error != "" {
		return Result{}, fmt.Errorf("simulate %s: %s", fn, sim.Error)
	}
	if sim.RestorePreamble != nil {
		return Result{}, errors.New("simulate: ledger entries need restoring first")
	}
	if len(sim.Results) != 1 {
		return Result{}, fmt.Errorf("simulate: want 1 result, got %d", len(sim.Results))
	}

	var data xdr.SorobanTransactionData
	if err := xdr.SafeUnmarshalBase64(sim.TransactionDataXDR, &data); err != nil {
		return Result{}, fmt.Errorf("decode transaction data: %w", err)
	}
	var auth []xdr.SorobanAuthorizationEntry
	if sim.Results[0].AuthXDR != nil {
		for _, a := range *sim.Results[0].AuthXDR {
			var entry xdr.SorobanAuthorizationEntry
			if err := xdr.SafeUnmarshalBase64(a, &entry); err != nil {
				return Result{}, fmt.Errorf("decode auth entry: %w", err)
			}
			auth = append(auth, entry)
		}
	}

	// 2. Rebuild with resources and auth, sign and send.
	op := &txnbuild.InvokeHostFunction{
		HostFunction:  hostFn,
		SourceAccount: inv.Signer.Address(),
		Auth:          auth,
		Ext:           xdr.TransactionExt{V: 1, SorobanData: &data},
	}
	tx, err := build(inv.Signer.Address(), seq, txnbuild.MinBaseFee+sim.MinResourceFee, op)
	if err != nil {
		return Result{}, err
	}
	tx, err = tx.Sign(inv.Passphrase, inv.Signer)
	if err != nil {
		return Result{}, fmt.Errorf("sign: %w", err)
	}
	txB64, err := tx.Base64()
	if err != nil {
		return Result{}, err
	}
	sent, err := inv.RPC.SendTransaction(ctx, protocol.SendTransactionRequest{Transaction: txB64})
	if err != nil {
		return Result{}, fmt.Errorf("send: %w", err)
	}
	if sent.Status == "ERROR" || sent.Status == "TRY_AGAIN_LATER" {
		return Result{}, fmt.Errorf("send %s: status %s %s", fn, sent.Status, sent.ErrorResultXDR)
	}

	pollCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	got, err := inv.RPC.PollTransaction(pollCtx, sent.Hash)
	if err != nil {
		return Result{}, fmt.Errorf("poll %s: %w", sent.Hash, err)
	}
	if got.Status != protocol.TransactionStatusSuccess {
		return Result{}, fmt.Errorf("transaction %s: status %s", sent.Hash, got.Status)
	}
	return Result{TxHash: sent.Hash, Ledger: got.Ledger}, nil
}

func build(source string, seq int64, fee int64, op txnbuild.Operation) (*txnbuild.Transaction, error) {
	account := txnbuild.NewSimpleAccount(source, seq)
	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &account,
		IncrementSequenceNum: true,
		Operations:           []txnbuild.Operation{op},
		BaseFee:              fee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewTimeout(120)},
	})
	if err != nil {
		return nil, fmt.Errorf("build transaction: %w", err)
	}
	return tx, nil
}
