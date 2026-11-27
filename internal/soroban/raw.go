package soroban

import (
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
)

// RawEvent is a contract event exactly as RPC returned it, before decoding.
//
// Ingestion stores one for every event it receives, including the ones Decode
// does not track, so the typed tables can be rebuilt with a newer decoder
// after RPC has stopped serving the event.
type RawEvent struct {
	ID       string
	Contract string
	Ledger   uint32
	// ClosedAt is the zero time when RPC's ledgerClosedAt did not parse.
	ClosedAt time.Time
	TxHash   string
	TopicXDR []string
	ValueXDR string
}

// NewRawEvent keeps the parts of an RPC event that Decode reads.
func NewRawEvent(e protocol.EventInfo) RawEvent {
	closedAt, _ := time.Parse(time.RFC3339, e.LedgerClosedAt)
	return RawEvent{
		ID:       e.ID,
		Contract: e.ContractID,
		Ledger:   uint32(e.Ledger),
		ClosedAt: closedAt.UTC(),
		TxHash:   e.TransactionHash,
		TopicXDR: e.TopicXDR,
		ValueXDR: e.ValueXDR,
	}
}

// Info rebuilds the RPC event, so Decode reads a stored event the way it
// read the live one.
func (r RawEvent) Info() protocol.EventInfo {
	info := protocol.EventInfo{
		EventType:       protocol.EventTypeContract,
		Ledger:          int32(r.Ledger),
		ContractID:      r.Contract,
		ID:              r.ID,
		TransactionHash: r.TxHash,
		TopicXDR:        r.TopicXDR,
		ValueXDR:        r.ValueXDR,
	}
	if !r.ClosedAt.IsZero() {
		info.LedgerClosedAt = r.ClosedAt.UTC().Format(time.RFC3339Nano)
	}
	return info
}
