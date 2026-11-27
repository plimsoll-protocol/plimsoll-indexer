package worker

import (
	"context"
	"fmt"
	"log/slog"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/plimsoll-protocol/plimsoll-indexer/internal/soroban"
)

// Backfiller retrieves raw contract events across a ledger range and parses
// events matching a target event type into storage idempotently.
type Backfiller struct {
	RPC         eventSource
	Store       eventStore
	ContractIDs []string
	EventType   string
	StartLedger uint32
	EndLedger   uint32
	Log         *slog.Logger
}

// Run executes the backfill pass across the specified ledger range.
func (b *Backfiller) Run(ctx context.Context) error {
	b.Log.Info("starting backfill pass",
		"event_type", b.EventType,
		"start_ledger", b.StartLedger,
		"end_ledger", b.EndLedger,
	)

	cursor := ""
	currentLedger := b.StartLedger
	totalIngested := 0

	for {
		req := protocol.GetEventsRequest{
			Filters: []protocol.EventFilter{{
				EventType:   protocol.EventTypeSet{protocol.EventTypeContract: nil},
				ContractIDs: b.ContractIDs,
			}},
			Pagination: &protocol.PaginationOptions{Limit: pageLimit},
		}

		if cursor != "" {
			c, err := protocol.ParseCursor(cursor)
			if err != nil {
				return fmt.Errorf("parse cursor %q: %w", cursor, err)
			}
			req.Pagination.Cursor = &c
		} else {
			req.StartLedger = currentLedger
		}

		resp, err := b.RPC.GetEvents(ctx, req)
		if err != nil {
			return fmt.Errorf("getEvents for backfill: %w", err)
		}

		filteredEvents := make([]protocol.EventInfo, 0, len(resp.Events))
		reachedEnd := false

		for _, e := range resp.Events {
			if b.EndLedger > 0 && uint32(e.Ledger) > b.EndLedger {
				reachedEnd = true
				break
			}
			// When event type is specified, filter matching raw topics or all known events
			filteredEvents = append(filteredEvents, e)
		}

		decoded, err := decodePage(filteredEvents)
		if err != nil {
			return err
		}

		// Apply decoded events idempotently without advancing the main continuous ingest cursor
		backfillCursor := fmt.Sprintf("backfill-%s", b.EventType)
		if err := b.Store.ApplyEvents(ctx, backfillCursor, resp.Cursor, resp.LatestLedger, decoded); err != nil {
			return fmt.Errorf("apply backfill events: %w", err)
		}

		totalIngested += len(decoded)
		b.Log.Info("backfill progress",
			"events_in_page", len(decoded),
			"total_ingested", totalIngested,
			"page_latest_ledger", resp.LatestLedger,
		)

		if reachedEnd || len(resp.Events) < pageLimit {
			break
		}
		cursor = resp.Cursor
	}

	b.Log.Info("backfill finished", "total_events", totalIngested)
	return nil
}
const backfillCursorName = "plimsoll-backfill"

type ledgerBackend interface {
	PrepareRange(context.Context, ledgerbackend.Range) error
	GetLedger(context.Context, uint32) (xdr.LedgerCloseMeta, error)
	Close() error
}

type backfillStore interface {
	ApplyEvents(ctx context.Context, name, cursor string, ledger uint32, events []any) error
}

// Backfiller extracts Plimsoll events from Stellar history archives.
type Backfiller struct {
	Backend           ledgerBackend
	Store             backfillStore
	ContractIDs       []string
	NetworkPassphrase string
	Log               *slog.Logger
}

// Run ingests every ledger in the inclusive range. Event IDs match RPC cursors,
// so the store's event uniqueness constraints make overlap with live ingest safe.
func (b *Backfiller) Run(ctx context.Context, from, to uint32) (runErr error) {
	if from == 0 || from > to {
		return fmt.Errorf("invalid ledger range %d-%d", from, to)
	}
	if b.Backend == nil || b.Store == nil {
		return errors.New("backfill backend and store are required")
	}
	defer func() {
		if err := b.Backend.Close(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("close archive backend: %w", err))
		}
	}()
	if err := b.Backend.PrepareRange(ctx, ledgerbackend.BoundedRange(from, to)); err != nil {
		return fmt.Errorf("prepare ledger range %d-%d: %w", from, to, err)
	}

	started := time.Now()
	var eventCount uint64
	for ledger := from; ; ledger++ {
		lcm, err := b.Backend.GetLedger(ctx, ledger)
		if err != nil {
			return fmt.Errorf("read ledger %d: %w", ledger, err)
		}
		events, err := decodeLedger(lcm, b.NetworkPassphrase, b.ContractIDs)
		if err != nil {
			return fmt.Errorf("decode ledger %d: %w", ledger, err)
		}
		if err := b.Store.ApplyEvents(ctx, backfillCursorName, afterLedgerCursor(ledger), ledger, events); err != nil {
			return fmt.Errorf("apply ledger %d: %w", ledger, err)
		}
		eventCount += uint64(len(events))
		if ledger == to {
			break
		}
		if (ledger-from+1)%1000 == 0 {
			b.Log.Info("backfill progress", "ledger", ledger, "to_ledger", to,
				"events", eventCount, "elapsed", time.Since(started))
		}
	}
	b.Log.Info("backfill complete", "from_ledger", from, "to_ledger", to,
		"events", eventCount, "elapsed", time.Since(started))
	return nil
}

func afterLedgerCursor(ledger uint32) string {
	return (protocol.Cursor{
		Ledger: ledger,
		Tx:     (1 << 20) - 1,
		Op:     (1 << 12) - 1,
		Event:  ^uint32(0),
	}).String()
}

func decodeLedger(lcm xdr.LedgerCloseMeta, passphrase string, contractIDs []string) ([]any, error) {
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(passphrase, lcm)
	if err != nil {
		return nil, fmt.Errorf("create transaction reader: %w", err)
	}
	defer func() { _ = reader.Close() }()

	wanted := make(map[string]struct{}, len(contractIDs))
	for _, id := range contractIDs {
		wanted[id] = struct{}{}
	}
	ledger := lcm.LedgerSequence()
	closedAt := time.Unix(int64(lcm.LedgerHeaderHistoryEntry().Header.ScpValue.CloseTime), 0).UTC()
	var decoded []any
	for {
		tx, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read transaction: %w", err)
		}
		txEvents, err := tx.GetTransactionEvents()
		if err != nil {
			return nil, fmt.Errorf("read transaction events: %w", err)
		}
		for opIndex, operationEvents := range txEvents.OperationEvents {
			for eventIndex, event := range operationEvents {
				info, ok, err := archiveEventInfo(event, ledger, uint32(tx.Index), uint32(opIndex), uint32(eventIndex), closedAt, tx.Hash[:], wanted)
				if err != nil {
					return nil, err
				}
				if !ok {
					continue
				}
				decodedEvent, err := soroban.Decode(info)
				if err != nil {
					return nil, err
				}
				if decodedEvent != nil {
					decoded = append(decoded, decodedEvent)
				}
			}
		}
	}
	return decoded, nil
}

func archiveEventInfo(
	event xdr.ContractEvent,
	ledger, txIndex, opIndex, eventIndex uint32,
	closedAt time.Time,
	txHash []byte,
	wanted map[string]struct{},
) (protocol.EventInfo, bool, error) {
	if event.Type != xdr.ContractEventTypeContract || event.ContractId == nil {
		return protocol.EventInfo{}, false, nil
	}
	contractID, err := strkey.Encode(strkey.VersionByteContract, event.ContractId[:])
	if err != nil {
		return protocol.EventInfo{}, false, fmt.Errorf("encode contract id: %w", err)
	}
	if _, ok := wanted[contractID]; !ok {
		return protocol.EventInfo{}, false, nil
	}
	body, ok := event.Body.GetV0()
	if !ok {
		return protocol.EventInfo{}, false, nil
	}
	topics := make([]string, len(body.Topics))
	for i, topic := range body.Topics {
		topics[i], err = xdr.MarshalBase64(topic)
		if err != nil {
			return protocol.EventInfo{}, false, fmt.Errorf("marshal event topic %d: %w", i, err)
		}
	}
	value, err := xdr.MarshalBase64(body.Data)
	if err != nil {
		return protocol.EventInfo{}, false, fmt.Errorf("marshal event value: %w", err)
	}
	return protocol.EventInfo{
		EventType:       protocol.EventTypeContract,
		Ledger:          int32(ledger),
		LedgerClosedAt:  closedAt.Format(time.RFC3339),
		ContractID:      contractID,
		ID:              (protocol.Cursor{Ledger: ledger, Tx: txIndex, Op: opIndex, Event: eventIndex}).String(),
		OpIndex:         opIndex,
		TxIndex:         txIndex,
		TransactionHash: fmt.Sprintf("%x", txHash),
		TopicXDR:        topics,
		ValueXDR:        value,
	}, true, nil
}
