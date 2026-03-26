package indexer

import (
	"math"
	"strings"

	"github.com/koinos/koinos-proto-golang/v2/koinos/protocol"
	"github.com/mr-tron/base58"
	"google.golang.org/protobuf/encoding/protowire"
)

const maxSafeValue = uint64(math.MaxInt64)

// Token contract addresses.
const (
	KoinContract    = "19GYjDBVXU7keLbYvMLazsGQn3GTWHjHkK" // new KCS-4
	VhpContract     = "12Y5vW6gk8GceH53YfRkRre2Rrcsgw7Naq" // new KCS-4
	OldKoinContract = "15DJN4a8SgrbGhhGksSBASiSYjGnMU8dGL" // old KCS-1
	OldVhpContract  = "1AdzuXSpC6K9qtXdCBgD5NUpDNwHjMgrc9" // old KCS-1
)

// isTokenEvent returns true if the source is any tracked token (old or new).
func isTokenEvent(source string) bool {
	return source == KoinContract || source == VhpContract ||
		source == OldKoinContract || source == OldVhpContract
}

// KCS4MigrationHeight is the block where old KCS-1 contracts were replaced by KCS-4.
// At this height, VHP balances must be reset to zero because the new VHP contract
// was deployed with pre-loaded state (no burn on old contract = double-counting).
const KCS4MigrationHeight = uint64(24804034)

// affectsBalance returns true for contracts whose events should update balances.
// Old KOIN is always tracked (migration did proper burn/mint, net zero).
// Old VHP is tracked ONLY before migration height. At migration, VHP balances
// are reset and only new VHP events are counted from that point forward.
func affectsBalance(source string, height uint64) bool {
	switch source {
	case KoinContract, OldKoinContract:
		return true
	case VhpContract:
		return true
	case OldVhpContract:
		return height < KCS4MigrationHeight
	default:
		return false
	}
}

// normalizeToken maps old contract addresses to new ones for unified transfer history.
func normalizeToken(source string) string {
	if source == OldKoinContract {
		return KoinContract
	}
	if source == OldVhpContract {
		return VhpContract
	}
	return source
}

// BlockResult contains all data extracted from processing a single block.
type BlockResult struct {
	Height    uint64
	BlockID   []byte
	Timestamp uint64
	Signer    string
	TxCount   int

	// Unique addresses found in this block.
	Addresses []string

	// Balance changes from token transfer/mint/burn events.
	BalanceChanges []BalanceChange

	// Transfer records for history tracking.
	Transfers []TransferRecord
}

// TransferRecord represents a token transfer/mint/burn to be stored in history.
type TransferRecord struct {
	TxID      string
	Token     string
	From      string
	To        string
	Value     uint64
	EventType string // "transfer", "mint", "burn"
}

// BalanceChange represents a single balance modification from a token event.
type BalanceChange struct {
	Address string
	Token   string // contract address (KoinContract or VhpContract)
	Delta   int64  // positive = received, negative = sent (in satoshis)
}

// ProcessBlock extracts addresses and balance changes from a block and its receipt.
// It returns nil if block is nil.
func ProcessBlock(block *protocol.Block, receipt *protocol.BlockReceipt) *BlockResult {
	if block == nil {
		return nil
	}

	result := &BlockResult{}
	addrSet := make(map[string]struct{})

	// --- Block header fields ---
	if block.Header != nil {
		result.Height = block.Header.Height
		result.Timestamp = block.Header.Timestamp
		result.Signer = encodeAddr(block.Header.Signer)
		if result.Signer != "" {
			addrSet[result.Signer] = struct{}{}
		}
	}

	result.BlockID = block.Id
	result.TxCount = len(block.Transactions)

	// --- Transaction header fields (payer, payee) ---
	for _, tx := range block.Transactions {
		if tx == nil || tx.Header == nil {
			continue
		}
		if addr := encodeAddr(tx.Header.Payer); addr != "" {
			addrSet[addr] = struct{}{}
		}
		if addr := encodeAddr(tx.Header.Payee); addr != "" {
			addrSet[addr] = struct{}{}
		}
	}

	// --- Events from receipt ---
	if receipt != nil {
		// Block-level events (no tx ID)
		for _, ev := range receipt.Events {
			processEvent(ev, "", result.Height, addrSet, &result.BalanceChanges, &result.Transfers)
		}

		// Per-transaction events (skip reverted transactions)
		for i, txReceipt := range receipt.TransactionReceipts {
			if txReceipt == nil || txReceipt.Reverted {
				continue
			}
			// Get tx ID from the block's transaction list
			txID := ""
			if i < len(block.Transactions) && block.Transactions[i] != nil {
				txID = encodeAddr(block.Transactions[i].Id)
			}
			for _, ev := range txReceipt.Events {
				processEvent(ev, txID, result.Height, addrSet, &result.BalanceChanges, &result.Transfers)
			}
		}
	}

	// Collect addresses from balance changes into the address set too,
	// since they are participants in this block.
	for i := range result.BalanceChanges {
		if result.BalanceChanges[i].Address != "" {
			addrSet[result.BalanceChanges[i].Address] = struct{}{}
		}
	}

	// Deduplicate into sorted slice.
	result.Addresses = make([]string, 0, len(addrSet))
	for addr := range addrSet {
		result.Addresses = append(result.Addresses, addr)
	}

	return result
}

// processEvent extracts addresses from an event and, if the event source is a
// tracked token contract, parses the event data for balance changes.
func processEvent(ev *protocol.EventData, txID string, height uint64, addrSet map[string]struct{}, changes *[]BalanceChange, transfers *[]TransferRecord) {
	if ev == nil {
		return
	}

	// Collect the event source address.
	source := encodeAddr(ev.Source)
	if source != "" {
		addrSet[source] = struct{}{}
	}

	// Collect all impacted addresses.
	for _, imp := range ev.Impacted {
		if addr := encodeAddr(imp); addr != "" {
			addrSet[addr] = struct{}{}
		}
	}

	// Only parse events from known token contracts (old + new).
	if !isTokenEvent(source) {
		return
	}

	// Normalize old contract addresses to new ones for unified history.
	token := normalizeToken(source)
	// Balance tracking depends on contract and height (old VHP excluded after migration).
	updateBalance := affectsBalance(source, height)

	name := ev.Name
	switch {
	case strings.Contains(name, "transfer_event") || name == "koinos.contracts.token.transfer_event":
		from, to, value := parseTransferEvent(ev.Data)
		if value == 0 || value > maxSafeValue {
			return
		}
		fromAddr := encodeAddr(from)
		toAddr := encodeAddr(to)
		if updateBalance {
			if fromAddr != "" {
				*changes = append(*changes, BalanceChange{Address: fromAddr, Token: token, Delta: -int64(value)})
			}
			if toAddr != "" {
				*changes = append(*changes, BalanceChange{Address: toAddr, Token: token, Delta: int64(value)})
			}
		}
		*transfers = append(*transfers, TransferRecord{TxID: txID, Token: token, From: fromAddr, To: toAddr, Value: value, EventType: "transfer"})

	case strings.Contains(name, "mint_event") || name == "koinos.contracts.token.mint_event":
		to, value := parseMintEvent(ev.Data)
		if value == 0 || value > maxSafeValue {
			return
		}
		toAddr := encodeAddr(to)
		if updateBalance && toAddr != "" {
			*changes = append(*changes, BalanceChange{Address: toAddr, Token: token, Delta: int64(value)})
		}
		*transfers = append(*transfers, TransferRecord{TxID: txID, Token: token, From: "", To: toAddr, Value: value, EventType: "mint"})

	case strings.Contains(name, "burn_event") || name == "koinos.contracts.token.burn_event":
		from, value := parseBurnEvent(ev.Data)
		if value == 0 || value > maxSafeValue {
			return
		}
		fromAddr := encodeAddr(from)
		if updateBalance && fromAddr != "" {
			*changes = append(*changes, BalanceChange{Address: fromAddr, Token: token, Delta: -int64(value)})
		}
		*transfers = append(*transfers, TransferRecord{TxID: txID, Token: token, From: fromAddr, To: "", Value: value, EventType: "burn"})
	}
}

// parseTransferEvent decodes a transfer_event protobuf message:
//
//	field 1 (bytes): from address
//	field 2 (bytes): to address
//	field 3 (varint): value
func parseTransferEvent(data []byte) (from, to []byte, value uint64) {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return
		}
		data = data[n:]

		switch {
		case num == 1 && typ == protowire.BytesType:
			v, vn := protowire.ConsumeBytes(data)
			if vn < 0 {
				return
			}
			from = append([]byte(nil), v...)
			data = data[vn:]
		case num == 2 && typ == protowire.BytesType:
			v, vn := protowire.ConsumeBytes(data)
			if vn < 0 {
				return
			}
			to = append([]byte(nil), v...)
			data = data[vn:]
		case num == 3 && typ == protowire.VarintType:
			v, vn := protowire.ConsumeVarint(data)
			if vn < 0 {
				return
			}
			value = v
			data = data[vn:]
		default:
			n := consumeUnknownField(typ, data)
			if n < 0 {
				return
			}
			data = data[n:]
		}
	}
	return
}

// parseMintEvent decodes a mint_event protobuf message:
//
//	field 1 (bytes): to address
//	field 2 (varint): value
func parseMintEvent(data []byte) (to []byte, value uint64) {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return
		}
		data = data[n:]

		switch {
		case num == 1 && typ == protowire.BytesType:
			v, vn := protowire.ConsumeBytes(data)
			if vn < 0 {
				return
			}
			to = append([]byte(nil), v...)
			data = data[vn:]
		case num == 2 && typ == protowire.VarintType:
			v, vn := protowire.ConsumeVarint(data)
			if vn < 0 {
				return
			}
			value = v
			data = data[vn:]
		default:
			n := consumeUnknownField(typ, data)
			if n < 0 {
				return
			}
			data = data[n:]
		}
	}
	return
}

// parseBurnEvent decodes a burn_event protobuf message:
//
//	field 1 (bytes): from address
//	field 2 (varint): value
func parseBurnEvent(data []byte) (from []byte, value uint64) {
	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return
		}
		data = data[n:]

		switch {
		case num == 1 && typ == protowire.BytesType:
			v, vn := protowire.ConsumeBytes(data)
			if vn < 0 {
				return
			}
			from = append([]byte(nil), v...)
			data = data[vn:]
		case num == 2 && typ == protowire.VarintType:
			v, vn := protowire.ConsumeVarint(data)
			if vn < 0 {
				return
			}
			value = v
			data = data[vn:]
		default:
			n := consumeUnknownField(typ, data)
			if n < 0 {
				return
			}
			data = data[n:]
		}
	}
	return
}

// consumeUnknownField skips over an unknown protobuf field and returns the
// number of bytes consumed. Returns -1 on error.
func consumeUnknownField(typ protowire.Type, data []byte) int {
	switch typ {
	case protowire.VarintType:
		_, n := protowire.ConsumeVarint(data)
		return n
	case protowire.Fixed32Type:
		_, n := protowire.ConsumeFixed32(data)
		return n
	case protowire.Fixed64Type:
		_, n := protowire.ConsumeFixed64(data)
		return n
	case protowire.BytesType:
		_, n := protowire.ConsumeBytes(data)
		return n
	case protowire.StartGroupType:
		// proto3 does not use groups; skip safely
		return -1
	default:
		return -1
	}
}

// encodeAddr converts raw address bytes to a base58 string.
// Returns an empty string for nil or empty input.
func encodeAddr(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	return base58.Encode(raw)
}
