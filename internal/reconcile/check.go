package reconcile

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/koinos/koinos-token-tracker/internal/store"
	"github.com/mr-tron/base58"
	"google.golang.org/protobuf/encoding/protowire"
)

// balanceOfEntryPoint is the KCS-1/KCS-4 balance_of entry point (0x5c721497).
const balanceOfEntryPoint = 1550980247

// CheckResult is the outcome of comparing stored balances with the chain.
type CheckResult struct {
	Checked    int
	Mismatches int
	Failures   int      // lookups that could not be answered (not counted as mismatches)
	Details    []string // first mismatches, human readable
}

// Verified is true when every holder was answered and none differed.
func (r *CheckResult) Verified() bool { return r.Failures == 0 && r.Mismatches == 0 }

// CheckToken compares every stored non-zero holder of one token with the
// chain's balance_of, read through JSON-RPC chain.read_contract so that a
// zero balance and a failed call are distinguishable (koinos-rest answers
// both with HTTP 400). It never writes: the node answers with head state
// while the store follows the last irreversible block, so a difference is a
// finding for the operator, not something to overwrite.
func CheckToken(ctx context.Context, s store.Store, rpcURL, token string) (*CheckResult, error) {
	holders, _, err := s.GetTopHolders(token, 1<<30, 0) // one snapshot
	if err != nil {
		return nil, fmt.Errorf("holders: %w", err)
	}
	client := &http.Client{Timeout: httpTimeout}
	res := &CheckResult{Checked: len(holders)}
	for _, h := range holders {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		chain, err := BalanceOf(ctx, client, rpcURL, token, h.Address)
		if err != nil {
			res.Failures++
			if len(res.Details) < 20 {
				res.Details = append(res.Details, fmt.Sprintf("%s: lookup failed: %v", h.Address, err))
			}
			continue
		}
		if chain.String() != h.Balance {
			res.Mismatches++
			if len(res.Details) < 20 {
				res.Details = append(res.Details, fmt.Sprintf("%s: stored %s, chain %s", h.Address, h.Balance, chain))
			}
		}
	}
	return res, nil
}

// BalanceOf reads a token balance in base units through JSON-RPC
// chain.read_contract (balance_of_arguments{bytes owner=1} →
// balance_of_result{uint64 value=1}; an empty result means zero).
func BalanceOf(ctx context.Context, client *http.Client, rpcURL, token, address string) (*big.Int, error) {
	owner, err := base58.Decode(address)
	if err != nil {
		return nil, fmt.Errorf("address: %w", err)
	}
	args := protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), owner)
	body, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "id": 1, "method": "chain.read_contract",
		"params": map[string]interface{}{"contract_id": token, "entry_point": balanceOfEntryPoint, "args": base64.URLEncoding.EncodeToString(args)},
	})
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt) * 500 * time.Millisecond):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, rpcURL, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("HTTP %d", resp.StatusCode)
			continue
		}
		var rpc struct {
			Result *struct {
				Result string `json:"result"`
			} `json:"result"`
			Error *struct {
				Message string `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(raw, &rpc); err != nil {
			return nil, fmt.Errorf("bad json: %w", err)
		}
		if rpc.Error != nil {
			return nil, fmt.Errorf("rpc: %s", rpc.Error.Message)
		}
		if rpc.Result == nil {
			return nil, fmt.Errorf("rpc: empty response")
		}
		payload, err := base64.URLEncoding.DecodeString(padB64(rpc.Result.Result))
		if err != nil {
			if payload, err = base64.StdEncoding.DecodeString(padB64(rpc.Result.Result)); err != nil {
				return nil, fmt.Errorf("bad result encoding: %w", err)
			}
		}
		return decodeUint64Field1(payload)
	}
	return nil, lastErr
}

// decodeUint64Field1 reads field 1 (varint) of a protobuf message; an empty
// message is zero (protobuf omits default scalars).
func decodeUint64Field1(b []byte) (*big.Int, error) {
	value := new(big.Int)
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, fmt.Errorf("bad protobuf tag")
		}
		b = b[n:]
		switch typ {
		case protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return nil, fmt.Errorf("bad varint")
			}
			b = b[n:]
			if num == 1 {
				value.SetUint64(v)
			}
		case protowire.BytesType:
			_, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return nil, fmt.Errorf("bad bytes field")
			}
			b = b[n:]
		case protowire.Fixed32Type:
			if len(b) < 4 {
				return nil, fmt.Errorf("short fixed32")
			}
			b = b[4:]
		case protowire.Fixed64Type:
			if len(b) < 8 {
				return nil, fmt.Errorf("short fixed64")
			}
			b = b[8:]
		default:
			return nil, fmt.Errorf("unsupported wire type %d", typ)
		}
	}
	return value, nil
}

func padB64(s string) string {
	s = strings.TrimRight(s, "=")
	return s + strings.Repeat("=", (4-len(s)%4)%4)
}
