// Package backfill imports a token contract's pre-tracking history so that
// holder balances and transfer history are complete for tokens added after
// the indexer started following the chain.
//
// Source of truth is the Koinos account history of the contract (served by
// koinos-rest from koinos-account-history), read in irreversible, ascending
// order and resumable by sequence number. Balances are derived from the
// mint/transfer/burn events exactly like the live block processor does,
// decoding the protobuf payload ourselves when the node could not (no ABI).
// A transaction entry carries no block height, so it is resolved through the
// transaction's containing blocks, keeping only the one that is the canonical
// block at its height, and cached. Entries above the cutoff height belong to
// the live sync, which tracks the token from cutoff+1 on.
//
// The backfill must run to completion before any block above the cutoff is
// processed: balances are clamped at zero, so a post-cutoff spend applied
// before its pre-cutoff funding would be lost. main.go therefore runs pending
// backfills before the historical sync.
package backfill

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	log "github.com/koinos/koinos-log-golang/v2"
	"github.com/koinos/koinos-token-tracker/internal/store"
	"github.com/mr-tron/base58"
	"google.golang.org/protobuf/encoding/protowire"
)

// PageSize is the history page size; the end of history is detected by a
// short page. Tests shrink it.
var PageSize = 100

const (
	httpTimeout  = 20 * time.Second
	maxRetries   = 4
	maxSafeValue = uint64(math.MaxInt64)
)

var validBase58 = regexp.MustCompile(`^[1-9A-HJ-NP-Za-km-z]{20,50}$`)

// Op is one balance-affecting token event with its block position.
type Op struct {
	Seq       uint64
	Height    uint64
	Timestamp uint64
	TxID      string
	EventType string // transfer, mint, burn
	From      string
	To        string
	Value     uint64
}

// Result summarises a run.
type Result struct {
	Entries   int
	Ops       int
	Done      bool
	LastSeq   uint64
	StoppedAt uint64 // first height above the cutoff, 0 when history was exhausted
}

// Client reads koinos-rest. Block lookups fall back to a second node when
// the primary cannot serve a transaction (a node's transaction store may
// lack an old transaction its account history still references).
type Client struct {
	rest     string
	fallback string
	http     *http.Client
	blocks   map[string]blockInfo // canonical block id → position
	canon    map[uint64]string    // height → canonical block id
}

type blockInfo struct{ height, timestamp uint64 }

// NewClient talks to a koinos-rest base URL such as http://127.0.0.1:3000;
// fallbackURL (may be empty) is asked when the primary fails a lookup.
func NewClient(restURL, fallbackURL string) *Client {
	return &Client{
		rest: strings.TrimRight(restURL, "/"), fallback: strings.TrimRight(fallbackURL, "/"),
		http: &http.Client{Timeout: httpTimeout}, blocks: make(map[string]blockInfo), canon: make(map[uint64]string),
	}
}

// --- wire shapes (only the fields we read) --------------------------------

type historyEntry struct {
	SeqNum string `json:"seq_num"`
	Trx    *struct {
		Transaction struct {
			ID string `json:"id"`
		} `json:"transaction"`
		Receipt struct {
			Events []event `json:"events"`
		} `json:"receipt"`
	} `json:"trx"`
	Block *struct {
		Header struct {
			Height    string `json:"height"`
			Timestamp string `json:"timestamp"`
		} `json:"header"`
		Receipt struct {
			Events []event `json:"events"`
		} `json:"receipt"`
	} `json:"block"`
}

type event struct {
	Source string          `json:"source"`
	Name   string          `json:"name"`
	Data   json.RawMessage `json:"data"` // decoded object, or a base64 string when the node had no ABI
}

type eventData struct {
	From  string `json:"from"`
	To    string `json:"to"`
	Value string `json:"value"`
}

type txResponse struct {
	ContainingBlocks []string `json:"containing_blocks"`
}

type blockResponse struct {
	BlockID     string `json:"block_id"`
	BlockHeight string `json:"block_height"`
	Block       struct {
		Header struct {
			Timestamp string `json:"timestamp"`
		} `json:"header"`
	} `json:"block"`
}

// --- REST access ----------------------------------------------------------

func (c *Client) getJSON(ctx context.Context, path string, out interface{}) error {
	return c.getJSONFrom(ctx, c.rest, path, out)
}

// getJSONOrFallback tries the primary node, then the fallback node.
func (c *Client) getJSONOrFallback(ctx context.Context, path string, out interface{}) error {
	err := c.getJSONFrom(ctx, c.rest, path, out)
	if err == nil || c.fallback == "" || ctx.Err() != nil {
		return err
	}
	log.Warnf("Backfill: primary node failed %s (%v), trying %s", path, err, c.fallback)
	if ferr := c.getJSONFrom(ctx, c.fallback, path, out); ferr != nil {
		return fmt.Errorf("primary: %v; fallback: %w", err, ferr)
	}
	return nil
}

func (c *Client) getJSONFrom(ctx context.Context, base, path string, out interface{}) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 700 * time.Millisecond):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+path, nil)
		if err != nil {
			return err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode == http.StatusOK {
			if err := json.Unmarshal(body, out); err != nil {
				return fmt.Errorf("%s: bad json: %w", path, err)
			}
			return nil
		}
		if resp.StatusCode >= 400 && resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
			return fmt.Errorf("%s: HTTP %d: %s", path, resp.StatusCode, strings.TrimSpace(string(body[:min(len(body), 200)])))
		}
		lastErr = fmt.Errorf("%s: HTTP %d", path, resp.StatusCode)
	}
	return lastErr
}

// History returns one page of the contract's irreversible history, ascending
// from seq. koinos-rest answers an empty history with {} rather than [].
func (c *Client) History(ctx context.Context, token string, seq uint64, limit int) ([]historyEntry, error) {
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))
	q.Set("ascending", "true")
	q.Set("irreversible", "true")
	q.Set("sequence_number", strconv.FormatUint(seq, 10))
	var raw json.RawMessage
	if err := c.getJSON(ctx, "/v1/account/"+token+"/history?"+q.Encode(), &raw); err != nil {
		return nil, err
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return nil, nil
	}
	var page []historyEntry
	if err := json.Unmarshal(trimmed, &page); err != nil {
		return nil, fmt.Errorf("history page: bad json: %w", err)
	}
	return page, nil
}

// HistoryHeight returns the irreversible height the history indexer
// (koinos-account-history behind the primary node) has reached, read from
// the newest irreversible entry of a reference account with activity in every
// block — the KOIN contract, whose per-block mint is a block-level event. The
// chain's own LIB says nothing about that separate indexer's progress.
func (c *Client) HistoryHeight(ctx context.Context, ref string) (uint64, error) {
	q := url.Values{}
	q.Set("limit", "5")
	q.Set("ascending", "false")
	q.Set("irreversible", "true")
	var raw json.RawMessage
	if err := c.getJSON(ctx, "/v1/account/"+ref+"/history?"+q.Encode(), &raw); err != nil {
		return 0, err
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || trimmed[0] != '[' {
		return 0, fmt.Errorf("reference account %s has no history", ref)
	}
	var page []historyEntry
	if err := json.Unmarshal(trimmed, &page); err != nil {
		return 0, fmt.Errorf("reference history: bad json: %w", err)
	}
	for _, e := range page {
		if e.Block != nil {
			h, err := strconv.ParseUint(e.Block.Header.Height, 10, 64)
			if err != nil || h == 0 {
				return 0, fmt.Errorf("reference history: bad block height %q", e.Block.Header.Height)
			}
			return h, nil
		}
	}
	for _, e := range page {
		if e.Trx != nil && e.Trx.Transaction.ID != "" {
			b, err := c.blockOf(ctx, e.Trx.Transaction.ID)
			if err != nil {
				return 0, err
			}
			return b.height, nil
		}
	}
	return 0, fmt.Errorf("reference account %s has no usable history entry", ref)
}

// blockOf resolves the canonical block containing a transaction: of the
// candidate blocks the transaction store knows, only the one that is the
// block at its height on the main chain counts (the others are forks). The
// canonical lookup is always answered by the primary node, which is also the
// history source, so heights are anchored to one chain view.
func (c *Client) blockOf(ctx context.Context, txID string) (blockInfo, error) {
	var tx txResponse
	if err := c.getJSONOrFallback(ctx, "/v1/transaction/"+url.PathEscape(txID)+"?return_receipt=false", &tx); err != nil {
		return blockInfo{}, err
	}
	if len(tx.ContainingBlocks) == 0 {
		return blockInfo{}, fmt.Errorf("transaction %s: no containing block", txID)
	}
	var lastErr error
	for _, id := range tx.ContainingBlocks {
		if b, ok := c.blocks[id]; ok {
			return b, nil
		}
		var br blockResponse
		if err := c.getJSONOrFallback(ctx, "/v1/block/"+url.PathEscape(id), &br); err != nil {
			lastErr = err
			continue
		}
		h, err := strconv.ParseUint(br.BlockHeight, 10, 64)
		if err != nil || h == 0 {
			lastErr = fmt.Errorf("block %s: bad height %q", id, br.BlockHeight)
			continue
		}
		canonID, ok := c.canon[h]
		if !ok {
			var atHeight blockResponse
			if err := c.getJSON(ctx, "/v1/block/"+strconv.FormatUint(h, 10), &atHeight); err != nil {
				lastErr = err
				continue
			}
			canonID = atHeight.BlockID
			c.canon[h] = canonID
		}
		if !strings.EqualFold(canonID, id) {
			continue // a fork block that also included the transaction
		}
		ts, _ := strconv.ParseUint(br.Block.Header.Timestamp, 10, 64)
		b := blockInfo{height: h, timestamp: ts}
		c.blocks[id] = b
		return b, nil
	}
	if lastErr != nil {
		return blockInfo{}, fmt.Errorf("transaction %s: %w", txID, lastErr)
	}
	return blockInfo{}, fmt.Errorf("transaction %s: none of its %d containing blocks is on the canonical chain", txID, len(tx.ContainingBlocks))
}

// --- history → ops ---------------------------------------------------------

var hexTxID = regexp.MustCompile(`^0x1220[0-9a-fA-F]{64}$`)

// TxIDBase58 converts a transaction id as koinos-rest prints it (0x-hex of
// the SHA-256 multihash) to the base58 form the live block processor stores
// (base58 of the same bytes), so backfilled rows look exactly like live ones.
func TxIDBase58(id string) (string, error) {
	if !hexTxID.MatchString(id) {
		return "", fmt.Errorf("transaction id %q is not a 0x1220… multihash", id)
	}
	raw, err := hex.DecodeString(id[2:])
	if err != nil {
		return "", fmt.Errorf("transaction id %q: %w", id, err)
	}
	return base58.Encode(raw), nil
}

func parseSeq(s string) uint64 {
	if s == "" {
		return 0 // the first entry omits seq_num
	}
	n, _ := strconv.ParseUint(s, 10, 64)
	return n
}

func eventType(name string) string {
	switch {
	case strings.HasSuffix(name, "transfer_event"):
		return "transfer"
	case strings.HasSuffix(name, "mint_event"):
		return "mint"
	case strings.HasSuffix(name, "burn_event"):
		return "burn"
	}
	return ""
}

// decodeEvent returns from/to/value of a token event. The node decodes event
// data when it knows the contract's ABI; otherwise data is the raw protobuf
// payload as a base64 string, which is decoded strictly here: malformed
// bytes are an error, never a zero-value event.
func decodeEvent(typ string, data json.RawMessage) (from, to, value string, ok bool) {
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) > 0 && trimmed[0] == '{' {
		var d eventData
		if err := json.Unmarshal(trimmed, &d); err != nil {
			return "", "", "", false
		}
		return d.From, d.To, d.Value, true
	}
	var b64 string
	if err := json.Unmarshal(trimmed, &b64); err != nil || b64 == "" {
		return "", "", "", false
	}
	raw, err := base64.URLEncoding.DecodeString(padB64(b64))
	if err != nil {
		if raw, err = base64.StdEncoding.DecodeString(padB64(b64)); err != nil {
			return "", "", "", false
		}
	}
	f, t, v, err := decodeRawTokenEvent(typ, raw)
	if err != nil {
		return "", "", "", false
	}
	return f, t, strconv.FormatUint(v, 10), true
}

// decodeRawTokenEvent parses the KCS token event payloads
// (transfer_event{bytes from=1; bytes to=2; uint64 value=3},
// mint_event{bytes to=1; uint64 value=2}, burn_event{bytes from=1; uint64 value=2})
// and rejects anything that is not well-formed protobuf.
func decodeRawTokenEvent(typ string, raw []byte) (from, to string, value uint64, err error) {
	var addrs [3][]byte
	var nums [4]uint64
	b := raw
	for len(b) > 0 {
		num, wt, n := protowire.ConsumeTag(b)
		if n < 0 {
			return "", "", 0, fmt.Errorf("bad protobuf tag")
		}
		b = b[n:]
		switch wt {
		case protowire.BytesType:
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return "", "", 0, fmt.Errorf("bad bytes field %d", num)
			}
			if num >= 1 && num <= 2 {
				addrs[num] = v
			}
			b = b[n:]
		case protowire.VarintType:
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return "", "", 0, fmt.Errorf("bad varint field %d", num)
			}
			if num >= 1 && num <= 3 {
				nums[num] = v
			}
			b = b[n:]
		default:
			n := protowire.ConsumeFieldValue(num, wt, b)
			if n < 0 {
				return "", "", 0, fmt.Errorf("bad field %d", num)
			}
			b = b[n:]
		}
	}
	enc := func(x []byte) string {
		if len(x) == 0 {
			return ""
		}
		return base58.Encode(x)
	}
	switch typ {
	case "transfer":
		return enc(addrs[1]), enc(addrs[2]), nums[3], nil
	case "mint":
		return "", enc(addrs[1]), nums[2], nil
	case "burn":
		return enc(addrs[1]), "", nums[2], nil
	}
	return "", "", 0, fmt.Errorf("unknown event type %q", typ)
}

func padB64(s string) string {
	s = strings.TrimRight(s, "=")
	return s + strings.Repeat("=", (4-len(s)%4)%4)
}

// opsOf extracts this token's balance-affecting events from one history
// entry. An event of a recognised type that cannot be decoded is an error:
// skipping it would silently corrupt balances.
func opsOf(token string, seq uint64, b blockInfo, txID string, events []event) ([]Op, error) {
	var ops []Op
	for _, ev := range events {
		if ev.Source != token {
			continue
		}
		typ := eventType(ev.Name)
		if typ == "" {
			continue
		}
		from, to, valueStr, ok := decodeEvent(typ, ev.Data)
		if !ok {
			return nil, fmt.Errorf("seq %d: cannot decode %s event of %s", seq, typ, token)
		}
		value, err := strconv.ParseUint(valueStr, 10, 64)
		if err != nil || value > maxSafeValue {
			return nil, fmt.Errorf("seq %d: %s event of %s has an unusable value %q", seq, typ, token, valueStr)
		}
		if value == 0 {
			continue // no balance effect (the live processor skips these too)
		}
		if from != "" && !validBase58.MatchString(from) || to != "" && !validBase58.MatchString(to) {
			return nil, fmt.Errorf("seq %d: %s event of %s has a malformed address", seq, typ, token)
		}
		switch typ {
		case "transfer":
			if from == "" && to == "" {
				continue
			}
		case "mint":
			from = ""
			if to == "" {
				continue
			}
		case "burn":
			to = ""
			if from == "" {
				continue
			}
		}
		id := ""
		if txID != "" {
			var err error
			if id, err = TxIDBase58(txID); err != nil {
				return nil, fmt.Errorf("seq %d: %w", seq, err)
			}
		}
		ops = append(ops, Op{Seq: seq, Height: b.height, Timestamp: b.timestamp, TxID: id, EventType: typ, From: from, To: to, Value: value})
	}
	return ops, nil
}

// Page is one fetched, resolved page of history.
type Page struct {
	Entries int
	Ops     []Op
	LastSeq uint64
	Stopped uint64 // first height above the cutoff (entry not included), 0 if none
	End     bool   // history exhausted
}

// Collect fetches and resolves one page starting at seq. Entries whose block
// is above cutoff stop the page (cutoff math.MaxUint64 = no limit).
func (c *Client) Collect(ctx context.Context, token string, seq uint64, cutoff uint64) (*Page, error) {
	entries, err := c.History(ctx, token, seq, PageSize)
	if err != nil {
		return nil, err
	}
	p := &Page{Entries: len(entries), End: len(entries) < PageSize}
	for _, e := range entries {
		s := parseSeq(e.SeqNum)
		var b blockInfo
		var txID string
		var events []event
		switch {
		case e.Block != nil:
			h, err := strconv.ParseUint(e.Block.Header.Height, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("seq %d: bad block height %q", s, e.Block.Header.Height)
			}
			ts, _ := strconv.ParseUint(e.Block.Header.Timestamp, 10, 64)
			b = blockInfo{height: h, timestamp: ts}
			events = e.Block.Receipt.Events
		case e.Trx != nil:
			txID = e.Trx.Transaction.ID
			if txID == "" {
				return nil, fmt.Errorf("seq %d: transaction entry without id", s)
			}
			b, err = c.blockOf(ctx, txID)
			if err != nil {
				return nil, fmt.Errorf("seq %d: %w", s, err)
			}
			events = e.Trx.Receipt.Events
		default:
			continue
		}
		if b.height > cutoff {
			p.Stopped = b.height
			return p, nil
		}
		ops, err := opsOf(token, s, b, txID, events)
		if err != nil {
			return nil, err
		}
		p.Ops = append(p.Ops, ops...)
		p.LastSeq = s
	}
	return p, nil
}

// --- apply -----------------------------------------------------------------

// ErrNotRegistered means no backfill record exists for the token.
var ErrNotRegistered = errors.New("token has no backfill record")

// Run resumes (or starts) the backfill of one token and applies it to the
// store, page by page, each page in one batch with its progress record. ref
// is the reference account whose history tells how far the history indexer
// has come (see HistoryHeight).
func Run(ctx context.Context, s store.Store, restURL, fallbackURL, token, ref string) (*Result, error) {
	res := &Result{} // never nil: callers report res.LastSeq alongside any error
	bf, err := s.GetBackfill(token)
	if err != nil {
		return res, err
	}
	if bf == nil {
		return res, ErrNotRegistered
	}
	res.LastSeq = bf.NextSeq
	if bf.Done {
		res.Done = true
		return res, nil
	}
	c := NewClient(restURL, fallbackURL)
	seq := bf.NextSeq
	started := time.Now()
	covered := false // the history indexer is known to have passed the cutoff
	for {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		page, err := c.Collect(ctx, token, seq, bf.CutoffHeight)
		if err != nil {
			return res, err
		}
		res.Entries += page.Entries
		res.Ops += len(page.Ops)
		next := seq
		if page.Entries > 0 && page.LastSeq >= seq {
			next = page.LastSeq + 1
		}
		// An entry above the cutoff proves the history reaches past it. An
		// exhausted history proves nothing by itself: the history indexer may
		// simply not have got that far. It is complete only when the indexer
		// is known to have passed the cutoff — and only for pages fetched
		// after that was established, so entries indexed between a short
		// page and the check cannot slip through. A lagging indexer leaves
		// the record open with its cursor advanced; the caller retries.
		done := page.Stopped > 0
		var lagErr error
		if page.End && !done {
			if covered {
				done = true
			} else {
				h, err := c.HistoryHeight(ctx, ref)
				switch {
				case err != nil:
					lagErr = fmt.Errorf("history exhausted but the history indexer's height is unknown: %w", err)
				case h < bf.CutoffHeight:
					lagErr = fmt.Errorf("history exhausted while the history indexer is at height %d, below the cutoff %d: retry later", h, bf.CutoffHeight)
				default:
					covered = true // re-read the tail before accepting it
				}
			}
		}
		progress := *bf
		progress.NextSeq = next
		progress.Done = done
		if err := apply(s, token, page.Ops, &progress); err != nil {
			return res, err
		}
		res.LastSeq = next
		if lagErr != nil {
			return res, lagErr
		}
		if done {
			res.Done = true
			res.StoppedAt = page.Stopped
			log.Infof("Backfill %s: replay done — %d entries, %d events in %s", token, res.Entries, res.Ops, time.Since(started).Round(time.Second))
			return res, nil
		}
		seq = next
		if res.Entries%1000 == 0 {
			log.Infof("Backfill %s: %d entries, %d events so far (seq %d)", token, res.Entries, res.Ops, seq)
		}
	}
}

// apply writes one page atomically: addresses, balance deltas, transfer rows
// and the progress record, so a crash resumes exactly at the next page.
func apply(s store.Store, token string, ops []Op, progress *store.Backfill) error {
	if err := s.BeginBatch(); err != nil {
		return err
	}
	fail := func(err error) error { s.RollbackBatch(); return err }
	adjust := func(addr string, delta int64, height uint64) error {
		if addr == "" {
			return nil
		}
		cur, err := s.GetBalance(addr, token)
		if err != nil {
			return err
		}
		n, ok := new(big.Int).SetString(cur, 10)
		if !ok {
			n = new(big.Int)
		}
		n.Add(n, big.NewInt(delta))
		if n.Sign() < 0 {
			n = new(big.Int) // same clamp as the live processor
		}
		return s.SetBalance(addr, token, n.String(), height)
	}
	for _, op := range ops {
		for _, a := range []string{op.From, op.To} {
			if a != "" {
				if err := s.LowerFirstSeen(a, op.Height, op.Timestamp); err != nil {
					return fail(err)
				}
			}
		}
		switch op.EventType {
		case "transfer":
			if err := adjust(op.From, -int64(op.Value), op.Height); err != nil {
				return fail(err)
			}
			if err := adjust(op.To, int64(op.Value), op.Height); err != nil {
				return fail(err)
			}
		case "mint":
			if err := adjust(op.To, int64(op.Value), op.Height); err != nil {
				return fail(err)
			}
		case "burn":
			if err := adjust(op.From, -int64(op.Value), op.Height); err != nil {
				return fail(err)
			}
		}
		if err := s.InsertTransfer(op.Height, op.TxID, token, op.From, op.To, strconv.FormatUint(op.Value, 10), op.EventType, op.Timestamp); err != nil {
			return fail(err)
		}
	}
	if err := s.UpsertBackfill(progress); err != nil {
		return fail(err)
	}
	return s.CommitBatch()
}

// DryRun replays the whole history without touching any database and returns
// the balances it would produce, for checking against the chain before a
// token is registered.
func DryRun(ctx context.Context, restURL, fallbackURL, token string) (map[string]*big.Int, *Result, error) {
	c := NewClient(restURL, fallbackURL)
	balances := make(map[string]*big.Int)
	res := &Result{}
	add := func(addr string, delta int64) {
		if addr == "" {
			return
		}
		n, ok := balances[addr]
		if !ok {
			n = new(big.Int)
			balances[addr] = n
		}
		n.Add(n, big.NewInt(delta))
		if n.Sign() < 0 {
			n.SetInt64(0)
		}
	}
	var seq uint64
	for {
		if err := ctx.Err(); err != nil {
			return balances, res, err
		}
		page, err := c.Collect(ctx, token, seq, math.MaxUint64)
		if err != nil {
			return balances, res, err
		}
		res.Entries += page.Entries
		res.Ops += len(page.Ops)
		for _, op := range page.Ops {
			switch op.EventType {
			case "transfer":
				add(op.From, -int64(op.Value))
				add(op.To, int64(op.Value))
			case "mint":
				add(op.To, int64(op.Value))
			case "burn":
				add(op.From, -int64(op.Value))
			}
		}
		if page.Entries > 0 {
			res.LastSeq = page.LastSeq + 1 // next cursor, like Run
		}
		if page.End {
			res.Done = true
			return balances, res, nil
		}
		seq = page.LastSeq + 1
	}
}
