// Package backfill imports a token contract's pre-tracking history so that
// holder balances and transfer history are complete for tokens added after
// the indexer started following the chain.
//
// Source of truth is the Koinos account history of the contract (served by
// koinos-rest from koinos-account-history), read in irreversible, ascending
// order and resumable by sequence number. Balances are derived from the
// decoded mint/transfer/burn events exactly like the live block processor
// does. A transaction entry carries no block height, so the height and
// timestamp are looked up through the transaction's containing block and
// cached per block. Entries above the cutoff height are left to the live
// sync, which has tracked the token since the cutoff.
package backfill

import (
	"context"
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

// Client reads koinos-rest.
type Client struct {
	rest   string
	http   *http.Client
	blocks map[string]blockInfo
}

type blockInfo struct{ height, timestamp uint64 }

// NewClient talks to a koinos-rest base URL such as http://127.0.0.1:3000.
func NewClient(restURL string) *Client {
	return &Client{rest: strings.TrimRight(restURL, "/"), http: &http.Client{Timeout: httpTimeout}, blocks: make(map[string]blockInfo)}
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
	Data   json.RawMessage `json:"data"`
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
	BlockHeight string `json:"block_height"`
	Block       struct {
		Header struct {
			Timestamp string `json:"timestamp"`
		} `json:"header"`
	} `json:"block"`
}

// --- REST access ----------------------------------------------------------

func (c *Client) getJSON(ctx context.Context, path string, out interface{}) error {
	var lastErr error
	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Duration(attempt) * 700 * time.Millisecond):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.rest+path, nil)
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

// History returns one page of the contract's irreversible history, ascending from seq.
func (c *Client) History(ctx context.Context, token string, seq uint64, limit int) ([]historyEntry, error) {
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))
	q.Set("ascending", "true")
	q.Set("irreversible", "true")
	q.Set("sequence_number", strconv.FormatUint(seq, 10))
	var page []historyEntry
	if err := c.getJSON(ctx, "/v1/account/"+token+"/history?"+q.Encode(), &page); err != nil {
		return nil, err
	}
	return page, nil
}

// blockOf resolves the height and timestamp of the block containing a transaction.
func (c *Client) blockOf(ctx context.Context, txID string) (blockInfo, error) {
	var tx txResponse
	if err := c.getJSON(ctx, "/v1/transaction/"+url.PathEscape(txID)+"?return_receipt=false", &tx); err != nil {
		return blockInfo{}, err
	}
	if len(tx.ContainingBlocks) == 0 {
		return blockInfo{}, fmt.Errorf("transaction %s: no containing block", txID)
	}
	id := tx.ContainingBlocks[0]
	if b, ok := c.blocks[id]; ok {
		return b, nil
	}
	var br blockResponse
	if err := c.getJSON(ctx, "/v1/block/"+url.PathEscape(id), &br); err != nil {
		return blockInfo{}, err
	}
	h, err := strconv.ParseUint(br.BlockHeight, 10, 64)
	if err != nil || h == 0 {
		return blockInfo{}, fmt.Errorf("block %s: bad height %q", id, br.BlockHeight)
	}
	ts, _ := strconv.ParseUint(br.Block.Header.Timestamp, 10, 64)
	b := blockInfo{height: h, timestamp: ts}
	c.blocks[id] = b
	return b, nil
}

// --- history → ops ---------------------------------------------------------

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

// opsOf extracts this token's balance-affecting events from one history entry.
func opsOf(token string, seq uint64, b blockInfo, txID string, events []event) []Op {
	var ops []Op
	for _, ev := range events {
		if ev.Source != token {
			continue
		}
		typ := eventType(ev.Name)
		if typ == "" {
			continue
		}
		var d eventData
		if err := json.Unmarshal(ev.Data, &d); err != nil {
			continue
		}
		value, err := strconv.ParseUint(d.Value, 10, 64)
		if err != nil || value == 0 || value > maxSafeValue {
			continue
		}
		from, to := d.From, d.To
		if from != "" && !validBase58.MatchString(from) {
			continue
		}
		if to != "" && !validBase58.MatchString(to) {
			continue
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
		ops = append(ops, Op{Seq: seq, Height: b.height, Timestamp: b.timestamp, TxID: txID, EventType: typ, From: from, To: to, Value: value})
	}
	return ops
}

// Page is one fetched, resolved page of history.
type Page struct {
	Entries int
	Ops     []Op
	LastSeq uint64
	Stopped uint64 // first height above the cutoff (entry not included), 0 if none
	End     bool   // history exhausted
}

// Collect fetches and resolves one page starting at seq.
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
				continue
			}
			b, err = c.blockOf(ctx, txID)
			if err != nil {
				return nil, fmt.Errorf("seq %d: %w", s, err)
			}
			events = e.Trx.Receipt.Events
		default:
			continue
		}
		if cutoff > 0 && b.height > cutoff {
			p.Stopped = b.height
			return p, nil
		}
		p.Ops = append(p.Ops, opsOf(token, s, b, txID, events)...)
		p.LastSeq = s
	}
	return p, nil
}

// --- apply -----------------------------------------------------------------

// ErrNotRegistered means no backfill record exists for the token.
var ErrNotRegistered = errors.New("token has no backfill record")

// Run resumes (or starts) the backfill of one token and applies it to the store.
func Run(ctx context.Context, s store.Store, restURL, token string) (*Result, error) {
	bf, err := s.GetBackfill(token)
	if err != nil {
		return nil, err
	}
	if bf == nil {
		return nil, ErrNotRegistered
	}
	res := &Result{LastSeq: bf.NextSeq}
	if bf.Done {
		res.Done = true
		return res, nil
	}
	c := NewClient(restURL)
	seq := bf.NextSeq
	started := time.Now()
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
		done := page.End || page.Stopped > 0
		next := seq
		if page.Entries > 0 && page.Stopped == 0 {
			next = page.LastSeq + 1
		} else if page.Stopped > 0 {
			next = page.LastSeq + 1 // entries before the stop point are applied; the rest belongs to live sync
		}
		if err := apply(s, token, page.Ops, &store.Backfill{Token: token, NextSeq: next, CutoffHeight: bf.CutoffHeight, Done: done}); err != nil {
			return res, err
		}
		res.LastSeq = next
		if done {
			res.Done = true
			res.StoppedAt = page.Stopped
			log.Infof("Backfill %s: done — %d entries, %d events in %s", token, res.Entries, res.Ops, time.Since(started).Round(time.Second))
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
				if err := s.UpsertAddress(a, op.Height, op.Timestamp); err != nil {
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
// token is registered. cutoff 0 means "all of it".
func DryRun(ctx context.Context, restURL, token string, cutoff uint64) (map[string]*big.Int, *Result, error) {
	c := NewClient(restURL)
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
		page, err := c.Collect(ctx, token, seq, cutoff)
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
		if page.End || page.Stopped > 0 {
			res.Done = true
			res.StoppedAt = page.Stopped
			res.LastSeq = page.LastSeq
			return balances, res, nil
		}
		seq = page.LastSeq + 1
	}
}
