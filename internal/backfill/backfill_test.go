package backfill

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/koinos/koinos-token-tracker/internal/store"
)

const (
	tok  = "1TOKENxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	alfa = "1ALFAxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	beta = "1BETAxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
)

// fixture: a mint, a transfer, a burn, a foreign-token event, a block-level
// entry and one entry that lies above the cutoff.
type fixEntry struct {
	seq    int
	height uint64
	block  bool
	events []map[string]interface{}
}

func ev(source, name string, data map[string]string) map[string]interface{} {
	return map[string]interface{}{"source": source, "name": name, "data": data}
}

func fixture() []fixEntry {
	return []fixEntry{
		{0, 100, false, []map[string]interface{}{ev(tok, "koinos.contracts.token.mint_event", map[string]string{"to": alfa, "value": "100"})}},
		{1, 105, false, []map[string]interface{}{ev(tok, "koinos.contracts.token.transfer_event", map[string]string{"from": alfa, "to": beta, "value": "30"}), ev("1OTHERxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "koinos.contracts.token.transfer_event", map[string]string{"from": alfa, "to": beta, "value": "999"})}},
		{2, 105, false, []map[string]interface{}{ev(tok, "koinos.contracts.token.burn_event", map[string]string{"from": beta, "value": "10"})}},
		{3, 110, true, []map[string]interface{}{ev(tok, "koinos.contracts.token.mint_event", map[string]string{"to": beta, "value": "5"})}},
		{4, 500, false, []map[string]interface{}{ev(tok, "koinos.contracts.token.mint_event", map[string]string{"to": alfa, "value": "1000"})}},
	}
}

// server serves the fixture through the three koinos-rest routes the
// backfill uses. failPage>0 makes the history route fail once at that page.
func server(t *testing.T, entries []fixEntry, pageLimit int, failAtSeq int, calls *atomic.Int64) *httptest.Server {
	t.Helper()
	txID := func(seq int) string { return fmt.Sprintf("0x1220%064x", seq) }
	blockID := func(h uint64) string { return fmt.Sprintf("0x1220%064x", h) }
	var failed atomic.Bool
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		switch {
		case strings.HasPrefix(r.URL.Path, "/v1/account/"+tok+"/history"):
			seq, _ := strconv.Atoi(r.URL.Query().Get("sequence_number"))
			limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			if r.URL.Query().Get("ascending") != "true" || r.URL.Query().Get("irreversible") != "true" {
				http.Error(w, "bad params", 400)
				return
			}
			if failAtSeq > 0 && seq == failAtSeq && !failed.Swap(true) {
				http.Error(w, "boom", 400) // non-retryable: surfaces as a Run error
				return
			}
			if limit > pageLimit {
				limit = pageLimit
			}
			var out []map[string]interface{}
			for _, e := range entries {
				if e.seq < seq || len(out) >= limit {
					continue
				}
				item := map[string]interface{}{}
				if e.seq > 0 {
					item["seq_num"] = strconv.Itoa(e.seq)
				}
				if e.block {
					item["block"] = map[string]interface{}{"header": map[string]string{"height": strconv.FormatUint(e.height, 10), "timestamp": "1700000000000"}, "receipt": map[string]interface{}{"events": e.events}}
				} else {
					item["trx"] = map[string]interface{}{"transaction": map[string]string{"id": txID(e.seq)}, "receipt": map[string]interface{}{"events": e.events}}
				}
				out = append(out, item)
			}
			json.NewEncoder(w).Encode(out)
		case strings.HasPrefix(r.URL.Path, "/v1/transaction/"):
			id := strings.TrimPrefix(r.URL.Path, "/v1/transaction/")
			for _, e := range entries {
				if txID(e.seq) == id {
					json.NewEncoder(w).Encode(map[string]interface{}{"containing_blocks": []string{blockID(e.height)}})
					return
				}
			}
			http.Error(w, "unknown tx", 404)
		case strings.HasPrefix(r.URL.Path, "/v1/block/"):
			id := strings.TrimPrefix(r.URL.Path, "/v1/block/")
			for _, e := range entries {
				if blockID(e.height) == id {
					json.NewEncoder(w).Encode(map[string]interface{}{"block_height": strconv.FormatUint(e.height, 10), "block": map[string]interface{}{"header": map[string]string{"timestamp": strconv.FormatUint(e.height*1000, 10)}}})
					return
				}
			}
			http.Error(w, "unknown block", 404)
		default:
			http.Error(w, "unexpected "+r.URL.Path, 404)
		}
	}))
}

func newStore(t *testing.T) *store.SQLiteStore {
	t.Helper()
	s, err := store.Open(filepath.Join(t.TempDir(), "t.db"), "1KNxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "1VHPyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func balance(t *testing.T, s store.Store, addr string) string {
	t.Helper()
	b, err := s.GetBalance(addr, tok)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestRunAppliesHistoryUpToCutoff(t *testing.T) {
	var calls atomic.Int64
	srv := server(t, fixture(), 100, 0, &calls)
	defer srv.Close()
	s := newStore(t)
	if err := s.UpsertBackfill(&store.Backfill{Token: tok, NextSeq: 0, CutoffHeight: 200}); err != nil {
		t.Fatal(err)
	}

	res, err := Run(context.Background(), s, srv.URL, "", tok)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Done || res.Ops != 4 || res.StoppedAt != 500 {
		t.Fatalf("result %+v", res)
	}
	// alfa: +100 -30 = 70 ; beta: +30 -10 +5 = 25 ; the seq-4 mint (height 500) is above the cutoff
	if a, b := balance(t, s, alfa), balance(t, s, beta); a != "70" || b != "25" {
		t.Fatalf("balances alfa=%s beta=%s", a, b)
	}
	rows, err := s.GetRecentTransfersFiltered(tok, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 4 || rows[0].Height != 110 || rows[0].EventType != "mint" || rows[3].Height != 100 {
		t.Fatalf("transfers %+v", rows)
	}
	for _, r := range rows {
		if r.Token != tok || (r.Height != 110 && r.TxID == "") || (r.Height == 110 && r.TxID != "") {
			t.Fatalf("row %+v", r)
		}
	}
	bf, err := s.GetBackfill(tok)
	if err != nil || bf == nil || !bf.Done || bf.NextSeq != 4 {
		t.Fatalf("progress %+v err %v", bf, err)
	}
	if _, err := s.GetAddress(beta); err != nil {
		t.Fatalf("address rows missing: %v", err)
	}

	// a second run is a no-op: nothing double-counted, no REST calls
	before := calls.Load()
	res, err = Run(context.Background(), s, srv.URL, "", tok)
	if err != nil || !res.Done || calls.Load() != before {
		t.Fatalf("second run: %+v err=%v calls=%d→%d", res, err, before, calls.Load())
	}
	if balance(t, s, alfa) != "70" {
		t.Fatal("balance changed on the no-op run")
	}
}

func TestRunResumesAfterFailure(t *testing.T) {
	var calls atomic.Int64
	old := PageSize
	PageSize = 2
	defer func() { PageSize = old }()
	srv := server(t, fixture(), 2, 2, &calls) // pages of 2 entries; the page at seq 2 fails once
	defer srv.Close()
	s := newStore(t)
	if err := s.UpsertBackfill(&store.Backfill{Token: tok, NextSeq: 0, CutoffHeight: 0}); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(context.Background(), s, srv.URL, "", tok); err == nil {
		t.Fatal("expected the injected failure")
	}
	bf, _ := s.GetBackfill(tok)
	if bf.NextSeq != 2 || bf.Done {
		t.Fatalf("progress after failure %+v", bf)
	}
	if balance(t, s, alfa) != "70" || balance(t, s, beta) != "30" { // first page applied: mint 100, transfer 30
		t.Fatalf("partial balances alfa=%s beta=%s", balance(t, s, alfa), balance(t, s, beta))
	}
	res, err := Run(context.Background(), s, srv.URL, "", tok)
	if err != nil || !res.Done {
		t.Fatalf("resume: %+v %v", res, err)
	}
	// no cutoff: the height-500 mint counts too → alfa 70+1000, beta 30-10+5
	if a, b := balance(t, s, alfa), balance(t, s, beta); a != "1070" || b != "25" {
		t.Fatalf("final balances alfa=%s beta=%s", a, b)
	}
	rows, _ := s.GetRecentTransfersFiltered(tok, "", 10)
	if len(rows) != 5 {
		t.Fatalf("expected 5 transfer rows, got %d", len(rows))
	}
}

func TestDryRunMatchesRun(t *testing.T) {
	var calls atomic.Int64
	srv := server(t, fixture(), 100, 0, &calls)
	defer srv.Close()
	balances, res, err := DryRun(context.Background(), srv.URL, "", tok, 0)
	if err != nil || !res.Done || res.Ops != 5 {
		t.Fatalf("dry run %+v %v", res, err)
	}
	if balances[alfa].String() != "1070" || balances[beta].String() != "25" {
		t.Fatalf("dry balances %v", balances)
	}
}

func TestRunRequiresRegistration(t *testing.T) {
	s := newStore(t)
	if _, err := Run(context.Background(), s, "http://127.0.0.1:1", "", tok); err != ErrNotRegistered {
		t.Fatalf("expected ErrNotRegistered, got %v", err)
	}
}

func TestOpsOfRejectsGarbage(t *testing.T) {
	b := blockInfo{height: 1, timestamp: 1}
	raw := func(m map[string]interface{}) event {
		d, _ := json.Marshal(m["data"])
		return event{Source: m["source"].(string), Name: m["name"].(string), Data: d}
	}
	events := []event{
		raw(ev(tok, "koinos.contracts.token.transfer_event", map[string]string{"from": alfa, "to": beta, "value": "-5"})),
		raw(ev(tok, "koinos.contracts.token.transfer_event", map[string]string{"from": alfa, "to": beta, "value": "0"})),
		raw(ev(tok, "koinos.contracts.token.transfer_event", map[string]string{"from": "not*base58", "to": beta, "value": "5"})),
		raw(ev(tok, "koinos.contracts.token.transfer_event", map[string]string{"from": alfa, "to": beta, "value": "99999999999999999999"})),
		raw(ev(tok, "koinos.contracts.token.approve_event", map[string]string{"from": alfa, "to": beta, "value": "5"})),
		raw(ev(tok, "koinos.contracts.token.transfer_event", map[string]string{"from": alfa, "to": beta, "value": "7"})),
	}
	ops := opsOf(tok, 1, b, "0x1220", events)
	if len(ops) != 1 || ops[0].Value != 7 {
		t.Fatalf("ops %+v", ops)
	}
}

// The primary node answers 500 for one transaction (a transaction store gap);
// the fallback node resolves it and the run completes with correct heights.
func TestFallbackNodeResolvesTransaction(t *testing.T) {
	var calls, fallbackCalls atomic.Int64
	good := server(t, fixture(), 100, 0, &fallbackCalls)
	defer good.Close()
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if strings.HasPrefix(r.URL.Path, "/v1/transaction/") && strings.HasSuffix(r.URL.Path, fmt.Sprintf("%064x", 2)) {
			http.Error(w, `{"error":"unknown error"}`, 500)
			return
		}
		http.Redirect(w, r, good.URL+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	}))
	defer broken.Close()
	s := newStore(t)
	if err := s.UpsertBackfill(&store.Backfill{Token: tok, NextSeq: 0, CutoffHeight: 0}); err != nil {
		t.Fatal(err)
	}
	res, err := Run(context.Background(), s, broken.URL, good.URL, tok)
	if err != nil || !res.Done || res.Ops != 5 {
		t.Fatalf("run with fallback: %+v %v", res, err)
	}
	if fallbackCalls.Load() == 0 {
		t.Fatal("fallback node was never asked")
	}
	rows, _ := s.GetRecentTransfersFiltered(tok, "burn", 5)
	if len(rows) != 1 || rows[0].Height != 105 {
		t.Fatalf("burn row should carry the fallback-resolved height: %+v", rows)
	}
	// without a fallback the same gap is fatal but resumable
	s2 := newStore(t)
	_ = s2.UpsertBackfill(&store.Backfill{Token: tok, NextSeq: 0, CutoffHeight: 0})
	if _, err := Run(context.Background(), s2, broken.URL, "", tok); err == nil {
		t.Fatal("expected failure without a fallback node")
	}
}
