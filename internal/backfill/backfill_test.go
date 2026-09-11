package backfill

import (
	"context"
	"encoding/base64"
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
	"github.com/mr-tron/base58"
	"google.golang.org/protobuf/encoding/protowire"
)

const tok = "1TKNxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"

var (
	alfa = base58.Encode(append([]byte{0}, []byte("alfa-xxxxxxxxxxxxxxxxxxx")...))
	beta = base58.Encode(append([]byte{0}, []byte("beta-xxxxxxxxxxxxxxxxxxx")...))
)

// fixture: a mint, a transfer, a burn, a foreign-token event, a block-level
// entry, and one entry above a 200 cutoff.
type fixEntry struct {
	seq    int
	height uint64
	block  bool
	forks  []string // extra containing blocks reported before the canonical one
	events []map[string]interface{}
}

func ev(source, name string, data interface{}) map[string]interface{} {
	return map[string]interface{}{"source": source, "name": name, "data": data}
}

func fixture() []fixEntry {
	return []fixEntry{
		{0, 100, false, nil, []map[string]interface{}{ev(tok, "koinos.contracts.token.mint_event", map[string]string{"to": alfa, "value": "100"})}},
		{1, 105, false, nil, []map[string]interface{}{ev(tok, "koinos.contracts.token.transfer_event", map[string]string{"from": alfa, "to": beta, "value": "30"}), ev("1OTHERxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "koinos.contracts.token.transfer_event", map[string]string{"from": alfa, "to": beta, "value": "999"})}},
		{2, 105, false, nil, []map[string]interface{}{ev(tok, "koinos.contracts.token.burn_event", map[string]string{"from": beta, "value": "10"})}},
		{3, 110, true, nil, []map[string]interface{}{ev(tok, "koinos.contracts.token.mint_event", map[string]string{"to": beta, "value": "5"})}},
		{4, 500, false, nil, []map[string]interface{}{ev(tok, "koinos.contracts.token.mint_event", map[string]string{"to": alfa, "value": "1000"})}},
	}
}

func txID(seq int) string     { return fmt.Sprintf("0x1220%064x", seq) }
func blockID(h uint64) string { return fmt.Sprintf("0x1220%064x", h) }
func forkID(h uint64) string  { return fmt.Sprintf("0x1220f%063x", h) }

// fakeREST serves a fixture through the koinos-rest routes the backfill uses.
type fakeREST struct {
	entries   []fixEntry
	pageLimit int
	failAtSeq int // history page at this seq fails once with 400
	emptyObj  bool
	lib       uint64 // last irreversible block reported by head_info (0 = far ahead)
	calls     atomic.Int64
	failed    atomic.Bool
}

func (f *fakeREST) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f.calls.Add(1)
		switch {
		case r.URL.Path == "/v1/chain/head_info":
			lib := f.lib
			if lib == 0 {
				lib = 1 << 40
			}
			json.NewEncoder(w).Encode(map[string]interface{}{"head_topology": map[string]string{"height": strconv.FormatUint(lib+60, 10)}, "last_irreversible_block": strconv.FormatUint(lib, 10)})
		case strings.HasPrefix(r.URL.Path, "/v1/account/"+tok+"/history"):
			seq, _ := strconv.Atoi(r.URL.Query().Get("sequence_number"))
			limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
			if r.URL.Query().Get("ascending") != "true" || r.URL.Query().Get("irreversible") != "true" {
				http.Error(w, "bad params", 400)
				return
			}
			if f.failAtSeq > 0 && seq == f.failAtSeq && !f.failed.Swap(true) {
				http.Error(w, "boom", 400)
				return
			}
			if limit > f.pageLimit {
				limit = f.pageLimit
			}
			var out []map[string]interface{}
			for _, e := range f.entries {
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
			if len(out) == 0 && f.emptyObj {
				w.Write([]byte("{}")) // what koinos-rest actually sends for an empty history
				return
			}
			json.NewEncoder(w).Encode(out)
		case strings.HasPrefix(r.URL.Path, "/v1/transaction/"):
			id := strings.TrimPrefix(r.URL.Path, "/v1/transaction/")
			for _, e := range f.entries {
				if txID(e.seq) == id {
					json.NewEncoder(w).Encode(map[string]interface{}{"containing_blocks": append(append([]string{}, e.forks...), blockID(e.height))})
					return
				}
			}
			http.Error(w, "unknown tx", 404)
		case strings.HasPrefix(r.URL.Path, "/v1/block/"):
			key := strings.TrimPrefix(r.URL.Path, "/v1/block/")
			for _, e := range f.entries {
				canon := map[string]interface{}{"block_id": blockID(e.height), "block_height": strconv.FormatUint(e.height, 10), "block": map[string]interface{}{"header": map[string]string{"timestamp": strconv.FormatUint(e.height*1000, 10)}}}
				if key == blockID(e.height) || key == strconv.FormatUint(e.height, 10) {
					json.NewEncoder(w).Encode(canon)
					return
				}
				for _, fk := range e.forks {
					if key == fk { // a fork block: same height, different id
						json.NewEncoder(w).Encode(map[string]interface{}{"block_id": fk, "block_height": strconv.FormatUint(e.height, 10), "block": map[string]interface{}{"header": map[string]string{"timestamp": "1"}}})
						return
					}
				}
			}
			http.Error(w, "unknown block", 404)
		default:
			http.Error(w, "unexpected "+r.URL.Path, 404)
		}
	}
}

func newServer(t *testing.T, f *fakeREST) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(f.handler())
	t.Cleanup(srv.Close)
	return srv
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

func register(t *testing.T, s store.Store, cutoff uint64) {
	t.Helper()
	if err := s.UpsertBackfill(&store.Backfill{Token: tok, NextSeq: 0, CutoffHeight: cutoff}); err != nil {
		t.Fatal(err)
	}
}

func TestRunAppliesHistoryUpToCutoff(t *testing.T) {
	f := &fakeREST{entries: fixture(), pageLimit: 100}
	srv := newServer(t, f)
	s := newStore(t)
	register(t, s, 200)

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
	bf, err := s.GetBackfill(tok)
	if err != nil || bf == nil || !bf.Done || bf.NextSeq != 4 || bf.Verified {
		t.Fatalf("progress %+v err %v", bf, err)
	}
	addr, err := s.GetAddress(beta)
	if err != nil || addr.FirstSeenHeight != 105 {
		t.Fatalf("first seen of beta: %+v %v", addr, err)
	}

	// a second run is a no-op: nothing double-counted, no REST calls
	before := f.calls.Load()
	res, err = Run(context.Background(), s, srv.URL, "", tok)
	if err != nil || !res.Done || f.calls.Load() != before {
		t.Fatalf("second run: %+v err=%v calls=%d→%d", res, err, before, f.calls.Load())
	}
	if balance(t, s, alfa) != "70" {
		t.Fatal("balance changed on the no-op run")
	}
}

func TestExactCutoffHeightIsIncluded(t *testing.T) {
	srv := newServer(t, &fakeREST{entries: fixture(), pageLimit: 100})
	s := newStore(t)
	register(t, s, 105) // the two entries at 105 belong to the backfill, 110 to live sync
	res, err := Run(context.Background(), s, srv.URL, "", tok)
	if err != nil || !res.Done || res.Ops != 3 || res.StoppedAt != 110 {
		t.Fatalf("result %+v %v", res, err)
	}
	if a, b := balance(t, s, alfa), balance(t, s, beta); a != "70" || b != "20" {
		t.Fatalf("balances alfa=%s beta=%s", a, b)
	}
}

func TestFreshDatabaseCutoffZeroBackfillsNothing(t *testing.T) {
	// registered on an empty database: the historical sync will index the
	// token from genesis itself, the backfill must not replay anything
	srv := newServer(t, &fakeREST{entries: fixture(), pageLimit: 100})
	s := newStore(t)
	register(t, s, 0)
	res, err := Run(context.Background(), s, srv.URL, "", tok)
	if err != nil || !res.Done || res.Ops != 0 || res.StoppedAt != 100 {
		t.Fatalf("result %+v %v", res, err)
	}
	if balance(t, s, alfa) != "0" {
		t.Fatal("nothing must be applied below a zero cutoff")
	}
}

func TestRunResumesAfterFailure(t *testing.T) {
	old := PageSize
	PageSize = 2
	defer func() { PageSize = old }()
	f := &fakeREST{entries: fixture(), pageLimit: 2, failAtSeq: 2}
	srv := newServer(t, f)
	s := newStore(t)
	register(t, s, 1000)
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
	if a, b := balance(t, s, alfa), balance(t, s, beta); a != "1070" || b != "25" {
		t.Fatalf("final balances alfa=%s beta=%s", a, b)
	}
	if rows, _ := s.GetRecentTransfersFiltered(tok, "", 10); len(rows) != 5 {
		t.Fatalf("expected 5 transfer rows, got %d", len(rows))
	}
}

func TestEmptyHistoryObjectMeansNoEntries(t *testing.T) {
	old := PageSize
	PageSize = 5
	defer func() { PageSize = old }()
	// exactly one full page, then koinos-rest answers {} for the next one
	srv := newServer(t, &fakeREST{entries: fixture(), pageLimit: 5, emptyObj: true})
	s := newStore(t)
	register(t, s, 1000)
	res, err := Run(context.Background(), s, srv.URL, "", tok)
	if err != nil || !res.Done || res.Ops != 5 {
		t.Fatalf("result %+v %v", res, err)
	}
	srv2 := newServer(t, &fakeREST{entries: nil, pageLimit: 100, emptyObj: true})
	s2 := newStore(t)
	register(t, s2, 1000)
	if res, err := Run(context.Background(), s2, srv2.URL, "", tok); err != nil || !res.Done || res.Entries != 0 {
		t.Fatalf("empty token: %+v %v", res, err)
	}
}

func TestForkBlockIsIgnored(t *testing.T) {
	entries := fixture()
	entries[1].forks = []string{forkID(105)} // the transaction store lists a fork block first
	srv := newServer(t, &fakeREST{entries: entries, pageLimit: 100})
	s := newStore(t)
	register(t, s, 200)
	res, err := Run(context.Background(), s, srv.URL, "", tok)
	if err != nil || !res.Done || res.Ops != 4 {
		t.Fatalf("result %+v %v", res, err)
	}
	rows, _ := s.GetRecentTransfersFiltered(tok, "transfer", 5)
	if len(rows) != 1 || rows[0].Height != 105 || rows[0].Timestamp != 105000 {
		t.Fatalf("the transfer must carry the canonical block's position: %+v", rows)
	}
}

func TestUndecodedEventsAreDecodedLocally(t *testing.T) {
	// the node had no ABI for the token: event data arrives as base64 protobuf
	rawTransfer := protowire.AppendVarint(protowire.AppendTag(
		protowire.AppendBytes(protowire.AppendTag(
			protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), mustDecode(alfa)), 2, protowire.BytesType), mustDecode(beta)), 3, protowire.VarintType), 30)
	rawMint := protowire.AppendVarint(protowire.AppendTag(protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), mustDecode(alfa)), 2, protowire.VarintType), 100)
	entries := []fixEntry{
		{0, 100, false, nil, []map[string]interface{}{ev(tok, "koinos.contracts.token.mint_event", base64.URLEncoding.EncodeToString(rawMint))}},
		{1, 105, false, nil, []map[string]interface{}{ev(tok, "koinos.contracts.token.transfer_event", base64.URLEncoding.EncodeToString(rawTransfer))}},
	}
	srv := newServer(t, &fakeREST{entries: entries, pageLimit: 100})
	s := newStore(t)
	register(t, s, 200)
	res, err := Run(context.Background(), s, srv.URL, "", tok)
	if err != nil || !res.Done || res.Ops != 2 {
		t.Fatalf("result %+v %v", res, err)
	}
	if a, b := balance(t, s, alfa), balance(t, s, beta); a != "70" || b != "30" {
		t.Fatalf("balances alfa=%s beta=%s", a, b)
	}
	// garbage that is neither an object nor base64 must stop the run, not be skipped
	bad := []fixEntry{{0, 100, false, nil, []map[string]interface{}{ev(tok, "koinos.contracts.token.mint_event", "%%%not-base64%%%")}}}
	srv2 := newServer(t, &fakeREST{entries: bad, pageLimit: 100})
	s2 := newStore(t)
	register(t, s2, 200)
	if _, err := Run(context.Background(), s2, srv2.URL, "", tok); err == nil {
		t.Fatal("an undecodable token event must fail the run")
	}
}

func mustDecode(addr string) []byte {
	b, err := base58.Decode(addr)
	if err != nil {
		panic(err)
	}
	return b
}

func TestLowerFirstSeen(t *testing.T) {
	s := newStore(t)
	if err := s.UpsertAddress(alfa, 500, 500000); err != nil {
		t.Fatal(err)
	}
	srv := newServer(t, &fakeREST{entries: fixture(), pageLimit: 100})
	register(t, s, 200)
	if _, err := Run(context.Background(), s, srv.URL, "", tok); err != nil {
		t.Fatal(err)
	}
	a, err := s.GetAddress(alfa)
	if err != nil || a.FirstSeenHeight != 100 || a.FirstSeenTime != 100000 {
		t.Fatalf("first seen must move back to the backfilled occurrence: %+v %v", a, err)
	}
}

func TestDryRunMatchesRun(t *testing.T) {
	srv := newServer(t, &fakeREST{entries: fixture(), pageLimit: 100})
	balances, res, err := DryRun(context.Background(), srv.URL, "", tok)
	if err != nil || !res.Done || res.Ops != 5 {
		t.Fatalf("dry run %+v %v", res, err)
	}
	s := newStore(t)
	register(t, s, 1000)
	if _, err := Run(context.Background(), s, srv.URL, "", tok); err != nil {
		t.Fatal(err)
	}
	for addr, want := range balances {
		if got := balance(t, s, addr); got != want.String() {
			t.Fatalf("%s: dry run %s, run %s", addr, want, got)
		}
	}
	if len(balances) != 2 {
		t.Fatalf("holders %v", balances)
	}
}

func TestFallbackNodeResolvesTransaction(t *testing.T) {
	good := &fakeREST{entries: fixture(), pageLimit: 100}
	goodSrv := newServer(t, good)
	broken := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/transaction/") && strings.HasSuffix(r.URL.Path, fmt.Sprintf("%064x", 2)) {
			http.Error(w, `{"error":"unknown error"}`, 500)
			return
		}
		http.Redirect(w, r, goodSrv.URL+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	}))
	defer broken.Close()
	s := newStore(t)
	register(t, s, 1000)
	res, err := Run(context.Background(), s, broken.URL, goodSrv.URL, tok)
	if err != nil || !res.Done || res.Ops != 5 {
		t.Fatalf("run with fallback: %+v %v", res, err)
	}
	if good.calls.Load() == 0 {
		t.Fatal("fallback node was never asked")
	}
	s2 := newStore(t)
	register(t, s2, 1000)
	if _, err := Run(context.Background(), s2, broken.URL, "", tok); err == nil {
		t.Fatal("expected failure without a fallback node")
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
	ok := []event{
		raw(ev(tok, "koinos.contracts.token.transfer_event", map[string]string{"from": alfa, "to": beta, "value": "0"})), // no effect, skipped
		raw(ev(tok, "koinos.contracts.token.approve_event", map[string]string{"from": alfa, "to": beta, "value": "5"})),  // not a balance event
		raw(ev(tok, "koinos.contracts.token.transfer_event", map[string]string{"from": alfa, "to": beta, "value": "7"})),
	}
	ops, err := opsOf(tok, 1, b, "0x1220", ok)
	if err != nil || len(ops) != 1 || ops[0].Value != 7 {
		t.Fatalf("ops %+v %v", ops, err)
	}
	for _, bad := range [][]event{
		{raw(ev(tok, "koinos.contracts.token.transfer_event", map[string]string{"from": alfa, "to": beta, "value": "-5"}))},
		{raw(ev(tok, "koinos.contracts.token.transfer_event", map[string]string{"from": "not*base58", "to": beta, "value": "5"}))},
		{raw(ev(tok, "koinos.contracts.token.transfer_event", map[string]string{"from": alfa, "to": beta, "value": "99999999999999999999"}))},
	} {
		if _, err := opsOf(tok, 1, b, "0x1220", bad); err == nil {
			t.Fatalf("garbage must be an error, not skipped: %+v", bad)
		}
	}
}

func TestLaggingHistoryIsNotDone(t *testing.T) {
	// history ends (short page) but the node's LIB is still below the cutoff:
	// apply what arrived, keep the cursor, but do not call the replay done
	f := &fakeREST{entries: fixture()[:2], pageLimit: 100, lib: 150}
	srv := newServer(t, f)
	s := newStore(t)
	register(t, s, 200)
	res, err := Run(context.Background(), s, srv.URL, "", tok)
	if err == nil || res.Done {
		t.Fatalf("lagging history must not complete: %+v %v", res, err)
	}
	bf, _ := s.GetBackfill(tok)
	if bf.Done || bf.NextSeq != 2 || balance(t, s, alfa) != "70" {
		t.Fatalf("progress %+v alfa=%s", bf, balance(t, s, alfa))
	}
	f.lib = 250 // the node caught up
	res, err = Run(context.Background(), s, srv.URL, "", tok)
	if err != nil || !res.Done {
		t.Fatalf("after catch-up: %+v %v", res, err)
	}
	if bf, _ = s.GetBackfill(tok); !bf.Done || balance(t, s, alfa) != "70" {
		t.Fatalf("no double counting on resume: %+v alfa=%s", bf, balance(t, s, alfa))
	}
}

func TestMalformedProtobufStopsRun(t *testing.T) {
	bad := []fixEntry{{0, 100, false, nil, []map[string]interface{}{ev(tok, "koinos.contracts.token.mint_event", base64.URLEncoding.EncodeToString([]byte{0xff}))}}}
	srv := newServer(t, &fakeREST{entries: bad, pageLimit: 100})
	s := newStore(t)
	register(t, s, 200)
	if _, err := Run(context.Background(), s, srv.URL, "", tok); err == nil {
		t.Fatal("malformed protobuf must fail the run instead of becoming a zero-value event")
	}
	if _, _, _, err := decodeRawTokenEvent("mint", []byte{0x0a, 0x05, 0x01}); err == nil {
		t.Fatal("truncated bytes field must be rejected")
	}
	f, to, v, err := decodeRawTokenEvent("transfer", protowire.AppendVarint(protowire.AppendTag(
		protowire.AppendBytes(protowire.AppendTag(protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), mustDecode(alfa)), 2, protowire.BytesType), mustDecode(beta)), 3, protowire.VarintType), 42))
	if err != nil || f != alfa || to != beta || v != 42 {
		t.Fatalf("decode transfer: %s %s %d %v", f, to, v, err)
	}
}

func TestCanonicalLookupUsesPrimaryOnly(t *testing.T) {
	// the fallback must never answer the block-at-height question: if the
	// primary cannot, the run stops (resumable) instead of trusting another chain view
	good := newServer(t, &fakeREST{entries: fixture(), pageLimit: 100})
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/block/") && !strings.HasPrefix(strings.TrimPrefix(r.URL.Path, "/v1/block/"), "0x") {
			http.Error(w, `{"error":"unknown error"}`, 500) // block-by-height unavailable
			return
		}
		http.Redirect(w, r, good.URL+r.URL.RequestURI(), http.StatusTemporaryRedirect)
	}))
	defer primary.Close()
	s := newStore(t)
	register(t, s, 200)
	if _, err := Run(context.Background(), s, primary.URL, good.URL, tok); err == nil {
		t.Fatal("canonical lookup must not be served by the fallback node")
	}
}
