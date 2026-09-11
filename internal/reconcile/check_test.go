package reconcile

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/koinos/koinos-token-tracker/internal/store"
	"github.com/mr-tron/base58"
	"google.golang.org/protobuf/encoding/protowire"
)

func TestCheckTokenAgainstJSONRPC(t *testing.T) {
	const tok = "1TKNxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	a := base58.Encode(append([]byte{0x00}, []byte("holder-a-xxxxxxxxxxxxxxx")...))
	b := base58.Encode(append([]byte{0x00}, []byte("holder-b-xxxxxxxxxxxxxxx")...))
	c := base58.Encode(append([]byte{0x00}, []byte("holder-c-xxxxxxxxxxxxxxx")...))
	chain := map[string]uint64{a: 70, b: 99} // b differs from the store, c fails
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Params struct {
				Args string `json:"args"`
			} `json:"params"`
		}
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &req)
		raw, _ := base64.URLEncoding.DecodeString(req.Params.Args)
		_, _, n := protowire.ConsumeTag(raw)
		owner, _ := protowire.ConsumeBytes(raw[n:])
		addr := base58.Encode(owner)
		if addr == c {
			http.Error(w, "boom", 500)
			return
		}
		var payload []byte
		if v := chain[addr]; v > 0 {
			payload = protowire.AppendVarint(protowire.AppendTag(nil, 1, protowire.VarintType), v)
		}
		json.NewEncoder(w).Encode(map[string]interface{}{"jsonrpc": "2.0", "id": 1, "result": map[string]string{"result": base64.URLEncoding.EncodeToString(payload)}})
	}))
	defer srv.Close()

	s, err := store.Open(filepath.Join(t.TempDir(), "t.db"), "1KNxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "1VHPyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for addr, bal := range map[string]string{a: "70", b: "25", c: "1"} {
		if err := s.SetBalance(addr, tok, bal, 1); err != nil {
			t.Fatal(err)
		}
	}
	res, err := CheckToken(context.Background(), s, srv.URL, tok)
	if err != nil {
		t.Fatal(err)
	}
	if res.Checked != 3 || res.Mismatches != 1 || res.Failures != 1 || res.Verified() {
		t.Fatalf("result %+v", res)
	}
	if got, _ := s.GetBalance(b, tok); got != "25" {
		t.Fatalf("check must not write; stored balance of b is now %s", got)
	}
	// an empty protobuf result is a zero balance
	v, err := decodeUint64Field1(nil)
	if err != nil || v.Sign() != 0 {
		t.Fatalf("empty result: %v %v", v, err)
	}
}

func TestZeroHoldersIsNotVerified(t *testing.T) {
	r := &CheckResult{}
	if r.Verified() {
		t.Fatal("no holders checked must not count as verified")
	}
	r = &CheckResult{Checked: 1}
	if !r.Verified() {
		t.Fatal("one holder checked without findings is verified")
	}
	r = &CheckResult{Checked: 1, Failures: 1}
	if r.Verified() {
		t.Fatal("a failed lookup must not count as verified")
	}
}
