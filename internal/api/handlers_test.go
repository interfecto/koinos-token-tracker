package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/koinos/koinos-token-tracker/internal/store"
)

func TestHandleTransfersRecentFilters(t *testing.T) {
	const koin, vhp = "1KNxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "1VHPyyyyyyyyyyyyyyyyyyyyyyyyyyyyy"
	s, err := store.Open(filepath.Join(t.TempDir(), "t.db"), koin, vhp)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for h := uint64(1); h <= 30; h++ {
		_ = s.InsertTransfer(h, "tx", koin, "from", "to", "1", "mint", h)
		if h%10 == 0 {
			_ = s.InsertTransfer(h, "tx", koin, "from", "to", "1", "transfer", h)
		}
	}
	_ = s.SetSyncState(30, "head")
	h := &handlers{store: s}

	call := func(url string) (int, map[string]interface{}) {
		t.Helper()
		rr := httptest.NewRecorder()
		h.handleTransfers(rr, httptest.NewRequest(http.MethodGet, url, nil))
		var body map[string]interface{}
		if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
			t.Fatalf("%s: bad json %q", url, rr.Body.String())
		}
		return rr.Code, body
	}

	prefixes := []string{"/v1/indexer", "/v1/token-tracker"}
	for _, prefix := range prefixes {
		code, body := call(prefix + "/transfers/recent?limit=5&token=" + koin + "&type=transfer")
		if code != 200 {
			t.Fatalf("%s: code %d %v", prefix, code, body)
		}
		rows := body["transfers"].([]interface{})
		if len(rows) != 3 || body["type"] != "transfer" || body["token"] != koin || body["window_blocks"] == nil {
			t.Fatalf("%s: filtered response %v", prefix, body)
		}
		for _, r := range rows {
			if r.(map[string]interface{})["event_type"] != "transfer" {
				t.Fatalf("unfiltered row leaked: %v", r)
			}
		}
	}

	code, body := call("/v1/indexer/transfers/recent?limit=4")
	if code != 200 || len(body["transfers"].([]interface{})) != 4 || body["token"] != nil {
		t.Fatalf("unfiltered: %d %v", code, body)
	}

	bad := map[string]string{
		"/transfers/recent?type=transfer":                "type requires token",
		"/transfers/recent?token=" + koin + "&type=swap": "invalid type (transfer, mint or burn)",
		"/transfers/recent?token=not*valid":              "invalid token address",
	}
	for _, prefix := range prefixes {
		for path, want := range bad {
			code, body := call(prefix + path)
			if code != 400 || body["error"] != want {
				t.Fatalf("%s%s: got %d %v, want 400 %q", prefix, path, code, body, want)
			}
		}
	}
}
