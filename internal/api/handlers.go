package api

import (
	"encoding/json"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/koinos/koinos-token-tracker/internal/store"
)

const maxOffset = 100000

// Valid base58 characters (Bitcoin alphabet)
var validBase58 = regexp.MustCompile(`^[1-9A-HJ-NP-Za-km-z]+$`)

type handlers struct {
	store store.Store
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("JSON encode error: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func internalError(w http.ResponseWriter, err error) {
	log.Printf("internal error: %v", err)
	writeError(w, http.StatusInternalServerError, "internal server error")
}

func queryInt(r *http.Request, key string, def int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return def
	}
	return n
}

func queryUint64(r *http.Request, key string, def uint64) uint64 {
	v := r.URL.Query().Get(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}

// extractPathParam extracts and validates a base58 path parameter.
func extractPathParam(r *http.Request, index int) (string, bool) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) <= index || parts[index] == "" {
		return "", false
	}
	param := parts[index]
	if len(param) > 50 || !validBase58.MatchString(param) {
		return "", false
	}
	return param, true
}

// clampOffset caps offset to prevent expensive DB scans.
func clampOffset(offset int) int {
	if offset > maxOffset {
		return maxOffset
	}
	return offset
}

// GET /v1/token-tracker/status
func (h *handlers) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	height, blockID, err := h.store.GetSyncState()
	if err != nil {
		internalError(w, err)
		return
	}

	stats, err := h.store.GetStats()
	if err != nil {
		internalError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"last_indexed_height": height,
		"last_block_id":       blockID,
		"total_addresses":     stats.TotalAddresses,
		"koin_holders":        stats.KoinHolders,
		"vhp_holders":         stats.VhpHolders,
	})
}

// GET /v1/token-tracker/stats
func (h *handlers) handleStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	stats, err := h.store.GetStats()
	if err != nil {
		internalError(w, err)
		return
	}

	tokens, err := h.store.GetAllTokens()
	if err != nil {
		internalError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"total_addresses":     stats.TotalAddresses,
		"koin_holders":        stats.KoinHolders,
		"vhp_holders":         stats.VhpHolders,
		"last_indexed_height": stats.LastHeight,
		"tokens":              tokens,
	})
}

// GET /v1/token-tracker/addresses?limit=50&offset=0
func (h *handlers) handleAddresses(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	limit := queryInt(r, "limit", 50)
	offset := clampOffset(queryInt(r, "offset", 0))
	if limit > 1000 {
		limit = 1000
	}

	addresses, total, err := h.store.GetAddresses(limit, offset)
	if err != nil {
		internalError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"total":     total,
		"limit":     limit,
		"offset":    offset,
		"addresses": addresses,
	})
}

// GET /v1/token-tracker/address/{address}
func (h *handlers) handleAddress(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	addr, ok := extractPathParam(r, 4)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid address")
		return
	}

	address, err := h.store.GetAddress(addr)
	if err != nil {
		internalError(w, err)
		return
	}
	if address == nil {
		writeError(w, http.StatusNotFound, "address not found")
		return
	}

	balances, err := h.store.GetBalances(addr)
	if err != nil {
		internalError(w, err)
		return
	}

	balanceMap := make(map[string]string)
	for _, b := range balances {
		balanceMap[b.Token] = b.Balance
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"address":           address.Address,
		"first_seen_height": address.FirstSeenHeight,
		"first_seen_time":   address.FirstSeenTime,
		"balances":          balanceMap,
	})
}

// GET /v1/token-tracker/holders/{token}?limit=50&offset=0
func (h *handlers) handleHolders(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	token, ok := extractPathParam(r, 4)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid token address")
		return
	}

	limit := queryInt(r, "limit", 50)
	offset := clampOffset(queryInt(r, "offset", 0))
	if limit > 1000 {
		limit = 1000
	}

	holders, total, err := h.store.GetTopHolders(token, limit, offset)
	if err != nil {
		internalError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"token":   token,
		"total":   total,
		"limit":   limit,
		"offset":  offset,
		"holders": holders,
	})
}

// GET /v1/token-tracker/blocks?from=100&to=200
func (h *handlers) handleBlocks(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	from := queryUint64(r, "from", 0)
	to := queryUint64(r, "to", 0)

	if to == 0 {
		height, _, err := h.store.GetSyncState()
		if err != nil {
			internalError(w, err)
			return
		}
		to = height
		if to > 49 {
			from = to - 49
		}
	}

	if from > to {
		writeError(w, http.StatusBadRequest, "from must be <= to")
		return
	}
	if to-from > 1000 {
		to = from + 1000
	}

	blocks, err := h.store.GetBlocks(from, to)
	if err != nil {
		internalError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"from":   from,
		"to":     to,
		"blocks": blocks,
	})
}

// GET /v1/token-tracker/transfers/{address}?limit=50&offset=0
func (h *handlers) handleTransfers(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	query, ok := extractPathParam(r, 4)
	if !ok {
		writeError(w, http.StatusBadRequest, "invalid address")
		return
	}

	limit := queryInt(r, "limit", 50)
	offset := clampOffset(queryInt(r, "offset", 0))
	if limit > 500 {
		limit = 500
	}

	// "recent" is not a valid base58 address, so it can't shadow a real one.
	// Optional filters: ?token=<contract> and ?type=transfer|mint|burn — the
	// explorer's token pages use them to show real transfers instead of the
	// block-reward mints that dominate the unfiltered feed.
	if query == "recent" {
		token := r.URL.Query().Get("token")
		eventType := r.URL.Query().Get("type")
		if token != "" && (len(token) > 50 || !validBase58.MatchString(token)) {
			writeError(w, http.StatusBadRequest, "invalid token address")
			return
		}
		if eventType != "" && eventType != "transfer" && eventType != "mint" && eventType != "burn" {
			writeError(w, http.StatusBadRequest, "invalid type (transfer, mint or burn)")
			return
		}
		if eventType != "" && token == "" {
			writeError(w, http.StatusBadRequest, "type requires token")
			return
		}
		var (
			transfers []store.Transfer
			err       error
		)
		if token != "" {
			transfers, err = h.store.GetRecentTransfersFiltered(token, eventType, limit)
		} else {
			transfers, err = h.store.GetRecentTransfers(limit)
		}
		if err != nil {
			internalError(w, err)
			return
		}
		resp := map[string]interface{}{
			"limit":     limit,
			"transfers": transfers,
		}
		if token != "" {
			resp["token"] = token
			resp["window_blocks"] = store.RecentFilterWindow // filtered feeds look this far back from the sync head
		}
		if eventType != "" {
			resp["type"] = eventType
		}
		writeJSON(w, http.StatusOK, resp)
		return
	}

	transfers, total, capped, err := h.store.GetTransfersByAddress(query, limit, offset)
	if err != nil {
		internalError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"query":        query,
		"total":        total,
		"total_capped": capped,
		"limit":        limit,
		"offset":       offset,
		"transfers":    transfers,
	})
}

// GET /v1/token-tracker/producers
func (h *handlers) handleProducers(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	// 24h at 3s block time
	producers, total, err := h.store.GetProducers(28800)
	if err != nil {
		internalError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"producers":    producers,
		"total_blocks": total,
	})
}

// GET /v1/token-tracker/tokens
func (h *handlers) handleTokens(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	tokens, err := h.store.GetAllTokens()
	if err != nil {
		internalError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"tokens": tokens,
	})
}
