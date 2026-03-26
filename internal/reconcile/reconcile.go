package reconcile

import (
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/koinos/koinos-log-golang/v2"
	"github.com/koinos/koinos-token-tracker/internal/indexer"
	"github.com/koinos/koinos-token-tracker/internal/store"
)

const (
	concurrency = 50
	httpTimeout = 10 * time.Second
	maxRetries  = 3
	writeBatch  = 5000 // flush to DB every N updates
)

type balanceResponse struct {
	Value string `json:"value"`
}

type balanceUpdate struct {
	Address string
	Token   string
	Balance string
}

// Run queries the REST API for every known address's KOIN and VHP balance
// and updates the store with authoritative values.
func Run(s store.Store, restURL string) error {
	_, total, err := s.GetAddresses(1, 0)
	if err != nil {
		return fmt.Errorf("get address count: %w", err)
	}

	log.Infof("Reconciling balances for %d addresses against %s", total, restURL)

	client := &http.Client{
		Timeout: httpTimeout,
		Transport: &http.Transport{
			MaxIdleConns:        concurrency + 10,
			MaxIdleConnsPerHost: concurrency + 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// Only reconcile VHP — KOIN is already accurate from event tracking.
	// VHP needs reconciliation because the KCS-4 migration pre-loaded balances
	// without emitting events for existing holders.
	tokens := []struct {
		contract string
		symbol   string
	}{
		{indexer.VhpContract, "VHP"},
	}

	var checked atomic.Int64
	var fetchErrors atomic.Int64
	var written atomic.Int64
	start := time.Now()

	lastHeight, _, _ := s.GetSyncState()

	// Writer goroutine: receives updates via channel, flushes in batches
	updateCh := make(chan balanceUpdate, concurrency*4)
	writerDone := make(chan error, 1)

	go func() {
		var buf []balanceUpdate
		flush := func() error {
			if len(buf) == 0 {
				return nil
			}
			if err := s.BeginBatch(); err != nil {
				return fmt.Errorf("begin batch: %w", err)
			}
			for _, u := range buf {
				if err := s.SetBalance(u.Address, u.Token, u.Balance, lastHeight); err != nil {
					s.RollbackBatch()
					return fmt.Errorf("set balance for %s: %w", u.Address, err)
				}
			}
			if err := s.CommitBatch(); err != nil {
				return fmt.Errorf("commit batch: %w", err)
			}
			written.Add(int64(len(buf)))
			buf = buf[:0]
			return nil
		}

		for u := range updateCh {
			buf = append(buf, u)
			if len(buf) >= writeBatch {
				if err := flush(); err != nil {
					writerDone <- err
					// Drain remaining channel items
					for range updateCh {
					}
					return
				}
			}
		}
		// Final flush
		writerDone <- flush()
	}()

	// Progress reporter
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c := checked.Load()
				elapsed := time.Since(start).Seconds()
				rate := float64(c) / elapsed
				pct := float64(c) / float64(total) * 100
				log.Infof("Reconcile: %d/%d (%.1f%%), %d written, %.0f/sec, %d errors",
					c, total, pct, written.Load(), rate, fetchErrors.Load())
			case <-done:
				return
			}
		}
	}()

	// Fetch balances concurrently
	work := make(chan string, concurrency*2)
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for addr := range work {
				for _, tok := range tokens {
					bal, ok := fetchBalanceWithRetry(client, restURL, addr, tok.contract)
					if !ok {
						fetchErrors.Add(1)
						continue
					}
					updateCh <- balanceUpdate{
						Address: addr,
						Token:   tok.contract,
						Balance: bal,
					}
				}
				checked.Add(1)
			}
		}()
	}

	// Feed addresses page by page
	pageSize := 1000
	for offset := 0; offset < total; offset += pageSize {
		addrs, _, err := s.GetAddresses(pageSize, offset)
		if err != nil {
			return fmt.Errorf("get addresses at offset %d: %w", offset, err)
		}
		for _, a := range addrs {
			work <- a.Address
		}
	}
	close(work)
	wg.Wait()
	close(updateCh)
	close(done)

	// Wait for writer to finish
	writeErr := <-writerDone

	fetchElapsed := time.Since(start)
	errCount := fetchErrors.Load()
	errRate := float64(errCount) / float64(total) * 100

	log.Infof("Reconciliation: %d addresses, %d written, %d fetch errors (%.1f%%) in %s",
		checked.Load(), written.Load(), errCount, errRate, fetchElapsed.Round(time.Second))

	if writeErr != nil {
		return fmt.Errorf("write failed: %w", writeErr)
	}
	if errRate > 10 {
		return fmt.Errorf("too many fetch errors: %d/%d (%.1f%%)", errCount, total, errRate)
	}

	return nil
}

func fetchBalanceWithRetry(client *http.Client, restURL, address, contract string) (string, bool) {
	for attempt := 0; attempt < maxRetries; attempt++ {
		bal, status := fetchBalance(client, restURL, address, contract)
		if status == "ok" {
			return bal, true
		}
		if status == "error" {
			time.Sleep(time.Duration(attempt+1) * 500 * time.Millisecond)
			continue
		}
	}
	return "", false
}

func fetchBalance(client *http.Client, restURL, address, contract string) (string, string) {
	url := fmt.Sprintf("%s/v1/account/%s/balance/%s", restURL, address, contract)
	resp, err := client.Get(url)
	if err != nil {
		return "", "error"
	}
	defer resp.Body.Close()

	if resp.StatusCode == 400 || resp.StatusCode == 404 {
		io.Copy(io.Discard, resp.Body)
		return "0", "ok" // no balance or invalid address
	}
	if resp.StatusCode != 200 {
		io.Copy(io.Discard, resp.Body)
		return "", "error" // 429, 5xx — retry
	}

	var data balanceResponse
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return "", "error"
	}

	sat := decimalToSatoshis(data.Value)
	if sat == "" {
		return "", "error"
	}
	return sat, "ok"
}

// decimalToSatoshis converts "147242.54875823" → "14724254875823".
func decimalToSatoshis(s string) string {
	if s == "" {
		return "" // empty = error, not zero
	}
	if s == "0" || s == "0.0" || s == "0.00000000" {
		return "0"
	}
	if strings.HasPrefix(s, "-") {
		return ""
	}

	parts := strings.SplitN(s, ".", 2)
	whole := parts[0]
	frac := ""
	if len(parts) == 2 {
		frac = parts[1]
	}

	// Reject >8 fractional digits unless trailing zeros
	if len(frac) > 8 {
		// Check if extra digits are all zeros
		extra := frac[8:]
		for _, c := range extra {
			if c != '0' {
				return "" // unexpected precision
			}
		}
		frac = frac[:8]
	}

	if len(frac) < 8 {
		frac = frac + strings.Repeat("0", 8-len(frac))
	}

	combined := whole + frac
	val, ok := new(big.Int).SetString(combined, 10)
	if !ok || val.Sign() < 0 {
		return ""
	}
	return val.String()
}
