package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path"
	"runtime"
	"strings"
	"syscall"

	"time"

	log "github.com/koinos/koinos-log-golang/v2"
	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-token-tracker/internal/api"
	"github.com/koinos/koinos-token-tracker/internal/backfill"
	"github.com/koinos/koinos-token-tracker/internal/config"
	"github.com/koinos/koinos-token-tracker/internal/reconcile"
	"github.com/koinos/koinos-token-tracker/internal/store"
	syncpkg "github.com/koinos/koinos-token-tracker/internal/sync"
	"github.com/mr-tron/base58"
	flag "github.com/spf13/pflag"
)

const (
	appName    = "koinos-token-tracker"
	appVersion = "0.3.0"
)

func main() {
	// -----------------------------------------------------------------------
	// CLI flags
	// -----------------------------------------------------------------------
	var (
		basedir       = flag.StringP("basedir", "d", getDefaultBaseDir(), "Base directory for data")
		amqpURL       = flag.StringP("amqp", "a", "amqp://guest:guest@localhost:5672/", "AMQP connection URL")
		logLevel      = flag.String("log-level", "info", "Log level (debug, info, warn, error)")
		logDir        = flag.String("log-dir", "", "Log directory (empty = stderr only)")
		logColor      = flag.Bool("log-color", true, "Enable colorized log output")
		logDatetime   = flag.Bool("log-datetime", true, "Include datetime in log output")
		instanceID    = flag.String("instance-id", "", "Instance ID for AMQP")
		_             = flag.UintP("jobs", "j", uint(runtime.NumCPU()), "Number of AMQP handler jobs (reserved)")
		version       = flag.BoolP("version", "v", false, "Print version and exit")
		port          = flag.IntP("port", "p", 8080, "HTTP API listen port")
		reset         = flag.Bool("reset", false, "Delete database and re-sync from genesis")
		reconcileFlag = flag.Bool("reconcile", false, "After sync, reconcile balances against REST API")
		apiOnly       = flag.Bool("api-only", false, "Serve the HTTP API from the existing database only: no AMQP, no sync, no writes (side-by-side testing)")
		restURL       = flag.String("rest-url", "http://127.0.0.1:3000", "REST API URL for balance reconciliation")
		configPath    = flag.StringP("config", "c", "", "Path to Koinos node config.yml (token addresses and migration height)")
		trackTokens   = flag.StringSlice("track-token", nil, "Token contract to index in addition to KOIN/VHP (repeatable; remembered in the database, history backfilled from the REST node on the next start)")
		dryRunToken   = flag.String("backfill-dry-run", "", "Replay a token's history from the REST node without touching any database, compare the resulting balances with the chain and exit")
		restFallback  = flag.String("rest-fallback-url", "https://api.koinos.io", "Second REST node asked when the primary cannot serve a transaction or block during a history backfill (empty = none)")
		rpcURL        = flag.String("jsonrpc-url", "https://api.koinos.io/", "Koinos JSON-RPC endpoint used to verify backfilled balances with balance_of")
	)

	flag.Parse()

	if *version {
		fmt.Printf("%s v%s\n", appName, appVersion)
		os.Exit(0)
	}

	// -----------------------------------------------------------------------
	// Logger
	// -----------------------------------------------------------------------
	if err := log.InitLogger(appName, *instanceID, *logLevel, *logDir, *logColor, *logDatetime); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to init logger: %v\n", err)
		os.Exit(1)
	}

	log.Infof("%s v%s starting", appName, appVersion)

	_ = instanceID // reserved for future multi-instance support

	// -----------------------------------------------------------------------
	// Config (token addresses + migration height)
	// -----------------------------------------------------------------------
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Errorf("Failed to load config: %v", err)
		os.Exit(1)
	}

	if *configPath != "" {
		log.Infof("Loaded config from %s", *configPath)
	}
	log.Infof("KOIN contract: %s", cfg.KoinContract)
	log.Infof("VHP contract: %s", cfg.VhpContract)
	if cfg.KCS4MigrationHeight > 0 {
		log.Infof("KCS-4 migration height: %d", cfg.KCS4MigrationHeight)
	}

	// -----------------------------------------------------------------------
	// Database
	// -----------------------------------------------------------------------
	dbPath := path.Join(*basedir, "token-tracker", "db", "token-tracker.db")
	// Backward compat: pre-rename deployments keep their database at
	// indexer/db/indexer.db — reuse it instead of re-indexing from genesis.
	if legacy := path.Join(*basedir, "indexer", "db", "indexer.db"); pathExists(legacy) && !pathExists(dbPath) {
		dbPath = legacy
	}

	if *dryRunToken != "" {
		os.Exit(runBackfillDryRun(*dryRunToken, *restURL, *restFallback, *rpcURL))
	}

	if *apiOnly && *reset {
		log.Error("--api-only and --reset are mutually exclusive: api-only must never delete the database")
		os.Exit(2)
	}
	if *apiOnly {
		runAPIOnly(dbPath, cfg.KoinContract, cfg.VhpContract, *port)
		return
	}

	if *reset {
		log.Infof("Reset requested, removing database at %s", dbPath)
		os.Remove(dbPath)
		os.Remove(dbPath + "-wal")
		os.Remove(dbPath + "-shm")
	}

	db, err := store.Open(dbPath, cfg.KoinContract, cfg.VhpContract)
	if err != nil {
		log.Errorf("Failed to open database: %v", err)
		os.Exit(1)
	}
	defer db.Close()
	log.Infof("Database opened at %s", dbPath)

	// Seed tracked token metadata
	if err := db.UpsertToken(cfg.KoinContract, "KOIN", 8, ""); err != nil {
		log.Errorf("Failed to seed KOIN token: %v", err)
		os.Exit(1)
	}
	if err := db.UpsertToken(cfg.VhpContract, "VHP", 8, ""); err != nil {
		log.Errorf("Failed to seed VHP token: %v", err)
		os.Exit(1)
	}

	// Additional tokens: register the ones given on the command line, then
	// track every token the database knows about from this run's first block.
	if err := registerTrackedTokens(db, cfg, *restURL, *trackTokens); err != nil {
		log.Errorf("Failed to register tracked tokens: %v", err)
		os.Exit(1)
	}
	if extras, err := loadExtraTokens(db, cfg); err != nil {
		log.Errorf("Failed to load tracked tokens: %v", err)
		os.Exit(1)
	} else if len(extras) > 0 {
		log.Infof("Tracking %d additional token(s): %v", len(extras), extras)
	}

	// -----------------------------------------------------------------------
	// AMQP client (for outgoing RPC calls)
	// -----------------------------------------------------------------------
	client := koinosmq.NewClient(*amqpURL, koinosmq.ExponentialBackoff)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Install signal handler early so Ctrl-C works during sync
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Infof("Received signal %s, shutting down...", sig)
		cancel()
	}()

	client.Start(ctx)
	log.Infof("AMQP client connected")

	// -----------------------------------------------------------------------
	// HTTP API server (starts immediately, serves partial data during sync)
	// -----------------------------------------------------------------------
	apiServer := api.NewServer(db, *port)
	go func() {
		if err := apiServer.Start(ctx); err != nil {
			log.Errorf("API server error: %v", err)
		}
	}()
	log.Infof("API server listening on port %d", *port)

	// -----------------------------------------------------------------------
	// Historical sync
	// -----------------------------------------------------------------------
	syncer := syncpkg.NewSyncer(client, db, cfg)

	// Pending history backfills must finish before any block above their
	// cutoff is processed (balances clamp at zero, so a later spend applied
	// before its earlier funding would be lost). The API keeps serving
	// meanwhile; block sync waits — and keeps waiting while a replay fails,
	// retrying every minute, because syncing past the cutoff with an
	// incomplete replay would corrupt balances permanently.
	for {
		err := runBackfills(ctx, db, *restURL, *restFallback, *rpcURL)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return
		}
		log.Errorf("Backfill incomplete: %v — block sync stays paused, retrying in 60s", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(60 * time.Second):
		}
	}

	log.Info("Starting historical sync...")
	if err := syncer.SyncToHead(ctx); err != nil {
		log.Errorf("Historical sync failed: %v", err)
		os.Exit(1)
	}
	log.Info("Historical sync complete")

	// -----------------------------------------------------------------------
	// Balance reconciliation (optional, fixes VHP migration gap)
	// -----------------------------------------------------------------------
	if *reconcileFlag {
		log.Infof("Reconciling balances against REST API at %s", *restURL)
		if err := reconcile.Run(db, *restURL, cfg.VhpContract); err != nil {
			log.Errorf("Reconciliation failed: %v", err)
		} else {
			log.Info("Balance reconciliation complete")
		}
	}

	// -----------------------------------------------------------------------
	// Live sync loop: periodically catch up to LIB (confirmed blocks only)
	// -----------------------------------------------------------------------
	log.Info("Entering live sync mode (confirmed-only, syncs to LIB every 10s)")

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Info("Shutdown complete")
			return
		case <-ticker.C:
			if err := syncer.SyncToHead(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Warnf("Live sync failed: %v", err)
			}
		}
	}
}

// getDefaultBaseDir returns the default base directory for data storage.
func getDefaultBaseDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".koinos"
	}
	return path.Join(home, ".koinos")
}

func pathExists(p string) bool {
	_, err := os.Stat(p)
	return err == nil
}

// runAPIOnly serves the HTTP API from an existing database that another
// instance keeps in sync (SQLite WAL allows concurrent readers). It opens the
// file read-only, never migrates, seeds or syncs, exits non-zero when the
// port cannot be bound, and on SIGINT/SIGTERM drains in-flight requests
// before closing the database. Used to try a new build on a side port before
// replacing the live service.
func runAPIOnly(dbPath, koinContract, vhpContract string, port int) {
	db, err := store.OpenReadOnly(dbPath, koinContract, vhpContract)
	if err != nil {
		log.Errorf("API-only: %v", err)
		os.Exit(1)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	apiServer := api.NewServer(db, port)
	errCh := make(chan error, 1)
	go func() { errCh <- apiServer.Start(ctx) }()
	log.Infof("API-only mode: serving port %d read-only from %s (no sync)", port, dbPath)

	select {
	case err := <-errCh:
		if err != nil {
			log.Errorf("API-only: server failed: %v", err)
			os.Exit(1)
		}
	case sig := <-sigCh:
		log.Infof("Received signal %s, shutting down...", sig)
		cancel()
		// Start returns once Shutdown has drained in-flight requests, or after
		// it gave up and force-closed the rest (reported as an error).
		if err := <-errCh; err != nil {
			log.Warnf("API-only: %v", err)
		}
	}
	log.Info("API-only: shutdown complete")
}

// isContractAddress accepts only a base58check-shaped Koinos address (25
// decoded bytes), never aliases like "koin" that the node would resolve but
// that never appear as an event source.
func isContractAddress(addr string) bool {
	raw, err := base58.Decode(addr)
	return err == nil && len(raw) == 25
}

// registerTrackedTokens records new --track-token contracts atomically:
// symbol and decimals from the REST node, plus a backfill record whose
// cutoff is the current sync height — live sync covers everything after it.
func registerTrackedTokens(db *store.SQLiteStore, cfg *config.TokenTrackerConfig, restURL string, addrs []string) error {
	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}
		if addr == cfg.KoinContract || addr == cfg.VhpContract {
			log.Warnf("--track-token %s: KOIN/VHP are always tracked", addr)
			continue
		}
		if !isContractAddress(addr) {
			return fmt.Errorf("--track-token %s: not a Koinos contract address", addr)
		}
		if bf, err := db.GetBackfill(addr); err != nil {
			return err
		} else if bf != nil {
			log.Infof("--track-token %s: already registered (replay done=%v, verified=%v, next seq %d)", addr, bf.Done, bf.Verified, bf.NextSeq)
			continue
		}
		info, err := fetchTokenInfo(restURL, addr)
		if err != nil {
			return fmt.Errorf("--track-token %s: %w", addr, err)
		}
		if err := db.BeginBatch(); err != nil {
			return err
		}
		head, _, err := db.GetSyncState()
		if err != nil {
			db.RollbackBatch()
			return err
		}
		if err := db.UpsertToken(addr, info.Symbol, info.Decimals, ""); err != nil {
			db.RollbackBatch()
			return err
		}
		if err := db.UpsertBackfill(&store.Backfill{Token: addr, NextSeq: 0, CutoffHeight: head}); err != nil {
			db.RollbackBatch()
			return err
		}
		if err := db.CommitBatch(); err != nil {
			return err
		}
		log.Infof("--track-token %s: registered %s (%d decimals), history up to height %d will be backfilled", addr, info.Symbol, info.Decimals, head)
	}
	return nil
}

// loadExtraTokens makes every token in the database (except KOIN/VHP) a
// tracked token for this run.
func loadExtraTokens(db *store.SQLiteStore, cfg *config.TokenTrackerConfig) ([]string, error) {
	tokens, err := db.GetAllTokens()
	if err != nil {
		return nil, err
	}
	var extras []string
	for _, t := range tokens {
		if t.Address != cfg.KoinContract && t.Address != cfg.VhpContract {
			extras = append(extras, t.Address)
		}
	}
	cfg.SetExtraTokens(extras)
	return extras, nil
}

// runBackfills replays the history of every token whose backfill is not done
// yet, one token at a time, then checks every holder against the chain and
// records the outcome. It returns an error as soon as one replay fails (the
// caller must not sync past that token's cutoff); a failed verification is
// only logged and retried on the next start.
func runBackfills(ctx context.Context, db *store.SQLiteStore, restURL, fallbackURL, rpcURL string) error {
	pending, err := db.ListBackfills()
	if err != nil {
		return err
	}
	for _, bf := range pending {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !bf.Done {
			log.Infof("Backfill %s: replaying history from seq %d (cutoff height %d)", bf.Token, bf.NextSeq, bf.CutoffHeight)
			res, err := backfill.Run(ctx, db, restURL, fallbackURL, bf.Token)
			if err != nil {
				return fmt.Errorf("%s: replay stopped at seq %d: %w", bf.Token, res.LastSeq, err)
			}
			bf.Done = true
		}
		if bf.Verified {
			continue
		}
		verifyBackfill(ctx, db, rpcURL, bf.Token)
	}
	return nil
}

// verifyBackfill compares the token's holders with the chain and stores the
// outcome; mismatches are reported, never overwritten (see reconcile.CheckToken).
func verifyBackfill(ctx context.Context, db *store.SQLiteStore, rpcURL, token string) {
	res, err := reconcile.CheckToken(ctx, db, rpcURL, token)
	if err != nil {
		log.Warnf("Backfill %s: verification did not run: %v (retried on next start)", token, err)
		return
	}
	bf, err := db.GetBackfill(token)
	if err != nil || bf == nil {
		log.Warnf("Backfill %s: cannot record verification: %v", token, err)
		return
	}
	bf.Verified = res.Verified()
	bf.Mismatches = res.Mismatches
	bf.Failures = res.Failures
	if err := db.UpsertBackfill(bf); err != nil {
		log.Warnf("Backfill %s: cannot record verification: %v", token, err)
	}
	if res.Verified() {
		log.Infof("Backfill %s: verified — all %d holders match the chain", token, res.Checked)
		return
	}
	log.Errorf("Backfill %s: NOT verified — %d of %d holders differ from the chain, %d lookups failed", token, res.Mismatches, res.Checked, res.Failures)
	for _, d := range res.Details {
		log.Errorf("Backfill %s:   %s", token, d)
	}
}

type tokenInfo struct {
	Symbol   string `json:"symbol"`
	Decimals int    `json:"decimals"`
}

func fetchTokenInfo(restURL, addr string) (*tokenInfo, error) {
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Get(strings.TrimRight(restURL, "/") + "/v1/token/" + addr + "/info")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token info: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var info tokenInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, fmt.Errorf("token info: %w", err)
	}
	if info.Symbol == "" || info.Decimals < 0 || info.Decimals > 30 {
		return nil, fmt.Errorf("token info: not a token (symbol %q, decimals %d)", info.Symbol, info.Decimals)
	}
	return &info, nil
}

// runBackfillDryRun replays a token's whole history in memory and compares
// every resulting balance with the chain's balance_of. Exit code 0 only when
// every holder was answered and matched. Touches no database.
func runBackfillDryRun(token, restURL, fallbackURL, rpcURL string) int {
	if !isContractAddress(token) {
		fmt.Fprintln(os.Stderr, "not a Koinos contract address")
		return 2
	}
	info, err := fetchTokenInfo(restURL, token)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	start := time.Now()
	balances, res, err := backfill.DryRun(context.Background(), restURL, fallbackURL, token)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dry run failed after %d entries: %v\n", res.Entries, err)
		return 1
	}
	fmt.Printf("%s (%s): %d history entries, %d token events, %d holders, %s\n", token, info.Symbol, res.Entries, res.Ops, len(balances), time.Since(start).Round(time.Second))
	client := &http.Client{Timeout: 15 * time.Second}
	mismatches, failures, checked := 0, 0, 0
	for addr, bal := range balances {
		chain, err := reconcile.BalanceOf(context.Background(), client, rpcURL, token, addr)
		if err != nil {
			failures++
			fmt.Printf("  LOOKUP FAILED %s: %v\n", addr, err)
			continue
		}
		checked++
		if chain.String() != bal.String() {
			mismatches++
			fmt.Printf("  MISMATCH %s: computed %s, chain %s\n", addr, bal.String(), chain)
		}
	}
	fmt.Printf("checked %d holders against the chain: %d mismatches, %d lookups failed\n", checked, mismatches, failures)
	if mismatches > 0 || failures > 0 || checked == 0 {
		return 1
	}
	return 0
}
