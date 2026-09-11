package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path"
	"runtime"
	"syscall"

	"time"

	log "github.com/koinos/koinos-log-golang/v2"
	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-token-tracker/internal/api"
	"github.com/koinos/koinos-token-tracker/internal/config"
	"github.com/koinos/koinos-token-tracker/internal/reconcile"
	"github.com/koinos/koinos-token-tracker/internal/store"
	syncpkg "github.com/koinos/koinos-token-tracker/internal/sync"
	flag "github.com/spf13/pflag"
)

const (
	appName    = "koinos-token-tracker"
	appVersion = "0.2.0"
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
