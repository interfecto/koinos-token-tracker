package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/spf13/pflag"

	klog "github.com/koinos/koinos-log-golang/v2"
	"github.com/koinos/koinos-token-tracker/internal/api"
	"github.com/koinos/koinos-token-tracker/internal/config"
	"github.com/koinos/koinos-token-tracker/internal/reconcile"
	"github.com/koinos/koinos-token-tracker/internal/store"
	"github.com/koinos/koinos-token-tracker/internal/sync"
)

var Commit = "dev"

func main() {
	var (
		amqpURL     string
		basedir     string
		instanceID  string
		jobs        uint
		logColor    bool
		logDatetime bool
		logDir      string
		logLevel    string
		port        int
		doReconcile bool
		doReset     bool
		restURL     string
		showVersion bool
	)

	pflag.StringVarP(&amqpURL, "amqp", "a", "amqp://guest:guest@localhost:5672/", "AMQP connection URL")
	pflag.StringVarP(&basedir, "basedir", "d", "/root/.koinos", "Base directory for data")
	pflag.StringVar(&instanceID, "instance-id", "", "Instance ID for AMQP")
	pflag.UintVarP(&jobs, "jobs", "j", 16, "Number of AMQP handler jobs (reserved)")
	pflag.BoolVar(&logColor, "log-color", true, "Enable colorized log output")
	pflag.BoolVar(&logDatetime, "log-datetime", true, "Include datetime in log output")
	pflag.StringVar(&logDir, "log-dir", "", "Log directory (empty = stderr only)")
	pflag.StringVar(&logLevel, "log-level", "info", "Log level (debug, info, warn, error)")
	pflag.IntVarP(&port, "port", "p", 8080, "HTTP API listen port")
	pflag.BoolVar(&doReconcile, "reconcile", false, "After sync, reconcile balances against REST API")
	pflag.BoolVar(&doReset, "reset", false, "Delete database and re-sync from genesis")
	pflag.StringVar(&restURL, "rest-url", "http://127.0.0.1:3000", "REST API URL for balance reconciliation")
	pflag.BoolVarP(&showVersion, "version", "v", false, "Print version and exit")
	pflag.Parse()

	if showVersion {
		fmt.Printf("koinos-token-tracker %s\n", Commit)
		os.Exit(0)
	}

	// Logger
	if err := klog.InitLogger("token-tracker", instanceID, logLevel, logDir, logColor, logDatetime); err != nil {
		fmt.Fprintf(os.Stderr, "logger init failed: %v\n", err)
		os.Exit(1)
	}
	klog.Infof("starting koinos-token-tracker commit=%s", Commit)

	// Config
	configPath := filepath.Join(basedir, "config", "config.yml")
	cfg, err := config.Load(configPath)
	if err != nil {
		klog.Warnf("config load failed, using defaults: %v", err)
		cfg = config.DefaultConfig()
	}

	// Database
	dbPath := filepath.Join(basedir, "indexer", "db", "indexer.db")
	if doReset {
		os.RemoveAll(filepath.Dir(dbPath))
		klog.Info("database reset requested, starting fresh")
	}

	db, err := store.Open(dbPath, cfg.KoinContract, cfg.VhpContract)
	if err != nil {
		klog.Errorf("failed to open database: %v", err)
		os.Exit(1)
	}
	defer db.Close()

	// Context with signal handling
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// AMQP client
	client := koinosmq.NewClient(amqpURL, koinosmq.ExponentialBackoff)
	client.Start(ctx)

	// Wait for AMQP to connect
	time.Sleep(2 * time.Second)

	// API server (runs in background)
	srv := api.NewServer(db, port)
	go func() {
		klog.Infof("API server starting on port %d", port)
		if err := srv.Start(ctx); err != nil {
			klog.Errorf("API server failed: %v", err)
		}
	}()

	// Syncer (blocks until context cancelled)
	syncer := sync.NewSyncer(client, db, cfg)
	go func() {
		for {
			if err := syncer.SyncToHead(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				klog.Errorf("sync error, retrying in 10s: %v", err)
				time.Sleep(10 * time.Second)
				continue
			}
			time.Sleep(1 * time.Second)
		}
	}()

	// Optional reconciliation
	if doReconcile {
		go func() {
			time.Sleep(30 * time.Second)
			klog.Info("starting balance reconciliation")
			if err := reconcile.Run(db, restURL, cfg.VhpContract); err != nil {
				klog.Errorf("reconciliation failed: %v", err)
			} else {
				klog.Info("reconciliation complete")
			}
		}()
	}

	<-ctx.Done()
	klog.Info("shutting down")
}
