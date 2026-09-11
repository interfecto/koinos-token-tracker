package indexer

import (
	"testing"

	"github.com/koinos/koinos-token-tracker/internal/config"
)

func TestExtraTokensAreTracked(t *testing.T) {
	cfg := config.DefaultConfig()
	extra := "12VoHz41a4HtfiyhTWbg9RXqGMRbYk6pXh"
	if isTokenEvent(cfg, extra) || affectsBalance(cfg, extra, 1) {
		t.Fatal("untracked token must be ignored")
	}
	cfg.SetExtraTokens([]string{extra, cfg.KoinContract, ""})
	if !cfg.IsExtra(extra) || cfg.IsExtra(cfg.KoinContract) || cfg.IsExtra("") {
		t.Fatalf("extra set %v", cfg.ExtraTokens)
	}
	if !isTokenEvent(cfg, extra) || !affectsBalance(cfg, extra, 1) || !affectsBalance(cfg, extra, cfg.KCS4MigrationHeight+1) {
		t.Fatal("extra token must be tracked at every height")
	}
	if normalizeToken(cfg, extra) != extra {
		t.Fatal("extra tokens are not remapped")
	}
	// the KOIN/VHP rules are untouched
	if !isTokenEvent(cfg, cfg.OldVhpContract) || affectsBalance(cfg, cfg.OldVhpContract, cfg.KCS4MigrationHeight) {
		t.Fatal("old VHP rule changed")
	}
}
