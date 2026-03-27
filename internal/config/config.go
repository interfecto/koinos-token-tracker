package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// TokenTrackerConfig holds token contract addresses and migration parameters.
// All fields default to Koinos mainnet values if not specified in config.yml.
type TokenTrackerConfig struct {
	KoinContract        string `yaml:"koin-contract"`
	VhpContract         string `yaml:"vhp-contract"`
	OldKoinContract     string `yaml:"old-koin-contract"`
	OldVhpContract      string `yaml:"old-vhp-contract"`
	KCS4MigrationHeight uint64 `yaml:"kcs4-migration-height"`
}

// DefaultConfig returns mainnet defaults.
func DefaultConfig() *TokenTrackerConfig {
	return &TokenTrackerConfig{
		KoinContract:        "19GYjDBVXU7keLbYvMLazsGQn3GTWHjHkK",
		VhpContract:         "12Y5vW6gk8GceH53YfRkRre2Rrcsgw7Naq",
		OldKoinContract:     "15DJN4a8SgrbGhhGksSBASiSYjGnMU8dGL",
		OldVhpContract:      "1AdzuXSpC6K9qtXdCBgD5NUpDNwHjMgrc9",
		KCS4MigrationHeight: 24804034,
	}
}

// yamlTokenTracker uses pointer fields to distinguish "absent" from "zero value".
type yamlTokenTracker struct {
	KoinContract        *string `yaml:"koin-contract"`
	VhpContract         *string `yaml:"vhp-contract"`
	OldKoinContract     *string `yaml:"old-koin-contract"`
	OldVhpContract      *string `yaml:"old-vhp-contract"`
	KCS4MigrationHeight *uint64 `yaml:"kcs4-migration-height"`
}

// nodeConfig is the top-level structure of the Koinos node config.yml.
type nodeConfig struct {
	TokenTracker yamlTokenTracker `yaml:"token-tracker"`
}

// Load reads the token-tracker section from a Koinos node config.yml file.
// If the file does not exist or the section is absent, mainnet defaults are used.
func Load(configPath string) (*TokenTrackerConfig, error) {
	cfg := DefaultConfig()

	if configPath == "" {
		return cfg, nil
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var node nodeConfig
	if err := yaml.Unmarshal(data, &node); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	tc := node.TokenTracker
	if tc.KoinContract != nil && *tc.KoinContract != "" {
		cfg.KoinContract = *tc.KoinContract
	}
	if tc.VhpContract != nil && *tc.VhpContract != "" {
		cfg.VhpContract = *tc.VhpContract
	}
	if tc.OldKoinContract != nil && *tc.OldKoinContract != "" {
		cfg.OldKoinContract = *tc.OldKoinContract
	}
	if tc.OldVhpContract != nil && *tc.OldVhpContract != "" {
		cfg.OldVhpContract = *tc.OldVhpContract
	}
	if tc.KCS4MigrationHeight != nil {
		cfg.KCS4MigrationHeight = *tc.KCS4MigrationHeight
	}

	return cfg, nil
}
