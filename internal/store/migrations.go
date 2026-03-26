package store

import "database/sql"

const schema = `
CREATE TABLE IF NOT EXISTS sync_state (
    id              INTEGER PRIMARY KEY CHECK (id = 1),
    last_height     INTEGER NOT NULL DEFAULT 0,
    last_block_id   TEXT NOT NULL DEFAULT '',
    updated_at      INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS addresses (
    address           TEXT PRIMARY KEY,
    first_seen_height INTEGER NOT NULL,
    first_seen_time   INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS balances (
    address        TEXT NOT NULL,
    token          TEXT NOT NULL,
    balance        TEXT NOT NULL DEFAULT '0',
    updated_height INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (address, token)
);
CREATE INDEX IF NOT EXISTS idx_bal_token ON balances(token);

CREATE TABLE IF NOT EXISTS blocks (
    height      INTEGER PRIMARY KEY,
    block_id    TEXT NOT NULL,
    timestamp   INTEGER NOT NULL,
    signer      TEXT NOT NULL DEFAULT '',
    tx_count    INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS tokens (
    address      TEXT PRIMARY KEY,
    symbol       TEXT NOT NULL DEFAULT '',
    decimals     INTEGER NOT NULL DEFAULT 8,
    total_supply TEXT NOT NULL DEFAULT '0'
);

CREATE TABLE IF NOT EXISTS transfers (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    height      INTEGER NOT NULL,
    tx_id       TEXT NOT NULL DEFAULT '',
    token       TEXT NOT NULL,
    from_addr   TEXT NOT NULL DEFAULT '',
    to_addr     TEXT NOT NULL DEFAULT '',
    value       TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    timestamp   INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_transfers_from ON transfers(from_addr, height DESC);
CREATE INDEX IF NOT EXISTS idx_transfers_to ON transfers(to_addr, height DESC);
CREATE INDEX IF NOT EXISTS idx_transfers_token ON transfers(token, height DESC);
CREATE INDEX IF NOT EXISTS idx_transfers_height ON transfers(height);

INSERT OR IGNORE INTO sync_state (id, last_height, last_block_id, updated_at) VALUES (1, 0, '', 0);
`

func migrate(db *sql.DB) error {
	_, err := db.Exec(schema)
	return err
}
