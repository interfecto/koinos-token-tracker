package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// SQLiteStore implements Store using a SQLite database.
type SQLiteStore struct {
	mu           sync.Mutex
	db           *sql.DB
	tx           *sql.Tx // active batch transaction, nil when not in a batch
	koinContract string
	vhpContract  string
}

// Open creates (or opens) the SQLite database at dbPath, creates parent
// directories if needed, runs migrations, and returns a ready Store.
func Open(dbPath, koinContract, vhpContract string) (*SQLiteStore, error) {
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create db directory: %w", err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Single connection eliminates pool-related pragma issues and races.
	db.SetMaxOpenConns(1)

	// Pragmas for performance.
	for _, pragma := range []string{
		"PRAGMA journal_mode=WAL",
		"PRAGMA synchronous=NORMAL",
		"PRAGMA busy_timeout=5000",
	} {
		if _, err := db.Exec(pragma); err != nil {
			db.Close()
			return nil, fmt.Errorf("exec %s: %w", pragma, err)
		}
	}

	if err := migrate(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}

	return &SQLiteStore{db: db, koinContract: koinContract, vhpContract: vhpContract}, nil
}

// OpenReadOnly opens an existing database for reading only: no directory
// creation, no migrations, no journal-mode change — nothing that needs the
// write lock a live indexer may be holding. Meant for --api-only, which runs
// beside the writing process (SQLite WAL permits concurrent readers).
func OpenReadOnly(dbPath, koinContract, vhpContract string) (*SQLiteStore, error) {
	if _, err := os.Stat(dbPath); err != nil {
		return nil, fmt.Errorf("open read-only: %w", err)
	}
	db, err := sql.Open("sqlite", "file:"+dbPath+"?mode=ro&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("open read-only: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("open read-only: %w", err)
	}
	return &SQLiteStore{db: db, koinContract: koinContract, vhpContract: vhpContract}, nil
}

// exec returns the active batch transaction if one exists, otherwise the raw db.
func (s *SQLiteStore) exec() execer {
	if s.tx != nil {
		return s.tx
	}
	return s.db
}

// execer is satisfied by both *sql.DB and *sql.Tx.
type execer interface {
	Exec(query string, args ...any) (sql.Result, error)
	QueryRow(query string, args ...any) *sql.Row
	Query(query string, args ...any) (*sql.Rows, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// ---------------------------------------------------------------------------
// Sync state
// ---------------------------------------------------------------------------

func (s *SQLiteStore) GetSyncState() (uint64, string, error) {
	var height uint64
	var blockID string
	err := s.exec().QueryRow("SELECT last_height, last_block_id FROM sync_state WHERE id = 1").
		Scan(&height, &blockID)
	if err != nil {
		return 0, "", fmt.Errorf("get sync state: %w", err)
	}
	return height, blockID, nil
}

func (s *SQLiteStore) SetSyncState(height uint64, blockID string) error {
	_, err := s.exec().Exec(
		"UPDATE sync_state SET last_height = ?, last_block_id = ?, updated_at = ? WHERE id = 1",
		height, blockID, time.Now().Unix(),
	)
	if err != nil {
		return fmt.Errorf("set sync state: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Addresses
// ---------------------------------------------------------------------------

func (s *SQLiteStore) UpsertAddress(address string, firstSeenHeight uint64, firstSeenTime uint64) error {
	_, err := s.exec().Exec(
		`INSERT INTO addresses (address, first_seen_height, first_seen_time)
		 VALUES (?, ?, ?)
		 ON CONFLICT(address) DO NOTHING`,
		address, firstSeenHeight, firstSeenTime,
	)
	if err != nil {
		return fmt.Errorf("upsert address: %w", err)
	}
	return nil
}

// LowerFirstSeen inserts the address or moves its first-seen position back
// when the given one is earlier (a history backfill discovers activity that
// predates what the live sync recorded).
func (s *SQLiteStore) LowerFirstSeen(address string, height uint64, timestamp uint64) error {
	_, err := s.exec().Exec(
		`INSERT INTO addresses (address, first_seen_height, first_seen_time)
		 VALUES (?, ?, ?)
		 ON CONFLICT(address) DO UPDATE SET first_seen_height=excluded.first_seen_height, first_seen_time=excluded.first_seen_time
		 WHERE excluded.first_seen_height < addresses.first_seen_height`,
		address, height, timestamp,
	)
	if err != nil {
		return fmt.Errorf("lower first seen: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetAddress(address string) (*Address, error) {
	a := &Address{}
	err := s.exec().QueryRow(
		"SELECT address, first_seen_height, first_seen_time FROM addresses WHERE address = ?",
		address,
	).Scan(&a.Address, &a.FirstSeenHeight, &a.FirstSeenTime)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get address: %w", err)
	}
	return a, nil
}

func (s *SQLiteStore) GetAddresses(limit, offset int) ([]Address, int, error) {
	var total int
	if err := s.exec().QueryRow("SELECT COUNT(*) FROM addresses").Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count addresses: %w", err)
	}

	rows, err := s.exec().Query(
		"SELECT address, first_seen_height, first_seen_time FROM addresses ORDER BY first_seen_height DESC LIMIT ? OFFSET ?",
		limit, offset,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("list addresses: %w", err)
	}
	defer rows.Close()

	var addrs []Address
	for rows.Next() {
		var a Address
		if err := rows.Scan(&a.Address, &a.FirstSeenHeight, &a.FirstSeenTime); err != nil {
			return nil, 0, fmt.Errorf("scan address: %w", err)
		}
		addrs = append(addrs, a)
	}
	return addrs, total, rows.Err()
}

// ---------------------------------------------------------------------------
// Balances
// ---------------------------------------------------------------------------

func (s *SQLiteStore) SetBalance(address, token, balance string, height uint64) error {
	_, err := s.exec().Exec(
		`INSERT INTO balances (address, token, balance, updated_height)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(address, token) DO UPDATE SET balance=excluded.balance, updated_height=excluded.updated_height`,
		address, token, balance, height,
	)
	if err != nil {
		return fmt.Errorf("set balance: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetBalance(address, token string) (string, error) {
	var balance string
	err := s.exec().QueryRow(
		"SELECT balance FROM balances WHERE address = ? AND token = ?",
		address, token,
	).Scan(&balance)
	if err == sql.ErrNoRows {
		return "0", nil
	}
	if err != nil {
		return "", fmt.Errorf("get balance: %w", err)
	}
	return balance, nil
}

func (s *SQLiteStore) GetBalances(address string) ([]Balance, error) {
	rows, err := s.exec().Query(
		"SELECT address, token, balance, updated_height FROM balances WHERE address = ?",
		address,
	)
	if err != nil {
		return nil, fmt.Errorf("get balances: %w", err)
	}
	defer rows.Close()

	var balances []Balance
	for rows.Next() {
		var b Balance
		if err := rows.Scan(&b.Address, &b.Token, &b.Balance, &b.UpdatedHeight); err != nil {
			return nil, fmt.Errorf("scan balance: %w", err)
		}
		balances = append(balances, b)
	}
	return balances, rows.Err()
}

func (s *SQLiteStore) GetTopHolders(token string, limit, offset int) ([]Balance, int, error) {
	var total int
	if err := s.exec().QueryRow(
		"SELECT COUNT(*) FROM balances WHERE token = ? AND balance != '0'",
		token,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count holders: %w", err)
	}

	rows, err := s.exec().Query(
		`SELECT address, token, balance, updated_height FROM balances
		 WHERE token = ? AND balance != '0'
		 ORDER BY LENGTH(balance) DESC, balance DESC
		 LIMIT ? OFFSET ?`,
		token, limit, offset,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("top holders: %w", err)
	}
	defer rows.Close()

	var balances []Balance
	for rows.Next() {
		var b Balance
		if err := rows.Scan(&b.Address, &b.Token, &b.Balance, &b.UpdatedHeight); err != nil {
			return nil, 0, fmt.Errorf("scan holder: %w", err)
		}
		balances = append(balances, b)
	}
	return balances, total, rows.Err()
}

// ---------------------------------------------------------------------------
// Blocks
// ---------------------------------------------------------------------------

func (s *SQLiteStore) InsertBlock(height uint64, blockID string, timestamp uint64, signer string, txCount int) error {
	_, err := s.exec().Exec(
		`INSERT INTO blocks (height, block_id, timestamp, signer, tx_count)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT(height) DO UPDATE SET block_id=excluded.block_id, timestamp=excluded.timestamp, signer=excluded.signer, tx_count=excluded.tx_count`,
		height, blockID, timestamp, signer, txCount,
	)
	if err != nil {
		return fmt.Errorf("insert block: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetBlock(height uint64) (*Block, error) {
	b := &Block{}
	err := s.exec().QueryRow(
		"SELECT height, block_id, timestamp, signer, tx_count FROM blocks WHERE height = ?",
		height,
	).Scan(&b.Height, &b.BlockID, &b.Timestamp, &b.Signer, &b.TxCount)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get block: %w", err)
	}
	return b, nil
}

func (s *SQLiteStore) GetBlocks(from, to uint64) ([]Block, error) {
	rows, err := s.exec().Query(
		"SELECT height, block_id, timestamp, signer, tx_count FROM blocks WHERE height >= ? AND height <= ? ORDER BY height",
		from, to,
	)
	if err != nil {
		return nil, fmt.Errorf("get blocks: %w", err)
	}
	defer rows.Close()

	var blocks []Block
	for rows.Next() {
		var b Block
		if err := rows.Scan(&b.Height, &b.BlockID, &b.Timestamp, &b.Signer, &b.TxCount); err != nil {
			return nil, fmt.Errorf("scan block: %w", err)
		}
		blocks = append(blocks, b)
	}
	return blocks, rows.Err()
}

// ---------------------------------------------------------------------------
// Tokens
// ---------------------------------------------------------------------------

func (s *SQLiteStore) UpsertToken(address, symbol string, decimals int, totalSupply string) error {
	_, err := s.exec().Exec(
		`INSERT INTO tokens (address, symbol, decimals, total_supply)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT(address) DO UPDATE SET symbol=excluded.symbol, decimals=excluded.decimals, total_supply=excluded.total_supply`,
		address, symbol, decimals, totalSupply,
	)
	if err != nil {
		return fmt.Errorf("upsert token: %w", err)
	}
	return nil
}

func (s *SQLiteStore) GetToken(address string) (*Token, error) {
	t := &Token{}
	err := s.exec().QueryRow(
		"SELECT address, symbol, decimals, total_supply FROM tokens WHERE address = ?",
		address,
	).Scan(&t.Address, &t.Symbol, &t.Decimals, &t.TotalSupply)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get token: %w", err)
	}
	return t, nil
}

func (s *SQLiteStore) GetBackfill(token string) (*Backfill, error) {
	b := &Backfill{}
	var done int
	var verified int
	err := s.exec().QueryRow(
		"SELECT token, next_seq, cutoff_height, done, verified, mismatches, failures, updated_at FROM token_backfill WHERE token = ?", token,
	).Scan(&b.Token, &b.NextSeq, &b.CutoffHeight, &done, &verified, &b.Mismatches, &b.Failures, &b.UpdatedAt)
	if isNoSuchColumn(err) { // first-build schema, read-only: no verification columns yet
		err = s.exec().QueryRow(
			"SELECT token, next_seq, cutoff_height, done, updated_at FROM token_backfill WHERE token = ?", token,
		).Scan(&b.Token, &b.NextSeq, &b.CutoffHeight, &done, &b.UpdatedAt)
	}
	if err == sql.ErrNoRows || isNoSuchTable(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get backfill: %w", err)
	}
	b.Done = done != 0
	b.Verified = verified != 0
	return b, nil
}

// isNoSuchTable is true when a query hit a database created by an older
// build (--api-only never migrates, so it may read a schema without the
// token_backfill table).
func isNoSuchTable(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no such table")
}

// isNoSuchColumn is true when a query hit a token_backfill table from the
// first build of the feature (without the verification columns); a writable
// instance migrates it, --api-only reads it with defaults.
func isNoSuchColumn(err error) bool {
	return err != nil && strings.Contains(err.Error(), "no such column")
}

func (s *SQLiteStore) UpsertBackfill(b *Backfill) error {
	done, verified := 0, 0
	if b.Done {
		done = 1
	}
	if b.Verified {
		verified = 1
	}
	_, err := s.exec().Exec(
		`INSERT INTO token_backfill (token, next_seq, cutoff_height, done, verified, mismatches, failures, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(token) DO UPDATE SET next_seq=excluded.next_seq, cutoff_height=excluded.cutoff_height, done=excluded.done,
		   verified=excluded.verified, mismatches=excluded.mismatches, failures=excluded.failures, updated_at=excluded.updated_at`,
		b.Token, b.NextSeq, b.CutoffHeight, done, verified, b.Mismatches, b.Failures, time.Now().Unix(),
	)
	if err != nil {
		return fmt.Errorf("upsert backfill: %w", err)
	}
	return nil
}

func (s *SQLiteStore) ListBackfills() ([]Backfill, error) {
	rows, err := s.exec().Query("SELECT token, next_seq, cutoff_height, done, verified, mismatches, failures, updated_at FROM token_backfill ORDER BY token")
	if isNoSuchTable(err) {
		return []Backfill{}, nil // older schema (read-only side instance): nothing registered
	}
	legacy := false
	if isNoSuchColumn(err) { // first-build schema, read-only: verification columns default to zero
		legacy = true
		rows, err = s.exec().Query("SELECT token, next_seq, cutoff_height, done, updated_at FROM token_backfill ORDER BY token")
	}
	if err != nil {
		return nil, fmt.Errorf("list backfills: %w", err)
	}
	defer rows.Close()
	out := make([]Backfill, 0)
	for rows.Next() {
		var b Backfill
		var done, verified int
		if legacy {
			err = rows.Scan(&b.Token, &b.NextSeq, &b.CutoffHeight, &done, &b.UpdatedAt)
		} else {
			err = rows.Scan(&b.Token, &b.NextSeq, &b.CutoffHeight, &done, &verified, &b.Mismatches, &b.Failures, &b.UpdatedAt)
		}
		if err != nil {
			return nil, fmt.Errorf("scan backfill: %w", err)
		}
		b.Done = done != 0
		b.Verified = verified != 0
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *SQLiteStore) GetAllTokens() ([]Token, error) {
	rows, err := s.exec().Query("SELECT address, symbol, decimals, total_supply FROM tokens")
	if err != nil {
		return nil, fmt.Errorf("get all tokens: %w", err)
	}
	defer rows.Close()

	var tokens []Token
	for rows.Next() {
		var t Token
		if err := rows.Scan(&t.Address, &t.Symbol, &t.Decimals, &t.TotalSupply); err != nil {
			return nil, fmt.Errorf("scan token: %w", err)
		}
		tokens = append(tokens, t)
	}
	return tokens, rows.Err()
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

func (s *SQLiteStore) InsertTransfer(height uint64, txID, token, fromAddr, toAddr, value, eventType string, timestamp uint64) error {
	_, err := s.exec().Exec(
		`INSERT INTO transfers (height, tx_id, token, from_addr, to_addr, value, event_type, timestamp)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		height, txID, token, fromAddr, toAddr, value, eventType, timestamp,
	)
	if err != nil {
		return fmt.Errorf("insert transfer: %w", err)
	}
	return nil
}

func (s *SQLiteStore) ResetTokenBalances(token string) error {
	_, err := s.exec().Exec("DELETE FROM balances WHERE token = ?", token)
	if err != nil {
		return fmt.Errorf("reset token balances for %s: %w", token, err)
	}
	return nil
}

func (s *SQLiteStore) DeleteTransfersAtHeight(height uint64) error {
	_, err := s.exec().Exec("DELETE FROM transfers WHERE height = ?", height)
	if err != nil {
		return fmt.Errorf("delete transfers at height %d: %w", height, err)
	}
	return nil
}

func (s *SQLiteStore) GetTransfersByAddress(address string, limit, offset int) ([]Transfer, int, bool, error) {
	// Capped counts: a full COUNT(*) for a busy address (mining wallets have
	// millions of reward rows) wedges the DB and times out the whole API.
	// When either side hits the cap, total is a lower bound (capped=true).
	const countCap = 10001
	var fromCount, toCount int
	if err := s.exec().QueryRow(
		"SELECT COUNT(*) FROM (SELECT 1 FROM transfers WHERE from_addr = ? LIMIT ?)", address, countCap,
	).Scan(&fromCount); err != nil {
		return nil, 0, false, fmt.Errorf("count from transfers: %w", err)
	}
	if err := s.exec().QueryRow(
		"SELECT COUNT(*) FROM (SELECT 1 FROM transfers WHERE to_addr = ? LIMIT ?)", address, countCap,
	).Scan(&toCount); err != nil {
		return nil, 0, false, fmt.Errorf("count to transfers: %w", err)
	}
	total := fromCount + toCount
	capped := fromCount >= countCap || toCount >= countCap

	// Pull only the newest limit+offset rows per side (index-served, stops
	// early) before merging — the old plain UNION materialized every row for
	// the address. UNION (not ALL) still dedups self-transfers. id is the
	// rowid alias, so "height DESC, id DESC" is the exact reverse-index-scan
	// order of idx_transfers_from/to — deterministic ties, no sort step.
	per := limit + offset
	rows, err := s.exec().Query(
		`SELECT id, height, tx_id, token, from_addr, to_addr, value, event_type, timestamp FROM (
			SELECT * FROM (SELECT * FROM transfers WHERE from_addr = ? ORDER BY height DESC, id DESC LIMIT ?)
			UNION
			SELECT * FROM (SELECT * FROM transfers WHERE to_addr = ? ORDER BY height DESC, id DESC LIMIT ?)
		) ORDER BY height DESC, id DESC LIMIT ? OFFSET ?`,
		address, per, address, per, limit, offset,
	)
	if err != nil {
		return nil, 0, false, fmt.Errorf("get transfers by address: %w", err)
	}
	defer rows.Close()

	transfers := make([]Transfer, 0)
	for rows.Next() {
		var t Transfer
		if err := rows.Scan(&t.ID, &t.Height, &t.TxID, &t.Token, &t.From, &t.To, &t.Value, &t.EventType, &t.Timestamp); err != nil {
			return nil, 0, false, fmt.Errorf("scan transfer: %w", err)
		}
		transfers = append(transfers, t)
	}
	return transfers, total, capped, rows.Err()
}

func (s *SQLiteStore) GetTransfersByToken(token string, limit, offset int) ([]Transfer, int, error) {
	var total int
	err := s.exec().QueryRow(
		`SELECT COUNT(*) FROM transfers WHERE token = ?`, token,
	).Scan(&total)
	if err != nil {
		return nil, 0, fmt.Errorf("count transfers: %w", err)
	}

	rows, err := s.exec().Query(
		`SELECT id, height, tx_id, token, from_addr, to_addr, value, event_type, timestamp
		 FROM transfers WHERE token = ?
		 ORDER BY height DESC, id DESC LIMIT ? OFFSET ?`,
		token, limit, offset,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get transfers by token: %w", err)
	}
	defer rows.Close()

	transfers := make([]Transfer, 0)
	for rows.Next() {
		var t Transfer
		if err := rows.Scan(&t.ID, &t.Height, &t.TxID, &t.Token, &t.From, &t.To, &t.Value, &t.EventType, &t.Timestamp); err != nil {
			return nil, 0, fmt.Errorf("scan transfer: %w", err)
		}
		transfers = append(transfers, t)
	}
	return transfers, total, rows.Err()
}

// GetRecentTransfers returns the latest transfers across all addresses,
// newest first by chain height (a history backfill inserts old transfers with
// new rowids, so rowid order alone would show them as recent), ties by
// insertion order. Served by idx_transfers_height.
func (s *SQLiteStore) GetRecentTransfers(limit int) ([]Transfer, error) {
	rows, err := s.exec().Query(
		`SELECT id, height, tx_id, token, from_addr, to_addr, value, event_type, timestamp
		 FROM transfers ORDER BY height DESC, id DESC LIMIT ?`, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("get recent transfers: %w", err)
	}
	defer rows.Close()

	transfers := make([]Transfer, 0)
	for rows.Next() {
		var t Transfer
		if err := rows.Scan(&t.ID, &t.Height, &t.TxID, &t.Token, &t.From, &t.To, &t.Value, &t.EventType, &t.Timestamp); err != nil {
			return nil, fmt.Errorf("scan transfer: %w", err)
		}
		transfers = append(transfers, t)
	}
	return transfers, rows.Err()
}

// RecentFilterWindow bounds GetRecentTransfersFiltered to the newest this
// many blocks (~3.5 days at 3 s). Without a bound a token/type pair with no
// recent matches would walk the token's entire history on the single
// connection; within the window the worst case is a few hundred thousand
// index rows (KOIN: ~3 rows per block). Tests shrink it.
var RecentFilterWindow uint64 = 100_000

// RecentFilterTimeout caps one filtered query. The bound that actually
// limits work is RecentFilterWindow (the index range scanned is at most the
// window's rows for that token); the deadline is defence in depth: the
// driver interrupts SQLite while the query starts, and the scan loop stops
// stepping once the context has expired.
const RecentFilterTimeout = 5 * time.Second

// GetRecentTransfersFiltered returns the newest transfers of one token,
// optionally of one event type (transfer, mint, burn), newest first, within
// RecentFilterWindow blocks of the sync head. It walks idx_transfers_token
// (token, height DESC) from the top until limit rows match, so even KOIN —
// where block-reward mints outnumber real transfers roughly a thousand to
// one — answers in milliseconds.
func (s *SQLiteStore) GetRecentTransfersFiltered(token, eventType string, limit int) ([]Transfer, error) {
	if token == "" {
		return nil, fmt.Errorf("get recent transfers filtered: token is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), RecentFilterTimeout)
	defer cancel()
	head, _, err := s.GetSyncState()
	if err != nil {
		return nil, fmt.Errorf("get recent transfers filtered: %w", err)
	}
	var since uint64
	if head > RecentFilterWindow {
		since = head - RecentFilterWindow
	}
	where := []string{"token = ?", "height >= ?"}
	args := []interface{}{token, since}
	if eventType != "" {
		where = append(where, "event_type = ?")
		args = append(args, eventType)
	}
	q := `SELECT id, height, tx_id, token, from_addr, to_addr, value, event_type, timestamp FROM transfers WHERE ` +
		strings.Join(where, " AND ") + " ORDER BY height DESC, id DESC LIMIT ?"
	args = append(args, limit)

	rows, err := s.exec().QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("get recent transfers filtered: %w", err)
	}
	defer rows.Close()

	transfers := make([]Transfer, 0)
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("get recent transfers filtered: %w", err)
		}
		var t Transfer
		if err := rows.Scan(&t.ID, &t.Height, &t.TxID, &t.Token, &t.From, &t.To, &t.Value, &t.EventType, &t.Timestamp); err != nil {
			return nil, fmt.Errorf("scan transfer: %w", err)
		}
		transfers = append(transfers, t)
	}
	return transfers, rows.Err()
}

// GetProducers returns every address that produced a block in the last
// windowBlocks blocks or holds VHP, with 24h block counts and the timestamp
// of their most recent block. Second return is the total block count in the
// window. Per-signer MAX(height) is index-only via idx_blocks_signer
// (height is the rowid), so this stays fast on a full-history blocks table.
func (s *SQLiteStore) GetProducers(windowBlocks uint64) ([]Producer, int, error) {
	var head uint64
	if err := s.exec().QueryRow("SELECT COALESCE(MAX(height), 0) FROM blocks").Scan(&head); err != nil {
		return nil, 0, fmt.Errorf("get head height: %w", err)
	}
	var since uint64
	if head > windowBlocks {
		since = head - windowBlocks
	}

	type entry struct {
		vhp    string
		blocks int
	}
	entries := make(map[string]*entry)

	// NOT INDEXED: the planner otherwise full-scans the 37M-entry signer index
	// for the GROUP BY instead of range-scanning ~29k rows via the height PK
	// (2.3s vs 0.2s measured).
	rows, err := s.exec().Query(
		"SELECT signer, COUNT(*) FROM blocks NOT INDEXED WHERE height > ? AND signer != '' GROUP BY signer", since,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get producers blocks: %w", err)
	}
	total := 0
	for rows.Next() {
		var signer string
		var n int
		if err := rows.Scan(&signer, &n); err != nil {
			rows.Close()
			return nil, 0, fmt.Errorf("scan producer: %w", err)
		}
		entries[signer] = &entry{vhp: "0", blocks: n}
		total += n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate producers: %w", err)
	}

	rows, err = s.exec().Query(
		"SELECT address, balance FROM balances WHERE token = ? AND balance != '0'", s.vhpContract,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("count vhp holders: %w", err)
	}
	for rows.Next() {
		var addr, bal string
		if err := rows.Scan(&addr, &bal); err != nil {
			rows.Close()
			return nil, 0, fmt.Errorf("scan vhp holder: %w", err)
		}
		if e, ok := entries[addr]; ok {
			e.vhp = bal
		} else {
			entries[addr] = &entry{vhp: bal}
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("iterate vhp holders: %w", err)
	}

	producers := make([]Producer, 0, len(entries))
	for addr, e := range entries {
		p := Producer{Address: addr, VhpBalance: e.vhp, Blocks24h: e.blocks}
		var lastHeight uint64
		if err := s.exec().QueryRow(
			"SELECT COALESCE(MAX(height), 0) FROM blocks WHERE signer = ?", addr,
		).Scan(&lastHeight); err != nil {
			return nil, 0, fmt.Errorf("get last block: %w", err)
		}
		if lastHeight > 0 {
			if err := s.exec().QueryRow(
				"SELECT timestamp FROM blocks WHERE height = ?", lastHeight,
			).Scan(&p.LastBlockTime); err != nil {
				return nil, 0, fmt.Errorf("get last block time: %w", err)
			}
		}
		producers = append(producers, p)
	}
	return producers, total, nil
}

func (s *SQLiteStore) GetStats() (*Stats, error) {
	st := &Stats{}

	if err := s.exec().QueryRow("SELECT COUNT(*) FROM addresses").Scan(&st.TotalAddresses); err != nil {
		return nil, fmt.Errorf("count addresses: %w", err)
	}

	if err := s.exec().QueryRow(
		"SELECT COUNT(*) FROM balances WHERE token = ? AND balance != '0'",
		s.koinContract,
	).Scan(&st.KoinHolders); err != nil {
		return nil, fmt.Errorf("count koin holders: %w", err)
	}

	if err := s.exec().QueryRow(
		"SELECT COUNT(*) FROM balances WHERE token = ? AND balance != '0'",
		s.vhpContract,
	).Scan(&st.VhpHolders); err != nil {
		return nil, fmt.Errorf("count vhp holders: %w", err)
	}

	if err := s.exec().QueryRow(
		"SELECT last_height, last_block_id FROM sync_state WHERE id = 1",
	).Scan(&st.LastHeight, &st.LastBlockID); err != nil {
		return nil, fmt.Errorf("get sync state for stats: %w", err)
	}

	return st, nil
}

// ---------------------------------------------------------------------------
// Batch (transaction) support
// ---------------------------------------------------------------------------

func (s *SQLiteStore) BeginBatch() error {
	s.mu.Lock()
	if s.tx != nil {
		s.mu.Unlock()
		return fmt.Errorf("batch already in progress")
	}
	tx, err := s.db.Begin()
	if err != nil {
		s.mu.Unlock()
		return fmt.Errorf("begin batch: %w", err)
	}
	s.tx = tx
	// mutex stays locked until Commit/Rollback
	return nil
}

func (s *SQLiteStore) CommitBatch() error {
	if s.tx == nil {
		return fmt.Errorf("no batch in progress")
	}
	err := s.tx.Commit()
	s.tx = nil
	s.mu.Unlock()
	if err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}
	return nil
}

func (s *SQLiteStore) RollbackBatch() error {
	if s.tx == nil {
		return fmt.Errorf("no batch in progress")
	}
	err := s.tx.Rollback()
	s.tx = nil
	s.mu.Unlock()
	if err != nil {
		return fmt.Errorf("rollback batch: %w", err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Close
// ---------------------------------------------------------------------------

func (s *SQLiteStore) Close() error {
	if s.tx != nil {
		s.tx.Rollback()
		s.tx = nil
		s.mu.Unlock() // release lock held since BeginBatch
	}
	return s.db.Close()
}
