package store

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
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

func (s *SQLiteStore) GetTransfersByAddress(address string, limit, offset int) ([]Transfer, int, error) {
	// Fast approximate count: sum of from + to counts (may double-count self-transfers, rare)
	var fromCount, toCount int
	if err := s.exec().QueryRow("SELECT COUNT(*) FROM transfers WHERE from_addr = ?", address).Scan(&fromCount); err != nil {
		return nil, 0, fmt.Errorf("count from transfers: %w", err)
	}
	if err := s.exec().QueryRow("SELECT COUNT(*) FROM transfers WHERE to_addr = ?", address).Scan(&toCount); err != nil {
		return nil, 0, fmt.Errorf("count to transfers: %w", err)
	}
	total := fromCount + toCount

	rows, err := s.exec().Query(
		`SELECT id, height, tx_id, token, from_addr, to_addr, value, event_type, timestamp FROM (
			SELECT * FROM transfers WHERE from_addr = ?
			UNION
			SELECT * FROM transfers WHERE to_addr = ?
		) ORDER BY height DESC, id DESC LIMIT ? OFFSET ?`,
		address, address, limit, offset,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("get transfers by address: %w", err)
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
