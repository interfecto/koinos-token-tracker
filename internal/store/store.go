package store

// Store defines the persistence interface for the koinos-indexer.
type Store interface {
	// Sync state
	GetSyncState() (lastHeight uint64, lastBlockID string, err error)
	SetSyncState(height uint64, blockID string) error

	// Addresses
	UpsertAddress(address string, firstSeenHeight uint64, firstSeenTime uint64) error
	GetAddress(address string) (*Address, error)
	GetAddresses(limit, offset int) ([]Address, int, error) // returns total count too

	// Balances
	SetBalance(address, token, balance string, height uint64) error
	GetBalance(address, token string) (string, error)
	GetBalances(address string) ([]Balance, error)
	GetTopHolders(token string, limit, offset int) ([]Balance, int, error) // returns total count

	// Blocks
	InsertBlock(height uint64, blockID string, timestamp uint64, signer string, txCount int) error
	GetBlock(height uint64) (*Block, error)
	GetBlocks(from, to uint64) ([]Block, error)

	// Tokens
	UpsertToken(address, symbol string, decimals int, totalSupply string) error
	GetToken(address string) (*Token, error)
	GetAllTokens() ([]Token, error)

	// Transfers
	InsertTransfer(height uint64, txID, token, fromAddr, toAddr, value, eventType string, timestamp uint64) error
	DeleteTransfersAtHeight(height uint64) error
	ResetTokenBalances(token string) error
	GetTransfersByAddress(address string, limit, offset int) ([]Transfer, int, error)
	GetTransfersByToken(token string, limit, offset int) ([]Transfer, int, error)

	// Stats
	GetStats() (*Stats, error)

	// Transaction support for batch operations
	BeginBatch() error
	CommitBatch() error
	RollbackBatch() error

	Close() error
}

// Address represents a seen on-chain address.
type Address struct {
	Address         string
	FirstSeenHeight uint64
	FirstSeenTime   uint64
}

// Balance represents a token balance for an address.
type Balance struct {
	Address       string
	Token         string
	Balance       string
	UpdatedHeight uint64
}

// Block represents a produced block.
type Block struct {
	Height    uint64
	BlockID   string
	Timestamp uint64
	Signer    string
	TxCount   int
}

// Token represents a tracked token contract.
type Token struct {
	Address     string
	Symbol      string
	Decimals    int
	TotalSupply string
}

// Transfer represents a token transfer/mint/burn event.
type Transfer struct {
	ID        int64  `json:"id"`
	Height    uint64 `json:"height"`
	TxID      string `json:"tx_id"`
	Token     string `json:"token"`
	From      string `json:"from"`
	To        string `json:"to"`
	Value     string `json:"value"`
	EventType string `json:"event_type"` // transfer, mint, burn
	Timestamp uint64 `json:"timestamp"`
}

// Stats holds aggregate statistics.
type Stats struct {
	TotalAddresses int
	KoinHolders    int
	VhpHolders     int
	LastHeight     uint64
	LastBlockID    string
}
