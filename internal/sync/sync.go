package sync

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"time"

	log "github.com/koinos/koinos-log-golang/v2"
	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-proto-golang/v2/koinos/protocol"
	"github.com/koinos/koinos-proto-golang/v2/koinos/rpc/block_store"
	chainrpc "github.com/koinos/koinos-proto-golang/v2/koinos/rpc/chain"
	"github.com/koinos/koinos-token-tracker/internal/config"
	"github.com/koinos/koinos-token-tracker/internal/indexer"
	"github.com/koinos/koinos-token-tracker/internal/store"
	"google.golang.org/protobuf/proto"
)

const (
	chainRPC      = "chain"
	blockStoreRPC = "block_store"
	batchSize     = 1000
)

// Syncer handles historical catch-up by making AMQP RPC calls to the
// block_store service.
type Syncer struct {
	client *koinosmq.Client
	store  store.Store
	cfg    *config.TokenTrackerConfig
}

// NewSyncer creates a new Syncer backed by the given AMQP client and store.
func NewSyncer(client *koinosmq.Client, store store.Store, cfg *config.TokenTrackerConfig) *Syncer {
	return &Syncer{client: client, store: store, cfg: cfg}
}

// GetHeadInfo calls chain.GetHeadInfo via AMQP RPC and returns the current
// head block height and ID.
// GetHeadInfo returns current head height, block ID, and last irreversible block height.
func (s *Syncer) GetHeadInfo(ctx context.Context) (height uint64, blockID []byte, lib uint64, err error) {
	req := &chainrpc.ChainRequest{
		Request: &chainrpc.ChainRequest_GetHeadInfo{
			GetHeadInfo: &chainrpc.GetHeadInfoRequest{},
		},
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return 0, nil, 0, fmt.Errorf("marshal GetHeadInfo request: %w", err)
	}

	respBytes, err := s.client.RPC(ctx, "application/octet-stream", chainRPC, data)
	if err != nil {
		return 0, nil, 0, fmt.Errorf("GetHeadInfo RPC: %w", err)
	}

	resp := &chainrpc.ChainResponse{}
	if err := proto.Unmarshal(respBytes, resp); err != nil {
		return 0, nil, 0, fmt.Errorf("unmarshal GetHeadInfo response: %w", err)
	}

	switch r := resp.Response.(type) {
	case *chainrpc.ChainResponse_GetHeadInfo:
		info := r.GetHeadInfo
		return info.GetHeadTopology().GetHeight(), info.GetHeadTopology().GetId(), info.GetLastIrreversibleBlock(), nil
	case *chainrpc.ChainResponse_Error:
		return 0, nil, 0, fmt.Errorf("GetHeadInfo error: %s", r.Error.GetMessage())
	default:
		return 0, nil, 0, fmt.Errorf("unexpected GetHeadInfo response type: %T", resp.Response)
	}
}

// GetBlocksByHeight calls block_store.GetBlocksByHeight via AMQP RPC and
// returns the requested block items with both block and receipt data.
func (s *Syncer) GetBlocksByHeight(ctx context.Context, headBlockID []byte, startHeight uint64, count uint32) ([]*block_store.BlockItem, error) {
	req := &block_store.BlockStoreRequest{
		Request: &block_store.BlockStoreRequest_GetBlocksByHeight{
			GetBlocksByHeight: &block_store.GetBlocksByHeightRequest{
				HeadBlockId:         headBlockID,
				AncestorStartHeight: startHeight,
				NumBlocks:           count,
				ReturnBlock:         true,
				ReturnReceipt:       true,
			},
		},
	}
	data, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal GetBlocksByHeight request: %w", err)
	}

	respBytes, err := s.client.RPC(ctx, "application/octet-stream", blockStoreRPC, data)
	if err != nil {
		return nil, fmt.Errorf("GetBlocksByHeight RPC: %w", err)
	}

	resp := &block_store.BlockStoreResponse{}
	if err := proto.Unmarshal(respBytes, resp); err != nil {
		return nil, fmt.Errorf("unmarshal GetBlocksByHeight response: %w", err)
	}

	switch r := resp.Response.(type) {
	case *block_store.BlockStoreResponse_GetBlocksByHeight:
		return r.GetBlocksByHeight.GetBlockItems(), nil
	case *block_store.BlockStoreResponse_Error:
		return nil, fmt.Errorf("GetBlocksByHeight error: %s", r.Error.GetMessage())
	default:
		return nil, fmt.Errorf("unexpected GetBlocksByHeight response type: %T", resp.Response)
	}
}

// SyncToHead performs historical catch-up by fetching blocks from the
// block_store in batches. Only processes blocks up to the Last Irreversible
// Block (LIB) to ensure all indexed data is final and fork-safe.
func (s *Syncer) SyncToHead(ctx context.Context) error {
	lastHeight, _, err := s.store.GetSyncState()
	if err != nil {
		return fmt.Errorf("get sync state: %w", err)
	}

	headHeight, headBlockID, lib, err := s.GetHeadInfo(ctx)
	if err != nil {
		return fmt.Errorf("get head info: %w", err)
	}

	// Only sync up to LIB (irreversible blocks) to avoid indexing reversible state.
	// This means balances lag ~60 blocks (~3 min) behind head but are guaranteed correct.
	targetHeight := lib
	if targetHeight == 0 {
		targetHeight = headHeight // fallback if LIB not available
	}

	startHeight := lastHeight + 1
	if lastHeight == 0 {
		startHeight = 1
	}

	if startHeight > targetHeight {
		log.Infof("Already synced to LIB (height %d, head %d)", targetHeight, headHeight)
		return nil
	}

	log.Infof("Syncing from height %d to LIB %d (head %d, %d blocks)",
		startHeight, targetHeight, headHeight, targetHeight-startHeight+1)

	syncStart := time.Now()
	blocksProcessed := uint64(0)
	emptyRetries := 0

	for currentHeight := startHeight; currentHeight <= targetHeight; currentHeight += batchSize {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context cancelled during sync: %w", err)
		}

		remaining := targetHeight - currentHeight + 1
		count := uint32(batchSize)
		if uint64(count) > remaining {
			count = uint32(remaining)
		}

		items, err := s.GetBlocksByHeight(ctx, headBlockID, currentHeight, count)
		if err != nil {
			return fmt.Errorf("fetch blocks at height %d: %w", currentHeight, err)
		}

		if len(items) == 0 {
			emptyRetries++
			if emptyRetries > 5 {
				// Refresh head/LIB in case of stale headBlockID
				headHeight, headBlockID, lib, err = s.GetHeadInfo(ctx)
				if err != nil {
					return fmt.Errorf("refresh head info: %w", err)
				}
				targetHeight = lib
				if targetHeight == 0 {
					targetHeight = headHeight
				}
				log.Infof("Refreshed LIB to %d (head %d)", targetHeight, headHeight)
				emptyRetries = 0
				if currentHeight > targetHeight {
					break
				}
			}
			log.Warnf("No blocks returned for height %d, retry %d/5", currentHeight, emptyRetries)
			time.Sleep(2 * time.Second)
			continue
		}
		emptyRetries = 0

		if err := s.store.BeginBatch(); err != nil {
			return fmt.Errorf("begin batch at height %d: %w", currentHeight, err)
		}

		var batchErr error
		for _, item := range items {
			block := item.GetBlock()
			receipt := item.GetReceipt()

			if err := s.processAndStoreInternal(block, receipt); err != nil {
				batchErr = fmt.Errorf("process block at height %d: %w",
					block.GetHeader().GetHeight(), err)
				break
			}
			blocksProcessed++
		}

		if batchErr != nil {
			s.store.RollbackBatch()
			return batchErr
		}

		if err := s.store.CommitBatch(); err != nil {
			return fmt.Errorf("commit batch at height %d: %w", currentHeight, err)
		}

		if blocksProcessed%1000 < batchSize {
			elapsed := time.Since(syncStart)
			blocksPerSec := float64(blocksProcessed) / elapsed.Seconds()
			remainingBlocks := targetHeight - (currentHeight + uint64(len(items)) - 1)
			log.Infof("Sync progress: %d/%d blocks (%.0f blocks/sec, ~%d remaining)",
				blocksProcessed, targetHeight-startHeight+1, blocksPerSec, remainingBlocks)
		}
	}

	elapsed := time.Since(syncStart)
	log.Infof("Historical sync complete: %d blocks in %s (%.0f blocks/sec)",
		blocksProcessed, elapsed.Round(time.Second), float64(blocksProcessed)/elapsed.Seconds())

	return nil
}

// ProcessAndStore processes a single block and stores the results. This is
// the public entry point used by both the sync loop and the live broadcast
// handler.
func (s *Syncer) ProcessAndStore(block *protocol.Block, receipt *protocol.BlockReceipt) error {
	if err := s.store.BeginBatch(); err != nil {
		return fmt.Errorf("begin batch: %w", err)
	}

	if err := s.processAndStoreInternal(block, receipt); err != nil {
		s.store.RollbackBatch()
		return err
	}

	if err := s.store.CommitBatch(); err != nil {
		return fmt.Errorf("commit batch: %w", err)
	}

	return nil
}

// processAndStoreInternal does the actual work of processing a block and
// writing results to the store. It assumes a batch is already active.
func (s *Syncer) processAndStoreInternal(block *protocol.Block, receipt *protocol.BlockReceipt) error {
	result := indexer.ProcessBlock(s.cfg, block, receipt)
	if result == nil {
		return nil
	}

	height := result.Height
	timestamp := result.Timestamp
	blockIDHex := hex.EncodeToString(result.BlockID)

	// Guard against double-processing (e.g., live broadcast replay).
	lastHeight, _, _ := s.store.GetSyncState()
	if height <= lastHeight {
		return nil
	}

	// At KCS-4 migration height, reset VHP balances to avoid double-counting.
	// Old VHP was never burned during migration; new VHP events provide fresh starting balances.
	if height == s.cfg.KCS4MigrationHeight {
		log.Infof("KCS-4 migration at height %d: resetting VHP balances", height)
		if err := s.store.ResetTokenBalances(s.cfg.VhpContract); err != nil {
			return fmt.Errorf("reset VHP balances at migration: %w", err)
		}
	}

	// Upsert all addresses found in this block.
	for _, addr := range result.Addresses {
		if err := s.store.UpsertAddress(addr, height, timestamp); err != nil {
			return fmt.Errorf("upsert address %s: %w", addr, err)
		}
	}

	// Apply balance changes: read current balance, apply delta, write back.
	for _, change := range result.BalanceChanges {
		currentStr, err := s.store.GetBalance(change.Address, change.Token)
		if err != nil {
			return fmt.Errorf("get balance for %s: %w", change.Address, err)
		}

		current, ok := new(big.Int).SetString(currentStr, 10)
		if !ok {
			current = new(big.Int)
		}

		delta := big.NewInt(change.Delta)
		newBalance := new(big.Int).Add(current, delta)

		// Clamp to zero if underflow.
		if newBalance.Sign() < 0 {
			newBalance = new(big.Int)
		}

		if err := s.store.SetBalance(change.Address, change.Token, newBalance.String(), height); err != nil {
			return fmt.Errorf("set balance for %s: %w", change.Address, err)
		}
	}

	// Delete any existing transfers at this height (handles reprocessing/reorgs).
	if err := s.store.DeleteTransfersAtHeight(height); err != nil {
		return fmt.Errorf("delete transfers at height %d: %w", height, err)
	}

	// Store transfer records for history.
	for _, tr := range result.Transfers {
		valueStr := fmt.Sprintf("%d", tr.Value)
		if err := s.store.InsertTransfer(height, tr.TxID, tr.Token, tr.From, tr.To, valueStr, tr.EventType, timestamp); err != nil {
			return fmt.Errorf("insert transfer at height %d: %w", height, err)
		}
	}

	// Insert block metadata.
	if err := s.store.InsertBlock(height, blockIDHex, timestamp, result.Signer, result.TxCount); err != nil {
		return fmt.Errorf("insert block %d: %w", height, err)
	}

	// Update sync state.
	if err := s.store.SetSyncState(height, blockIDHex); err != nil {
		return fmt.Errorf("update sync state to %d: %w", height, err)
	}

	return nil
}
