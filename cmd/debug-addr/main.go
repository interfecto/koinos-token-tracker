package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	koinosmq "github.com/koinos/koinos-mq-golang"
	"github.com/koinos/koinos-proto-golang/v2/koinos/rpc/block_store"
	chainrpc "github.com/koinos/koinos-proto-golang/v2/koinos/rpc/chain"
	"github.com/mr-tron/base58"
	"google.golang.org/protobuf/proto"
)

func main() {
	amqpURL := "amqp://guest:guest@localhost:5672/"
	if len(os.Args) > 1 {
		amqpURL = os.Args[1]
	}

	client := koinosmq.NewClient(amqpURL, koinosmq.ExponentialBackoff)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client.Start(ctx)

	// Get head info
	headReq := &chainrpc.ChainRequest{
		Request: &chainrpc.ChainRequest_GetHeadInfo{GetHeadInfo: &chainrpc.GetHeadInfoRequest{}},
	}
	data, _ := proto.Marshal(headReq)
	resp, err := client.RPC(ctx, "application/octet-stream", "chain", data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetHeadInfo failed: %v\n", err)
		os.Exit(1)
	}
	chainResp := &chainrpc.ChainResponse{}
	proto.Unmarshal(resp, chainResp)
	headInfo := chainResp.GetGetHeadInfo()
	headHeight := headInfo.GetHeadTopology().GetHeight()
	headID := headInfo.GetHeadTopology().GetId()
	fmt.Printf("Head: height=%d id=%s\n\n", headHeight, hex.EncodeToString(headID[:16]))

	// Fetch a recent block (10 blocks back to ensure it's available)
	startHeight := headHeight - 10
	bsReq := &block_store.BlockStoreRequest{
		Request: &block_store.BlockStoreRequest_GetBlocksByHeight{
			GetBlocksByHeight: &block_store.GetBlocksByHeightRequest{
				HeadBlockId:         headID,
				AncestorStartHeight: startHeight,
				NumBlocks:           5,
				ReturnBlock:         true,
				ReturnReceipt:       true,
			},
		},
	}
	data, _ = proto.Marshal(bsReq)
	resp, err = client.RPC(ctx, "application/octet-stream", "block_store", data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "GetBlocksByHeight failed: %v\n", err)
		os.Exit(1)
	}
	bsResp := &block_store.BlockStoreResponse{}
	proto.Unmarshal(resp, bsResp)
	items := bsResp.GetGetBlocksByHeight().GetBlockItems()
	fmt.Printf("Got %d blocks\n\n", len(items))

	koinAddr := "19GYjDBVXU7keLbYvMLazsGQn3GTWHjHkK"
	koinBytes, _ := base58.Decode(koinAddr)
	fmt.Printf("KOIN contract base58-decoded: hex=%s len=%d\n\n", hex.EncodeToString(koinBytes), len(koinBytes))

	for _, item := range items {
		block := item.GetBlock()
		receipt := item.GetReceipt()
		height := block.GetHeader().GetHeight()

		signerRaw := block.GetHeader().GetSigner()
		signerB58 := base58.Encode(signerRaw)
		fmt.Printf("Block %d: signer_hex=%s signer_b58=%s signer_len=%d\n",
			height, hex.EncodeToString(signerRaw), signerB58, len(signerRaw))

		if receipt == nil {
			fmt.Printf("  No receipt\n")
			continue
		}

		// Check block-level events
		for i, ev := range receipt.Events {
			srcHex := hex.EncodeToString(ev.Source)
			srcB58 := base58.Encode(ev.Source)
			fmt.Printf("  Block event %d: source_hex=%s source_b58=%s name=%s impacted=%d match_koin=%v\n",
				i, srcHex, srcB58, ev.Name, len(ev.Impacted), srcB58 == koinAddr)
		}

		// Check tx receipt events
		for ti, txr := range receipt.TransactionReceipts {
			for ei, ev := range txr.Events {
				srcHex := hex.EncodeToString(ev.Source)
				srcB58 := base58.Encode(ev.Source)
				match := srcB58 == koinAddr
				if match || ei < 3 {
					fmt.Printf("  Tx%d event %d: source_hex=%s source_b58=%s name=%s match_koin=%v\n",
						ti, ei, srcHex, srcB58, ev.Name, match)
				}
			}
		}
		fmt.Println()
	}
}
