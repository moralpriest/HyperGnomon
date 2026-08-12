package rpc

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/creachadair/jrpc2/channel"
	"github.com/deroproject/derohe/rpc"
	"github.com/gorilla/websocket"
	"github.com/sirupsen/logrus"

	hgrpc "github.com/hypergnomon/hypergnomon/rpc/rwc"
)

var logger = logrus.WithField("pkg", "rpc")

var websocketCompression atomic.Bool

func init() {
	websocketCompression.Store(true)
}

// SetWebSocketCompression controls permessage-deflate on new daemon RPC
// connections. Existing pooled connections are unchanged. Default is true to
// preserve the historical behavior; benchmarks can disable it for LAN daemons.
func SetWebSocketCompression(enabled bool) {
	websocketCompression.Store(enabled)
}

func WebSocketCompressionEnabled() bool {
	return websocketCompression.Load()
}

// Client wraps a WebSocket connection with JSON-RPC capabilities.
type Client struct {
	WS       *websocket.Conn
	RPC      *jrpc2.Client
	Endpoint string
	mu       sync.RWMutex
}

// NewClient creates a new RPC client connected to the given daemon endpoint.
func NewClient(endpoint string) (*Client, error) {
	c := &Client{Endpoint: endpoint}
	if err := c.Connect(); err != nil {
		return nil, err
	}
	return c, nil
}

// resolveDialURL normalizes a daemon endpoint into a WebSocket dial URL.
// Accepted input forms:
//   - bare "host:port"           -> "ws://host:port/ws"
//   - "ws://host:port"           -> "ws://host:port/ws"
//   - "wss://host:port"          -> "wss://host:port/ws"
//   - "http://host:port"         -> "ws://host:port/ws"
//   - "https://host:port"        -> "wss://host:port/ws"
//
// If the input already ends with "/ws", it is not appended twice.
func resolveDialURL(endpoint string) string {
	scheme := "ws://"
	rest := endpoint
	switch {
	case strings.HasPrefix(endpoint, "wss://"):
		scheme = "wss://"
		rest = strings.TrimPrefix(endpoint, "wss://")
	case strings.HasPrefix(endpoint, "ws://"):
		scheme = "ws://"
		rest = strings.TrimPrefix(endpoint, "ws://")
	case strings.HasPrefix(endpoint, "https://"):
		scheme = "wss://"
		rest = strings.TrimPrefix(endpoint, "https://")
	case strings.HasPrefix(endpoint, "http://"):
		scheme = "ws://"
		rest = strings.TrimPrefix(endpoint, "http://")
	}
	if strings.HasSuffix(rest, "/ws") {
		return scheme + rest
	}
	return scheme + rest + "/ws"
}

// Connect establishes the WebSocket connection and JSON-RPC client.
func (c *Client) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.WS != nil {
		c.WS.Close()
	}

	dialer := websocket.Dialer{
		ReadBufferSize:    65536, // 64KB (up from 4KB default)
		WriteBufferSize:   65536,
		HandshakeTimeout:  3 * time.Second,
		EnableCompression: WebSocketCompressionEnabled(),
	}
	ws, _, err := dialer.Dial(resolveDialURL(c.Endpoint), nil)
	if err != nil {
		return fmt.Errorf("dial %s: %w", c.Endpoint, err)
	}

	c.WS = ws
	inputOutput := hgrpc.New(ws)
	c.RPC = jrpc2.NewClient(channel.RawJSON(inputOutput, inputOutput), nil)
	return nil
}

// Close shuts down the RPC client and WebSocket.
func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.RPC != nil {
		c.RPC.Close()
	}
	if c.WS != nil {
		c.WS.Close()
	}
}

// GetInfo calls DERO.GetInfo to get current chain state.
func (c *Client) GetInfo() (*rpc.GetInfo_Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result rpc.GetInfo_Result
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()

	if err := c.RPC.CallResult(ctx, "DERO.GetInfo", nil, &result); err != nil {
		return nil, fmt.Errorf("GetInfo: %w", err)
	}
	return &result, nil
}

// GetBlock retrieves a block by its hash.
func (c *Client) GetBlock(hash string) (*rpc.GetBlock_Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result rpc.GetBlock_Result
	params := rpc.GetBlock_Params{Hash: hash}
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()

	if err := c.RPC.CallResult(ctx, "DERO.GetBlock", params, &result); err != nil {
		return nil, fmt.Errorf("GetBlock(%s): %w", hash, err)
	}
	return &result, nil
}

// ErrBlocksMissing reports a batch in which every requested height was served
// as "empty block" — the daemon is sparse (e.g. a fastsync node still
// backfilling) and does not have that history yet. Callers may skip past the
// missing range instead of retrying it forever.
var ErrBlocksMissing = errors.New("batch blocks missing (empty block)")

// FirstAvailableTopo finds the lowest topoheight the daemon can serve.
// Fully-synced daemons answer every height and this returns 0. Sparse
// daemons (fastsync mid-backfill) return empty-block errors for the earliest
// heights, so this binary-searches [0, tip] for the first height that answers.
// Only "empty block" errors are treated as unavailable — any other RPC
// failure (e.g. unreachable daemon) aborts the probe so callers never skip
// valid history based on a transient error.
func (c *Client) FirstAvailableTopo(tip uint64) (int64, error) {
	isMissing := func(err error) bool {
		return err != nil && strings.Contains(strings.ToLower(err.Error()), "empty block")
	}

	if _, err := c.GetBlockHeaderByTopoHeight(0); err == nil {
		return 0, nil
	} else if !isMissing(err) {
		return 0, err
	}
	if tip <= 1 {
		return int64(tip), nil
	}

	lo, hi := uint64(1), tip // lo unavailable, hi available
	for lo < hi {
		mid := lo + (hi-lo)/2
		if _, err := c.GetBlockHeaderByTopoHeight(mid); err == nil {
			hi = mid
		} else if isMissing(err) {
			lo = mid + 1
		} else {
			return 0, err
		}
	}
	return int64(lo), nil
}

// GetBlockHeaderByTopoHeight retrieves a block header by topoheight.
func (c *Client) GetBlockHeaderByTopoHeight(height uint64) (*rpc.GetBlockHeaderByHeight_Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result rpc.GetBlockHeaderByHeight_Result
	params := rpc.GetBlockHeaderByTopoHeight_Params{TopoHeight: height}
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()

	if err := c.RPC.CallResult(ctx, "DERO.GetBlockHeaderByTopoHeight", params, &result); err != nil {
		return nil, fmt.Errorf("GetBlockHeaderByTopoHeight(%d): %w", height, err)
	}
	return &result, nil
}

// GetTransaction fetches transactions by hash. Supports batch (multiple hashes).
// This is the key optimization: one RPC call for all TXs in a block.
func (c *Client) GetTransaction(txHashes []string) (*rpc.GetTransaction_Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result rpc.GetTransaction_Result
	params := rpc.GetTransaction_Params{Tx_Hashes: txHashes}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := c.RPC.CallResult(ctx, "DERO.GetTransaction", params, &result); err != nil {
		return nil, fmt.Errorf("GetTransaction(%d hashes): %w", len(txHashes), err)
	}
	return &result, nil
}

// GetSC retrieves smart contract variables. Uses longer timeout for large contracts.
func (c *Client) GetSC(scid string, topoheight int64, keysstring []string, keysuint64 []uint64, code bool) (*rpc.GetSC_Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result rpc.GetSC_Result
	params := rpc.GetSC_Params{
		SCID:       scid,
		Code:       code,
		Variables:  true,
		TopoHeight: topoheight,
		KeysString: keysstring,
		KeysUint64: keysuint64,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	if err := c.RPC.CallResult(ctx, "DERO.GetSC", params, &result); err != nil {
		return nil, fmt.Errorf("GetSC(%s): %w", scid, err)
	}
	return &result, nil
}

// GetBlockHash returns the block hash at a given topoheight.
func (c *Client) GetBlockHash(height uint64) (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result rpc.GetBlockHeaderByHeight_Result
	params := rpc.GetBlockHeaderByTopoHeight_Params{TopoHeight: height}
	ctx, cancel := context.WithTimeout(context.Background(), 9*time.Second)
	defer cancel()

	if err := c.RPC.CallResult(ctx, "DERO.GetBlockHeaderByTopoHeight", params, &result); err != nil {
		return "", fmt.Errorf("GetBlockHash(%d): %w", height, err)
	}
	return result.Block_Header.Hash, nil
}

// GetBlockByHeight retrieves a block directly by topoheight.
// This eliminates the GetBlockHash→GetBlock two-step, halving RPC calls.
func (c *Client) GetBlockByHeight(height uint64) (*rpc.GetBlock_Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var result rpc.GetBlock_Result
	params := rpc.GetBlock_Params{Height: height}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := c.RPC.CallResult(ctx, "DERO.GetBlock", params, &result); err != nil {
		return nil, fmt.Errorf("GetBlockByHeight(%d): %w", height, err)
	}
	return &result, nil
}

// BatchGetBlocks fetches multiple blocks in a single JSON-RPC batch call.
// This is the nuclear optimization: 50 blocks per round trip instead of 1.
func (c *Client) BatchGetBlocks(heights []uint64) ([]*rpc.GetBlock_Result, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	specs := make([]jrpc2.Spec, len(heights))
	for i, h := range heights {
		specs[i] = jrpc2.Spec{
			Method: "DERO.GetBlock",
			Params: rpc.GetBlock_Params{Height: h},
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	responses, err := c.RPC.Batch(ctx, specs)
	if err != nil {
		return nil, fmt.Errorf("BatchGetBlocks(%d heights): %w", len(heights), err)
	}

	// Partial success: a single bad block in a batch of 50 should not poison
	// the other 49. Callers already handle nil entries (fetcherLoop skips them).
	results := make([]*rpc.GetBlock_Result, len(responses))
	failures := 0
	missing := 0
	for i, resp := range responses {
		var result rpc.GetBlock_Result
		if err := resp.UnmarshalResult(&result); err != nil {
			logger.Warnf("BatchGetBlocks: skipping height %d: %v", heights[i], err)
			failures++
			if strings.Contains(strings.ToLower(err.Error()), "empty block") {
				missing++
			}
			continue
		}
		results[i] = &result
	}
	if failures == len(responses) {
		if missing == len(responses) {
			return results, fmt.Errorf("%w (heights %d..%d)", ErrBlocksMissing, heights[0], heights[len(heights)-1])
		}
		return results, fmt.Errorf("BatchGetBlocks: all %d responses failed to unmarshal", len(responses))
	}
	return results, nil
}
