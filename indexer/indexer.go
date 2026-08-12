package indexer

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/deroproject/derohe/block"
	"github.com/deroproject/derohe/cryptography/crypto"
	"github.com/deroproject/derohe/rpc"
	"github.com/deroproject/derohe/transaction"
	"github.com/sirupsen/logrus"

	"github.com/hypergnomon/hypergnomon/eventbus"
	hgpool "github.com/hypergnomon/hypergnomon/pool"
	hgrpc "github.com/hypergnomon/hypergnomon/rpc"
	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

var logger = logrus.WithField("pkg", "indexer")

// Indexer is the arena-optimized DERO blockchain scanner.
type Indexer struct {
	// Chain state
	LastIndexedHeight atomic.Int64
	ChainHeight       atomic.Int64

	// Configuration
	SearchFilter           []string
	SCIDExclusions         map[string]struct{} // O(1) lookup, not linear scan
	ValidatedSCs           sync.Map            // concurrent map[string]struct{}
	Endpoint               string
	DBDir                  string
	ParallelBlocks         int
	BatchSize              int    // blocks per DB flush
	ClassifyProbeBatchSize int    // SCIDs per phase-1 classify GetSC batch
	TurboMode              bool   // skip GetSC during scan, fetch variables post-scan
	PostScanVarsMode       string // "lazy" skips all-SCID post-scan vars, "all" keeps the full sweep
	AdaptBatchSize         bool   // dynamically adjust batch size based on flush latency
	RecentBlocks           int64  // scan only last N blocks (0 = all)
	FinalityDepth          int64  // blocks behind tip considered "safe" (default 10)
	CodePolicy             string // sccode persistence policy: "none" | "tela" | "all"
	ClassifySeedCacheDir   string // cross-DB classify seed cache directory (empty = OS user cache dir)
	adaptiveBatchSize      atomic.Int64

	// Backends
	RPCPool  *hgrpc.Pool
	Store    storage.Storage
	ownStore bool // true if New opened Store (and Close must release it); false when injected
	Closing  atomic.Bool

	// Route B finality: SafeHeight = max(LastIndexedHeight - FinalityDepth, 0).
	// Exposed in every API response so clients can distinguish "indexed"
	// from "confirmed beyond reorg risk."
	SafeHeight atomic.Int64

	// ReorgDetected tallies how many times CheckReorgAt has fired a mismatch.
	// M1 only observes; M2 will actually truncate+replay. Exported (like
	// SafeHeight) so main.go can hand the API server a live *atomic.Int64
	// pointer for /getstats — read it via the ReorgDetectedCount getter.
	ReorgDetected atomic.Int64

	// Bus is the optional event fan-out. nil => publishing is a no-op
	// (library embeddings that don't need realtime push can pass nil).
	Bus *eventbus.Bus

	// syncedOnce ensures EnableSync + optional post-scan variable work run
	// exactly once after initial catch-up, even if the caught-up condition is
	// detected multiple times.
	syncedOnce atomic.Bool

	// timer, when enabled, accumulates per-stage nanoseconds and emits a
	// grouped summary every TimingEvery batches. Nil-safe via enabled flag.
	timer *stageTimer
}

// Config holds indexer configuration.
type Config struct {
	Endpoint string
	DBDir    string
	// Backend selects the storage engine via storage.Open. Empty defaults to
	// "bbolt" (the only shipped backend); "sqlite" is planned, "graviton"
	// unsupported. See storage/factory.go. Ignored when Store is non-nil.
	Backend string
	// Store, when non-nil, is an already-opened store the indexer uses instead
	// of opening its own via Backend/DBDir (external-store injection — used by
	// the pkg/gnomes compat shim, which hands over a consumer's pre-opened
	// store). Close() will NOT release an injected store; the caller owns it.
	Store                  storage.Storage
	SearchFilter           []string
	SCIDExclusions         []string
	ParallelBlocks         int
	BatchSize              int
	ClassifyProbeBatchSize int
	PoolSize               int
	TurboMode              bool
	PostScanVarsMode       string // "lazy" (default) or "all"
	AdaptBatchSize         bool
	RecentBlocks           int64 // scan only the last N blocks from chain tip (0 = scan all)
	FinalityDepth          int64 // blocks behind tip considered safe (0 = default 10)

	// Timing, when true, turns on per-stage timers. TimingEvery controls how
	// many processed batches pass between log lines (0 = default 10).
	Timing      bool
	TimingEvery int

	// Bus is the optional event bus for subscription fan-out.
	// If nil, indexing still works; just no push notifications.
	Bus *eventbus.Bus

	// CodePolicy selects which SC classes get their install-time code
	// persisted to the sccode bucket. One of:
	//
	//   "none" — never persist; /api/initialscidcode lazy-fills per call.
	//   "tela" — only TELA-{INDEX,DOC,MOD}-1 classes. Default; matches
	//            the real use case (TELA content server needs DOC source
	//            to parse the /* ... */ body block).
	//   "all"  — persist every classified SCID's code. Previous behavior.
	//            Grows the DB ~134 MB on mainnet; use only if an operator
	//            actually serves GetInitialSCIDCode for arbitrary SCIDs.
	//
	// Empty-string defaults to "tela" inside Indexer.
	CodePolicy string

	// ClassifySeedCacheDir overrides the OS user cache directory used for the
	// cross-DB classify seed cache. Empty uses the platform default.
	ClassifySeedCacheDir string
}

// New creates a new Indexer with the given configuration.
func New(cfg Config) (*Indexer, error) {
	if cfg.ParallelBlocks <= 0 {
		cfg.ParallelBlocks = structures.DefaultParallelBlocks
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = structures.DefaultBatchSize
	}
	cfg.ClassifyProbeBatchSize = normalizeClassifyProbeBatchSize(cfg.ClassifyProbeBatchSize)
	cfg.PostScanVarsMode = normalizePostScanVarsMode(cfg.PostScanVarsMode)
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = structures.DefaultPoolSize
	}

	// Build exclusion map (O(1) lookups vs O(n) linear scan)
	exclusions := make(map[string]struct{}, len(cfg.SCIDExclusions)+len(structures.DefaultExclusions))
	for _, scid := range cfg.SCIDExclusions {
		exclusions[scid] = struct{}{}
	}
	// Add default exclusions (large non-TELA contracts)
	for _, scid := range structures.DefaultExclusions {
		exclusions[scid] = struct{}{}
	}

	// Use a caller-injected store (external-store injection) when provided;
	// otherwise open one via the backend factory. ownStore records whether
	// Close should release it — an injected store is the caller's to close.
	store := cfg.Store
	ownStore := false
	if store == nil {
		s, err := storage.Open(cfg.Backend, cfg.DBDir, strings.Join(cfg.SearchFilter, ";;;"))
		if err != nil {
			return nil, fmt.Errorf("open store: %w", err)
		}
		store, ownStore = s, true
	}

	// Create RPC connection pool early so API server can use it
	pool, err := hgrpc.NewPool(cfg.Endpoint, cfg.PoolSize)
	if err != nil {
		if ownStore {
			store.Close()
		}
		return nil, fmt.Errorf("rpc pool: %w", err)
	}

	idx := &Indexer{
		SearchFilter:           cfg.SearchFilter,
		SCIDExclusions:         exclusions,
		Endpoint:               cfg.Endpoint,
		DBDir:                  cfg.DBDir,
		ParallelBlocks:         cfg.ParallelBlocks,
		BatchSize:              cfg.BatchSize,
		ClassifyProbeBatchSize: cfg.ClassifyProbeBatchSize,
		TurboMode:              cfg.TurboMode,
		PostScanVarsMode:       cfg.PostScanVarsMode,
		AdaptBatchSize:         cfg.AdaptBatchSize,
		RecentBlocks:           cfg.RecentBlocks,
		FinalityDepth:          cfg.FinalityDepth,
		CodePolicy:             normalizeCodePolicy(cfg.CodePolicy),
		ClassifySeedCacheDir:   cfg.ClassifySeedCacheDir,
		Store:                  store,
		ownStore:               ownStore,
		RPCPool:                pool,
		Bus:                    cfg.Bus,
		timer:                  newStageTimer(cfg.Timing, cfg.TimingEvery),
	}
	idx.adaptiveBatchSize.Store(int64(idx.BatchSize))
	if idx.FinalityDepth <= 0 {
		idx.FinalityDepth = structures.DefaultFinalityDepth
	}

	// Restore last indexed height
	lastHeight, err := store.GetLastIndexHeight()
	if err != nil {
		logger.Warnf("Could not read last index height: %v", err)
	}
	idx.LastIndexedHeight.Store(lastHeight)
	// On startup SafeHeight is lastIndexed - FinalityDepth; safe if unknown.
	safe := lastHeight - idx.FinalityDepth
	if safe < 0 {
		safe = 0
	}
	idx.SafeHeight.Store(safe)

	return idx, nil
}

// StartDaemonMode connects to the daemon and begins indexing.
func (idx *Indexer) StartDaemonMode() error {
	// Start background chain height monitor
	go idx.monitorChainHeight()

	// Wait for first height update
	for idx.ChainHeight.Load() == 0 {
		if idx.Closing.Load() {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	// Sparse-history daemons (e.g. fastsync before backfill completes) cannot
	// serve the earliest topo heights. Find the first block the daemon can
	// return and jump the scan there, so we index everything it actually has
	// instead of retrying the missing region forever.
	var firstAvailable int64
	probeErr := idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
		fa, err := c.FirstAvailableTopo(uint64(idx.ChainHeight.Load()))
		if err != nil {
			return err
		}
		firstAvailable = fa
		return nil
	})
	if probeErr == nil && firstAvailable > idx.LastIndexedHeight.Load() {
		idx.LastIndexedHeight.Store(firstAvailable - 1)
		logger.Warnf("Daemon history sparse: first available topoheight=%d (chain=%d); starting scan there",
			firstAvailable, idx.ChainHeight.Load())
	}

	// Quick-scan: start from chain tip - N blocks instead of genesis
	if idx.RecentBlocks > 0 {
		skipTo := idx.ChainHeight.Load() - idx.RecentBlocks
		if skipTo > idx.LastIndexedHeight.Load() {
			idx.LastIndexedHeight.Store(skipTo)
			logger.Infof("Quick-scan: jumping to height %d (last %d blocks)", skipTo, idx.RecentBlocks)
		}
	}

	logger.Infof("Connected to %s | Chain height: %d | Last indexed: %d",
		idx.Endpoint, idx.ChainHeight.Load(), idx.LastIndexedHeight.Load())
	if idx.TurboMode {
		logger.Infof("Turbo mode enabled: skipping GetSC during scan, post-scan vars=%s", idx.PostScanVarsMode)
	}
	if idx.AdaptBatchSize {
		logger.Info("Adaptive batch sizing enabled")
	}

	// Main 3-stage pipeline (fetcher → processor → flusher)
	idx.scanLoop()
	return nil
}

// telaCacheHeightThrottle is how many blocks must pass before monitorChainHeight
// rewrites the tela_cache.bin height field. At chain tip (~1 block per 18s)
// this means a rewrite every ~30 minutes instead of every block.
const telaCacheHeightThrottle = 100

// monitorChainHeight polls daemon for current chain height.
func (idx *Indexer) monitorChainHeight() {
	var lastCacheHeight int64
	for !idx.Closing.Load() {
		err := idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
			info, err := c.GetInfo()
			if err != nil {
				return err
			}
			newHeight := info.TopoHeight
			oldHeight := idx.ChainHeight.Load()
			idx.ChainHeight.Store(newHeight)

			// Persist the GetInfo snapshot for the civilware GetGetInfoDetails
			// surface. Best-effort: a write failure must not disrupt polling.
			if idx.Store != nil {
				if err := idx.Store.StoreGetInfo(info); err != nil {
					logger.Debugf("store getinfo snapshot: %v", err)
				}
			}

			// Throttle tela_cache height rewrites: full-file rewrite every block
			// is gratuitous I/O when the only change is the Height field.
			if newHeight > oldHeight && oldHeight > 0 &&
				newHeight-lastCacheHeight >= telaCacheHeightThrottle {
				if cached, err := loadTELACache(idx.DBDir); err == nil {
					cached.Height = newHeight
					if err := saveTELACache(idx.DBDir, cached.IndexSCIDs, cached.DocSCIDs, cached.Classes, newHeight); err == nil {
						lastCacheHeight = newHeight
					}
				}
			}
			return nil
		})
		if err != nil {
			logger.Errorf("getInfo: %v", err)
		}
		// Adaptive polling: fast when caught up, slow during sync to reduce daemon load
		if idx.LastIndexedHeight.Load() >= idx.ChainHeight.Load()-10 {
			time.Sleep(500 * time.Millisecond) // caught up: fast polling
		} else {
			time.Sleep(5 * time.Second) // syncing: slow polling
		}
	}
}

// fetchedBatch carries raw block/TX data from the fetcher to the processor.
// Inspired by Kelvin (2504.06151) zero-copy pipeline stages: data flows
// through typed channels with no intermediate serialization.
type fetchedBatch struct {
	blockResults []*rpc.GetBlock_Result
	heights      []uint64
	fetchCount   int
	// Pre-collected TX data from the second RPC round trip
	txResult    *rpc.GetTransaction_Result
	allTxHashes []string
	// Per-block parsed info carried forward so processor skips re-parsing
	blocks []blockInfo
	// Registration TXs counted during block parsing (PoW prefix filter)
	regCount int64
}

// blockInfo holds parsed TX hashes and the block hash for a single height.
// The block hash is committed atomically with the batch in FlushBatch; a
// mismatch against stored hash at h-1 is the reorg signal (DESIGN.md §6).
type blockInfo struct {
	height    int64
	blockHash string // 64-hex; empty only on parse failure
	txHashes  []string
}

// processedBatch carries indexed data from the processor to the flusher.
type processedBatch struct {
	batch      *storage.WriteBatch
	newHeight  int64
	blockCount int
	// Timing for adaptive batch sizing
	batchStart time.Time
}

// scanLoop implements a 3-stage prefetch pipeline inspired by:
//   - Kelvin (arxiv 2504.06151): zero-copy pipeline stages with typed channels
//   - StreamDiffusion (arxiv 2312.12491): batch-streaming overlap
//
// Three concurrent goroutines connected by buffered channels:
//
//	[Fetcher] → fetchChan(cap:2) → [Processor] → flushChan(cap:2) → [Flusher]
//
// The fetcher issues RPC calls while the processor decodes the previous batch
// and the flusher writes the batch before that. At steady state all three
// stages run concurrently, tripling throughput vs the old sequential loop.
func (idx *Indexer) scanLoop() {
	fetchChan := make(chan *fetchedBatch, 2)
	flushChan := make(chan *processedBatch, 2)

	var wg sync.WaitGroup
	wg.Add(3)

	// --- Stage 1: Fetcher goroutine ---
	go func() {
		defer wg.Done()
		defer close(fetchChan)
		idx.fetcherLoop(fetchChan)
	}()

	// --- Stage 2: Processor goroutine ---
	go func() {
		defer wg.Done()
		defer close(flushChan)
		idx.processorLoop(fetchChan, flushChan)
	}()

	// --- Stage 3: Flusher goroutine ---
	go func() {
		defer wg.Done()
		idx.flusherLoop(flushChan)
	}()

	wg.Wait()
}

func (idx *Indexer) currentBatchSize() int {
	if !idx.AdaptBatchSize {
		return idx.BatchSize
	}
	n := idx.adaptiveBatchSize.Load()
	if n <= 0 {
		return idx.BatchSize
	}
	return int(n)
}

func (idx *Indexer) setCurrentBatchSize(n int) {
	if n < 1 {
		n = 1
	}
	idx.adaptiveBatchSize.Store(int64(n))
}

// fetcherLoop continuously fetches blocks and transactions, sending
// fetchedBatch values downstream. It owns all RPC I/O so the processor
// and flusher never block on the network.
func (idx *Indexer) fetcherLoop(out chan<- *fetchedBatch) {
	batchBlockCount := idx.ParallelBlocks
	caughtUp := false
	// fetcher advances its own cursor so it doesn't race against the lagging
	// LastIndexedHeight atomic (which is now only advanced by the flusher on
	// successful commit). Reorg detection & restart still read the atomic.
	nextHeight := idx.LastIndexedHeight.Load()
	// consecutiveSparseSkips bounds how many missing ranges we skip past, so a
	// pathological daemon can't make the fetcher spin across the whole chain.
	consecutiveSparseSkips := 0

	for !idx.Closing.Load() {
		lastHeight := nextHeight
		chainHeight := idx.ChainHeight.Load()

		// --- Speculative prefetch when caught up ---
		if lastHeight >= chainHeight {
			if !caughtUp {
				caughtUp = true
				// Signal downstream that we reached tip (processor handles sync/turbo)
				out <- &fetchedBatch{fetchCount: 0}
			}
			// Try speculative fetch of the next block
			var result *rpc.GetBlock_Result
			err := idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
				var e error
				result, e = c.GetBlockByHeight(uint64(lastHeight + 1)) // #nosec G115 -- DERO chain heights are 0..2^62, far below the conversion bound.
				return e
			})
			if err != nil {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			// Got a new block! Build a single-block fetchedBatch.
			h := uint64(lastHeight + 1) // #nosec G115 -- chain heights are 0..2^62; lastHeight is never negative.
			fb := idx.fetchSingleBlock(result, h)
			if fb != nil {
				caughtUp = false
				out <- fb
				nextHeight++
			}
			continue
		}
		caughtUp = false

		remaining := int(chainHeight - lastHeight)
		fetchCount := batchBlockCount
		if fetchCount > remaining {
			fetchCount = remaining
		}

		// === ROUND TRIP 1: Batch fetch all blocks ===
		heights := make([]uint64, fetchCount)
		for i := 0; i < fetchCount; i++ {
			heights[i] = uint64(lastHeight) + uint64(i) + 1 // #nosec G115 -- chain heights are 0..2^62; lastHeight is never negative.
		}

		var blockResults []*rpc.GetBlock_Result
		tBlocks := time.Now()
		err := idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
			var e error
			blockResults, e = c.BatchGetBlocks(heights)
			return e
		})
		idx.timer.record(stageFetchBlocksRPC, time.Since(tBlocks))
		if err != nil {
			logger.Errorf("BatchGetBlocks at %d: %v", lastHeight+1, err)
			if errors.Is(err, hgrpc.ErrBlocksMissing) && consecutiveSparseSkips < 5 {
				// Sparse daemon: the whole requested range doesn't exist yet
				// (fastsync node still backfilling). Skip past it so the scan
				// keeps making progress instead of stalling on missing history.
				consecutiveSparseSkips++
				nextHeight += int64(len(heights))
				logger.Warnf("Skipping missing block range %d..%d (sparse daemon history, skip %d/5); next=%d",
					heights[0], heights[len(heights)-1], consecutiveSparseSkips, nextHeight+1)
				continue
			}
			time.Sleep(1 * time.Second)
			continue
		}

		// A clean batch means the sparse region (if any) is behind us.
		consecutiveSparseSkips = 0

		// Parse blocks and collect all TX hashes
		blocks := make([]blockInfo, 0, fetchCount)
		allTxHashes := make([]string, 0, fetchCount*10)
		var regCount int64
		firstBlockChecked := false

		tDecode := time.Now()
		for i, result := range blockResults {
			if result == nil {
				continue
			}
			bi := blockInfo{height: int64(heights[i])} // #nosec G115 -- DERO chain heights are 0..2^62, far below the conversion bound.

			var bl block.Block
			// Binary path first (faster than JSON for the hot loop)
			if blobBytes, err := hex.DecodeString(result.Blob); err == nil && len(blobBytes) > 0 {
				if err2 := bl.Deserialize(blobBytes); err2 != nil {
					if err3 := json.Unmarshal([]byte(result.Json), &bl); err3 != nil {
						logger.Errorf("parse block %d: blob=%v json=%v", heights[i], err2, err3)
						continue
					}
				}
			} else if err := json.Unmarshal([]byte(result.Json), &bl); err != nil {
				logger.Errorf("parse block %d: %v", heights[i], err)
				continue
			}

			// Block hash — committed atomically with batch data for reorg detection.
			bi.blockHash = bl.GetHash().String()

			// M1 reorg detection: on the FIRST successfully-parsed block of each
			// batch, verify the block's parent tip chains onto the stored hash
			// at h-1 (see checkReorgForBlock for the Tips/DAG rationale). Cheap
			// (one bbolt read per batch) and it runs in the fetcher so the
			// processor never sees a bad chain. The len(bl.Tips) > 0 guard is
			// kept here so firstBlockChecked latches on the first block that
			// actually has a parent tip. Mismatch only logs+counts in M1;
			// M2 will truncate+replay.
			if !firstBlockChecked && len(bl.Tips) > 0 {
				firstBlockChecked = true
				idx.checkReorgForBlock(int64(heights[i]), &bl) // #nosec G115 -- heights derive from lastHeight+i, far below 2^62.
			}

			for _, h := range bl.Tx_hashes {
				// Check the registration prefix BEFORE hex-encoding: h.String()
				// allocates and is pure waste for every registration TX (#9).
				if isRegistrationTxHash(h) {
					regCount++
					continue
				}
				hashStr := h.String()
				bi.txHashes = append(bi.txHashes, hashStr)
				allTxHashes = append(allTxHashes, hashStr)
			}
			blocks = append(blocks, bi)
		}
		idx.timer.record(stageFetchBlockDecode, time.Since(tDecode))

		// === ROUND TRIP 2: Mega-batch fetch ALL transactions ===
		var txResult *rpc.GetTransaction_Result
		if len(allTxHashes) > 0 {
			tTxs := time.Now()
			err = idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
				var e error
				txResult, e = c.GetTransaction(allTxHashes)
				return e
			})
			idx.timer.record(stageFetchTxsRPC, time.Since(tTxs))
			if err != nil {
				logger.Errorf("BatchGetTransaction (%d hashes): %v", len(allTxHashes), err)
			}
		}

		tSend := time.Now()
		out <- &fetchedBatch{
			blockResults: blockResults,
			heights:      heights,
			fetchCount:   fetchCount,
			txResult:     txResult,
			allTxHashes:  allTxHashes,
			blocks:       blocks,
			regCount:     regCount,
		}
		idx.timer.record(stageFetchSendWait, time.Since(tSend))
		nextHeight += int64(fetchCount)
	}
}

// fetchSingleBlock builds a fetchedBatch from one speculative block result.
func (idx *Indexer) fetchSingleBlock(result *rpc.GetBlock_Result, height uint64) *fetchedBatch {
	bi := blockInfo{height: int64(height)} // #nosec G115 -- DERO chain heights are 0..2^62, far below the conversion bound.
	var regCount int64

	var bl block.Block
	// Binary path first (faster than JSON for speculative blocks)
	if blobBytes, err := hex.DecodeString(result.Blob); err == nil && len(blobBytes) > 0 {
		if err2 := bl.Deserialize(blobBytes); err2 != nil {
			if err3 := json.Unmarshal([]byte(result.Json), &bl); err3 != nil {
				logger.Errorf("parse speculative block %d: blob=%v json=%v", height, err2, err3)
				return nil
			}
		}
	} else if err := json.Unmarshal([]byte(result.Json), &bl); err != nil {
		logger.Errorf("parse speculative block %d: %v", height, err)
		return nil
	}

	bi.blockHash = bl.GetHash().String()

	// M1 reorg detection on the LIVE tip. Catch-up batches check their first
	// block (indexer.go above); this speculative single-block path is where the
	// indexer sits at the chain tip, which is exactly where real DERO reorgs
	// occur — so without this call tip reorgs were invisible. Same shared helper
	// and semantics as the batch site.
	idx.checkReorgForBlock(int64(height), &bl) // #nosec G115 -- chain heights are 0..2^62, far below the conversion bound.

	allTxHashes := make([]string, 0, len(bl.Tx_hashes))
	for _, h := range bl.Tx_hashes {
		// Same ordering as the batch fetcher: prefix check first, hex-encode
		// only the hashes we keep (#9).
		if isRegistrationTxHash(h) {
			regCount++
			continue
		}
		hashStr := h.String()
		bi.txHashes = append(bi.txHashes, hashStr)
		allTxHashes = append(allTxHashes, hashStr)
	}

	var txResult *rpc.GetTransaction_Result
	if len(allTxHashes) > 0 {
		_ = idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
			var e error
			txResult, e = c.GetTransaction(allTxHashes)
			return e
		})
	}

	return &fetchedBatch{
		blockResults: []*rpc.GetBlock_Result{result},
		heights:      []uint64{height},
		fetchCount:   1,
		txResult:     txResult,
		allTxHashes:  allTxHashes,
		blocks:       []blockInfo{bi},
		regCount:     regCount,
	}
}

// processorLoop receives fetched batches, decodes transactions, builds
// WriteBatch objects, and sends them to the flusher. All CPU-bound work
// (hex decode, protobuf deserialize, SC argument parsing) lives here,
// isolated from I/O stages.
//
// Progress invariant: processorLoop tracks a local runningHeight but does NOT
// update idx.LastIndexedHeight. Only flusherLoop advances the atomic, and only
// after FlushBatch commits. This prevents the indexer from skipping blocks on
// restart after a mid-flight flush error.
func (idx *Indexer) processorLoop(in <-chan *fetchedBatch, out chan<- *processedBatch) {
	batch := storage.NewWriteBatch()
	blocksInBatch := 0
	syncSignaled := false
	runningHeight := idx.LastIndexedHeight.Load()

	for {
		tRecv := time.Now()
		fb, ok := <-in
		idx.timer.record(stageProcRecvWait, time.Since(tRecv))
		if !ok {
			break
		}
		if idx.Closing.Load() {
			break
		}

		// fetchCount == 0 is the "caught up" sentinel from the fetcher
		if fb.fetchCount == 0 {
			// Flush any accumulated partial batch before signaling caught-up
			if blocksInBatch > 0 {
				out <- &processedBatch{
					batch:      batch,
					newHeight:  batch.LastHeight,
					blockCount: blocksInBatch,
					batchStart: time.Now(),
				}
				batch = storage.NewWriteBatch()
				blocksInBatch = 0
			}
			// Send a zero-block sentinel so flusher enables sync + turbo
			if !syncSignaled {
				syncSignaled = true
				out <- &processedBatch{blockCount: 0}
			}
			continue
		}
		syncSignaled = false

		batchStart := time.Now()

		// Decode and index all transactions
		var burnCount, normCount int64

		// Route B: record the per-block hashes atomically with the batch.
		// Empty hashes (parse failed) are skipped — downstream reorg logic
		// tolerates gaps and will verify on the next successful fetch.
		for _, bi := range fb.blocks {
			if bi.blockHash != "" {
				batch.AddBlockHash(bi.height, bi.blockHash)
			}
		}

		if fb.txResult != nil && len(fb.txResult.Txs_as_hex) > 0 {
			// txIdx tracks position in fb.allTxHashes, which the fetcher builds as
			// the in-order concatenation of every block's txHashes (fetcherLoop and
			// fetchSingleBlock append to bi.txHashes and allTxHashes adjacently under
			// identical filtering). The nested traversal below visits hashes in that
			// exact append order, so a running counter reproduces the old
			// map[hash]index lookup without allocating a map (tx hashes are unique,
			// so map and counter agree). ti is captured and the counter advanced at
			// the top of each iteration — including the skip branch — so the mid-loop
			// decode skips stay aligned with Txs_as_hex when the RPC returns fewer
			// Txs_as_hex than hashes.
			txIdx := 0
			for _, bi := range fb.blocks {
				for _, hashStr := range bi.txHashes {
					ti := txIdx
					txIdx++
					if ti >= len(fb.txResult.Txs_as_hex) {
						continue
					}

					tDec := time.Now()
					txHex := fb.txResult.Txs_as_hex[ti]
					txBin, err := hex.DecodeString(txHex)
					if err != nil {
						idx.timer.record(stageProcTxDecode, time.Since(tDec))
						continue
					}

					var tx transaction.Transaction
					if err := tx.Deserialize(txBin); err != nil {
						idx.timer.record(stageProcTxDecode, time.Since(tDec))
						continue
					}
					idx.timer.record(stageProcTxDecode, time.Since(tDec))

					switch tx.TransactionType {
					case transaction.SC_TX:
						tSC := time.Now()
						idx.processSCTx(&tx, fb.txResult.Txs[ti], hashStr, bi.height, batch)
						idx.timer.record(stageProcSCTx, time.Since(tSC))
					case transaction.REGISTRATION:
						fb.regCount++
					case transaction.BURN_TX:
						burnCount++
					case transaction.NORMAL:
						normCount++
						tN := time.Now()
						idx.processNormalTx(&tx, fb.txResult.Txs[ti], hashStr, bi.height, batch)
						idx.timer.record(stageProcNormal, time.Since(tN))
					}
				}
			}
		}

		batch.RegTxCount += fb.regCount
		batch.BurnTxCount += burnCount
		batch.NormTxCount += normCount

		newHeight := runningHeight + int64(fb.fetchCount)
		runningHeight = newHeight
		batch.LastHeight = newHeight
		blocksInBatch += fb.fetchCount

		// Flush when threshold reached
		batchSize := idx.currentBatchSize()
		if blocksInBatch >= batchSize {
			tSend := time.Now()
			out <- &processedBatch{
				batch:      batch,
				newHeight:  newHeight,
				blockCount: blocksInBatch,
				batchStart: batchStart,
			}
			idx.timer.record(stageProcSendWait, time.Since(tSend))
			idx.timer.onBatch()
			batch = storage.NewWriteBatch()
			blocksInBatch = 0
		}
	}

	// Final partial batch
	if blocksInBatch > 0 && batch.LastHeight > 0 {
		out <- &processedBatch{
			batch:      batch,
			newHeight:  batch.LastHeight,
			blockCount: blocksInBatch,
			batchStart: time.Now(),
		}
	}
}

// flusherLoop writes processedBatch values to storage. It owns all disk I/O
// so neither the fetcher nor the processor blocks on bbolt commits.
func (idx *Indexer) flusherLoop(in <-chan *processedBatch) {
	startTime := time.Now()
	scanStartHeight := idx.LastIndexedHeight.Load()
	syncEnabled := false

	for {
		tRecv := time.Now()
		pb, ok := <-in
		idx.timer.record(stageFlushRecvWait, time.Since(tRecv))
		if !ok {
			break
		}
		if idx.Closing.Load() {
			// Drain: still flush remaining batches for data integrity
			if pb.batch != nil && pb.batch.LastHeight > 0 {
				if err := idx.Store.FlushBatch(pb.batch); err != nil {
					logger.Errorf("FlushBatch (drain): %v", err)
				}
				storage.PutWriteBatch(pb.batch)
			}
			continue
		}

		// blockCount == 0 is the "caught up" sentinel
		if pb.blockCount == 0 {
			if !syncEnabled {
				syncEnabled = true
				if idx.syncedOnce.CompareAndSwap(false, true) {
					idx.Store.EnableSync()
					if idx.TurboMode {
						switch idx.PostScanVarsMode {
						case PostScanVarsAll:
							idx.postScanVariableFetch()
						default:
							logger.Info("Post-scan variable fetch skipped (--postscan-vars=lazy)")
						}
					}
					// Kick off the 60s TELA variable refresher so rating
					// updates/state changes from post-sync invokes land in
					// scvars without waiting for the next fastsync cycle.
					idx.startTELARefresher()
				}
			}
			continue
		}
		syncEnabled = false

		tFlush := time.Now()
		flushErr := idx.Store.FlushBatch(pb.batch)
		idx.timer.record(stageFlushBBolt, time.Since(tFlush))
		if flushErr != nil {
			logger.Errorf("FlushBatch: %v", flushErr)
		} else {
			// Only advance the durable progress atomic after the DB commit succeeds.
			// If the process dies between here and the next flush, restart resumes
			// from this point — we never claim progress we haven't persisted.
			idx.LastIndexedHeight.Store(pb.newHeight)
			// SafeHeight = indexed - finality_depth, bounded at 0.
			prevSafe := idx.SafeHeight.Load()
			safe := pb.newHeight - idx.FinalityDepth
			if safe < 0 {
				safe = 0
			}
			idx.SafeHeight.Store(safe)
			// Route B M1: publish events AFTER durable commit. Bus is nil-safe.
			idx.publishBatchEvents(pb.batch, pb.newHeight, safe, prevSafe)
			chainHeight := idx.ChainHeight.Load()
			elapsed := time.Since(startTime)
			// bps was previously float64(pb.newHeight)/elapsed, i.e. absolute chain
			// height divided by uptime — reporting 177,000 blk/s on mainnet when
			// the real rate was 70. Subtract the scan start so the number means
			// "blocks this process has scanned per second."
			scanned := pb.newHeight - scanStartHeight
			var bps float64
			if elapsed.Seconds() > 0 {
				bps = float64(scanned) / elapsed.Seconds()
			}
			var eta time.Duration
			if bps > 0 && pb.newHeight < chainHeight {
				eta = time.Duration(float64(chainHeight-pb.newHeight)/bps) * time.Second
			}
			batchSize := idx.currentBatchSize()
			logger.Infof("Height: %d/%d | %.1f blk/s | ETA: %v | Batch: %d",
				pb.newHeight, chainHeight, bps, eta.Round(time.Second), batchSize)
		}
		// Recycle the batch whether flush succeeded or failed — either way we're
		// done with it. Losing one batch to GC on error is cheap; leaking is not.
		storage.PutWriteBatch(pb.batch)

		// Adaptive batch sizing
		if idx.AdaptBatchSize {
			batchElapsed := time.Since(pb.batchStart)
			batchSize := idx.currentBatchSize()
			if next := nextAdaptiveBatchSize(batchElapsed, batchSize); next != batchSize {
				idx.setCurrentBatchSize(next)
			}
		}
	}
}

// postScanVariableFetch runs after turbo-mode scanning completes.
// It fetches SC variables for all discovered SCIDs using a parallel worker pool,
// making up for the GetSC calls that were skipped during the scan phase.
func (idx *Indexer) postScanVariableFetch() {
	scids, err := idx.Store.GetAllSCIDs()
	if err != nil {
		logger.Errorf("post-scan: GetAllSCIDs: %v", err)
		return
	}
	if len(scids) == 0 {
		logger.Info("Post-scan: no SCIDs to fetch variables for")
		return
	}
	logger.Infof("Post-scan: fetching variables for %d SCIDs", len(scids))

	// Parallel fetch with worker pool
	work := make(chan string, len(scids))
	for _, scid := range scids {
		work <- scid
	}
	close(work)

	var wg sync.WaitGroup
	batch := storage.NewWriteBatch()
	var mu sync.Mutex
	workers := runtime.GOMAXPROCS(0)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for scid := range work {
				_ = idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
					result, err := c.GetSC(scid, -1, nil, nil, false)
					if err != nil {
						return err
					}
					vars := parseSCVariables(result)
					if len(vars) > 0 {
						// Re-classify off the freshly fetched vars. Turbo's
						// scan-time AddClass ran with nil vars (code only), so
						// without this the sweep pays for every GetSC and then
						// throws away the only metadata those vars carry —
						// Name/Desc/IconURL and the G45 media URLs all stay
						// empty. Same shape as refreshClassMetaOnInvoke: keep
						// the code-derived class, refresh the var-derived
						// fields, preserve InstallHeight.
						// One height for BOTH writes. ClassMeta.LastHeight is
						// the key readers use to locate the matching snapshot,
						// and GetSCIDVariableDetailsAtHeight builds an EXACT
						// "<scid>:<height>" key with no floor scan — so a
						// LastHeight that disagrees with the height the vars
						// were written at silently resolves to nothing.
						h := idx.ChainHeight.Load()
						meta := idx.reclassifyFromVars(scid, vars, h)
						mu.Lock()
						batch.AddVariables(scid, h, vars)
						if meta != nil {
							batch.AddClass(scid, meta)
						}
						mu.Unlock()
					}
					return nil
				})
			}
		}()
	}
	wg.Wait()

	if err := idx.Store.FlushBatch(batch); err != nil {
		logger.Errorf("post-scan flush: %v", err)
	}
	storage.PutWriteBatch(batch)
	logger.Info("Post-scan variable fetch complete")
}

// reclassifyFromVars rebuilds an SCID's ClassMeta from a variable snapshot,
// keeping the class that was derived from SC code at install time. Returns nil
// when the record should be left alone.
//
// Safe for concurrent use: it only reads the store (bbolt View) and returns a
// fresh record; the caller serializes the batch write.
//
// The nil returns are the important part. Classifying var-only (no SC code)
// can only ever produce UNKNOWN, and an UNKNOWN record would overwrite a good
// install-time class permanently — the same trap documented on
// refreshClassMetaOnInvoke. So a read error or a missing prior record means
// "skip", never "reclassify from scratch".
// height must be the SAME height the caller writes the variable snapshot at —
// it becomes ClassMeta.LastHeight, which is how readers find that snapshot.
// Only InstallHeight is carried forward; this is a refresh, not an install.
// Matches refreshClassMetaOnInvoke and the fastsync/tela_refresher sites.
func (idx *Indexer) reclassifyFromVars(scid string, vars []*structures.SCIDVariable, height int64) *structures.ClassMeta {
	existing, err := idx.Store.GetSCIDClass(scid)
	if err != nil || existing == nil {
		return nil
	}
	sc := ClassifySCVarsWithClass(scid, existing.Class, vars)
	return classMetaFrom(&sc, existing.InstallHeight, height)
}

// processSCTx handles a smart contract transaction.
// In turbo mode, GetSC calls are skipped entirely -- only TX metadata is recorded.
func (idx *Indexer) processSCTx(tx *transaction.Transaction, txInfo rpc.Tx_Related_Info, txid string, height int64, batch *storage.WriteBatch) {
	scArgs := tx.SCDATA
	scFees := tx.Fees()
	entrypoint := argString(scArgs, "entrypoint", rpc.DataString)
	scAction := argString(scArgs, "SC_ACTION", rpc.DataUint64)

	var method uint8
	var scid string

	if scAction == "1" {
		method = structures.MethodInstallSC
		scid = txid
	} else {
		method = structures.MethodInvokeSC
		scid = scidArgString(scArgs.Value("SC_ID", "H"))
		if scid == "" {
			// Malformed invoke with no SC_ID. The old fmt.Sprintf path
			// rendered this as "<nil>" and stored junk harmlessly; an empty
			// scid must not reach FlushBatch, where an empty bucket name
			// would abort the whole atomic batch.
			return
		}
	}

	scid = hgpool.InternSCID(scid)

	// Check exclusions (O(1) map lookup)
	if _, excluded := idx.SCIDExclusions[scid]; excluded {
		return
	}

	var sender string
	if len(tx.Payloads) > 0 && tx.Payloads[0].Statement.RingSize == 2 {
		sender = hgpool.InternAddress(txInfo.Signer)
	}

	if method == structures.MethodInstallSC {
		if idx.TurboMode {
			// Turbo: record install from TX data, skip GetSC.
			// We still have SC_CODE in scArgs — classify from that alone
			// (no variables yet, but code is enough for class detection).
			idx.ValidatedSCs.Store(scid, struct{}{})
			batch.AddOwner(scid, sender)
			batch.AddInvocation(structures.InvokeRecord{
				Scid: scid, Sender: sender, Entrypoint: entrypoint,
				Height: height, Details: &structures.SCTXParse{
					Txid: scid, Scid: scid, Entrypoint: entrypoint,
					Method: structures.MethodInstallSC, Sender: sender,
					Fees: scFees, Height: height,
				},
			})
			batch.AddInteractionHeight(scid, height)
			batch.AddInstall(scid, height, &structures.InstallRecord{
				Owner: sender, Entrypoint: entrypoint, Fees: scFees,
			})
			// Classify from code only (turbo skips variable fetch).
			code := argString(scArgs, "SC_CODE", rpc.DataString)
			sc := ClassifySC(scid, code, nil)
			batch.AddClass(scid, classMetaFrom(&sc, height, height))
			if idx.shouldPersistCode(sc.Class) {
				batch.AddSCCode(scid, height, code)
			}
			batch.AddAddrSCID(sender, scid, height)
		} else {
			idx.handleInstallSC(scid, sender, entrypoint, height, scArgs, scFees, tx, batch)
		}
	} else {
		if idx.TurboMode {
			// Turbo: record invoke from TX data, skip GetSC.
			idx.ValidatedSCs.Store(scid, struct{}{})
			batch.AddInvocation(structures.InvokeRecord{
				Scid: scid, Sender: sender, Entrypoint: entrypoint,
				Height: height, Details: &structures.SCTXParse{
					Txid: txid, Scid: scid, Entrypoint: entrypoint,
					Method: structures.MethodInvokeSC, Sender: sender,
					Fees: scFees, Height: height,
				},
			})
			batch.AddInteractionHeight(scid, height)
			batch.AddAddrSCID(sender, scid, height)
		} else {
			idx.handleInvokeSC(scid, sender, entrypoint, height, scArgs, scFees, txid, batch)
		}
	}
}

// isRegistrationTxHash reports whether a TX hash carries the registration
// 3-zero-byte prefix. Callers check this BEFORE paying for h.String(): the
// hex encoding allocates and registration TXs are skipped entirely (#9).
func isRegistrationTxHash(h crypto.Hash) bool {
	return h[0] == 0 && h[1] == 0 && h[2] == 0
}

// scidArgString renders an SC_ID argument value as its canonical string form
// without fmt's reflection cost (#10). derohe delivers DataHash args as
// crypto.Hash; missing SC_ID (nil) maps to "" rather than fmt's "<nil>".
func scidArgString(v interface{}) string {
	switch x := v.(type) {
	case crypto.Hash:
		return x.String()
	case string:
		return x
	case fmt.Stringer:
		return x.String()
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", x)
	}
}

// classMetaFrom projects a classifier result onto the stored ClassMeta record.
//
// Every AddClass site funnels through here on purpose. The projection used to
// be an inline struct literal repeated at four call sites, which meant adding a
// field to SCClass populated it on whichever paths the author remembered and
// silently dropped it on the rest — a bug with no failing test, because each
// path is exercised by a different sync mode. One function, one place to keep
// in sync with SCClass.
func classMetaFrom(sc *SCClass, installHeight, lastHeight int64) *structures.ClassMeta {
	return &structures.ClassMeta{
		Class:         sc.Class,
		Tags:          sc.Tags,
		Name:          sc.Name,
		Desc:          sc.Desc,
		IconURL:       sc.IconURL,
		DURL:          sc.DURL,
		Version:       sc.Version,
		Image:         sc.Image,
		AltImage:      sc.AltImage,
		Audio:         sc.Audio,
		Video:         sc.Video,
		ImagesJSON:    sc.ImagesJSON,
		InstallHeight: installHeight,
		LastHeight:    lastHeight,
	}
}

// varsToMap converts parseSCVariables output to the string-keyed map
// ClassifySC expects. Non-string keys are ignored.
func varsToMap(vars []*structures.SCIDVariable) map[string]interface{} {
	m := make(map[string]interface{}, len(vars))
	for _, v := range vars {
		if k, ok := v.Key.(string); ok {
			m[k] = v.Value
		}
	}
	return m
}

func argString(args rpc.Arguments, name string, dtype rpc.DataType) string {
	for _, arg := range args {
		if arg.Name != name || arg.DataType != dtype {
			continue
		}
		switch v := arg.Value.(type) {
		case string:
			return v
		case uint64:
			return strconv.FormatUint(v, 10)
		case int64:
			return strconv.FormatInt(v, 10)
		case int:
			return strconv.Itoa(v)
		case fmt.Stringer:
			return v.String()
		case nil:
			return ""
		default:
			return fmt.Sprintf("%v", v)
		}
	}
	return ""
}

// handleInstallSC processes a new SC deployment.
func (idx *Indexer) handleInstallSC(scid, sender, entrypoint string, height int64, scArgs rpc.Arguments, fees uint64, tx *transaction.Transaction, batch *storage.WriteBatch) {
	code := argString(scArgs, "SC_CODE", rpc.DataString)

	// Check search filter
	if !idx.matchesFilter(code) {
		return
	}

	// Fetch SC variables
	var scVars []*structures.SCIDVariable
	tGetSC := time.Now()
	_ = idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
		result, err := c.GetSC(scid, height, nil, nil, false)
		if err != nil {
			return err
		}
		scVars = parseSCVariables(result)
		return nil
	})
	idx.timer.record(stageProcSCTxGetSC, time.Since(tGetSC))

	if len(scVars) == 0 {
		batch.InvalidSCIDs[scid] = fees
		return
	}

	// Register validated SC
	idx.ValidatedSCs.Store(scid, struct{}{})

	// Add to batch
	batch.AddOwner(scid, sender)
	batch.AddInvocation(structures.InvokeRecord{
		Scid:       scid,
		Sender:     sender,
		Entrypoint: entrypoint,
		Height:     height,
		Details: &structures.SCTXParse{
			Txid:       scid,
			Scid:       scid,
			Entrypoint: entrypoint,
			Method:     structures.MethodInstallSC,
			ScArgs:     scArgs,
			Sender:     sender,
			Fees:       fees,
			Height:     height,
		},
	})
	batch.AddVariables(scid, height, scVars)
	batch.AddInteractionHeight(scid, height)

	// Route B: classify, record install, record addr→scid edge.
	batch.AddInstall(scid, height, &structures.InstallRecord{
		Owner: sender, Entrypoint: entrypoint, Fees: fees,
	})
	sc := ClassifySCVars(scid, code, scVars)
	batch.AddClass(scid, classMetaFrom(&sc, height, height))
	if idx.shouldPersistCode(sc.Class) {
		batch.AddSCCode(scid, height, code)
	}
	batch.AddAddrSCID(sender, scid, height)
}

// shouldPersistCode routes through CodePolicy. "none" skips all; "tela"
// only persists the TELA family (INDEX/DOC/MOD -1); "all" persists every
// classified SCID. Default "tela" matches the TELA content server's
// requirement (body lives in the DOC source) without bloating the DB
// with 48 k copies of largely-duplicated G45 NFT template code.
func (idx *Indexer) shouldPersistCode(class string) bool {
	if idx == nil {
		return false
	}
	switch idx.CodePolicy {
	case "all":
		return true
	case "none":
		return false
	case "tela", "":
		return isTELAClass(class)
	}
	return isTELAClass(class) // unknown policy falls back to safe default
}

// isTELAClass reports whether class is in the TELA family. Used by the
// code-persistence policy and (elsewhere) by downstream consumers that
// want to filter TELA-only content.
func isTELAClass(class string) bool {
	switch class {
	case "TELA-INDEX-1", "TELA-DOC-1", "TELA-MOD-1":
		return true
	}
	return false
}

// normalizeCodePolicy maps an empty string (config default) to "tela" and
// validates known values. Unknown strings pass through so the test in
// shouldPersistCode can still reason about them.
func normalizeCodePolicy(p string) string {
	switch p {
	case "", "tela":
		return "tela"
	case "none", "all":
		return p
	}
	return "tela"
}

const maxClassifyProbeBatchSize = 1000
const maxAdaptiveBatchSize = 1000
const minAdaptiveBatchSize = 10

func normalizeClassifyProbeBatchSize(n int) int {
	if n <= 0 {
		return structures.DefaultClassifyProbeBatchSize
	}
	if n > maxClassifyProbeBatchSize {
		return maxClassifyProbeBatchSize
	}
	return n
}

func nextAdaptiveBatchSize(batchElapsed time.Duration, batchSize int) int {
	if batchElapsed < time.Second && batchSize < maxAdaptiveBatchSize {
		return min(batchSize*2, maxAdaptiveBatchSize)
	}
	if batchElapsed > 5*time.Second && batchSize > minAdaptiveBatchSize {
		return max(batchSize/2, minAdaptiveBatchSize)
	}
	return batchSize
}

const (
	PostScanVarsLazy = "lazy"
	PostScanVarsAll  = "all"
)

func normalizePostScanVarsMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", PostScanVarsLazy:
		return PostScanVarsLazy
	case PostScanVarsAll:
		return PostScanVarsAll
	default:
		return structures.DefaultPostScanVarsMode
	}
}

// handleInvokeSC processes an SC invocation.
//
// This does exactly one GetSC regardless of whether the SCID is already
// validated: the result is used both to validate (non-empty vars) and to
// record the post-invoke variable snapshot. The previous implementation fetched
// twice on first encounter of an unknown SCID.
func (idx *Indexer) handleInvokeSC(scid, sender, entrypoint string, height int64, scArgs rpc.Arguments, fees uint64, txid string, batch *storage.WriteBatch) {
	var scVars []*structures.SCIDVariable
	tGetSC := time.Now()
	_ = idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
		result, err := c.GetSC(scid, height, nil, nil, false)
		if err != nil {
			return err
		}
		scVars = parseSCVariables(result)
		return nil
	})
	idx.timer.record(stageProcSCTxGetSC, time.Since(tGetSC))

	if _, known := idx.ValidatedSCs.Load(scid); !known {
		if len(scVars) == 0 {
			return // unknown and invalid; drop
		}
		idx.ValidatedSCs.Store(scid, struct{}{})
	}

	batch.AddInvocation(structures.InvokeRecord{
		Scid:       scid,
		Sender:     sender,
		Entrypoint: entrypoint,
		Height:     height,
		Details: &structures.SCTXParse{
			Txid:       txid,
			Scid:       scid,
			Entrypoint: entrypoint,
			Method:     structures.MethodInvokeSC,
			ScArgs:     scArgs,
			Sender:     sender,
			Fees:       fees,
			Height:     height,
		},
	})

	if len(scVars) > 0 {
		batch.AddVariables(scid, height, scVars)
	}
	batch.AddInteractionHeight(scid, height)

	// Route B: refresh class metadata (TELA apps bump version via STORE) and
	// record addr→scid edge.
	if len(scVars) > 0 {
		idx.refreshClassMetaOnInvoke(scid, height, scVars, batch)
	}
	batch.AddAddrSCID(sender, scid, height)
}

// refreshClassMetaOnInvoke rebuilds the stored ClassMeta from a fresh
// post-invoke variable snapshot, seeding the refresh with the STORED class: an
// invoke carries no SC code, so ClassifySCVars(scid, "", …) can only ever
// yield UNKNOWN — which used to overwrite the install-time Class/Tags and
// drop the class-gated fields (Version etc.) on every invoke, i.e. on every
// normal TELA update. ClassifySCVarsWithClass re-applies the known class and
// extracts the class-gated fields from the fresh vars in one pass (same shape
// as the fastsync + tela_refresher refreshes).
func (idx *Indexer) refreshClassMetaOnInvoke(scid string, height int64, scVars []*structures.SCIDVariable, batch *storage.WriteBatch) {
	// The authoritative prior meta may still be PENDING in this very batch —
	// install + invoke of the same SC inside one flush window is the normal
	// pattern during catch-up sync. batch.Classes must win over the flushed
	// store: a store miss here would route to the code-less fallback, whose
	// UNKNOWN record overwrites the pending install-time AddClass (last
	// AddClass wins per scid), and the stored-class seeding would then
	// re-store UNKNOWN on every later invoke, permanently.
	existingMeta := batch.Classes[scid]
	if existingMeta == nil {
		stored, err := idx.Store.GetSCIDClass(scid)
		if err != nil {
			// A failed read must not degrade the stored class (the code-less
			// fallback can only yield UNKNOWN, and that sticks). Skip this
			// refresh; the next invoke retries. A missing record is
			// (nil, nil), not an error, so the fresh-SC path is unaffected.
			return
		}
		existingMeta = stored
	}
	// Note: if a FUTURE binary stored a class this binary's tagsForClass does
	// not know, the refresh keeps the Class string but resets Tags to ["all"]
	// (version-skew only; every class this binary can produce is covered).
	var sc SCClass
	installH := height
	if existingMeta != nil {
		// Preserve InstallHeight; this is a refresh.
		installH = existingMeta.InstallHeight
		sc = ClassifySCVarsWithClass(scid, existingMeta.Class, scVars)
	} else {
		// No prior meta (invoke seen before its install, or install not
		// indexed): classify code-less as before.
		sc = ClassifySCVars(scid, "", scVars)
	}
	batch.AddClass(scid, classMetaFrom(&sc, installH, height))
}

// processNormalTx handles a normal transaction with SCID payload.
func (idx *Indexer) processNormalTx(tx *transaction.Transaction, txInfo rpc.Tx_Related_Info, txid string, height int64, batch *storage.WriteBatch) {
	fees := tx.Fees() // constant per tx — hoisted out of the ring loop
	for j := 0; j < len(tx.Payloads); j++ {
		scidStr := tx.Payloads[j].SCID.String()
		// Zero hash means no SCID
		if scidStr == "0000000000000000000000000000000000000000000000000000000000000000" {
			continue
		}
		scidStr = hgpool.InternSCID(scidStr)
		for _, addr := range txInfo.Ring[j] {
			addr = hgpool.InternAddress(addr)
			// Route B: ring member touched this SCID. Useful for "this address
			// might be a participant of this SCID." Heavy but cardinality is
			// bounded by ring size. The record is arena-carved (see AddNormalTx)
			// rather than a per-ring-member heap alloc.
			batch.AddAddrSCID(addr, scidStr, height)
			batch.AddNormalTx(addr, txid, scidStr, fees, height)
		}
	}
}

// matchesFilter checks if SC code matches any search filter.
func (idx *Indexer) matchesFilter(code string) bool {
	if len(idx.SearchFilter) == 0 {
		return true // no filter = index all
	}
	for _, filter := range idx.SearchFilter {
		if strings.Contains(code, filter) {
			return true
		}
	}
	return false
}

// parseSCVariables extracts variables from a GetSC result.
func parseSCVariables(result *rpc.GetSC_Result) []*structures.SCIDVariable {
	if result == nil {
		return nil
	}
	n := len(result.VariableStringKeys) + len(result.VariableUint64Keys)
	if n == 0 {
		return nil
	}
	// Arena allocation (#11): one backing array for all variable structs
	// instead of one heap object per variable (2 allocs total vs n+1).
	// Callers never append to or reslice the returned slice, so the interior
	// pointers stay valid for the arena's lifetime.
	arena := make([]structures.SCIDVariable, n)
	vars := make([]*structures.SCIDVariable, n)
	i := 0
	for k, v := range result.VariableStringKeys {
		arena[i] = structures.SCIDVariable{Key: k, Value: v}
		vars[i] = &arena[i]
		i++
	}
	for k, v := range result.VariableUint64Keys {
		arena[i] = structures.SCIDVariable{Key: k, Value: v}
		vars[i] = &arena[i]
		i++
	}
	return vars
}

// Close shuts down the indexer gracefully.
// Setting Closing causes the fetcher to exit, which closes fetchChan,
// which causes the processor to exit, which closes flushChan,
// which causes the flusher to drain and exit. The WaitGroup in scanLoop
// ensures all three goroutines have finished before StartDaemonMode returns.
func (idx *Indexer) Close() {
	idx.Closing.Store(true)
	if idx.RPCPool != nil {
		idx.RPCPool.Close()
	}
	if idx.Store != nil && idx.ownStore {
		if err := idx.Store.Close(); err != nil {
			logger.Warnf("store close: %v", err)
		}
	}
}

// IndexSingleSCIDResult is returned by IndexSingleSCID. It mirrors the WS
// method's JSON response shape but in Go-native types so additional callers
// (HTTP handlers, tests) can consume it without a second parse.
type IndexSingleSCIDResult struct {
	SCID      string                `json:"scid"`
	ClassMeta *structures.ClassMeta `json:"-"`
	VarsCount int                   `json:"vars_count"`
	// FromCache indicates this SCID was already in the class bucket and
	// skipfsrecheck=true caused us to return cached metadata without hitting
	// the daemon.
	FromCache bool `json:"-"`
}

// IndexSingleSCID imports a single SCID on demand, bypassing the normal scan
// pipeline. This is the counterpart to civilware/Gnomon's "addscid_toindex":
// useful for SCs that don't show up in the GnomonSC registry (the usual
// discovery mechanism) but that a client still wants indexed.
//
// Flow:
//  1. If skipfsrecheck && meta already in class bucket, return cached.
//  2. GetSC(scid, -1, nil, nil, !varsonly) via the RPC pool. -1 means "tip".
//  3. If vars empty, return ErrSCIDNotFound (SC doesn't exist on-chain).
//  4. ClassifySCVars(scid, code, vars) → class meta.
//  5. Persist: owner (if extractable), variables at tip height, class, install.
//  6. Publish an EventInstall via the bus so subscribers learn about it.
//
// varsonly skips the SC_CODE fetch (smaller/faster, but class will typically
// be UNKNOWN since most classifiers need code). skipfsrecheck short-circuits
// the fetch entirely if we already have class metadata.
//
// This path does not use the scan-loop WriteBatch pool — it allocates a fresh
// batch, fills it, flushes it, and publishes. One-shot. Height used is the
// current chain tip (idx.ChainHeight); LastIndexedHeight is NOT advanced
// because we haven't actually scanned the block at tip.
func (idx *Indexer) IndexSingleSCID(scid string, varsonly, skipfsrecheck bool) (*IndexSingleSCIDResult, error) {
	// Skip recheck fast-path: if we already have class metadata for this
	// SCID, return it without contacting the daemon. Variables are not
	// reloaded; callers that want fresh variables should pass
	// skipfsrecheck=false.
	if skipfsrecheck {
		if meta, err := idx.Store.GetSCIDClass(scid); err == nil && meta != nil {
			// Count stored variables at the last known height to fill vars_count.
			vars, _ := idx.Store.GetSCIDVariableDetailsAtHeight(scid, meta.LastHeight)
			return &IndexSingleSCIDResult{
				SCID:      scid,
				ClassMeta: meta,
				VarsCount: len(vars),
				FromCache: true,
			}, nil
		}
	}

	// Fetch from the daemon at chain tip (-1). Code is fetched unless varsonly.
	var result *rpc.GetSC_Result
	err := idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
		var e error
		result, e = c.GetSC(scid, -1, nil, nil, !varsonly)
		return e
	})
	if err != nil {
		return nil, fmt.Errorf("GetSC: %w", err)
	}

	scVars := parseSCVariables(result)
	if len(scVars) == 0 {
		return nil, ErrSCIDNotFound
	}

	// Height used for all stored records. Chain tip is best-effort; falls back
	// to LastIndexedHeight if the tip monitor hasn't populated yet.
	height := idx.ChainHeight.Load()
	if height <= 0 {
		height = idx.LastIndexedHeight.Load()
	}

	owner := ""
	if v, ok := lookupVar(scVars, "owner"); ok {
		owner = varString(v)
	}

	// Code is only present when !varsonly. ClassifySC handles "" gracefully.
	code := ""
	if result != nil {
		code = result.Code
	}

	sc := ClassifySCVars(scid, code, scVars)

	// Preserve an earlier install height if one was previously recorded. This
	// matters when addscid_toindex is called multiple times against the same
	// SCID or after it was discovered by the normal scan path.
	installH := height
	if existing, err := idx.Store.GetSCIDClass(scid); err == nil && existing != nil {
		if existing.InstallHeight > 0 {
			installH = existing.InstallHeight
		}
	}

	meta := classMetaFrom(&sc, installH, height)

	// Build a one-shot batch — same shape as the scan-loop path so we benefit
	// from the same atomic commit semantics (all or nothing).
	batch := storage.NewWriteBatch()
	defer storage.PutWriteBatch(batch)

	batch.LastHeight = height
	if owner != "" {
		batch.AddOwner(scid, owner)
	}
	batch.AddVariables(scid, height, scVars)
	batch.AddInteractionHeight(scid, height)
	batch.AddInstall(scid, installH, &structures.InstallRecord{
		Owner:      owner,
		Entrypoint: "",
		Fees:       0,
	})
	batch.AddClass(scid, meta)
	if code != "" && idx.shouldPersistCode(sc.Class) {
		batch.AddSCCode(scid, installH, code)
	}
	if owner != "" {
		batch.AddAddrSCID(owner, scid, height)
	}

	if err := idx.Store.FlushBatch(batch); err != nil {
		return nil, fmt.Errorf("FlushBatch: %w", err)
	}

	// Mark the SCID as validated so subsequent invoke processing skips a
	// redundant GetSC validation round-trip.
	idx.ValidatedSCs.Store(scid, struct{}{})

	// Publish an install event for subscribers. Matches the shape published
	// by the scan loop's flusher so subscribe filters stay consistent.
	if idx.Bus != nil {
		safe := idx.SafeHeight.Load()
		idx.Bus.Publish(eventbus.Event{
			Type:       eventbus.EventInstall,
			Height:     installH,
			SafeHeight: safe,
			SCID:       scid,
			Owner:      owner,
			Payload: &structures.InstallRecord{
				Owner: owner,
			},
		})
		if meta.Class != "" && meta.Class != "UNKNOWN" {
			idx.Bus.Publish(eventbus.Event{
				Type:       eventbus.EventClassMatch,
				Height:     height,
				SafeHeight: safe,
				SCID:       scid,
				Class:      meta.Class,
				Tags:       meta.Tags,
				Payload:    meta,
			})
		}
	}

	return &IndexSingleSCIDResult{
		SCID:      scid,
		ClassMeta: meta,
		VarsCount: len(scVars),
	}, nil
}

// ErrSCIDNotFound signals that GetSC returned no variables for the requested
// SCID, i.e. the SC doesn't exist on-chain. Callers (the WS handler) translate
// this to a JSON-RPC application error rather than an internal error.
var ErrSCIDNotFound = fmt.Errorf("scid not found")
