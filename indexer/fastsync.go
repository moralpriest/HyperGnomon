package indexer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vmihailenco/msgpack/v5"

	"github.com/creachadair/jrpc2"
	"github.com/deroproject/derohe/rpc"

	hgpool "github.com/hypergnomon/hypergnomon/pool"
	hgrpc "github.com/hypergnomon/hypergnomon/rpc"
	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

// registryEntry represents a SCID from the GnomonSC registry.
type registryEntry struct {
	scid   string
	owner  string
	height int64
}

// settleTELADiscovery marks TELA discovery finished for --tela-only AND emits
// the "Classify probe complete" readiness marker (cmd/benchvs waits on that
// exact phrase). The two are coupled in one helper on purpose: every terminal
// classify path must do both, and hand-placing them separately has twice
// produced a path that emitted the marker without settling — an indefinite
// --tela-only hang. probeTELA's full-probe path settles via its own defer
// (its markers are logged with per-phase timings internally).
func settleTELADiscovery(reason string, elapsed time.Duration) {
	structures.TELAProbeSettled.Store(true)
	logger.Infof("Classify probe complete: %s | Total: %s", reason, elapsed.Round(time.Millisecond))
}

// FastSync bootstraps the indexer from the on-chain GnomonSC registry.
// Instead of scanning from block 1, it:
//  1. Queries the Gnomon SC for all registered SCIDs
//  2. Extracts owner and deployment height for each
//  3. Fetches current variables for each matching SCID
//  4. Sets LastIndexedHeight to current chain height
//  5. Normal scanning resumes from chain tip
func (idx *Indexer) FastSync(testnet bool) error {
	start := time.Now()

	// Select the correct GnomonSC SCID for the network
	gnomonSCID := structures.GnomonSCID_Mainnet
	network := "mainnet"
	if testnet {
		gnomonSCID = structures.GnomonSCID_Testnet
		network = "testnet"
	}

	// JOFITO: Skip registry fetch if cache is fresh (within 1000 blocks)
	// AND written by the current schema version AND the on-disk scvars
	// bucket still yields non-empty variable reads for sampled TELA INDEX
	// SCIDs. Strict version equality invalidates any cache older than
	// telaCacheVersion — that forces a one-time full re-probe after an
	// HG upgrade so schema changes (like the 60s refresher's fresh-rating
	// contract) take effect without manual reindex.
	var chainHeight int64
	if cached, err := loadTELACache(idx.DBDir); err == nil && cached.Height > 0 && cached.Version == telaCacheVersion {
		if err := idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
			info, err := c.GetInfo()
			if err != nil {
				return err
			}
			chainHeight = info.TopoHeight
			idx.ChainHeight.Store(chainHeight)
			return nil
		}); err == nil {
			if chainHeight-cached.Height < 1000 {
				// Sanity-probe the scvars bucket. Sample up to 5 TELA INDEX
				// SCIDs from the cached list; read their variables at the
				// cache's height. If every sampled read comes back empty or
				// error, the persistence layer is broken — invalidate the
				// cache and fall through to a fresh probe. If at least one
				// succeeds, honor the cache.
				ok := cachedScvarsLookReadable(idx, cached)
				if ok {
					idx.LastIndexedHeight.Store(chainHeight)
					otherCount := 0
					for _, v := range cached.Classes {
						otherCount += len(v)
					}
					structures.TELACount.Store(int64(len(cached.IndexSCIDs) + len(cached.DocSCIDs)))
					elapsed := time.Since(start)
					logger.Infof("FastSync: cache fresh v%d (height %d, %d blocks behind) — %d INDEX + %d DOC + %d other classes loaded in %s",
						cached.Version, cached.Height, chainHeight-cached.Height,
						len(cached.IndexSCIDs), len(cached.DocSCIDs), otherCount, elapsed.Round(time.Millisecond))
					// No probe will run on this path.
					settleTELADiscovery("cache fresh", 0)
					return nil
				}
				logger.Warnf("FastSync: cache height-fresh but scvars reads failed — forcing re-probe to heal corrupted variable blobs (pre-fix HyperGnomon artifact)")
			}
		}
	}

	// Get current chain height so we know where to resume scanning
	if chainHeight == 0 {
		if err := idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
			info, err := c.GetInfo()
			if err != nil {
				return err
			}
			chainHeight = info.TopoHeight
			return nil
		}); err != nil {
			return fmt.Errorf("fastsync: get chain height: %w", err)
		}
		idx.ChainHeight.Store(chainHeight)
	}

	logger.Infof("FastSync: querying GnomonSC %s at height %d", gnomonSCID[:16]+"...", chainHeight)

	// Step 1: Fetch the GnomonSC with all variables
	var gnomonVarStrings map[string]interface{}
	if err := idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
		result, err := c.GetSC(gnomonSCID, -1, nil, nil, false)
		if err != nil {
			return err
		}
		if result == nil || len(result.VariableStringKeys) == 0 {
			return fmt.Errorf("GnomonSC returned no variables")
		}
		gnomonVarStrings = result.VariableStringKeys
		return nil
	}); err != nil {
		return fmt.Errorf("fastsync: query GnomonSC: %w", err)
	}

	// Step 2: Parse registry entries from VariableStringKeys
	// Key patterns: 64 chars = SCID, 64+"owner" = owner, 64+"height" = deploy height

	// Single pass: categorize by key length
	scidSet := make(map[string]*registryEntry, len(gnomonVarStrings)/3)
	for key, val := range gnomonVarStrings {
		klen := len(key)
		switch {
		case klen == 64 && isHexString(key):
			if _, ok := scidSet[key]; !ok {
				scidSet[key] = &registryEntry{scid: key}
			}
		case klen == 69 && key[64:] == "owner":
			scid := key[:64]
			if entry, ok := scidSet[scid]; ok {
				if s, ok := val.(string); ok {
					entry.owner = s
				}
			} else {
				// SCID entry not seen yet, create it
				e := &registryEntry{scid: scid}
				if s, ok := val.(string); ok {
					e.owner = s
				}
				scidSet[scid] = e
			}
		case klen == 70 && key[64:] == "height":
			scid := key[:64]
			entry, ok := scidSet[scid]
			if !ok {
				entry = &registryEntry{scid: scid}
				scidSet[scid] = entry
			}
			switch v := val.(type) {
			case float64:
				entry.height = int64(v)
			case int64:
				entry.height = v
			case uint64:
				entry.height = int64(v) // #nosec G115 -- DERO chain heights are 0..2^62, far below the conversion bound.
			}
		}
	}

	// Step 3: Filter SCIDs -- apply search filter, skip NameServiceSCID and exclusions
	candidates := make([]*registryEntry, 0, len(scidSet))
	for _, entry := range scidSet {
		// Skip the DERO Nameservice SCID (always present, not user-deployed)
		if entry.scid == structures.NameServiceSCID {
			continue
		}

		// Check exclusions (O(1) map lookup)
		if _, excluded := idx.SCIDExclusions[entry.scid]; excluded {
			continue
		}

		candidates = append(candidates, entry)
	}

	if len(candidates) == 0 {
		logger.Warn("FastSync: no SCIDs found in GnomonSC registry")
		// Nothing to classify — discovery is settled with zero apps.
		settleTELADiscovery("empty registry", 0)
		return idx.Store.StoreLastIndexHeight(chainHeight)
	}

	logger.Infof("FastSync: %d SCIDs in registry, %d candidates after filtering", len(scidSet), len(candidates))
	registryHash := classifySeedRegistryHash(candidates, chainHeight)
	seedCtx := &classifySeedProbeContext{
		Network:      network,
		GnomonSCID:   gnomonSCID,
		RegistryHash: registryHash,
	}

	// Step 4: Import SCIDs into database
	batch := storage.NewWriteBatch()

	if idx.TurboMode {
		// Launch TELA discovery FIRST: the cache decision and probe consume
		// only the in-memory candidates + the RPC pool — nothing from the
		// registry fill+flush below (owners/installs land in different
		// buckets than the probe writes, and its flushes just serialize
		// behind bbolt's single writer). Hoisting hides the entire flush
		// under the probe's multi-second phase-1 RPC fan-out.
		idx.launchTELADiscovery(candidates, chainHeight, network, gnomonSCID, seedCtx)

		// TURBO FASTSYNC: Skip ALL GetSC calls. Store SCID + owner + height
		// from the registry, then claim progress only after the DB commit.
		for _, entry := range candidates {
			scid := hgpool.InternSCID(entry.scid)
			idx.ValidatedSCs.Store(scid, struct{}{})
			batch.AddOwner(scid, entry.owner)
			batch.AddInteractionHeight(scid, entry.height)
			// Route B: install record + owner→scid edge.
			// Class is filled in later by probeTELA's phase-1 code probe.
			batch.AddInstall(scid, entry.height, &structures.InstallRecord{
				Owner: entry.owner,
			})
			if entry.owner != "" {
				batch.AddAddrSCID(entry.owner, scid, entry.height)
			}
		}

		batch.LastHeight = chainHeight
		flushStart := time.Now()
		if err := idx.Store.FlushBatch(batch); err != nil {
			storage.PutWriteBatch(batch)
			return fmt.Errorf("fastsync: turbo flush batch: %w", err)
		}
		persisted := len(batch.Owners)
		storage.PutWriteBatch(batch)
		idx.LastIndexedHeight.Store(chainHeight)
		elapsed := time.Since(start)
		logger.Infof("FastSync complete: %d SCIDs persisted to height %d in %s (flush=%s)",
			persisted, chainHeight, elapsed.Round(time.Millisecond), time.Since(flushStart).Round(time.Millisecond))

		return nil
	} else {
		// NORMAL FASTSYNC: Fetch SC code + variables for each candidate (parallel)
		workers := runtime.GOMAXPROCS(0)
		if workers > len(candidates) {
			workers = len(candidates)
		}

		var batchMu sync.Mutex
		var processed atomic.Int64
		total := int64(len(candidates))

		work := make(chan *registryEntry, workers*2)
		var wg sync.WaitGroup

		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for entry := range work {
					if idx.Closing.Load() {
						return
					}

					scid := hgpool.InternSCID(entry.scid)
					var scVars []*structures.SCIDVariable
					var matchesFilter bool

					err := idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
						result, err := c.GetSC(scid, chainHeight, nil, nil, len(idx.SearchFilter) > 0)
						if err != nil {
							return err
						}
						if len(idx.SearchFilter) > 0 && result.Code != "" {
							matchesFilter = idx.matchesFilter(result.Code)
						} else if len(idx.SearchFilter) == 0 {
							matchesFilter = true
						}
						if matchesFilter {
							scVars = parseSCVariables(result)
						}
						return nil
					})

					if err != nil || !matchesFilter || len(scVars) == 0 {
						count := processed.Add(1)
						if count%100 == 0 || count == total {
							logger.Infof("FastSync: %d/%d (%.1f%%)", count, total, float64(count)/float64(total)*100)
						}
						continue
					}

					idx.ValidatedSCs.Store(scid, struct{}{})
					batchMu.Lock()
					batch.AddOwner(scid, entry.owner)
					batch.AddVariables(scid, chainHeight, scVars)
					batch.AddInteractionHeight(scid, entry.height)
					batchMu.Unlock()

					count := processed.Add(1)
					if count%100 == 0 || count == total {
						logger.Infof("FastSync: %d/%d (%.1f%%)", count, total, float64(count)/float64(total)*100)
					}
				}
			}()
		}

		for _, entry := range candidates {
			if idx.Closing.Load() {
				break
			}
			work <- entry
		}
		close(work)
		wg.Wait()
	}

	if idx.Closing.Load() {
		return fmt.Errorf("fastsync: interrupted by shutdown")
	}

	// Step 5: Flush batch and set last indexed height to current chain height
	batch.LastHeight = chainHeight
	if err := idx.Store.FlushBatch(batch); err != nil {
		storage.PutWriteBatch(batch)
		return fmt.Errorf("fastsync: flush batch: %w", err)
	}

	idx.LastIndexedHeight.Store(chainHeight)

	scidCount := len(batch.Owners)
	storage.PutWriteBatch(batch)
	elapsed := time.Since(start)
	logger.Infof("FastSync complete: %d SCIDs indexed to height %d in %s", scidCount, chainHeight, elapsed.Round(time.Millisecond))

	// Non-turbo FastSync used to skip probeTELA entirely, leaving TELACount
	// at 0 and --tela-only hanging forever. Run the probe here so every
	// FastSync path populates the TELA bucket regardless of scan strategy.
	// The turbo branch above has its own cache-aware probe launch; this
	// only fires on the non-turbo codepath.
	go idx.probeTELA(candidates, chainHeight, false, seedCtx)

	return nil
}

// launchTELADiscovery makes the turbo-path classify decision and starts TELA
// discovery: honor a usable local cache (delta-probe only), apply the cross-DB
// seed cache, or run the full probe. Consumes only in-memory candidates + the
// RPC pool — deliberately independent of the registry fill+flush so FastSync
// can run both concurrently. Every branch settles TELAProbeSettled (directly,
// via settleTELADiscovery, or via the gating probe's completion).
func (idx *Indexer) launchTELADiscovery(candidates []*registryEntry, chainHeight int64, network, gnomonSCID string, seedCtx *classifySeedProbeContext) {
	// JOFITO CACHE: Check if we have a cached TELA list from a previous run.
	// If so, load it instantly and only probe NEW SCIDs (delta since cached height).
	// Apply the same gates the up-front cache-hit path uses — version match
	// and a scvars sanity sample — so we don't trust a corrupt cache twice
	// in the same FastSync call.
	cached, cacheErr := loadTELACache(idx.DBDir)
	cacheUsable := cacheErr == nil && cached != nil && cached.Height > 0 &&
		cached.Version == telaCacheVersion && cachedScvarsLookReadable(idx, cached)
	// A TRUE cache miss is "loadTELACache returned nothing". A cache that
	// loaded but was version-rejected or failed the scvars sanity probe is
	// NOT a miss — it is evidence of stale/corrupt local state, and the
	// documented self-heal contract is a FULL re-probe that overwrites the
	// bad blobs. The classify seed cache must never intercept that path:
	// applying a seed would leave the corrupt historical scvars in place.
	trueCacheMiss := cacheErr != nil || cached == nil
	if cacheUsable {
		// Cache hit! Load known TELA SCIDs instantly.
		structures.TELACount.Store(int64(len(cached.IndexSCIDs) + len(cached.DocSCIDs)))
		logger.Infof("TELA cache hit: %d INDEX + %d DOC from height %d (instant load)",
			len(cached.IndexSCIDs), len(cached.DocSCIDs), cached.Height)

		// Only probe SCIDs deployed AFTER the cached height (delta scan)
		var deltaCandidates []*registryEntry
		for _, c := range candidates {
			if c.height > cached.Height {
				deltaCandidates = append(deltaCandidates, c)
			}
		}

		if len(deltaCandidates) > 0 {
			// The delta probe gates settlement: --tela-only must not exit
			// while its FlushBatch of newly-deployed SCIDs is in flight
			// (delta finds are not re-cached, so an abandoned flush would
			// drop them again on every subsequent run).
			logger.Infof("TELA delta probe: %d new SCIDs since height %d", len(deltaCandidates), cached.Height)
			go func() {
				idx.probeTELA(deltaCandidates, chainHeight, true, nil)
				structures.TELAProbeSettled.Store(true)
			}()
		} else {
			logger.Info("TELA delta probe: no new SCIDs since last run")
			settleTELADiscovery("tela cache", 0)
		}
	} else if deltaCandidates, ok := trueLocalCacheMissSeed(idx, trueCacheMiss, network, gnomonSCID, candidates); ok {
		if len(deltaCandidates) > 0 {
			// Same gating as the tela-cache delta above.
			logger.Infof("Classify seed delta probe: %d new SCIDs since seed height", len(deltaCandidates))
			go func() {
				idx.probeTELA(deltaCandidates, chainHeight, true, nil)
				structures.TELAProbeSettled.Store(true)
			}()
		} else {
			logger.Info("Classify seed delta probe: no new SCIDs since seed cache")
			settleTELADiscovery("seed cache", 0)
		}
	} else {
		// Cache missing, old-version, or corrupt — full probe. This is the
		// self-heal path that overwrites historical malformed scvars blobs
		// with cleanly-encoded bytes from the current marshaler.
		if trueCacheMiss {
			logger.Info("TELA cache miss: running full probe")
		} else {
			logger.Infof("TELA cache reject: v%d != v%d or scvars sanity failed — running full probe to heal",
				cached.Version, telaCacheVersion)
		}
		go idx.probeTELA(candidates, chainHeight, false, seedCtx)
	}
}

// probeTELA discovers TELA apps using batch code probing, then fetches their variables.
// Phase 1: Batch GetSC(code=true) across 8 connections to find SCIDs with telaVersion/docVersion
// Phase 2: Batch GetSC(variables=true) for only the matched TELA SCIDs to get metadata
func (idx *Indexer) probeTELA(candidates []*registryEntry, chainHeight int64, allowEarlyExit bool, seedCtx *classifySeedProbeContext) {
	// A FULL probe settles when it returns — after the cache/seed saves in
	// the tail. Gating delta probes are settled by their launch-site wrapper.
	if !allowEarlyExit {
		defer structures.TELAProbeSettled.Store(true)
	}
	start := time.Now()
	probeBatchSize := normalizeClassifyProbeBatchSize(idx.ClassifyProbeBatchSize)
	var probed atomic.Int64
	total := int64(len(candidates))
	var phase1SpecNanos atomic.Int64
	var phase1RPCNanos atomic.Int64
	var phase1DecodeNanos atomic.Int64
	var phase1ScanNanos atomic.Int64
	var phase1Batches atomic.Int64
	var phase1RPCErrors atomic.Int64
	var phase1DecodeErrors atomic.Int64
	var phase1CodeBytes atomic.Int64
	var phase2SpecNanos atomic.Int64
	var phase2RPCNanos atomic.Int64
	var phase2DecodeNanos atomic.Int64
	var phase2ParseNanos atomic.Int64
	var phase2ClassifyNanos atomic.Int64
	var phase2Batches atomic.Int64
	var phase2RPCErrors atomic.Int64
	var phase2DecodeErrors atomic.Int64
	var phase2Vars atomic.Int64
	var codeFlushTime time.Duration
	var varFlushTime time.Duration
	var cacheSaveTime time.Duration

	// Sort candidates by height descending -- newer SCIDs first
	// TELA apps are more likely among recent deployments
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].height > candidates[j].height
	})

	scids := make([]string, len(candidates))
	heightByScid := make(map[string]int64, len(candidates))
	for i, c := range candidates {
		scids[i] = c.scid
		heightByScid[c.scid] = c.height
	}

	// codeBatch accumulates install-time code for every SCID whose phase-1
	// probe matched a known class. Flushed between phase 1 and phase 2 so
	// phase-2's varBatch stays focused on variables + class metadata.
	codeBatch := storage.NewWriteBatch()
	var codeMu sync.Mutex

	// Phase 1: Batch code probe to find TELA SCIDs (split INDEX vs DOC) plus
	// any other classes classify.go knows about (NFA, G45-*, T345). We reuse
	// the same rules slice the scanner uses so classification stays
	// consistent across the fastsync path and the live-scan path.
	telaIndexSCIDs := make([]string, 0, 256)
	telaDocSCIDs := make([]string, 0, 768)
	otherClassSCIDs := make(map[string][]string)
	var telaMu sync.Mutex
	work := make(chan []string, 16)

	var sinceLastFind atomic.Int64
	// probeComplete is the LOCAL early-exit signal between this probe's
	// workers ("found enough, stop probing") — unrelated to the global
	// structures.TELAProbeSettled exit gate set when the whole probe returns.
	var probeComplete atomic.Bool

	// Phase 2 runs CONCURRENTLY with phase 1 (scanLoop-style staged pipeline,
	// indexer.go:343): phase-1 workers stream matched INDEX/DOC SCIDs onto
	// varWork in varBatchSize chunks as they classify, instead of parking all
	// matches behind the phase-1 barrier — folding most of phase-2's variable
	// fetches under phase-1's much longer code-probe fan-out. The shared RPC
	// pool serializes wire work; the win is wall-clock overlap of the tail.
	const varBatchSize = 25 // variables are larger than code
	varBatch := storage.NewWriteBatch()
	var varMu sync.Mutex
	type varBatchItem struct {
		scids []string
		class string
	}
	varWork := make(chan varBatchItem, 16)
	// pendingIndex/pendingDoc accumulate matches under telaMu until a full
	// varBatchSize chunk is ready; the send happens OUTSIDE the lock.
	pendingIndex := make([]string, 0, varBatchSize)
	pendingDoc := make([]string, 0, varBatchSize)

	var wg sync.WaitGroup
	poolSize := idx.RPCPool.Size()

	var wg2 sync.WaitGroup
	for i := 0; i < poolSize; i++ {
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			for item := range varWork {
				if idx.Closing.Load() {
					// Drain, don't return — see the phase-1 worker comment.
					continue
				}
				var lastErr error
				for attempt := 1; attempt <= 2; attempt++ {
					lastErr = idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
						tSpec := time.Now()
						specs := make([]jrpc2.Spec, len(item.scids))
						for i, scid := range item.scids {
							specs[i] = jrpc2.Spec{
								Method: "DERO.GetSC",
								Params: rpc.GetSC_Params{SCID: scid, Variables: true},
							}
						}
						phase2SpecNanos.Add(time.Since(tSpec).Nanoseconds())
						ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
						defer cancel()

						tRPC := time.Now()
						results, err := c.RPC.Batch(ctx, specs)
						phase2RPCNanos.Add(time.Since(tRPC).Nanoseconds())
						phase2Batches.Add(1)
						if err != nil {
							return err
						}

						for i, resp := range results {
							var r rpc.GetSC_Result
							tDecode := time.Now()
							if err := resp.UnmarshalResult(&r); err != nil {
								phase2DecodeNanos.Add(time.Since(tDecode).Nanoseconds())
								phase2DecodeErrors.Add(1)
								continue
							}
							phase2DecodeNanos.Add(time.Since(tDecode).Nanoseconds())
							tParse := time.Now()
							vars := parseSCVariables(&r)
							phase2ParseNanos.Add(time.Since(tParse).Nanoseconds())
							phase2Vars.Add(int64(len(vars)))
							tClassify := time.Now()
							// Single-pass: the class was proven by the phase-1 code
							// probe, so seed it and let extractClassVars pull DURL/
							// Version/DocType/Mods in the same walk (audit #8).
							sc := ClassifySCVarsWithClass(item.scids[i], item.class, vars)
							meta := classMetaFrom(&sc, chainHeight, chainHeight)
							phase2ClassifyNanos.Add(time.Since(tClassify).Nanoseconds())

							varMu.Lock()
							if len(vars) > 0 {
								varBatch.AddVariables(item.scids[i], chainHeight, vars)
							}
							varBatch.AddClass(item.scids[i], meta)
							varMu.Unlock()
						}
						return nil
					})
					if lastErr == nil || idx.Closing.Load() {
						break
					}
					time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
				}
				if lastErr != nil && !idx.Closing.Load() {
					phase2RPCErrors.Add(1)
					logger.Warnf("Classify phase 2 batch failed after retry (%s, %d SCIDs): %v", item.class, len(item.scids), lastErr)
				}
			}
		}()
	}

	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range work {
				if idx.Closing.Load() || probeComplete.Load() {
					// Keep draining the channel instead of returning: a worker
					// that returns early leaves the feeder blocked on a full
					// channel forever, leaking the goroutine and both pooled
					// WriteBatches. `continue` skips the RPC work but lets the
					// feeder finish and close(work) unblock everyone.
					continue
				}
				var lastErr error
				for attempt := 1; attempt <= 2; attempt++ {
					lastErr = idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
						tSpec := time.Now()
						specs := make([]jrpc2.Spec, len(batch))
						for i, scid := range batch {
							specs[i] = jrpc2.Spec{
								Method: "DERO.GetSC",
								Params: rpc.GetSC_Params{SCID: scid, Code: true},
							}
						}
						phase1SpecNanos.Add(time.Since(tSpec).Nanoseconds())
						ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
						defer cancel()

						tRPC := time.Now()
						results, err := c.RPC.Batch(ctx, specs)
						phase1RPCNanos.Add(time.Since(tRPC).Nanoseconds())
						phase1Batches.Add(1)
						if err != nil {
							return err
						}

						for i, resp := range results {
							var r rpc.GetSC_Result
							tDecode := time.Now()
							if err := resp.UnmarshalResult(&r); err != nil {
								phase1DecodeNanos.Add(time.Since(tDecode).Nanoseconds())
								phase1DecodeErrors.Add(1)
								continue
							}
							phase1DecodeNanos.Add(time.Since(tDecode).Nanoseconds())
							if r.Code == "" {
								continue
							}
							phase1CodeBytes.Add(int64(len(r.Code)))
							// Walk classify.go's rule list in order — first match wins.
							// More specific patterns come first (G45-FAT before G45-C, etc.).
							tScan := time.Now()
							var matched string
							for _, rule := range rules {
								if strings.Contains(r.Code, rule.pattern) {
									matched = rule.class
									break
								}
							}
							phase1ScanNanos.Add(time.Since(tScan).Nanoseconds())
							if matched == "" {
								continue
							}
							sinceLastFind.Store(0)
							scidStr := batch[i]
							var emit varBatchItem
							telaMu.Lock()
							switch matched {
							case "TELA-INDEX-1":
								telaIndexSCIDs = append(telaIndexSCIDs, scidStr)
								pendingIndex = append(pendingIndex, scidStr)
								if len(pendingIndex) >= varBatchSize {
									emit = varBatchItem{scids: pendingIndex, class: "TELA-INDEX-1"}
									pendingIndex = make([]string, 0, varBatchSize)
								}
							case "TELA-DOC-1":
								telaDocSCIDs = append(telaDocSCIDs, scidStr)
								pendingDoc = append(pendingDoc, scidStr)
								if len(pendingDoc) >= varBatchSize {
									emit = varBatchItem{scids: pendingDoc, class: "TELA-DOC-1"}
									pendingDoc = make([]string, 0, varBatchSize)
								}
							default:
								otherClassSCIDs[matched] = append(otherClassSCIDs[matched], scidStr)
							}
							// Persist the install-time code per the --persist-install-code
							// policy (the legacy --skip-tela-doc-code flag was removed;
							// shouldPersistCode/CodePolicy is the single source of truth).
							persistCode := idx.shouldPersistCode(matched)
							telaMu.Unlock()
							// Stream the full chunk to the concurrent phase-2 pool —
							// send OUTSIDE telaMu (a full varWork must never block a
							// lock phase-2's own workers never take, but ordering
							// discipline keeps the lock small regardless).
							if emit.scids != nil && !idx.Closing.Load() {
								varWork <- emit
							}
							if persistCode {
								codeMu.Lock()
								codeBatch.AddSCCode(scidStr, heightByScid[scidStr], r.Code)
								codeMu.Unlock()
							}
							telaMu.Lock()
							found := len(telaIndexSCIDs) + len(telaDocSCIDs)
							for _, v := range otherClassSCIDs {
								found += len(v)
							}
							telaMu.Unlock()
							if found%200 == 0 {
								logger.Infof("Classify milestone: %d classified SCIDs discovered so far", found)
							}
						}

						count := probed.Add(int64(len(batch)))
						sinceLastFind.Add(int64(len(batch)))
						if count%5000 == 0 || count >= total {
							telaMu.Lock()
							found := len(telaIndexSCIDs) + len(telaDocSCIDs)
							for _, v := range otherClassSCIDs {
								found += len(v)
							}
							telaMu.Unlock()
							logger.Infof("Classify probe: %d/%d checked, %d classified (%.0f%%)",
								count, total, found, float64(count)/float64(total)*100)
						}

						// Early exit is only safe for cache-backed delta probes.
						if allowEarlyExit && sinceLastFind.Load() > 3000 {
							telaMu.Lock()
							found := len(telaIndexSCIDs) + len(telaDocSCIDs)
							for _, v := range otherClassSCIDs {
								found += len(v)
							}
							telaMu.Unlock()
							if found > 0 {
								probeComplete.Store(true)
								return nil
							}
						}
						return nil
					})
					if lastErr == nil || idx.Closing.Load() || probeComplete.Load() {
						break
					}
					time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
				}
				if lastErr != nil && !idx.Closing.Load() && !probeComplete.Load() {
					phase1RPCErrors.Add(1)
					logger.Warnf("Classify phase 1 batch failed after retry (%d SCIDs): %v", len(batch), lastErr)
				}
			}
		}()
	}

	for i := 0; i < len(scids); i += probeBatchSize {
		// Stop feeding on shutdown too — workers only drain past this point.
		if idx.Closing.Load() || probeComplete.Load() {
			break
		}
		end := min(i+probeBatchSize, len(scids))
		work <- scids[i:end]
	}
	close(work)
	wg.Wait()

	// Roll up class counts for logging
	totalClassified := len(telaIndexSCIDs) + len(telaDocSCIDs)
	for _, v := range otherClassSCIDs {
		totalClassified += len(v)
	}

	if probeComplete.Load() {
		checked := probed.Load()
		logger.Infof("Classify probe: early exit at %d/%d SCIDs — found %d total", checked, total, totalClassified)
	}

	phase1Time := time.Since(start)
	logger.Infof("Classify probe phase 1: %d INDEX + %d DOC + %d other classes = %d total in %s",
		len(telaIndexSCIDs), len(telaDocSCIDs), totalClassified-len(telaIndexSCIDs)-len(telaDocSCIDs),
		totalClassified, phase1Time.Round(time.Millisecond))
	telaCount := len(telaIndexSCIDs) + len(telaDocSCIDs) // kept for cache compatibility

	if totalClassified == 0 {
		logger.Infof("Classify probe timings: phase1_spec=%s phase1_rpc=%s phase1_decode=%s phase1_scan=%s phase1_batches=%d phase1_batch_size=%d phase1_rpc_errors=%d phase1_decode_errors=%d code_bytes=%d early_exit_allowed=%v",
			time.Duration(phase1SpecNanos.Load()).Round(time.Millisecond),
			time.Duration(phase1RPCNanos.Load()).Round(time.Millisecond),
			time.Duration(phase1DecodeNanos.Load()).Round(time.Millisecond),
			time.Duration(phase1ScanNanos.Load()).Round(time.Millisecond),
			phase1Batches.Load(), probeBatchSize, phase1RPCErrors.Load(), phase1DecodeErrors.Load(),
			phase1CodeBytes.Load(), allowEarlyExit)
		// Readiness marker: cmd/benchvs --ready-log-pattern waits for this
		// phrase, so it must fire even when a (delta) probe classifies nothing.
		logger.Infof("Classify probe complete: 0 INDEX + 0 DOC + 0 other | Phase1: %s Phase2: 0s Total: %s",
			phase1Time.Round(time.Millisecond), time.Since(start).Round(time.Millisecond))
		close(varWork)
		wg2.Wait()
		storage.PutWriteBatch(codeBatch)
		storage.PutWriteBatch(varBatch)
		return
	}

	// Flush phase-1 code snapshots before phase-2 starts so a crash between
	// phases still leaves the DB with authoritative install code for any
	// SCID we successfully classified.
	if len(codeBatch.SCCodes) > 0 {
		codeFlushStart := time.Now()
		if err := idx.Store.FlushBatch(codeBatch); err != nil {
			logger.Errorf("FastSync phase-1 code flush: %v", err)
		}
		codeFlushTime = time.Since(codeFlushStart)
	}

	// Phase-2 tail: most INDEX/DOC variable fetches already streamed to the
	// concurrent phase-2 pool during phase 1; feed the partial chunks that
	// never filled to varBatchSize, then drain. HOLOGRAM's GetMyDOCs /
	// SearchByKey("docVersion") / GetAllDOCTypes / GetMyINDEXes all read
	// scvars for TELA-DOC-1 SCIDs, so their variables must be persisted here.
	// phase2Start therefore measures only the UN-OVERLAPPED tail.
	phase2Start := time.Now()
	telaMu.Lock()
	remIndex, remDoc := pendingIndex, pendingDoc
	pendingIndex, pendingDoc = nil, nil
	telaMu.Unlock()
	if len(remIndex) > 0 && !idx.Closing.Load() {
		varWork <- varBatchItem{scids: remIndex, class: "TELA-INDEX-1"}
	}
	if len(remDoc) > 0 && !idx.Closing.Load() {
		varWork <- varBatchItem{scids: remDoc, class: "TELA-DOC-1"}
	}
	// Non-TELA classes: skip the variable fetch. HOLOGRAM's Browser renders
	// them from ClassMeta alone (class name + optional Name/Desc/Icon from
	// later on-invoke ClassifySC calls). Writing class meta directly here
	// keeps the class bucket populated without burning RPC on ~50k GetSC
	// variables-true calls that nobody currently reads.
	for class, scids := range otherClassSCIDs {
		meta := &structures.ClassMeta{
			Class:         class,
			Tags:          tagsForClass(class),
			InstallHeight: chainHeight,
			LastHeight:    chainHeight,
		}
		for _, scid := range scids {
			// Populate-only, never clobber. This meta is BARE — class + tags,
			// no Name/Desc/media — and fastsync runs on every startup. Blind
			// writes here silently wiped every G45 record's metadata on
			// restart, undoing a --postscan-vars=all sweep or an operator's
			// RefreshClassVars: caught live 2026-07-27 when a restarted node
			// served an asset catalog whose every media field had reverted to
			// empty. A record that already exists is always at least as good
			// as this placeholder.
			if existing, err := idx.Store.GetSCIDClass(scid); err == nil && existing != nil {
				continue
			}
			varMu.Lock()
			varBatch.AddClass(scid, meta)
			varMu.Unlock()
		}
	}
	close(varWork)
	wg2.Wait()

	// Flush classified variables + class metadata to DB
	varFlushOK := true
	varFlushStart := time.Now()
	if err := idx.Store.FlushBatch(varBatch); err != nil {
		logger.Errorf("Classify flush: %v", err)
		varFlushOK = false
	}
	varFlushTime = time.Since(varFlushStart)

	phase2Time := time.Since(phase2Start)
	totalTime := time.Since(start)
	otherCount := 0
	for _, v := range otherClassSCIDs {
		otherCount += len(v)
	}
	logger.Infof("Classify probe complete: %d INDEX + %d DOC + %d other | Phase1: %s Phase2: %s Total: %s",
		len(telaIndexSCIDs), len(telaDocSCIDs), otherCount, phase1Time.Round(time.Millisecond),
		phase2Time.Round(time.Millisecond), totalTime.Round(time.Millisecond))
	logger.Infof("Classify probe timings: phase1_spec=%s phase1_rpc=%s phase1_decode=%s phase1_scan=%s code_flush=%s phase2_spec=%s phase2_rpc=%s phase2_decode=%s phase2_parse=%s phase2_classify=%s var_flush=%s phase1_batches=%d phase1_batch_size=%d phase2_batches=%d rpc_errors=%d/%d decode_errors=%d/%d code_bytes=%d vars=%d early_exit_allowed=%v",
		time.Duration(phase1SpecNanos.Load()).Round(time.Millisecond),
		time.Duration(phase1RPCNanos.Load()).Round(time.Millisecond),
		time.Duration(phase1DecodeNanos.Load()).Round(time.Millisecond),
		time.Duration(phase1ScanNanos.Load()).Round(time.Millisecond),
		codeFlushTime.Round(time.Millisecond),
		time.Duration(phase2SpecNanos.Load()).Round(time.Millisecond),
		time.Duration(phase2RPCNanos.Load()).Round(time.Millisecond),
		time.Duration(phase2DecodeNanos.Load()).Round(time.Millisecond),
		time.Duration(phase2ParseNanos.Load()).Round(time.Millisecond),
		time.Duration(phase2ClassifyNanos.Load()).Round(time.Millisecond),
		varFlushTime.Round(time.Millisecond),
		phase1Batches.Load(), probeBatchSize, phase2Batches.Load(),
		phase1RPCErrors.Load(), phase2RPCErrors.Load(),
		phase1DecodeErrors.Load(), phase2DecodeErrors.Load(),
		phase1CodeBytes.Load(), phase2Vars.Load(), allowEarlyExit)

	if allowEarlyExit {
		structures.TELACount.Add(int64(telaCount))
	} else {
		structures.TELACount.Store(int64(telaCount))
	}

	// Save cache for instant subsequent startups (Jofito: never repeat work).
	//
	// Gated on a clean probe: the drain-on-shutdown rework means the tail now
	// RUNS on Ctrl-C (workers drain instead of leaking the feeder), so without
	// this gate a partially-covered probe would persist a cache claiming
	// completeness at chainHeight and the skipped SCIDs would never be
	// re-probed. Same hazard for RPC-failed batches (whole batches silently
	// dropped). Decode errors are tolerated here — unlike the cross-DB seed,
	// the local cache self-heals on version bumps and scvars sanity checks.
	cacheSaveStart := time.Now()
	localCacheClean := !idx.Closing.Load() && phase1RPCErrors.Load() == 0 && phase2RPCErrors.Load() == 0
	if !allowEarlyExit && localCacheClean {
		if err := saveTELACache(idx.DBDir, telaIndexSCIDs, telaDocSCIDs, otherClassSCIDs, chainHeight); err != nil {
			logger.Errorf("Classify cache save: %v", err)
		} else {
			cacheSaveTime = time.Since(cacheSaveStart)
			logger.Infof("Classify cache v%d saved: %d INDEX + %d DOC + %d other classes at height %d",
				telaCacheVersion, len(telaIndexSCIDs), len(telaDocSCIDs), otherCount, chainHeight)
		}
		if cacheSaveTime > 0 {
			logger.Infof("Classify cache save timing: cache_save=%s", cacheSaveTime.Round(time.Millisecond))
		}
	} else if !allowEarlyExit {
		logger.Warnf("Classify cache save skipped: degraded probe (closing=%v rpc_errs=%d/%d)",
			idx.Closing.Load(), phase1RPCErrors.Load(), phase2RPCErrors.Load())
	} else {
		logger.Info("Classify cache save skipped for delta probe")
	}
	saveSeed := seedCtx != nil && shouldSaveSeedCache(
		idx.Closing.Load(),
		phase1RPCErrors.Load(), phase2RPCErrors.Load(),
		phase1DecodeErrors.Load(), phase2DecodeErrors.Load(),
		allowEarlyExit, varFlushOK, seedCtx.RegistryHash,
	)
	if seedCtx != nil && !allowEarlyExit && !saveSeed {
		logger.Warnf("Classify seed cache save skipped: degraded probe (closing=%v rpc_errs=%d/%d decode_errs=%d/%d var_flush_ok=%v registry_hash_set=%v)",
			idx.Closing.Load(), phase1RPCErrors.Load(), phase2RPCErrors.Load(),
			phase1DecodeErrors.Load(), phase2DecodeErrors.Load(), varFlushOK, seedCtx.RegistryHash != "")
	}
	if saveSeed {
		seedCache := newClassifySeedCacheFromProbe(
			seedCtx.Network,
			seedCtx.GnomonSCID,
			seedCtx.RegistryHash,
			chainHeight,
			telaIndexSCIDs,
			telaDocSCIDs,
			otherClassSCIDs,
			codeBatch,
			varBatch,
		)
		seedSaveStart := time.Now()
		if err := saveClassifySeedCache(classifySeedCacheDir(idx), seedCache); err != nil {
			logger.Errorf("Classify seed cache save: %v", err)
		} else {
			logger.Infof("Classify seed cache v%d saved: %d class records, %d var snapshots, %d code snapshots in %s",
				classifySeedCacheVersion, len(seedCache.Classes), len(seedCache.Variables), len(seedCache.Codes),
				time.Since(seedSaveStart).Round(time.Millisecond))
		}
	}
	storage.PutWriteBatch(varBatch)
	storage.PutWriteBatch(codeBatch)
}

// shouldSaveSeedCache is the probe-tail gate deciding whether a finished
// classify probe is complete enough to persist as a cross-DB seed cache.
// The seed's registry-hash match asserts COMPLETENESS for that registry
// state: any machine that loads it will skip the full probe entirely. So a
// partial/degraded probe must never be saved — it would permanently mask
// the dropped SCIDs everywhere the seed is reused.
//
// Disqualifiers:
//   - closing: shutdown mid-probe; workers drained without doing RPC work.
//   - rpcErrs1/rpcErrs2: a phase-1/2 batch failed even after retry — every
//     SCID in that batch was silently dropped from classification.
//   - decodeErrs1/decodeErrs2: included deliberately. A failed
//     UnmarshalResult drops that SCID just as surely as a failed batch RPC;
//     the local TELA cache tolerates this (it self-heals on version bumps),
//     but the seed cache's completeness claim cannot. Conservative > fast.
//   - allowEarlyExit: delta probes stop scanning once finds dry up, so
//     their coverage is intentionally partial.
//   - !varFlushOK: the local DB never committed what the seed would claim.
//   - registryHash == "": no completeness anchor to verify against on load.
func shouldSaveSeedCache(closing bool, rpcErrs1, rpcErrs2, decodeErrs1, decodeErrs2 int64, allowEarlyExit, varFlushOK bool, registryHash string) bool {
	if closing || allowEarlyExit || !varFlushOK || registryHash == "" {
		return false
	}
	return rpcErrs1 == 0 && rpcErrs2 == 0 && decodeErrs1 == 0 && decodeErrs2 == 0
}

// telaCache stores discovered SCIDs for instant subsequent startups.
// The Jofito endgame: never probe what you already know.
//
// v1 shape had only IndexSCIDs/DocSCIDs. v2 adds Classes for the
// extended classify probe (NFA, G45-*). When loadTELACache decodes a v1
// file, Classes comes back nil — callers treat nil as "no cache" so a
// one-shot re-probe picks up the new classes and the on-disk cache
// upgrades itself naturally.
type telaCache struct {
	Version    int                 `msgpack:"v,omitempty"`
	IndexSCIDs []string            `msgpack:"index"`
	DocSCIDs   []string            `msgpack:"doc"`
	Classes    map[string][]string `msgpack:"classes,omitempty"`
	Height     int64               `msgpack:"height"`
	Timestamp  int64               `msgpack:"ts"`
}

// telaCacheVersion is the current on-disk cache version. Bump when the
// scvars/classify pipeline changes in a way that older snapshots can't
// serve — a mismatch forces a one-time full re-probe on next launch,
// which self-heals any stale or corrupted state from prior builds.
//
// v3 (2026-04-20): paired with the 60s TELA refresher — old v2 caches
// were written before the refresher existed, so their rating data is
// frozen at probe time. Strict equality (not >=) invalidates them.
const telaCacheVersion = 3

func telaCachePath(dbDir string) string {
	return filepath.Join(dbDir, "tela_cache.bin")
}

// telaCacheMu serializes concurrent reads/writes of tela_cache.bin.
// Writers: probeTELA (full refresh), monitorChainHeight (height bump),
// fastsync-cache-hit (no-op). A plain sync.Mutex is enough — the file is tiny
// and contention is low.
var telaCacheMu sync.Mutex

func loadTELACache(dbDir string) (*telaCache, error) {
	telaCacheMu.Lock()
	defer telaCacheMu.Unlock()
	data, err := os.ReadFile(telaCachePath(dbDir))
	if err != nil {
		return nil, err
	}
	var cache telaCache
	if err := msgpack.Unmarshal(data, &cache); err != nil {
		return nil, err
	}
	return &cache, nil
}

func saveTELACache(dbDir string, indexSCIDs, docSCIDs []string, otherClasses map[string][]string, height int64) error {
	telaCacheMu.Lock()
	defer telaCacheMu.Unlock()
	cache := telaCache{
		Version:    telaCacheVersion,
		IndexSCIDs: indexSCIDs,
		DocSCIDs:   docSCIDs,
		Classes:    otherClasses,
		Height:     height,
		Timestamp:  time.Now().Unix(),
	}
	data, err := msgpack.Marshal(&cache)
	if err != nil {
		return err
	}
	path := telaCachePath(dbDir)
	tmp := path + ".tmp"
	// 0600 matches BoltDB permissions — cache contains indexer-derived data only,
	// but tighter is fine. Write a temp file first so a crash does not leave
	// a truncated cache at the canonical path.
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		// Windows does not replace existing files with Rename. Remove+rename is
		// not fully atomic, but still avoids serving partially-written bytes.
		_ = os.Remove(path)
		if err2 := os.Rename(tmp, path); err2 != nil {
			_ = os.Remove(tmp)
			return err
		}
	}
	return nil
}

// TELACachedSCIDs returns the TELA INDEX + DOC SCIDs recorded in the on-disk
// tela_cache.bin (nil when missing/unreadable). This is the same cache the
// FastSync cache-fresh path uses; exposing it lets the compat shim seed the
// civilware telacandidates bucket after a probe settles, so consumers like
// Engram can take the pre-computed-candidates fast path instead of running
// their own multi-thousand-SCID prefilter on the first click.
func (idx *Indexer) TELACachedSCIDs() []string {
	if idx == nil {
		return nil
	}
	cached, err := loadTELACache(idx.DBDir)
	if err != nil || cached == nil {
		return nil
	}
	out := make([]string, 0, len(cached.IndexSCIDs)+len(cached.DocSCIDs))
	out = append(out, cached.IndexSCIDs...)
	out = append(out, cached.DocSCIDs...)
	return out
}

// TELAIndexSCIDs returns only the TELA INDEX (app) SCIDs recorded in the
// on-disk tela_cache.bin. INDEX contracts are the actual TELA apps users see
// in Engram; DOC contracts are each app's documentation and must not be
// surfaced as app candidates. Separate accessors let the compat shim seed the
// civilware telacandidates bucket with exactly the right classification.
func (idx *Indexer) TELAIndexSCIDs() []string {
	if idx == nil {
		return nil
	}
	cached, err := loadTELACache(idx.DBDir)
	if err != nil || cached == nil {
		return nil
	}
	return cached.IndexSCIDs
}

// TELADocSCIDs returns only the TELA DOC (documentation) SCIDs recorded in the
// on-disk tela_cache.bin. These are supporting contracts, not apps; the compat
// shim records them with a "doc" status so they never surface as candidates.
func (idx *Indexer) TELADocSCIDs() []string {
	if idx == nil {
		return nil
	}
	cached, err := loadTELACache(idx.DBDir)
	if err != nil || cached == nil {
		return nil
	}
	return cached.DocSCIDs
}

// cachedScvarsLookReadable samples cached TELA INDEX SCIDs and checks
// whether their variable snapshots decode AND contain the shape we
// expect for a TELA INDEX (at least one "dero1q…" rating-address key).
// Returns true only if the majority of the sample looks healthy.
//
// A TELA INDEX always initializes with STORE("likes",0)+STORE("dislikes",0)
// plus STORE(addr, ...) entries as users rate — so a healthy scvars for
// this SCID must contain `likes`/`dislikes` plus typically one or more
// rating-address keys. Pre-fix HyperGnomon wrote garbage payload bytes
// that decode as "valid" structurally but lose address keys; this check
// catches that drift in a bounded O(5) probe.
func cachedScvarsLookReadable(idx *Indexer, cached *telaCache) bool {
	if idx == nil || cached == nil {
		return false
	}
	sample := cached.IndexSCIDs
	if len(sample) == 0 {
		// No TELA INDEXes cached at all — nothing to validate; trust cache.
		return true
	}
	if len(sample) > 5 {
		sample = sample[:5]
	}
	healthy := 0
	for _, scid := range sample {
		vars, err := idx.Store.GetSCIDVariableDetailsAtHeight(scid, cached.Height)
		if err != nil {
			logger.Debugf("cache sanity: scid %s decode err: %v", scid[:16], err)
			continue
		}
		if len(vars) == 0 {
			continue
		}
		// A healthy TELA INDEX snapshot always carries the likes/dislikes
		// counter keys — their presence means the blob decoded into the
		// real variable set, not a truncated/scrambled payload.
		seenLikes := false
		seenDislikes := false
		for _, v := range vars {
			switch k := v.Key.(type) {
			case string:
				if k == "likes" {
					seenLikes = true
				}
				if k == "dislikes" {
					seenDislikes = true
				}
			}
			if seenLikes && seenDislikes {
				break
			}
		}
		if seenLikes && seenDislikes {
			healthy++
		}
	}
	// ≥ 60% of the sample must look healthy to trust the cache.
	return healthy*10 >= len(sample)*6
}

// hexTable is a lookup table for hex character validation.
var hexTable = func() [256]bool {
	var t [256]bool
	for c := byte('0'); c <= '9'; c++ {
		t[c] = true
	}
	for c := byte('a'); c <= 'f'; c++ {
		t[c] = true
	}
	for c := byte('A'); c <= 'F'; c++ {
		t[c] = true
	}
	return t
}()

// isHexString returns true if s contains only hexadecimal characters.
func isHexString(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !hexTable[s[i]] {
			return false
		}
	}
	return true
}
