package indexer

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/deroproject/derohe/rpc"

	hgrpc "github.com/hypergnomon/hypergnomon/rpc"
	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

// telaRefreshInterval is the base cadence of the TELA variable refresher.
// 60s is a compromise between rating freshness in the Browser (users expect
// sub-minute feedback after they rate or update an app) and daemon load
// (~90 GetSC calls per minute is ~2/s, well inside a healthy node's
// headroom).
const telaRefreshInterval = 60 * time.Second

// telaRefreshMaxBackoff caps the refresher's exponential backoff when the
// node is overloaded or unreachable. Once a cycle fails more than
// refreshOverloadThreshold of its SCIDs we stop hammering the node and poll
// at most this often until it recovers.
const telaRefreshMaxBackoff = 10 * time.Minute

// telaRefreshBatch is the chunk size for the RPC batch call. Mirrors
// the phase-2 probe's batch size so we reuse the same per-connection
// characteristics the probe already tuned.
const telaRefreshBatch = 25

// telaRefreshBatchTimeout bounds each batch RPC call. A healthy node answers
// a 25-SCID batch in well under a second (observed ~800ms for the whole
// 114-app set), so 20s is generous headroom while keeping a hung node from
// pinning the refresher for a full minute.
const telaRefreshBatchTimeout = 20 * time.Second

// refreshOverloadThreshold: when the fraction of SCIDs that failed a refresh
// cycle exceeds this, the node is considered overloaded and the refresher
// backs off.
const refreshOverloadThreshold = 0.5

// classRefreshStats reports the outcome of one RefreshClassVars cycle:
// how many SCIDs were persisted, how many were attempted, and how the
// failures break down between per-item RPC errors and genuine decode
// errors. overloaded is true when the failure ratio exceeded
// refreshOverloadThreshold.
type classRefreshStats struct {
	persisted    int
	total        int
	scidErrors   int64
	decodeErrors int64
	overloaded   bool
}

// startTELARefresher launches a goroutine that periodically re-fetches
// variable snapshots for every SCID in the TELA-INDEX-1 class bucket.
// Rationale: HOLOGRAM defaults HG to TurboMode, which skips GetSC on
// scan invokes — so without a second pass the scvars bucket freezes
// at fastsync time and new ratings / state changes never land. This
// goroutine closes that staleness window without changing the scan's
// hot path.
//
// The tick interval is adaptive: a cycle with >refreshOverloadThreshold
// failures (overloaded node, RPC outage, etc.) doubles the interval up to
// telaRefreshMaxBackoff so we stop adding load to a struggling node, and a
// clean cycle resets to the base interval.
//
// Cheap to call — no-op if the class bucket is empty. Exits on
// idx.Closing.
func (idx *Indexer) startTELARefresher() {
	go func() {
		// Stagger the first tick so the probe that ran moments before
		// us doesn't contend with our refresh. The probe already wrote
		// fresh data — our first meaningful refresh can wait a minute.
		interval := telaRefreshInterval
		for {
			time.Sleep(interval)
			if idx.Closing.Load() {
				return
			}
			stats, err := idx.refreshTELAVarsStats()
			if err != nil {
				logger.Debugf("TELA refresh: %v", err)
			} else if stats.persisted > 0 {
				logger.Debugf("TELA refresh: refreshed %d apps", stats.persisted)
			}
			if stats.overloaded {
				interval *= 2
				if interval > telaRefreshMaxBackoff {
					interval = telaRefreshMaxBackoff
				}
				logger.Warnf("TELA refresh: node overloaded (%d/%d SCIDs failed), backing off to %s",
					stats.scidErrors+stats.decodeErrors, stats.total, interval)
			} else {
				interval = telaRefreshInterval
			}
		}
	}()
}

// RefreshTELAVars is a convenience shim that refreshes TELA-INDEX-1
// apps. Kept for the existing background loop and HOLOGRAM's
// RefreshBrowserApps Wails method.
func (idx *Indexer) RefreshTELAVars() (int, error) {
	stats, err := idx.refreshTELAVarsStats()
	return stats.persisted, err
}

// refreshTELAVarsStats is the internal stats variant of RefreshTELAVars,
// used by the background loop so it can detect node overload and back off.
func (idx *Indexer) refreshTELAVarsStats() (classRefreshStats, error) {
	return idx.refreshClassVarsStats("TELA-INDEX-1")
}

// RefreshClassVars fetches current variables for every SCID in the named
// class bucket and writes them to scvars at current ChainHeight. Returns
// the number of SCIDs whose vars were persisted.
//
// Rationale: only TELA INDEX/DOC get their vars fetched during fastsync
// phase 2 (by design — saves time on a cold start). Other classes like
// NFA and G45 stay in the class bucket with empty metadata until a
// consumer asks for them. This method is the on-demand var-hydration
// path for those classes, called once per (class, user-session-window)
// when the Browser's NFA / G45 chip is selected.
//
// Batches GetSC(Variables=true) across the RPC pool. For ~2,600 NFA
// contracts on mainnet this is 5-15s on a fresh daemon, then subsequent
// calls are cache hits until the chain advances.
//
// Safe to call concurrently with the scan loop — bolt's MVCC handles
// reader/writer isolation. Touches scvars + classIdx + the class bucket
// (updates ClassMeta.Name/Desc/Icon from the freshly-fetched vars).
func (idx *Indexer) RefreshClassVars(class string) (int, error) {
	stats, err := idx.refreshClassVarsStats(class)
	return stats.persisted, err
}

// refreshClassVarsStats is the internal implementation of RefreshClassVars.
// It returns detailed stats (persisted/total + error breakdown + overload
// flag) instead of just the persisted count, so callers like the TELA
// refresher loop can adapt to a struggling node. The public RefreshClassVars
// wraps it and keeps the historical (int, error) signature.
func (idx *Indexer) refreshClassVarsStats(class string) (classRefreshStats, error) {
	var stats classRefreshStats
	if idx == nil || idx.Store == nil || idx.RPCPool == nil {
		return stats, nil
	}
	if idx.Closing.Load() {
		return stats, nil
	}
	if class == "" {
		return stats, nil
	}

	installs, err := idx.Store.GetClassInstalls(class, 0)
	if err != nil {
		return stats, err
	}
	if len(installs) == 0 {
		return stats, nil
	}

	chainHeight := idx.ChainHeight.Load()
	if chainHeight == 0 {
		return stats, nil
	}

	scids := make([]string, 0, len(installs))
	for _, inst := range installs {
		scids = append(scids, inst.SCID)
	}
	stats.total = len(scids)

	start := time.Now()
	batch := storage.NewWriteBatch()
	var batchMu sync.Mutex
	persisted := 0
	var scidErrors atomic.Int64
	var decodeErrors atomic.Int64

	workChan := make(chan []string, 8)
	var wg sync.WaitGroup
	poolSize := idx.RPCPool.Size()
	if poolSize < 1 {
		poolSize = 1
	}
	if poolSize > 8 {
		poolSize = 8
	}

	// Fast-path keys: when the class has a known manifest and we're not due
	// for a full refresh, ask the daemon for only those keys (Variables=false
	// + KeysString). Daemon skips its cursor scan; response is O(|keys|).
	// Every fullRefreshEvery'th call falls back to full Variables=true so
	// new keys still get picked up.
	fastKeys := pickRefreshKeys(class)

	for i := 0; i < poolSize; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for chunk := range workChan {
				if idx.Closing.Load() {
					return
				}
				var lastErr error
				for attempt := 1; attempt <= 2; attempt++ {
					lastErr = idx.RPCPool.WithConn(func(c *hgrpc.Client) error {
						specs := make([]jrpc2.Spec, len(chunk))
						for i, scid := range chunk {
							specs[i] = jrpc2.Spec{
								Method: "DERO.GetSC",
								Params: buildGetSCParams(scid, fastKeys),
							}
						}
						ctx, cancel := context.WithTimeout(context.Background(), telaRefreshBatchTimeout)
						defer cancel()

						results, err := c.RPC.Batch(ctx, specs)
						if err != nil {
							return err
						}

						for i, resp := range results {
							if i >= len(chunk) {
								continue // defensive: never index past the chunk
							}
							// A per-item error response (daemon overloaded,
							// SCID gone, rate-limited, etc.) is an RPC-level
							// failure — NOT a decode failure. Check it first
							// so the counters tell the truth about what went
							// wrong instead of lumping everything into
							// decode_errors.
							if resp.Error() != nil {
								scidErrors.Add(1)
								continue
							}
							var r rpc.GetSC_Result
							if err := resp.UnmarshalResult(&r); err != nil {
								decodeErrors.Add(1)
								continue
							}
							var vars []*structures.SCIDVariable
							if fastKeys != nil {
								vars = scVarsFromKeyValues(fastKeys, r.ValuesString)
							} else {
								vars = parseSCVariables(&r)
							}
							if len(vars) == 0 {
								continue
							}
							// Single-pass: class comes from the class bucket we're
							// refreshing, so seed it and let extractClassVars pull
							// DURL/Version in the same walk (audit #8).
							sc := ClassifySCVarsWithClass(chunk[i], class, vars)
							meta := classMetaFrom(&sc, chainHeight, chainHeight)
							batchMu.Lock()
							batch.AddVariables(chunk[i], chainHeight, vars)
							batch.AddClass(chunk[i], meta)
							persisted++
							batchMu.Unlock()
						}
						return nil
					})
					if lastErr == nil || idx.Closing.Load() {
						break
					}
					time.Sleep(time.Duration(attempt) * 100 * time.Millisecond)
				}
				if lastErr != nil {
					// The whole chunk's transport failed on both attempts:
					// count every SCID in the chunk as failed.
					scidErrors.Add(int64(len(chunk)))
				}
			}
		}()
	}

	for i := 0; i < len(scids); i += telaRefreshBatch {
		if idx.Closing.Load() {
			break
		}
		end := min(i+telaRefreshBatch, len(scids))
		workChan <- scids[i:end]
	}
	close(workChan)
	wg.Wait()

	stats.persisted = persisted
	stats.scidErrors = scidErrors.Load()
	stats.decodeErrors = decodeErrors.Load()
	stats.overloaded = stats.total > 0 &&
		float64(stats.scidErrors+stats.decodeErrors)/float64(stats.total) > refreshOverloadThreshold

	if persisted > 0 {
		if err := idx.Store.FlushBatch(batch); err != nil {
			storage.PutWriteBatch(batch)
			return stats, err
		}
	}
	storage.PutWriteBatch(batch)

	mode := "full"
	if fastKeys != nil {
		mode = "fast"
	}
	logger.Infof("Class refresh %s (%s): %d/%d apps persisted at height %d in %s",
		class, mode, persisted, stats.total, chainHeight, time.Since(start).Round(time.Millisecond))
	if stats.scidErrors > 0 || stats.decodeErrors > 0 {
		logger.Warnf("Class refresh %s (%s): rpc_scid_errors=%d decode_errors=%d (overloaded=%v)",
			class, mode, stats.scidErrors, stats.decodeErrors, stats.overloaded)
	}
	return stats, nil
}
