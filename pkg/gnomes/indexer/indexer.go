// Package indexer is the civilware/Gnomon compat surface for the
// running indexer. Consumers who previously imported
// `github.com/civilware/Gnomon/indexer` can rewrite to
// `github.com/hypergnomon/hypergnomon/pkg/gnomes/indexer` and rebuild.
//
// HOLOGRAM's `gnomon.go:Start()` was the reference caller for this
// surface design (see Agent F teardown). If something HOLOGRAM calls
// isn't wired here, that's a v1.x gap — file an issue.
package indexer

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/creachadair/jrpc2"
	"github.com/deroproject/derohe/rpc"

	hgindexer "github.com/hypergnomon/hypergnomon/indexer"
	hgrpc "github.com/hypergnomon/hypergnomon/rpc"
	"github.com/hypergnomon/hypergnomon/structures"

	compatstorage "github.com/hypergnomon/hypergnomon/pkg/gnomes/storage"
	compatstructures "github.com/hypergnomon/hypergnomon/pkg/gnomes/structures"
)

// logger routes compat-indexer diagnostics through HyperGnomon's global
// logrus entry so output lands in the same log stream as the internal
// indexer.
var logger = structures.Logger.WithFields(nil)

// Indexer is the civilware-shape facade over HyperGnomon's internal
// indexer. Exported fields are read-only snapshots of the underlying
// state, updated on every scan cycle via the refresh goroutine. Call
// `StartDaemonMode(n)` to begin scanning; `Close()` for clean
// shutdown. Both match civilware's semantics.
type Indexer struct {
	// LastIndexedHeight is the height at which the last scan batch
	// flushed. Updated atomically from the internal indexer.
	// Civilware exposes this as a plain int64 field; we use atomic
	// stores so concurrent reads from the main goroutine are race-
	// free.
	LastIndexedHeight int64
	// ChainHeight tracks the daemon's reported topo_height. Updated
	// whenever the internal indexer polls GetInfo.
	ChainHeight int64
	// DBType is "boltdb" for the HyperGnomon compat path. Civilware
	// also has "gravdb" but that backend errors here — see
	// storage.ErrGravDBNotSupported.
	DBType string
	// GravDBBackend is always nil in the HyperGnomon facade because
	// we don't support graviton. Field declared so type-asserting
	// consumers compile. Use BBSBackend instead.
	GravDBBackend *compatstorage.GravitonStore
	// BBSBackend is the bbolt store wrapper. Always non-nil in a
	// successful NewIndexer call.
	BBSBackend *compatstorage.BboltStore

	// inner is the HyperGnomon indexer driving everything. Kept
	// unexported so the civilware surface is the only supported API.
	inner *hgindexer.Indexer

	// fieldRefreshStop signals the field-sync goroutine to stop. The
	// goroutine pushes LastIndexedHeight + ChainHeight from the
	// internal atomic fields out to this struct's exported fields
	// every ~100ms; without it the exported fields stay zero.
	fieldRefreshStop chan struct{}
	fieldRefreshWG   sync.WaitGroup

	// closed guards against double Close calls.
	closed atomic.Bool
	// closing is set by Close() to signal long-running compat loops
	// (e.g. BackfillTelaCandidates) to stop at their next checkpoint.
	closing atomic.Bool

	// Endpoint is the daemon RPC address this indexer talks to. Civilware
	// consumers (Engram's getGnomon) set it AFTER construction; it is used to
	// (re)open a live daemon connection on demand for the live-read methods
	// below. Accepts "host:port", "ws(s)://host:port" or "http(s)://host:port".
	Endpoint string
	// Status mirrors civilware's STATUS: empty while starting/syncing,
	// "indexed" once the scan loop has connected to the daemon. Consumers key
	// their status indicator off this (green == "indexed").
	Status string

	// liveMu guards a lazily-(re)opened daemon RPC connection used by the
	// live (variables == nil) GetSCIDKeysByValue / GetSCIDValuesByKey paths.
	// The connection is cached per-Endpoint and dropped on Close.
	liveMu       sync.Mutex
	liveClient   *hgrpc.Client
	liveEndpoint string

	// RPC is the live daemon connection, exported for civilware parity:
	// consumers read `gnomon.Index.RPC.RPC` (a *jrpc2.Client) as a fallback
	// RPC handle. Kept in sync with the liveRPC cache — a non-nil value
	// means a connection exists for idx.Endpoint.
	RPC *hgrpc.Client

	// forceFastSync records whether the civilware FastSyncConfig asked for a
	// registry-based fast sync (Enabled or ForceFastSync). When true,
	// StartDaemonMode runs HyperGnomon's FastSync() first — the contract
	// query that discovers every registered SCID + TELA candidate in seconds
	// instead of scanning 7.4M blocks one by one — then the block scan picks
	// up only the blocks after the fastsync height.
	forceFastSync bool
}

// NewIndexer constructs an Indexer using the civilware shape. The parameter
// list mirrors civilware/Gnomon's current indexer.NewIndexer EXACTLY (commit
// 0280ea286474, the feat-addscidtoindex-wsserver line HOLOGRAM v1.0.7 pins), so
// a consumer's call site compiles unchanged after the import rewrite:
//
//	indexer.NewIndexer(gravDB, boltDB, dbType, searchFilter, height, endpoint,
//	    "daemon", mbllookup, closeOnDisconnect, fsc, exclusions, storeIntegrators)
//
// HyperGnomon runs on bbolt regardless of dbType: bbolt is the default, and a
// non-bbolt dbType ("gravdb"/"sqlite"/…) is accepted but logged and falls back
// to the bbolt store the caller passes as bolt. The pre-opened bolt store is
// injected into the internal indexer (re-opening its path would lock-conflict).
// grav is vestigial; mbllookup/storeIntegrators/closeOnDisconnect/height are
// accepted for signature parity but not modeled. `runmode` other than "daemon"
// is rejected.
func NewIndexer(
	grav *compatstorage.GravitonStore, // civilware: *storage.GravitonStore (vestigial here)
	bolt *compatstorage.BboltStore, // civilware: *storage.BboltStore (the real backend)
	dbType string,
	searchFilter []string,
	height int64,
	endpoint string,
	runmode string,
	mbllookup bool,
	closeOnDisconnect bool,
	config *compatstructures.FastSyncConfig,
	exclusions []string,
	storeIntegrators bool,
) *Indexer {
	if runmode != "" && runmode != "daemon" {
		return newDeadIndexer(fmt.Errorf("runmode %q unsupported in HyperGnomon — only \"daemon\" is implemented", runmode))
	}
	// bbolt is the only real backend; a non-bbolt dbType warns and falls back.
	switch strings.ToLower(strings.TrimSpace(dbType)) {
	case "", "bolt", "boltdb", "bbs":
		// default bbolt — no warning
	default: // gravdb, graviton, sqlite, anything else
		structures.Logger.Warnf("[gnomes] dbType=%q not available in HyperGnomon; using bbolt", dbType)
	}
	// The caller's pre-opened bbolt store is the real backend.
	if bolt == nil {
		return newDeadIndexer(fmt.Errorf("boltDB is nil — pre-open one with storage.NewBBoltDB(path, name) and pass it in"))
	}
	if bolt.Inner() == nil {
		return newDeadIndexer(fmt.Errorf("boltDB has no open store"))
	}
	_ = grav              // vestigial — HyperGnomon always runs on bbolt
	_ = height            // civilware resume hint; HyperGnomon resumes from the store
	_ = mbllookup         // civilware miniblock-lookup knob; not modeled
	_ = closeOnDisconnect // civilware reconnect knob; not modeled
	_ = storeIntegrators  // civilware integrator-store knob; not modeled
	forceFS := false
	if config != nil {
		// Honor civilware's fastsync request: Enabled or ForceFastSync both
		// mean "discover from the GnomonSC registry instead of a full block
		// scan". This is the 30-60s path civilware users expect; without it
		// the indexer would block-scan 7.4M blocks (hours) before TELA has
		// any candidates. SkipFSRecheck/NoCode map onto HyperGnomon's turbo
		// mode (registry fill + probeTELA, no per-SCID code revalidation).
		forceFS = config.Enabled || config.ForceFastSync
	}

	// Derive the DB dir from the injected store so the internal indexer's
	// tela_cache.bin / seed caches land next to the main store (instant
	// cache hits on subsequent launches).
	dbDir := ""
	if bolt.Inner().Path != "" {
		dbDir = filepath.Dir(bolt.Inner().Path)
	}

	inner, err := hgindexer.New(hgindexer.Config{
		Endpoint:       strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://"),
		Store:          bolt.Inner(), // external-store injection: borrow the caller's store
		DBDir:          dbDir,        // tela_cache.bin + classify seed land here
		SearchFilter:   searchFilter, // civilware ships []string; pass through unchanged
		SCIDExclusions: exclusions,
		TurboMode:      true,
	})
	if err != nil {
		return newDeadIndexer(fmt.Errorf("hypergnomon indexer init: %w", err))
	}
	idx := wrapIndexerWithStore(inner, bolt)
	idx.forceFastSync = forceFS
	return idx
}

// AddSCIDToIndex injects specific SCIDs into the index immediately — civilware's
// manual-add path (used by HOLOGRAM to index a SCID that fastsync missed, e.g.
// one deployed before the indexer started). Mirrors civilware's signature; each
// entry is indexed via the internal IndexSingleSCID (the same path the native WS
// API's "addscidtoindex" method uses). varstoreonly re-reads only variables;
// skipfsrecheck reuses existing class metadata when present. The internal
// indexer re-extracts the owner, so FastSyncImport.Owner is advisory. Returns
// the first error encountered (others are attempted regardless).
func (idx *Indexer) AddSCIDToIndex(scids map[string]*compatstructures.FastSyncImport, skipfsrecheck, varstoreonly bool) error {
	if idx == nil || idx.inner == nil {
		return fmt.Errorf("indexer not initialized")
	}
	var firstErr error
	for scid := range scids {
		if _, err := idx.inner.IndexSingleSCID(scid, varstoreonly, skipfsrecheck); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("AddSCIDToIndex(%s): %w", scid, err)
		}
	}
	return firstErr
}

// NewIndexerWithDBDir is the HyperGnomon-native constructor that the
// compat shim currently points callers at. Takes a dbDir path and
// builds a full HyperGnomon indexer with civilware-shape facade on
// top. Until HyperGnomon's internal indexer.New accepts an external
// Storage implementation, this is the practical entry point.
func NewIndexerWithDBDir(
	dbDir, filter string,
	endpoint, runmode string,
	config *compatstructures.FastSyncConfig,
	exclusions []string,
) (*Indexer, error) {
	if runmode != "" && runmode != "daemon" {
		return nil, fmt.Errorf("runmode %q unsupported", runmode)
	}
	hgCfg := hgindexer.Config{
		Endpoint:       strings.TrimPrefix(strings.TrimPrefix(endpoint, "http://"), "https://"),
		DBDir:          dbDir,
		SearchFilter:   splitCivilwareFilter(filter),
		SCIDExclusions: exclusions,
		TurboMode:      true,
	}
	inner, err := hgindexer.New(hgCfg)
	if err != nil {
		return nil, err
	}
	idx := wrapIndexer(inner)
	// Mirror NewIndexer: honor the civilware FastSyncConfig so this
	// convenience constructor actually runs registry fastsync when asked,
	// instead of silently falling back to a multi-hour block scan.
	if config != nil && (config.Enabled || config.ForceFastSync) {
		idx.forceFastSync = true
	}
	return idx, nil
}

// wrapIndexer is the internal facade-builder. Exposed so tests can
// construct an Indexer around a pre-built internal instance. The inner
// indexer owns its store (NewIndexerWithDBDir path) so BBSBackend is nil.
func wrapIndexer(inner *hgindexer.Indexer) *Indexer {
	return wrapIndexerWithStore(inner, nil)
}

// wrapIndexerWithStore builds the facade and records the caller-owned bbolt
// store (the NewIndexer injection path). When bolt is non-nil the internal
// indexer borrows it (does not close it), so the facade's Close releases it.
func wrapIndexerWithStore(inner *hgindexer.Indexer, bolt *compatstorage.BboltStore) *Indexer {
	idx := &Indexer{
		DBType:           "boltdb",
		inner:            inner,
		fieldRefreshStop: make(chan struct{}),
		BBSBackend:       bolt,
	}
	if bolt != nil {
		// Wire GravDBBackend as a DELEGATING handle over the same bbolt store. A
		// civilware consumer (HOLOGRAM) defaults to dbType="gravdb" and reads
		// through GravDBBackend — both its dbType=="gravdb" dispatch branches and
		// its hardcoded GravDBBackend.GetSCIDInteractionHeight path — so a nil
		// GravDBBackend would make those reads silently return empty. Delegating
		// to bbolt makes every read path land on real data.
		idx.GravDBBackend = compatstorage.NewGravDBDelegate(bolt)
	}
	idx.startFieldRefresh()
	return idx
}

// splitCivilwareFilter converts civilware's semicolon-separated
// filter string into HyperGnomon's slice form. Civilware uses `;;;`
// for groups and `;` for intra-group terms — we treat the whole thing
// as a flat substring-match list since HyperGnomon's filter is a
// substring match across `;;;`-joined patterns too.
func splitCivilwareFilter(filter string) []string {
	if filter == "" {
		return nil
	}
	return strings.Split(filter, ";;;")
}

// StartDaemonMode begins scanning. parallelBlocks controls the
// fetcher fan-out. Civilware passes 1..10; HyperGnomon accepts up to
// its internal cap. Returns once the scan loop has signaled it
// reached tip or the context cancels (via Close).
func (idx *Indexer) StartDaemonMode(parallelBlocks int) {
	if idx == nil || idx.inner == nil {
		return
	}
	// HyperGnomon's StartDaemonMode blocks. Civilware consumers
	// (HOLOGRAM) launch it in a goroutine. Mirror that by spawning
	// here so callers don't have to change their threading model.
	go func() {
		// When the civilware config asked for fastsync, run the registry
		// contract query FIRST: it discovers every registered SCID and TELA
		// candidate in seconds (turbo mode persists the registry in one
		// batch and probes TELA in parallel). FastSync sets LastIndexedHeight
		// to chain tip, so the block scan that follows only tracks new
		// blocks — no multi-hour initial sync.
		if idx.forceFastSync && idx.inner != nil {
			if err := idx.inner.FastSync(false); err != nil {
				logger.Warnf("[gnomes] FastSync failed, falling back to block scan: %v", err)
			} else {
				// FastSync's turbo probe runs in the background. Once it
				// settles, copy its discovered TELA SCIDs into the compat
				// telacandidates bucket so GetTelaCandidates() returns them
				// on the FIRST click — consumers then skip their own
				// multi-thousand-SCID prefilter entirely.
				go idx.seedTelaCandidatesFromProbe()
			}
		}
		_ = idx.inner.StartDaemonMode()
	}()
	// Flip Status to "indexed" once the scan loop has a live daemon
	// connection (detected via the first successful GetInfo poll that
	// populates ChainHeight, or the first block actually indexed).
	go func() {
		for {
			if idx.Status == "indexed" {
				return
			}
			if idx.inner != nil && (idx.inner.ChainHeight.Load() > 0 || idx.inner.LastIndexedHeight.Load() > 0) {
				idx.Status = "indexed"
				return
			}
			sleepQuick()
		}
	}()
}

// liveRPC returns a cached daemon RPC client for idx.Endpoint, reopening it
// when the endpoint changed or the previous connection died. The caller must
// hold idx.liveMu. Returns nil when no Endpoint is configured.
func (idx *Indexer) liveRPC() *hgrpc.Client {
	if idx.Endpoint == "" {
		return nil
	}
	if idx.liveClient != nil && idx.liveEndpoint == idx.Endpoint {
		return idx.liveClient
	}
	if idx.liveClient != nil {
		idx.liveClient.Close()
		idx.liveClient = nil
		idx.RPC = nil
	}
	c, err := hgrpc.NewClient(idx.Endpoint)
	if err != nil {
		return nil
	}
	idx.liveClient = c
	idx.liveEndpoint = idx.Endpoint
	idx.RPC = c
	return c
}

// GetSCIDKeysByValue returns the keys of scid's variables whose value equals
// value, observed at topoheight. Two modes, matching civilware:
//
//   - variables non-empty: pure in-memory filter over the provided snapshot
//     (callers that already hold one avoid a network round-trip).
//   - variables nil: live daemon GetSC at topoheight, then the same filter.
//
// String-typed keys land in keysstring, uint64-typed keys in keysuint64.
func (idx *Indexer) GetSCIDKeysByValue(variables []*compatstructures.SCIDVariable, scid string, value interface{}, topoheight int64) (keysstring []string, keysuint64 []uint64, err error) {
	if len(variables) > 0 {
		for _, v := range variables {
			if v != nil && valuesEqual(v.Value, value) {
				switch k := v.Key.(type) {
				case string:
					keysstring = append(keysstring, k)
				case uint64:
					keysuint64 = append(keysuint64, k)
				}
			}
		}
		return
	}

	idx.liveMu.Lock()
	defer idx.liveMu.Unlock()
	client := idx.liveRPC()
	if client == nil {
		return nil, nil, fmt.Errorf("[Gnomon] GetSCIDKeysByValue: no daemon endpoint configured")
	}
	resp, err := client.GetSC(scid, topoheight, nil, nil, false)
	if err != nil {
		return nil, nil, fmt.Errorf("[Gnomon] GetSCIDKeysByValue: %w", err)
	}
	for k, v := range resp.VariableUint64Keys {
		if valuesEqual(v, value) {
			keysuint64 = append(keysuint64, k)
		}
	}
	for k, v := range resp.VariableStringKeys {
		if valuesEqual(v, value) {
			keysstring = append(keysstring, k)
		}
	}
	return
}

// GetSCIDValuesByKey returns the values of scid's variables whose key equals
// key, observed at topoheight. Same two modes as GetSCIDKeysByValue.
// String-typed values land in valuesstring, uint64-typed in valuesuint64.
func (idx *Indexer) GetSCIDValuesByKey(variables []*compatstructures.SCIDVariable, scid string, key interface{}, topoheight int64) (valuesstring []string, valuesuint64 []uint64, err error) {
	if len(variables) > 0 {
		for _, v := range variables {
			if v != nil && valuesEqual(v.Key, key) {
				switch val := v.Value.(type) {
				case string:
					valuesstring = append(valuesstring, val)
				case uint64:
					valuesuint64 = append(valuesuint64, val)
				}
			}
		}
		return
	}

	idx.liveMu.Lock()
	defer idx.liveMu.Unlock()
	client := idx.liveRPC()
	if client == nil {
		return nil, nil, fmt.Errorf("[Gnomon] GetSCIDValuesByKey: no daemon endpoint configured")
	}
	resp, err := client.GetSC(scid, topoheight, nil, nil, false)
	if err != nil {
		return nil, nil, fmt.Errorf("[Gnomon] GetSCIDValuesByKey: %w", err)
	}
	for k, v := range resp.VariableUint64Keys {
		if valuesEqual(k, key) {
			if n, ok := v.(uint64); ok {
				valuesuint64 = append(valuesuint64, n)
			}
		}
	}
	for k, v := range resp.VariableStringKeys {
		if valuesEqual(k, key) {
			if s, ok := v.(string); ok {
				valuesstring = append(valuesstring, s)
			}
		}
	}
	return
}

// valuesEqual compares a stored variable value against the caller's query
// value. Interface equality is used when types already agree; a string/uint64
// mismatch (e.g. JSON float64 5.0 vs uint64 5) is bridged numerically so
// callers don't lose results over typing.
func valuesEqual(stored, query interface{}) bool {
	if stored == nil || query == nil {
		return stored == query
	}
	if reflect.DeepEqual(stored, query) {
		return true
	}
	sf, sOk := toFloat64(stored)
	qf, qOk := toFloat64(query)
	return sOk && qOk && sf == qf
}

// toFloat64 coerces numeric-ish values to float64; ok=false for non-numerics.
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case uint64:
		return float64(n), true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	case float64:
		return n, true
	case string:
		f, err := strconv.ParseFloat(n, 64)
		if err != nil {
			return 0, false
		}
		return f, true
	default:
		return 0, false
	}
}

// GetTelaCandidates returns every SCID classified as a TELA app during
// AddSCIDToIndex, filtered to valid entries. Mirrors civilware's fork method
// of the same name (moralpriest/Gnomon indexer/indexer.go:3305).
func (idx *Indexer) GetTelaCandidates() []string {
	if idx == nil || idx.BBSBackend == nil {
		return nil
	}
	candidates := idx.BBSBackend.GetAllTelaCandidates()
	var result []string
	for scid, status := range candidates {
		if status == "valid_index" || status == "tela" {
			result = append(result, scid)
		}
	}
	return result
}

// BackfillTelaCandidates scans every known SCID for TELA markers using a pool
// of dedicated RPC connections, recording hits into the compat TELA-candidate
// cache. Mirrors civilware's fork method (moralpriest/Gnomon
// indexer/indexer.go:3328). `workers` controls parallelism (<=0 => 4).
// Intended to run in a background goroutine; safe to call repeatedly.
func (idx *Indexer) BackfillTelaCandidates(workers int) error {
	if idx == nil || idx.BBSBackend == nil {
		return fmt.Errorf("[BackfillTelaCandidates] indexer not ready")
	}
	if workers <= 0 {
		workers = 4
	}

	// Skip every SCID already classified — valid_index apps AND doc contracts
	// (and any other status). Only the unclassified remainder needs probing;
	// re-probing the 800+ doc contracts on every backfill is pure waste.
	allClassified := idx.BBSBackend.GetAllTelaCandidates()
	existingMap := make(map[string]bool, len(allClassified))
	for scid := range allClassified {
		existingMap[scid] = true
	}

	allSCIDs := idx.BBSBackend.GetAllOwnersAndSCIDs()
	if len(allSCIDs) == 0 {
		logger.Printf("[BackfillTelaCandidates] No SCIDs in storage, skipping\n")
		return nil
	}
	var scids []string
	for scid := range allSCIDs {
		if !existingMap[scid] {
			scids = append(scids, scid)
		}
	}
	if len(scids) == 0 {
		logger.Printf("[BackfillTelaCandidates] All %d SCIDs already known as TELA candidates, skipping\n", len(allSCIDs))
		return nil
	}
	sort.Strings(scids)

	logger.Printf("[BackfillTelaCandidates] Starting backfill for %d unknown SCIDs (already know %d) with %d workers\n", len(scids), len(allClassified), workers)

	endpoint := idx.Endpoint
	if endpoint == "" {
		endpoint = idx.innerEndpoint()
	}
	if endpoint == "" {
		return fmt.Errorf("[BackfillTelaCandidates] no daemon endpoint configured")
	}

	pool := make([]*hgrpc.Client, 0, workers)
	for i := 0; i < workers; i++ {
		c, err := hgrpc.NewClient(endpoint)
		if err != nil {
			continue
		}
		pool = append(pool, c)
	}
	if len(pool) == 0 {
		return fmt.Errorf("[BackfillTelaCandidates] failed to dial any RPC connection")
	}
	defer func() {
		for _, c := range pool {
			c.Close()
		}
	}()

	const batchSize = 500
	total := len(scids)
	type result struct {
		scid   string
		isTela bool
	}
	workCh := make(chan []string, len(pool)*2)
	resultCh := make(chan result, len(pool)*2)
	var wg sync.WaitGroup

	for _, client := range pool {
		wg.Add(1)
		go func(client *hgrpc.Client) {
			defer wg.Done()
			for batch := range workCh {
				if idx.closing.Load() {
					return
				}
				specs := make([]jrpc2.Spec, len(batch))
				for j, scid := range batch {
					specs[j] = jrpc2.Spec{
						Method: "DERO.GetSC",
						Params: rpc.GetSC_Params{
							SCID:       scid,
							KeysString: []string{"telaVersion"},
						},
					}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				responses, err := client.RPC.Batch(ctx, specs)
				cancel()
				if err != nil {
					logger.Printf("[BackfillTelaCandidates] Worker batch error: %v\n", err)
					continue
				}
				for j, resp := range responses {
					if j >= len(batch) || resp == nil || resp.Error() != nil {
						continue
					}
					var out rpc.GetSC_Result
					if err := resp.UnmarshalResult(&out); err != nil {
						continue
					}
					isTela := len(out.ValuesString) > 0 && out.ValuesString[0] != "" &&
						!strings.HasPrefix(out.ValuesString[0], "NOT AVAILABLE")
					resultCh <- result{scid: batch[j], isTela: isTela}
				}
			}
		}(client)
	}

	var found, processed int
	var doneCh = make(chan struct{})
	go func() {
		defer close(doneCh)
		for r := range resultCh {
			processed++
			if r.isTela {
				found++
				_ = idx.BBSBackend.StoreTelaCandidate(r.scid, "valid_index")
			}
		}
	}()

	for i := 0; i < total; i += batchSize {
		if idx.closing.Load() {
			logger.Printf("[BackfillTelaCandidates] Interrupted: indexer closing\n")
			break
		}
		end := i + batchSize
		if end > total {
			end = total
		}
		workCh <- scids[i:end]
		if (i/batchSize+1)%10 == 0 {
			logger.Printf("[BackfillTelaCandidates] Progress: %d/%d SCIDs checked, found %d candidates\n", end, total, found)
		}
	}
	close(workCh)
	wg.Wait()
	close(resultCh)
	<-doneCh
	logger.Printf("[BackfillTelaCandidates] Done. Checked %d SCIDs, found %d TELA candidates\n", total, found)
	return nil
}

// seedTelaCandidatesFromProbe waits for the background TELA probe (launched
// by FastSync) to settle, then copies its discovered INDEX SCIDs into the
// compat telacandidates bucket as "valid_index" so GetTelaCandidates() serves
// them immediately on the first click. DOC contracts are recorded as "doc" so
// they never surface as app candidates (they are each app's documentation, not
// apps themselves). Best-effort and bounded: if the probe never settles or no
// cache was written, Engram's own prefilter over the (now-populated) registry
// SCIDs still finds the apps — this just skips that prefilter when the probe
// wins the race.
func (idx *Indexer) seedTelaCandidatesFromProbe() {
	if idx == nil || idx.inner == nil || idx.BBSBackend == nil {
		return
	}
	// Seed immediately from the on-disk cache when one exists (warm start):
	// the cache holds the previous run's probe output, which is exactly the
	// classification we want — and this instantly corrects any stale bucket
	// entries from an older seed (e.g. DOC contracts wrongly marked
	// valid_index) instead of leaving the slow path active until the probe
	// settles. Idempotent, so re-seeding after the probe is harmless.
	if idx.seedFromTELACache() {
		return
	}
	// No cache yet (fresh install): give the turbo probe up to 5 minutes to
	// settle (it probes ~50K SCIDs in batched GetSC calls across the RPC
	// pool), then seed from whatever it wrote.
	deadline := time.Now().Add(5 * time.Minute)
	for !structures.TELAProbeSettled.Load() && time.Now().Before(deadline) {
		if idx.closing.Load() {
			return
		}
		sleepQuick()
	}
	if idx.closing.Load() {
		return
	}
	idx.seedFromTELACache()
}

// seedFromTELACache copies the current tela_cache.bin classification into the
// compat telacandidates bucket: INDEX SCIDs as "valid_index" (real apps),
// DOC contracts as "doc" (excluded by GetTelaCandidates). Returns true when a
// readable cache was found and seeded.
func (idx *Indexer) seedFromTELACache() bool {
	indexSCIDs := idx.inner.TELAIndexSCIDs()
	if len(indexSCIDs) == 0 {
		return false
	}
	seeded := 0
	for _, scid := range indexSCIDs {
		if err := idx.BBSBackend.StoreTelaCandidate(scid, "valid_index"); err != nil {
			logger.Printf("[gnomes] StoreTelaCandidate(%s): %v\n", scid[:16], err)
			continue
		}
		seeded++
	}
	// Record DOC contracts separately so a later GetTelaCandidates() (which
	// filters to valid_index/tela) excludes them without re-probing them.
	docSeeded := 0
	for _, scid := range idx.inner.TELADocSCIDs() {
		if err := idx.BBSBackend.StoreTelaCandidate(scid, "doc"); err != nil {
			continue
		}
		docSeeded++
	}
	logger.Printf("[gnomes] Seeded %d TELA app candidates (+%d doc contracts) into telacandidates bucket from fastsync cache\n", seeded, docSeeded)
	return true
}

// innerEndpoint returns the endpoint the internal HyperGnomon indexer is
// wired to (used when the caller never set the compat Endpoint field).
func (idx *Indexer) innerEndpoint() string {
	if idx == nil || idx.inner == nil {
		return ""
	}
	return idx.inner.Endpoint
}

// Close cleanly shuts down the scan loop + all store handles.
// Idempotent.
func (idx *Indexer) Close() {
	if idx == nil || !idx.closed.CompareAndSwap(false, true) {
		return
	}
	idx.closing.Store(true)
	if idx.fieldRefreshStop != nil {
		close(idx.fieldRefreshStop)
		idx.fieldRefreshWG.Wait()
	}
	if idx.inner != nil {
		idx.inner.Close()
	}
	idx.liveMu.Lock()
	if idx.liveClient != nil {
		idx.liveClient.Close()
		idx.liveClient = nil
	}
	idx.RPC = nil
	idx.liveMu.Unlock()
	// Close the injected store the compat layer owns (NewIndexer path). The
	// internal indexer borrowed it (ownStore=false) so didn't close it. For the
	// NewIndexerWithDBDir path BBSBackend is nil and the inner indexer already
	// closed its own store above.
	if idx.BBSBackend != nil {
		_ = idx.BBSBackend.Close()
	}
}

// startFieldRefresh launches a goroutine that copies the internal
// indexer's atomic counters into this facade's exported fields. Runs
// at ~10 Hz until Close. Without it the exported fields stay at
// their zero values since Go has no field-level "alias" to an
// atomic.Int64.
func (idx *Indexer) startFieldRefresh() {
	idx.fieldRefreshWG.Add(1)
	go func() {
		defer idx.fieldRefreshWG.Done()
		for {
			select {
			case <-idx.fieldRefreshStop:
				return
			default:
			}
			if idx.inner != nil {
				atomic.StoreInt64(&idx.LastIndexedHeight, idx.inner.LastIndexedHeight.Load())
				atomic.StoreInt64(&idx.ChainHeight, idx.inner.ChainHeight.Load())
			}
			// Simple sleep — tick rate doesn't need to be exact; the
			// fields are a convenience, not a consensus primitive.
			sleepQuick()
		}
	}()
}

// InitLog is civilware's logger-init entrypoint. HyperGnomon uses
// logrus internally and routes its own output; this facade accepts
// the args for source compatibility and returns without doing
// anything. Callers who want actual log routing should set
// structures.Logger (HyperGnomon-native) or inject via a future
// Config.Logger option.
func InitLog(args interface{}, writer io.Writer) {
	_ = args
	_ = writer
}

// newDeadIndexer returns an Indexer whose methods all no-op or
// error. Used for unsupported arg combinations in NewIndexer so
// callers don't segfault — they get back a typed pointer and can
// check via DBType=="" as a "not ready" signal.
func newDeadIndexer(cause error) *Indexer {
	return &Indexer{
		DBType:           "",
		fieldRefreshStop: make(chan struct{}),
	}
}
