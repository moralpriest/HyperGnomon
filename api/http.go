package api

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/mux"

	"golang.org/x/sync/singleflight"

	"github.com/hypergnomon/hypergnomon/eventbus"
	"github.com/hypergnomon/hypergnomon/indexer"
	"github.com/hypergnomon/hypergnomon/media"
	"github.com/hypergnomon/hypergnomon/rpc"
	"github.com/hypergnomon/hypergnomon/storage"
	"github.com/hypergnomon/hypergnomon/structures"
)

// Server is the HTTP REST API server for HyperGnomon.
type Server struct {
	store      storage.Storage
	pool       *rpc.Pool
	listenAddr string

	// safeHeight is a pointer to the indexer's atomic.Int64 tracking
	// max(LastIndexedHeight - FinalityDepth, 0). nil is tolerated (returns 0).
	safeHeight *atomic.Int64

	// reorgDetected is a pointer to the indexer's reorg-mismatch counter
	// (indexer.Indexer.ReorgDetected). nil is tolerated (returns 0). Surfaced
	// in /getstats the same way safeHeight is.
	reorgDetected *atomic.Int64

	// TELA content server wiring. All three are optional: if bus is nil the
	// cache invalidator is a no-op; if tela is nil /tela/... returns 503
	// on cache misses (but served reads from the durable bucket still work).
	bus       *eventbus.Bus
	tela      *indexer.Indexer
	telaCache *telaContentCache

	// telaVerifySigs enables the X-TELA-Verify response header on /tela/…
	// responses. v1.0 surfaces signature presence without running the
	// bn256 Schnorr verification (that lands in v1.2). Default false so
	// existing deployments don't see unexpected header changes.
	telaVerifySigs bool

	mu         sync.RWMutex
	cachedInfo *structures.GetInfoResult

	// srv is the *http.Server created by Start; retained so Stop can release
	// the listener and unblock Start's Serve. Guarded by mu.
	srv *http.Server

	// stopCh is created by Start and closed by Stop to signal the background
	// loops to exit. Guarded by mu.
	stopCh chan struct{}
	// stopped is set by Stop and never cleared: a stopped Server refuses
	// Start. Guarded by mu.
	stopped bool
	// wg counts the background loops; Stop waits on it so no loop outlives
	// Stop's return.
	wg sync.WaitGroup

	assetCatalogMu       sync.RWMutex
	assetCatalogs        map[string]assetCatalogCacheEntry
	assetCatalogTTL      time.Duration
	assetCatalogEmptyTTL time.Duration

	// Media cache wiring (see api/media.go). mediaDir empty disables the
	// endpoint; mediaFetch gates on-demand network retrieval; mediaSF
	// coalesces concurrent misses on the same file.
	mediaDir     string
	mediaFetch   bool
	mediaFetcher *media.Fetcher
	mediaSF      singleflight.Group
}

// NewServer creates a new API server.
//
// safeHeight and reorgDetected may be nil; handlers treat nil as zero. bus/idx
// may be nil to disable the /tela/... content server's on-demand refresh +
// invalidation.
func NewServer(store storage.Storage, pool *rpc.Pool, listenAddr string, safeHeight *atomic.Int64, reorgDetected *atomic.Int64, bus *eventbus.Bus, idx *indexer.Indexer, telaCacheBytes int64) *Server {
	return &Server{
		store:         store,
		pool:          pool,
		listenAddr:    listenAddr,
		safeHeight:    safeHeight,
		reorgDetected: reorgDetected,
		bus:           bus,
		tela:          idx,
		telaCache:     newTELAContentCache(telaCacheBytes),
	}
}

// SetTELAVerifySigs toggles the X-TELA-Verify header on served /tela/…
// responses. Call before Start; flipping at runtime is safe but only
// affects subsequent requests. v1.0 limitation: the header reports
// signature presence, not cryptographic verification.
func (s *Server) SetTELAVerifySigs(on bool) { s.telaVerifySigs = on }

// loadSafeHeight returns the current safe height, or 0 if not wired.
func (s *Server) loadSafeHeight() int64 {
	if s.safeHeight == nil {
		return 0
	}
	return s.safeHeight.Load()
}

// loadReorgDetected returns the current reorg-detection count, or 0 if not wired.
func (s *Server) loadReorgDetected() int64 {
	if s.reorgDetected == nil {
		return 0
	}
	return s.reorgDetected.Load()
}

// Start registers routes and begins serving HTTP requests.
// Blocks until the server exits: it returns nil after a clean Stop,
// http.ErrServerClosed without serving if Stop already ran, and the
// bind or serve error otherwise. A Server serves at most once.
func (s *Server) Start() error {
	r := mux.NewRouter()

	r.HandleFunc("/api/getinfo", s.handleGetInfo).Methods(http.MethodGet)
	r.HandleFunc("/api/getstats", s.handleGetStats).Methods(http.MethodGet)
	r.HandleFunc("/api/getscids", s.handleGetSCIDs).Methods(http.MethodGet)
	r.HandleFunc("/api/indexedscs", s.handleIndexedSCs).Methods(http.MethodGet)
	r.HandleFunc("/api/indexbyscid", s.handleIndexBySCID).Methods(http.MethodGet)
	r.HandleFunc("/api/scvarsbyheight", s.handleSCVarsByHeight).Methods(http.MethodGet)
	r.HandleFunc("/api/invalidscids", s.handleInvalidSCIDs).Methods(http.MethodGet)
	r.HandleFunc("/api/scidprivtx", s.handleSCIDPrivTx).Methods(http.MethodGet)
	r.HandleFunc("/api/tela", s.handleGetTELA).Methods(http.MethodGet)
	r.HandleFunc("/api/tela/count", s.handleGetTELACount).Methods(http.MethodGet)
	r.HandleFunc("/api/tela/{scid}/ratings", s.handleGetTELARatings).Methods(http.MethodGet)
	r.HandleFunc("/api/assets", s.handleGetAssets).Methods(http.MethodGet)
	r.HandleFunc("/api/media/{scid}", s.handleGetMedia).Methods(http.MethodGet, http.MethodHead)
	r.HandleFunc("/api/assets/{scid}", s.handleGetAsset).Methods(http.MethodGet)
	r.HandleFunc("/api/initialscidcode", s.handleGetInitialSCIDCode).Methods(http.MethodGet)
	r.HandleFunc("/api/address/{address}/created-assets", s.handleGetAddressCreatedAssets).Methods(http.MethodGet)
	r.HandleFunc("/api/address/{address}/touched-assets", s.handleGetAddressTouchedAssets).Methods(http.MethodGet)
	r.HandleFunc("/api/address/{address}/scs", s.handleGetAddress).Methods(http.MethodGet)

	// TELA content server (DESIGN.md §10). {path:.*} lets the router
	// forward multi-segment paths like "app/js/main.js" intact.
	r.HandleFunc("/tela/{scid}/{path:.*}", s.handleGetTELAContent).Methods(http.MethodGet, http.MethodHead)

	// Bind before spawning anything so a bind failure leaves no goroutines
	// behind and the error reaches the caller first.
	ln, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:         s.listenAddr,
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Publish stopCh, srv, and the loop count in one critical section so
	// Stop either observes the fully-published server or Start observes
	// stopped and refuses — there is no half-started window.
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		_ = ln.Close()
		return http.ErrServerClosed
	}
	if s.srv != nil {
		s.mu.Unlock()
		_ = ln.Close()
		return errors.New("api: server already started")
	}
	s.stopCh = make(chan struct{})
	stopCh := s.stopCh
	s.srv = srv
	s.wg.Add(2)
	s.mu.Unlock()

	// Background info caching + TELA cache invalidator. Both exit when
	// stopCh is closed by Stop, which waits for them via wg.
	go s.refreshInfoLoop(stopCh)
	go s.runTELAInvalidator(stopCh)

	logger.Infof("HTTP API listening on %s", ln.Addr())
	err = srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// Stop shuts down the HTTP server and waits for its background loops to
// exit. In-flight requests drain until ctx expires; remaining connections
// are then force-closed and ctx's error is returned. Idempotent: later
// calls return nil. Safe to call before Start — a subsequent Start returns
// http.ErrServerClosed without serving. A stopped Server cannot be
// restarted; construct a new Server instead.
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	if s.stopCh != nil {
		close(s.stopCh)
	}
	srv := s.srv
	s.srv = nil
	s.mu.Unlock()

	var err error
	if srv != nil {
		if err = srv.Shutdown(ctx); err != nil {
			_ = srv.Close()
		}
	}
	// Block until both loops have exited so nothing touches the pool or
	// store after Stop returns; the wait is bounded by one in-flight RPC
	// or DB operation.
	s.wg.Wait()
	return err
}

// refreshInfoLoop periodically fetches daemon info and caches it.
// Exits when stopCh is closed by Stop; no-op when no RPC pool is wired.
func (s *Server) refreshInfoLoop(stopCh <-chan struct{}) {
	defer s.wg.Done()
	if s.pool == nil {
		return
	}
	s.refreshInfo()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			s.refreshInfo()
		}
	}
}

func (s *Server) refreshInfo() {
	err := s.pool.WithConn(func(c *rpc.Client) error {
		info, err := c.GetInfo()
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.cachedInfo = &structures.GetInfoResult{
			Height:       info.Height,
			TopoHeight:   info.TopoHeight,
			StableHeight: info.StableHeight,
			Status:       info.Status,
		}
		s.mu.Unlock()
		return nil
	})
	if err != nil {
		logger.Warnf("refresh daemon info: %v", err)
	}
}

// --- Handlers ---

// handleGetInfo returns cached daemon info.
//
// The response is a struct-like map rather than the bare GetInfoResult so we
// can tack on indexer-specific fields (safe_height) without changing the
// shared cache type. Existing field names are preserved verbatim for back-
// compat with callers that learned them from pre-M1 responses.
func (s *Server) handleGetInfo(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	info := s.cachedInfo
	s.mu.RUnlock()

	if info == nil {
		writeError(w, http.StatusServiceUnavailable, "daemon info not yet available")
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"Height":       info.Height,
		"TopoHeight":   info.TopoHeight,
		"StableHeight": info.StableHeight,
		"Status":       info.Status,
		"safe_height":  s.loadSafeHeight(),
	})
}

// handleGetStats returns indexer statistics.
func (s *Server) handleGetStats(w http.ResponseWriter, r *http.Request) {
	indexHeight, err := s.store.GetLastIndexHeight()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get index height: "+err.Error())
		return
	}

	scCount, err := s.store.GetSCIDCount()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get SCID count: "+err.Error())
		return
	}

	reg, burn, norm, err := s.store.GetTxCounts()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get tx counts: "+err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"app_name":       structures.AppName,
		"version":        structures.Version,
		"index_height":   indexHeight,
		"safe_height":    s.loadSafeHeight(),
		"reorg_detected": s.loadReorgDetected(),
		"sc_count":       scCount,
		"reg_tx_count":   reg,
		"burn_tx_count":  burn,
		"norm_tx_count":  norm,
		"total_tx_count": reg + burn + norm,
		"tela_count":     structures.TELACount.Load(),
	})
}

// handleGetSCIDs returns all indexed SCIDs.
func (s *Server) handleGetSCIDs(w http.ResponseWriter, r *http.Request) {
	scids, err := s.store.GetAllSCIDs()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get SCIDs: "+err.Error())
		return
	}
	if scids == nil {
		scids = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"scids": scids,
	})
}

// handleIndexedSCs returns all SCIDs with their owners.
func (s *Server) handleIndexedSCs(w http.ResponseWriter, r *http.Request) {
	owners, err := s.store.GetAllOwnersAndSCIDs()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get owners: "+err.Error())
		return
	}
	if owners == nil {
		owners = make(map[string]string)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"indexed_scs": owners,
	})
}

// handleIndexBySCID returns invocation details for a given SCID.
func (s *Server) handleIndexBySCID(w http.ResponseWriter, r *http.Request) {
	scid := queryParam(r, "scid")
	if scid == "" {
		writeError(w, http.StatusBadRequest, "missing required parameter: scid")
		return
	}

	details, err := s.store.GetInvokeDetailsBySCID(scid)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get invoke details: "+err.Error())
		return
	}
	if details == nil {
		details = []*structures.SCTXParse{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"scid":    scid,
		"details": details,
	})
}

// handleSCVarsByHeight returns SC variables at a specific height.
func (s *Server) handleSCVarsByHeight(w http.ResponseWriter, r *http.Request) {
	scid := queryParam(r, "scid")
	if scid == "" {
		writeError(w, http.StatusBadRequest, "missing required parameter: scid")
		return
	}

	heightStr := queryParam(r, "height")
	if heightStr == "" {
		writeError(w, http.StatusBadRequest, "missing required parameter: height")
		return
	}

	height, err := strconv.ParseInt(heightStr, 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid height parameter: "+err.Error())
		return
	}

	vars, err := s.store.GetSCIDVariableDetailsAtHeight(scid, height)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get SC variables: "+err.Error())
		return
	}
	if vars == nil {
		vars = []*structures.SCIDVariable{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"scid":      scid,
		"height":    height,
		"variables": vars,
	})
}

// handleInvalidSCIDs returns failed SC deploys.
func (s *Server) handleInvalidSCIDs(w http.ResponseWriter, r *http.Request) {
	invalid, err := s.store.GetInvalidSCIDDeploys()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get invalid SCIDs: "+err.Error())
		return
	}
	if invalid == nil {
		invalid = make(map[string]uint64)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"invalid_scids": invalid,
	})
}

// handleSCIDPrivTx returns normal TXs with SCID payload for a given address.
func (s *Server) handleSCIDPrivTx(w http.ResponseWriter, r *http.Request) {
	addr := queryParam(r, "address")
	if addr == "" {
		writeError(w, http.StatusBadRequest, "missing required parameter: address")
		return
	}

	txs, err := s.store.GetNormalTxWithSCIDByAddr(addr)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get normal TXs: "+err.Error())
		return
	}
	if txs == nil {
		txs = []*structures.NormalTXWithSCIDParse{}
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"address":      addr,
		"transactions": txs,
	})
}

// handleGetTELA returns all discovered TELA SCIDs with their metadata.
// Uses the class index (Route B) for an O(1) prefix scan instead of the old
// O(N * 3-reads) iteration over every SCID. Accepts an optional ?class= query
// param (defaults to "TELA-INDEX-1"; "TELA-DOC-1" is the other common value).
func (s *Server) handleGetTELA(w http.ResponseWriter, r *http.Request) {
	class := queryParam(r, "class")
	if class == "" {
		class = "TELA-INDEX-1"
	}

	installs, err := s.store.GetClassInstalls(class, 0)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get class installs: "+err.Error())
		return
	}

	type telaApp struct {
		SCID        string `json:"scid"`
		Name        string `json:"name,omitempty"`
		Description string `json:"description,omitempty"`
		DURL        string `json:"durl,omitempty"`
		Version     string `json:"version,omitempty"`
		Owner       string `json:"owner,omitempty"`
	}

	scids := make([]string, 0, len(installs))
	for _, inst := range installs {
		scids = append(scids, inst.SCID)
	}
	owners, err := s.store.GetOwnersForSCIDs(scids)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get owners: "+err.Error())
		return
	}

	apps := make([]telaApp, 0, len(installs))
	for _, inst := range installs {
		app := telaApp{
			SCID:  inst.SCID,
			Owner: owners[inst.SCID],
		}
		if inst.Meta != nil {
			app.Name = inst.Meta.Name
			app.Description = inst.Meta.Desc
			app.DURL = inst.Meta.DURL
			app.Version = inst.Meta.Version
		}
		apps = append(apps, app)
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"tela_apps": apps,
		"count":     len(apps),
	})
}

// assetCatalogClasses is the default /api/assets result set.
//
// G45-C (collections) is included so a client can render a collection's
// `backdropImage` and group its NFTs. Its absence was invisible while every
// G45 media field was dropped at classify time, but a collection is an asset
// contract by every other definition here, and G45 records carry tags
// ["all","g45"] rather than "asset" — so isAssetMeta's tag branch never
// reached them either.
var assetCatalogClasses = []string{
	"NFA",
	"G45-NFT",
	"G45-FAT",
	"G45-AT",
	"G45-C",
	"DERO-ASSET",
}

const (
	assetCatalogCacheKeyAll = "all"
	defaultAssetCatalogTTL  = 5 * time.Second
	defaultEmptyAssetTTL    = 1 * time.Second
)

type assetCatalogCacheEntry struct {
	entries []assetEntry
	expires time.Time
}

type assetEntry struct {
	SCID        string   `json:"scid"`
	Class       string   `json:"class"`
	Tags        []string `json:"tags"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	IconURL     string   `json:"icon_url"`
	// Media URLs from the asset's on-chain metadata. Omitted when empty so
	// existing consumers see an unchanged response shape for assets that carry
	// none. G45 assets populate Image (from `image`, or `backdropImage` on a
	// collection) rather than IconURL, which their metadata never sets.
	//
	// These are URLs, not content: HyperGnomon does not fetch, cache, or proxy
	// the bytes behind them. Almost all are `ipfs://` and need a gateway or
	// local node to resolve.
	Image            string `json:"image,omitempty"`
	AltImage         string `json:"alt_image,omitempty"`
	Audio            string `json:"audio,omitempty"`
	Video            string `json:"video,omitempty"`
	ImagesJSON       string `json:"images,omitempty"`
	Owner            string `json:"owner"`
	InstallHeight    int64  `json:"install_height"`
	LastHeight       int64  `json:"last_height"`
	FirstTouchHeight int64  `json:"first_touch_height,omitempty"`
	LastTouchHeight  int64  `json:"last_touch_height,omitempty"`
	TouchCount       int64  `json:"touch_count,omitempty"`
}

func assetClassesForParam(class string) ([]string, bool) {
	class = strings.TrimSpace(class)
	if class == "" || strings.EqualFold(class, "asset") || strings.EqualFold(class, "assets") {
		return assetCatalogClasses, true
	}
	class = strings.ToUpper(class)
	if isAssetClass(class) {
		return []string{class}, true
	}
	return nil, false
}

func assetCatalogCacheKey(class string) string {
	class = strings.TrimSpace(class)
	if class == "" || strings.EqualFold(class, "asset") || strings.EqualFold(class, "assets") {
		return assetCatalogCacheKeyAll
	}
	return strings.ToUpper(class)
}

func isAssetClass(class string) bool {
	switch class {
	case "NFA", "G45-NFT", "G45-FAT", "G45-AT", "G45-C", "DERO-ASSET":
		return true
	default:
		return false
	}
}

func isAssetMeta(meta *structures.ClassMeta) bool {
	if meta == nil {
		return false
	}
	if isAssetClass(strings.ToUpper(meta.Class)) {
		return true
	}
	for _, tag := range meta.Tags {
		if strings.EqualFold(tag, "asset") {
			return true
		}
	}
	return false
}

func parseHTTPPageParams(vals url.Values) (int, int, string) {
	offset := 0
	if raw := vals.Get("offset"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			return 0, 0, "offset must be >= 0"
		}
		offset = parsed
	}

	limit := listSCDefaultLimit
	if raw := vals.Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			return 0, 0, "limit must be an integer"
		}
		limit = parsed
	}
	return offset, clampListLimit(limit, listSCDefaultLimit), ""
}

func assetEntryFromMeta(scid, owner string, meta *structures.ClassMeta) assetEntry {
	tags := []string{}
	if meta != nil && len(meta.Tags) > 0 {
		tags = append(tags, meta.Tags...)
	}
	entry := assetEntry{
		SCID:  scid,
		Owner: owner,
		Tags:  tags,
	}
	if meta != nil {
		entry.Class = meta.Class
		entry.Name = meta.Name
		entry.Description = meta.Desc
		entry.IconURL = meta.IconURL
		entry.Image = meta.Image
		entry.AltImage = meta.AltImage
		entry.Audio = meta.Audio
		entry.Video = meta.Video
		entry.ImagesJSON = meta.ImagesJSON
		entry.InstallHeight = meta.InstallHeight
		entry.LastHeight = meta.LastHeight
	}
	return entry
}

func (s *Server) assetCatalogDurations() (time.Duration, time.Duration) {
	ttl := s.assetCatalogTTL
	if ttl <= 0 {
		ttl = defaultAssetCatalogTTL
	}
	emptyTTL := s.assetCatalogEmptyTTL
	if emptyTTL <= 0 {
		emptyTTL = defaultEmptyAssetTTL
	}
	return ttl, emptyTTL
}

func (s *Server) getCachedAssetCatalog(cacheKey string) ([]assetEntry, bool) {
	now := time.Now()
	s.assetCatalogMu.RLock()
	entry, ok := s.assetCatalogs[cacheKey]
	if ok && now.Before(entry.expires) {
		entries := entry.entries
		s.assetCatalogMu.RUnlock()
		return entries, true
	}
	s.assetCatalogMu.RUnlock()
	return nil, false
}

func (s *Server) storeCachedAssetCatalog(cacheKey string, entries []assetEntry) {
	ttl, emptyTTL := s.assetCatalogDurations()
	expires := time.Now().Add(ttl)
	if len(entries) == 0 {
		expires = time.Now().Add(emptyTTL)
	}
	s.assetCatalogMu.Lock()
	if s.assetCatalogs == nil {
		s.assetCatalogs = make(map[string]assetCatalogCacheEntry, len(assetCatalogClasses)+1)
	}
	s.assetCatalogs[cacheKey] = assetCatalogCacheEntry{
		entries: entries,
		expires: expires,
	}
	s.assetCatalogMu.Unlock()
}

func (s *Server) getAssetCatalog(cacheKey string, classes []string) ([]assetEntry, error) {
	if entries, ok := s.getCachedAssetCatalog(cacheKey); ok {
		return entries, nil
	}
	entries, err := s.buildAssetCatalog(classes)
	if err != nil {
		return nil, err
	}
	s.storeCachedAssetCatalog(cacheKey, entries)
	return entries, nil
}

func (s *Server) buildAssetCatalog(classes []string) ([]assetEntry, error) {
	seen := make(map[string]structures.ClassInstall)
	for _, class := range classes {
		installs, err := s.store.GetClassInstalls(class, 0)
		if err != nil {
			return nil, err
		}
		for _, inst := range installs {
			if !isAssetMeta(inst.Meta) {
				continue
			}
			if prev, ok := seen[inst.SCID]; !ok || inst.InstallHeight > prev.InstallHeight {
				seen[inst.SCID] = inst
			}
		}
	}

	out := make([]structures.ClassInstall, 0, len(seen))
	for _, inst := range seen {
		out = append(out, inst)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].InstallHeight == out[j].InstallHeight {
			return out[i].SCID < out[j].SCID
		}
		return out[i].InstallHeight < out[j].InstallHeight
	})

	scids := make([]string, 0, len(out))
	for _, inst := range out {
		scids = append(scids, inst.SCID)
	}
	owners, err := s.store.GetOwnersForSCIDs(scids)
	if err != nil {
		return nil, err
	}

	entries := make([]assetEntry, 0, len(out))
	for _, inst := range out {
		entries = append(entries, assetEntryFromMeta(inst.SCID, owners[inst.SCID], inst.Meta))
	}
	return entries, nil
}

func (s *Server) assetEntriesForWindow(installs []structures.ClassInstall, owners map[string]string) []assetEntry {
	out := make([]assetEntry, 0, len(installs))
	for _, inst := range installs {
		out = append(out, assetEntryFromMeta(inst.SCID, owners[inst.SCID], inst.Meta))
	}
	return out
}

// handleGetAssets returns recognized asset/NFT contracts from the class index.
// It is an asset catalog, not a private wallet-balance view.
func (s *Server) handleGetAssets(w http.ResponseWriter, r *http.Request) {
	vals := r.URL.Query() // parse once; net/http does not cache r.URL.Query()
	offset, limit, msg := parseHTTPPageParams(vals)
	if msg != "" {
		writeError(w, http.StatusBadRequest, msg)
		return
	}
	class := vals.Get("class")
	classes, ok := assetClassesForParam(class)
	if !ok {
		writeError(w, http.StatusBadRequest, "class is not an asset class")
		return
	}
	cacheKey := assetCatalogCacheKey(class)

	assets, err := s.getAssetCatalog(cacheKey, classes)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get asset catalog: "+err.Error())
		return
	}
	total := len(assets)
	win := sliceWindow(total, offset, limit)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"assets": assets[win.start:win.end],
		"count":  total,
		"offset": offset,
		"limit":  limit,
	})
}

// handleGetAsset returns one recognized asset/NFT contract by SCID.
func (s *Server) handleGetAsset(w http.ResponseWriter, r *http.Request) {
	scid := mux.Vars(r)["scid"]
	if scid == "" {
		writeError(w, http.StatusBadRequest, "missing scid")
		return
	}
	if !isValidSCID(scid) {
		writeError(w, http.StatusBadRequest, "invalid scid")
		return
	}

	meta, err := s.store.GetSCIDClass(scid)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get asset metadata: "+err.Error())
		return
	}
	if !isAssetMeta(meta) {
		writeError(w, http.StatusNotFound, "asset not found")
		return
	}
	owner, err := s.store.GetOwner(scid)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get owner: "+err.Error())
		return
	}
	writeJSON(w, http.StatusOK, assetEntryFromMeta(scid, owner, meta))
}

// handleGetAddressCreatedAssets returns asset contracts deployed by address.
// This is deployer/registry ownership, not proof of current wallet balance.
func (s *Server) handleGetAddressCreatedAssets(w http.ResponseWriter, r *http.Request) {
	addr := mux.Vars(r)["address"]
	if addr == "" {
		writeError(w, http.StatusBadRequest, "missing required path parameter: address")
		return
	}
	offset, limit, msg := parseHTTPPageParams(r.URL.Query())
	if msg != "" {
		writeError(w, http.StatusBadRequest, msg)
		return
	}

	scids, err := s.store.GetSCIDsByOwner(addr)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get owner SCIDs: "+err.Error())
		return
	}
	// Bulk-read class metadata in one View txn instead of one per SCID; for
	// an owner of N asset contracts this is 2 Views (the GetSCIDsByOwner
	// prefix scan above + this bulk lookup) rather than N+1.
	metas, err := s.store.GetSCIDClassBulk(scids)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get asset metadata: "+err.Error())
		return
	}
	installs := make([]structures.ClassInstall, 0, len(scids))
	for _, scid := range scids {
		meta := metas[scid]
		if !isAssetMeta(meta) {
			continue
		}
		installs = append(installs, structures.ClassInstall{
			SCID:          scid,
			InstallHeight: meta.InstallHeight,
			Meta:          meta,
		})
	}
	sort.Slice(installs, func(i, j int) bool {
		if installs[i].InstallHeight == installs[j].InstallHeight {
			return installs[i].SCID < installs[j].SCID
		}
		return installs[i].InstallHeight < installs[j].InstallHeight
	})

	total := len(installs)
	win := sliceWindow(total, offset, limit)
	owners := make(map[string]string, win.end-win.start)
	for i := win.start; i < win.end; i++ {
		owners[installs[i].SCID] = addr
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"address": addr,
		"assets":  s.assetEntriesForWindow(installs[win.start:win.end], owners),
		"count":   total,
		"offset":  offset,
		"limit":   limit,
	})
}

// handleGetAddressTouchedAssets returns asset contracts an address has
// interacted with. This is activity history, not proof of current balance.
func (s *Server) handleGetAddressTouchedAssets(w http.ResponseWriter, r *http.Request) {
	addr := mux.Vars(r)["address"]
	if addr == "" {
		writeError(w, http.StatusBadRequest, "missing required path parameter: address")
		return
	}
	offset, limit, msg := parseHTTPPageParams(r.URL.Query())
	if msg != "" {
		writeError(w, http.StatusBadRequest, msg)
		return
	}

	touched, err := s.store.GetAddressSCIDs(addr)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get address SCIDs: "+err.Error())
		return
	}

	// Bulk-read class metadata + owners in two Views instead of 2N per-SCID
	// Views; for an address that touched N asset contracts this collapses
	// 2N+1 Views to 3 (the GetAddressSCIDs prefix scan above + these two
	// bulk lookups). Both bulk methods reuse one cursor + one key-scratch
	// across all N lookups inside a single View txn.
	scids := make([]string, 0, len(touched))
	for scid, touch := range touched {
		if touch != nil {
			scids = append(scids, scid)
		}
	}
	metas, err := s.store.GetSCIDClassBulk(scids)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get asset metadata: "+err.Error())
		return
	}
	owners, err := s.store.GetOwnersForSCIDs(scids)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get owners: "+err.Error())
		return
	}

	entries := make([]assetEntry, 0, len(touched))
	for scid, touch := range touched {
		if touch == nil {
			continue
		}
		meta := metas[scid]
		if !isAssetMeta(meta) {
			continue
		}
		entry := assetEntryFromMeta(scid, owners[scid], meta)
		entry.FirstTouchHeight = touch.FirstHeight
		entry.LastTouchHeight = touch.LastHeight
		entry.TouchCount = touch.Count
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].LastTouchHeight == entries[j].LastTouchHeight {
			return entries[i].SCID < entries[j].SCID
		}
		return entries[i].LastTouchHeight > entries[j].LastTouchHeight
	})

	total := len(entries)
	win := sliceWindow(total, offset, limit)
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"address": addr,
		"assets":  entries[win.start:win.end],
		"count":   total,
		"offset":  offset,
		"limit":   limit,
	})
}

// handleGetAddress returns the list of SCIDs an address has interacted with,
// enriched with class + name. Sorted by last_height descending so the most
// recently-touched SCIDs come first.
func (s *Server) handleGetAddress(w http.ResponseWriter, r *http.Request) {
	addr := mux.Vars(r)["address"]
	if addr == "" {
		writeError(w, http.StatusBadRequest, "missing required path parameter: address")
		return
	}

	entries, err := s.store.GetAddressSCIDs(addr)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get address SCIDs: "+err.Error())
		return
	}

	type scidEntry struct {
		SCID        string `json:"scid"`
		FirstHeight int64  `json:"first_height"`
		LastHeight  int64  `json:"last_height"`
		Count       int64  `json:"count"`
		Class       string `json:"class,omitempty"`
		Name        string `json:"name,omitempty"`
	}


	// Bulk-read all class metadata in one View txn instead of one per SCID;
	// for an address that touched N SCIDs this is 2 Views total (the
	// GetAddressSCIDs prefix scan above + this bulk lookup) rather than N+1.
	scids := make([]string, 0, len(entries))
	for scid, e := range entries {
		if e != nil {
			scids = append(scids, scid)
		}
	}
	metas, _ := s.store.GetSCIDClassBulk(scids)

	out := make([]scidEntry, 0, len(entries))
	for scid, e := range entries {
		if e == nil {
			continue
		}
		item := scidEntry{
			SCID:        scid,
			FirstHeight: e.FirstHeight,
			LastHeight:  e.LastHeight,
			Count:       e.Count,
		}
		if meta := metas[scid]; meta != nil {
			item.Class = meta.Class
			item.Name = meta.Name
		}
		out = append(out, item)
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].LastHeight > out[j].LastHeight
	})

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"address": addr,
		"scids":   out,
		"count":   len(out),
	})
}

// handleGetTELARatings returns the ratings stored on the given SCID.
//
// Canonical TELA format (github.com/civilware/tela): per-rater STORE keys
// that equal the rater's wallet address, hex-encoded `"<score>_<height>"`
// values. No comment field. The response also includes the aggregate
// `likes` / `dislikes` counters from the TELA Rate() entrypoint plus a
// computed mean across per-rater scores.
func (s *Server) handleGetTELARatings(w http.ResponseWriter, r *http.Request) {
	scid := mux.Vars(r)["scid"]
	if scid == "" {
		writeError(w, http.StatusBadRequest, "missing scid")
		return
	}
	var height int64
	if h := queryParam(r, "height"); h != "" {
		parsed, err := strconv.ParseInt(h, 10, 64)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid height: "+err.Error())
			return
		}
		height = parsed
	}
	ratings, summary, err := s.store.GetRatingsAndSummaryForSCID(scid, height)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to read ratings: "+err.Error())
		return
	}
	if ratings == nil {
		ratings = []structures.Rating{}
	}
	var sum float64
	for _, r := range ratings {
		sum += r.Score
	}
	var avg float64
	if len(ratings) > 0 {
		avg = sum / float64(len(ratings))
	}
	resp := map[string]interface{}{
		"scid":    scid,
		"ratings": ratings,
		"count":   len(ratings),
		"avg":     avg,
	}
	if summary != nil {
		resp["summary"] = summary
	}
	writeJSON(w, http.StatusOK, resp)
}

// initialSCIDCodeResponse is the typed response for handleGetInitialSCIDCode.
// A struct (vs a map[string]interface{}) avoids the map allocation, the int64
// boxing, and json's key-sort pass. Fields are declared in alphabetical
// json-tag order so the encoded bytes match the previous sorted-map output.
type initialSCIDCodeResponse struct {
	Code          string `json:"code"`
	InstallHeight int64  `json:"install_height"`
	SCID          string `json:"scid"`
}

// handleGetInitialSCIDCode returns the install-time DVM code for scid.
// Drop-in compat with simple-gnomon's WS method of the same name. Reads
// from the sccode bucket; on miss, lazily backfills via idx.GetSCCode
// (one daemon round-trip per SCID per process lifetime).
func (s *Server) handleGetInitialSCIDCode(w http.ResponseWriter, r *http.Request) {
	if s.tela == nil {
		writeError(w, http.StatusServiceUnavailable, "indexer not configured")
		return
	}
	scid := queryParam(r, "scid")
	if scid == "" {
		writeError(w, http.StatusBadRequest, "missing scid")
		return
	}
	if len(scid) != 64 {
		writeError(w, http.StatusBadRequest, "invalid scid")
		return
	}
	entry, err := s.tela.GetSCCode(scid)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get sc code: "+err.Error())
		return
	}
	if entry == nil {
		writeError(w, http.StatusNotFound, "scid not found")
		return
	}
	writeJSON(w, http.StatusOK, initialSCIDCodeResponse{
		Code:          entry.Code,
		InstallHeight: entry.InstallHeight,
		SCID:          scid,
	})
}

// handleGetTELACount returns the current TELA discovery count from the atomic counter.
// Zero-allocation, no DB queries -- suitable for fast UI polling.
func (s *Server) handleGetTELACount(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int64{
		"tela_count": structures.TELACount.Load(),
	})
}

// --- Helpers ---

// queryParam returns the first value for key in r's raw query, matching
// url.Values.Get semantics for well-formed inputs while avoiding the
// url.Values map (+ per-key slices) that r.URL.Query() builds per call.
// Mirrors ParseQuery's tolerance: pairs containing ';' or with undecodable
// escapes are skipped; a bare "key" yields "". QueryUnescape returns its
// input unchanged (no allocation) when nothing needs decoding — the common
// case for hex scids.
func queryParam(r *http.Request, key string) string {
	q := r.URL.RawQuery
	for q != "" {
		var pair string
		pair, q, _ = strings.Cut(q, "&")
		if pair == "" || strings.Contains(pair, ";") {
			continue
		}
		k, v, _ := strings.Cut(pair, "=")
		if k != key {
			// The key itself may be percent-encoded (rare) — only then pay
			// the unescape to compare.
			if !strings.ContainsAny(k, "%+") {
				continue
			}
			ku, err := url.QueryUnescape(k)
			if err != nil || ku != key {
				continue
			}
		}
		val, err := url.QueryUnescape(v)
		if err != nil {
			continue
		}
		return val
	}
	return ""
}

// jsonContentType is the shared canonical Content-Type value slice. Header.Set
// builds a fresh []string{v} per call — the only allocation writeJSON made.
// Assigning one shared slice is safe because nothing mutates header value
// slices in place (net/http and this package only read or replace them).
var jsonContentType = []string{"application/json"}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header()["Content-Type"] = jsonContentType
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		logger.Errorf("json encode: %v", err)
	}
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
