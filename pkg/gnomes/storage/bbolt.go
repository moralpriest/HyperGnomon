// Package storage is the civilware/Gnomon compat surface for the
// two store backends. HyperGnomon is bbolt-only — the GravitonStore
// type is declared for source compatibility but errors on any
// operational call. See gravdb.go.
package storage

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"time"

	gitbbolt "go.etcd.io/bbolt"

	hgstorage "github.com/hypergnomon/hypergnomon/storage"
	hgstructures "github.com/hypergnomon/hypergnomon/structures"

	compatstructures "github.com/hypergnomon/hypergnomon/pkg/gnomes/structures"
)

// ErrStoreInUse is returned by ValidateStore when the bbolt database inside
// dbDir is healthy but currently locked by another open handle (the running
// indexer, or a second process). Callers MUST treat this as "valid, in use" —
// NOT as corruption. bbolt surfaces the failed lock acquisition as
// bolt.ErrTimeout after lockTimeout elapses.
var ErrStoreInUse = errors.New("gnomon database is in use (locked by another handle)")

// ValidateStore performs a non-destructive integrity check on the bbolt
// database inside dbDir. It opens the file read-only with lockTimeout to
// acquire bbolt's shared lock, walks every top-level bucket name, and closes.
//
//   - nil            → the database is intact and readable.
//   - ErrStoreInUse  → intact but locked by another open handle. This is the
//     expected state while the indexer is running — treating it as corruption
//     (the old check's failure mode) wipes a healthy DB on every reconnect.
//   - other errors   → the database cannot be read: genuinely corrupt,
//     permission problems, or dbDir does not contain a database yet. A missing
//     DB file returns nil (nothing to validate is not corruption).
//
// The old check opened a 25ms *write* handle and read the last-indexed height:
// any moment the real indexer held the lock tripped the "corrupted" branch and
// backing up + recreating the DB on a reconnect path. Read-only flock with a
// real timeout is the correct signal.
func ValidateStore(dbDir string, lockTimeout time.Duration) error {
	if lockTimeout <= 0 {
		lockTimeout = 5 * time.Second
	}
	dbPath := hgstorage.MainStoreFilePath(dbDir)
	if _, err := os.Stat(dbPath); err != nil {
		if os.IsNotExist(err) {
			return nil // no database file yet — nothing to validate; not corruption
		}
		return fmt.Errorf("[ValidateStore] stat %s: %w", dbPath, err)
	}

	db, err := gitbbolt.Open(dbPath, 0600, &gitbbolt.Options{
		Timeout:  lockTimeout,
		ReadOnly: true,
	})
	if err != nil {
		if errors.Is(err, gitbbolt.ErrTimeout) {
			return fmt.Errorf("%w: %w", ErrStoreInUse, err)
		}
		return fmt.Errorf("[ValidateStore] open %s: %w", dbPath, err)
	}
	defer db.Close()

	err = db.View(func(tx *gitbbolt.Tx) error {
		return tx.ForEach(func(_ []byte, _ *gitbbolt.Bucket) error { return nil })
	})
	if err != nil {
		return fmt.Errorf("[ValidateStore] read buckets %s: %w", dbPath, err)
	}
	return nil
}

// BboltStore wraps HyperGnomon's internal storage.BboltStore and
// re-exposes civilware's query surface. The underlying store is
// shared with the rest of the indexer via the embedded pointer, so
// changes made through one facade are visible to the other.
type BboltStore struct {
	inner *hgstorage.BboltStore
	// DB exposes the raw bbolt handle, matching civilware's exported
	// `BboltStore.DB` field. Used by consumers that reach into the
	// database directly (e.g. closing the file to release the lock) and
	// by the compat TELA-candidate cache.
	DB *gitbbolt.DB
}

// NewBBoltDB opens or creates a bbolt-backed store. Signature matches
// civilware's `storage.NewBBoltDB(path, name string)` so existing
// callers compile unchanged. HyperGnomon's native NewBboltStore takes
// (dbDir, searchFilter) — we map civilware's `name` onto the searchFilter
// slot because civilware uses `name` to distinguish the primary scan
// DB from secondary class/owner tables, a distinction HyperGnomon
// collapses into a single keyspace.
func NewBBoltDB(path, name string) (*BboltStore, error) {
	inner, err := hgstorage.NewBboltStore(path, name)
	if err != nil {
		return nil, err
	}
	return &BboltStore{inner: inner, DB: inner.DB}, nil
}

// Inner returns the underlying HyperGnomon bbolt store so advanced
// callers (or the indexer facade) can reach HyperGnomon-specific
// methods that don't exist on civilware. Not part of the civilware
// API — HyperGnomon-only escape hatch.
func (b *BboltStore) Inner() *hgstorage.BboltStore { return b.inner }

// Close releases the database handle. Idempotent.
func (b *BboltStore) Close() error { return b.inner.Close() }

// GetLastIndexHeight returns the height at which the last scan batch
// flushed. Zero for a fresh DB.
func (b *BboltStore) GetLastIndexHeight() (int64, error) {
	return b.inner.GetLastIndexHeight()
}

// GetAllOwnersAndSCIDs returns every indexed SCID mapped to its owner
// address. Civilware returns this as a plain map[string]string (no
// error), HyperGnomon returns (map, err) — we flatten by treating any
// internal error as an empty map (log via caller-provided logger is
// the right channel; the civilware shape can't surface errors).
func (b *BboltStore) GetAllOwnersAndSCIDs() map[string]string {
	m, err := b.inner.GetAllOwnersAndSCIDs()
	if err != nil || m == nil {
		return map[string]string{}
	}
	return m
}

// GetAllSCIDVariableDetails returns every variable snapshot stored
// for a given SCID across heights, flattened into one slice. Civilware
// returns []*structures.SCIDVariable; HyperGnomon stores per-height
// snapshots so we read the latest-height snapshot (the shape most
// consumers want). Heights earlier than the latest are available via
// GetSCIDVariableDetailsAtHeight on the inner store.
func (b *BboltStore) GetAllSCIDVariableDetails(scid string) []*compatstructures.SCIDVariable {
	// Inner height-addressed lookup: 0 means latest.
	vars, err := b.inner.GetSCIDVariableDetailsAtHeight(scid, 0)
	if err != nil || vars == nil {
		return nil
	}
	// SCIDVariable is aliased to the internal type so this slice is
	// already the right shape. The cast is a compile-time check that
	// the alias stays in sync.
	return vars
}

// GetSCIDValuesByKey returns the string values stored under `key` on
// `scid`. `height` selects the snapshot (0 = latest). `any=true` means
// search ALL stored heights and return every distinct value; `any=false`
// means just the snapshot at `height`. Returns (values, heights) so
// callers can correlate each value to the height it was recorded at.
func (b *BboltStore) GetSCIDValuesByKey(scid string, key interface{}, height int64, matchAny bool) ([]string, []uint64) {
	vals, hs := b.findVars(scid, "", civilwareStr(key), height, matchAny, true /*byKey*/)
	return vals, heightsToUint64(hs)
}

// GetSCIDKeysByValue is the reverse of GetSCIDValuesByKey: given a
// value, return every key that currently holds it on `scid`.
func (b *BboltStore) GetSCIDKeysByValue(scid string, value interface{}, height int64, matchAny bool) ([]string, []uint64) {
	keys, hs := b.findVars(scid, civilwareStr(value), "", height, matchAny, false /*byKey*/)
	return keys, heightsToUint64(hs)
}

// GetOwner returns the install-time owner address of scid, or "" if the SCID
// isn't indexed. Civilware-shape (no error return) — wraps the internal store's
// (string, error) form. HOLOGRAM calls this before AddSCIDToIndex to preserve an
// existing owner across a manual re-index.
func (b *BboltStore) GetOwner(scid string) string {
	owner, err := b.inner.GetOwner(scid)
	if err != nil {
		return ""
	}
	return owner
}

// civilwareStr renders a civilware interface{} key/value as the string the
// internal store compares against. DVM STORE keys/values are String or Uint64;
// this mirrors civilware's stringification so a caller passing either compiles
// and matches.
func civilwareStr(v interface{}) string {
	switch x := v.(type) {
	case string:
		return x
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", x)
	}
}

// heightsToUint64 converts the internal int64 heights to civilware's []uint64
// return shape. Indexed heights are non-negative.
func heightsToUint64(in []int64) []uint64 {
	if in == nil {
		return nil
	}
	out := make([]uint64, len(in))
	for i, h := range in {
		out[i] = uint64(h)
	}
	return out
}

// GetSCIDInteractionHeight returns every height at which `scid`'s
// variables changed. Civilware uses this as the primary "activity
// timeline" query.
func (b *BboltStore) GetSCIDInteractionHeight(scid string) []int64 {
	h, err := b.inner.GetSCIDInteractionHeights(scid)
	if err != nil || h == nil {
		return nil
	}
	return h
}

// findVars is the shared scan used by both value-by-key and
// key-by-value queries. Any-height mode reads every stored snapshot
// and deduplicates by (match, height) pair; single-height mode reads
// only the requested snapshot. Returns parallel slices so each match
// carries the height it was observed at.
func (b *BboltStore) findVars(scid, matchValue, matchKey string, height int64, anyHeight, byKey bool) ([]string, []int64) {
	var matches []string
	var heights []int64
	scan := func(h int64) {
		vars, err := b.inner.GetSCIDVariableDetailsAtHeight(scid, h)
		if err != nil {
			return
		}
		for _, v := range vars {
			k, _ := v.Key.(string)
			val, _ := v.Value.(string)
			if byKey {
				if k == matchKey && val != "" {
					matches = append(matches, val)
					heights = append(heights, h)
				}
			} else {
				if val == matchValue && k != "" {
					matches = append(matches, k)
					heights = append(heights, h)
				}
			}
		}
	}
	if !anyHeight {
		if height == 0 {
			// "Latest" semantics — let Storage pick.
			scan(0)
		} else {
			scan(height)
		}
		return matches, heights
	}
	// Any-height: walk every interaction height.
	for _, h := range b.GetSCIDInteractionHeight(scid) {
		scan(h)
	}
	return matches, heights
}

// GetTxCount returns the cumulative count of transactions of a given type.
// Civilware reads "stats"/"<txType>txcount"; HyperGnomon maintains the same
// three counters under regtxcount/burntxcount/normtxcount. Unknown txTypes
// return 0 (civilware never wrote other keys).
func (b *BboltStore) GetTxCount(txType string) int64 {
	reg, burn, norm, err := b.inner.GetTxCounts()
	if err != nil {
		return 0
	}
	switch txType {
	case "reg":
		return reg
	case "burn":
		return burn
	case "norm":
		return norm
	default:
		return 0
	}
}

// GetAllNormalTxWithSCIDByAddr returns every normal-TX record in which addr
// participated. NormalTXWithSCIDParse is aliased to the internal struct, so
// the returned slice needs no conversion.
func (b *BboltStore) GetAllNormalTxWithSCIDByAddr(addr string) []*compatstructures.NormalTXWithSCIDParse {
	txs, err := b.inner.GetNormalTxWithSCIDByAddr(addr)
	if err != nil || txs == nil {
		return nil
	}
	return txs
}

// GetAllNormalTxWithSCIDBySCID returns every normal-TX record referencing
// scid, backed by HyperGnomon's per-SCID reverse index.
func (b *BboltStore) GetAllNormalTxWithSCIDBySCID(scid string) []*compatstructures.NormalTXWithSCIDParse {
	txs, err := b.inner.GetNormalTxWithSCIDBySCID(scid)
	if err != nil || txs == nil {
		return nil
	}
	return txs
}

// toCompatSCTXParse converts internal SCTXParse records to the civilware wire
// shape.
func toCompatSCTXParse(in []*hgstructures.SCTXParse) []*compatstructures.SCTXParse {
	if in == nil {
		return nil
	}
	out := make([]*compatstructures.SCTXParse, 0, len(in))
	for _, s := range in {
		out = append(out, compatstructures.FromHGSCTXParse(s))
	}
	return out
}

// GetAllSCIDInvokeDetails returns every indexed invocation of scid.
func (b *BboltStore) GetAllSCIDInvokeDetails(scid string) []*compatstructures.SCTXParse {
	details, err := b.inner.GetInvokeDetailsBySCID(scid)
	if err != nil {
		return nil
	}
	return toCompatSCTXParse(details)
}

// GetAllSCIDInvokeDetailsByEntrypoint filters the scid's invocations to one
// entrypoint.
func (b *BboltStore) GetAllSCIDInvokeDetailsByEntrypoint(scid, entrypoint string) []*compatstructures.SCTXParse {
	details, err := b.inner.GetInvokeDetailsBySCID(scid)
	if err != nil {
		return nil
	}
	var out []*compatstructures.SCTXParse
	for _, d := range details {
		if d.Entrypoint == entrypoint {
			out = append(out, compatstructures.FromHGSCTXParse(d))
		}
	}
	return out
}

// GetAllSCIDInvokeDetailsBySigner filters the scid's invocations to one
// sender (civilware calls this "signer").
func (b *BboltStore) GetAllSCIDInvokeDetailsBySigner(scid, signer string) []*compatstructures.SCTXParse {
	details, err := b.inner.GetInvokeDetailsBySCID(scid)
	if err != nil {
		return nil
	}
	var out []*compatstructures.SCTXParse
	for _, d := range details {
		if d.Sender == signer {
			out = append(out, compatstructures.FromHGSCTXParse(d))
		}
	}
	return out
}

// GetGetInfoDetails returns the most recent daemon GetInfo snapshot persisted
// by the indexer's height monitor, or nil before the first successful poll.
func (b *BboltStore) GetGetInfoDetails() *compatstructures.GetInfo {
	info, err := b.inner.GetStoredGetInfo()
	if err != nil || info == nil {
		return nil
	}
	return info
}

// GetSCIDVariableDetailsAtTopoheight returns scid's variable state as of
// topoheight. HyperGnomon stores full-state snapshots per interaction height,
// so this resolves to the newest snapshot whose height <= topoheight (0 means
// "any" and resolves to the latest snapshot).
func (b *BboltStore) GetSCIDVariableDetailsAtTopoheight(scid string, topoheight int64) []*compatstructures.SCIDVariable {
	heights := b.GetSCIDInteractionHeight(scid)
	if len(heights) == 0 {
		return nil
	}
	best := int64(0)
	for _, h := range heights {
		if h > topoheight {
			continue
		}
		if h > best {
			best = h
		}
	}
	if best == 0 {
		// No snapshot at or before topoheight — nothing to return.
		if topoheight != 0 {
			return nil
		}
		// topoheight == 0 (latest): the newest snapshot overall.
		for _, h := range heights {
			if h > best {
				best = h
			}
		}
	}
	vars, err := b.inner.GetSCIDVariableDetailsAtHeight(scid, best)
	if err != nil || vars == nil {
		return nil
	}
	return vars
}

// GetInteractionIndex returns the interaction-height civilware would report
// for a given topoheight. Ported from civilware's BboltStore.GetInteractionIndex:
// heights are sorted most-recent-first; when topoheight exceeds the newest
// height (or rmax is set) the newest height wins; otherwise the first stored
// height at-or-below topoheight (skipping index 0, mirroring civilware) wins;
// else 0.
func (b *BboltStore) GetInteractionIndex(topoheight int64, heights []int64, rmax bool) int64 {
	if len(heights) <= 0 {
		return 0
	}
	sort.SliceStable(heights, func(i, j int) bool {
		return heights[i] > heights[j]
	})
	if topoheight > heights[0] || rmax {
		return heights[0]
	}
	for i := 1; i < len(heights); i++ {
		if heights[i] <= topoheight {
			return heights[i]
		}
	}
	return 0
}

// GetInvalidSCIDDeploys returns every SCID that attempted to deploy but
// failed, mapped to the fees burnt. HyperGnomon stores fees directly.
func (b *BboltStore) GetInvalidSCIDDeploys() map[string]uint64 {
	invalids, err := b.inner.GetInvalidSCIDDeploys()
	if err != nil || invalids == nil {
		return map[string]uint64{}
	}
	return invalids
}

// GetAllMiniblockDetails returns every stored miniblock record keyed by its
// parent block hash. Empty map for a fresh store.
func (b *BboltStore) GetAllMiniblockDetails() map[string][]*compatstructures.MBLInfo {
	mbls, err := b.inner.GetAllMiniblockDetails()
	if err != nil || mbls == nil {
		return map[string][]*compatstructures.MBLInfo{}
	}
	out := make(map[string][]*compatstructures.MBLInfo, len(mbls))
	for k, v := range mbls {
		out[k] = toCompatMBLInfo(v)
	}
	return out
}

// GetMiniblockDetailsByHash returns the miniblocks of one block, or nil when
// none are stored.
func (b *BboltStore) GetMiniblockDetailsByHash(blid string) []*compatstructures.MBLInfo {
	mbls, err := b.inner.GetMiniblockDetailsByHash(blid)
	if err != nil || mbls == nil {
		return nil
	}
	return toCompatMBLInfo(mbls)
}

// GetMiniblockCountByAddress counts stored miniblocks whose miner resolves to
// addr.
func (b *BboltStore) GetMiniblockCountByAddress(addr string) int64 {
	n, err := b.inner.GetMiniblockCountByAddress(addr)
	if err != nil {
		return 0
	}
	return n
}

// telaCandidateBucket stores scid -> status for TELA candidate backfill,
// mirroring civilware's storage/tela_metadata.go bucket. Kept on the compat
// store (not the internal store) because HyperGnomon's core indexer has no
// TELA-candidate concept — the compat surface owns that cache.
const telaCandidateBucket = "telacandidates"

// StoreTelaCandidate records scid's TELA classification status. Idempotent.
func (b *BboltStore) StoreTelaCandidate(scid string, status string) error {
	if scid == "" {
		return nil
	}
	return b.DB.Update(func(tx *gitbbolt.Tx) error {
		bucket, err := tx.CreateBucketIfNotExists([]byte(telaCandidateBucket))
		if err != nil {
			return fmt.Errorf("bucket: %w", err)
		}
		return bucket.Put([]byte(scid), []byte(status))
	})
}

// GetTelaCandidate returns scid's stored TELA status ("" when unknown).
func (b *BboltStore) GetTelaCandidate(scid string) string {
	var status string
	_ = b.DB.View(func(tx *gitbbolt.Tx) error {
		bucket := tx.Bucket([]byte(telaCandidateBucket))
		if bucket == nil {
			return nil
		}
		if v := bucket.Get([]byte(scid)); v != nil {
			status = string(v)
		}
		return nil
	})
	return status
}

// GetAllTelaCandidates returns every scid -> status pair classified so far.
func (b *BboltStore) GetAllTelaCandidates() map[string]string {
	results := make(map[string]string)
	_ = b.DB.View(func(tx *gitbbolt.Tx) error {
		bucket := tx.Bucket([]byte(telaCandidateBucket))
		if bucket == nil {
			return nil
		}
		c := bucket.Cursor()
		for k, v := c.First(); v != nil; k, v = c.Next() {
			results[string(k)] = string(v)
		}
		return nil
	})
	return results
}

// toCompatMBLInfo converts internal miniblock records to the civilware shape.
func toCompatMBLInfo(in []*hgstructures.MBLInfo) []*compatstructures.MBLInfo {
	if in == nil {
		return nil
	}
	out := make([]*compatstructures.MBLInfo, 0, len(in))
	for _, m := range in {
		if m == nil {
			continue
		}
		out = append(out, &compatstructures.MBLInfo{Hash: m.Hash, Miner: m.Miner})
	}
	return out
}

// GetSCIDInteractionByAddr returns every SCID an address interacted with,
// either as a ring participant in a normal TX or as a smart-contract invoker.
// Civilware-faithful: merges the addr's normal-TX SCIDs with every indexed
// owner's SCIDs where addr appears as signer (skipping the name-registry
// builtin, which is not pertinent).
func (b *BboltStore) GetSCIDInteractionByAddr(addr string) []string {
	seen := make(map[string]bool)
	var scids []string
	add := func(scid string) {
		if !seen[scid] {
			seen[scid] = true
			scids = append(scids, scid)
		}
	}
	for _, v := range b.GetAllNormalTxWithSCIDByAddr(addr) {
		add(v.Scid)
	}
	for scid := range b.GetAllOwnersAndSCIDs() {
		if scid == "0000000000000000000000000000000000000000000000000000000000000001" {
			continue
		}
		if len(b.GetAllSCIDInvokeDetailsBySigner(scid, addr)) > 0 {
			add(scid)
		}
	}
	return scids
}
