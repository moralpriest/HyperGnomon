package storage

import (
	"bytes"
	"cmp"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unsafe"

	"github.com/vmihailenco/msgpack/v5"

	rpcc "github.com/deroproject/derohe/rpc"
	"github.com/sirupsen/logrus"
	bolt "go.etcd.io/bbolt"

	"github.com/hypergnomon/hypergnomon/structures"
)

var logger = logrus.WithField("pkg", "storage")

// Bucket names
var (
	bucketStats   = []byte("stats")
	bucketOwners  = []byte("owners")
	bucketHeaders = []byte("headers")
	bucketClass   = []byte("class") // Route B: "<class>|<BE8:h>|<scid>" -> ClassMeta (typed v1 tag 0x04; legacy msgpack fixmap reads via decodeClassMeta)
	bucketTags    = []byte("tags")  // reserved; populated when tag indexes ship
	// bucketHeight: "<scid>:<BE8:h>" -> uvarint occurrence count. Append-only,
	// so a flush costs O(batch delta) instead of rewriting the SCID's full
	// history blob. Legacy layout "<scid>" -> msgpack []int64 stays readable;
	// readers merge legacy-then-composite (chronological for upgraded DBs).
	bucketHeight = []byte("height")
	bucketScVars = []byte("scvars")
	// bucketScVarsLatest stores scid -> BE8 latest snapshot height. It avoids
	// prefix-scanning every scvars snapshot for latest-rating reads.
	bucketScVarsLatest = []byte("scvars_latest")
	// bucketNormTx: "<addr>:<BE8:h>:<txid>:<scid>" -> msgpack
	// NormalTXWithSCIDParse (single record, append-only). Legacy layout
	// "<addr>" -> msgpack list stays readable; readers merge
	// legacy-then-composite.
	bucketNormTx  = []byte("normaltxwithscid")
	bucketInvalid = []byte("invalidscidinvokes")
	// bucketNormTxSCID is a reverse index over bucketNormTx keyed by SCID:
	// "<scid>|<addr>:<BE8:h>:<txid>" -> the same single-record value bytes as
	// bucketNormTx. Grows alongside the existing normaltx flush write so
	// GetAllNormalTxWithSCIDBySCID is a prefix scan. Read-only for compat;
	// not rewritten by truncate (it is derived from the same append-only data).
	bucketNormTxSCID = []byte("normaltxwithscid_scid")

	// bucketGetInfo holds the most recent daemon DERO.GetInfo result under a
	// fixed key. Written by the indexer's height monitor each poll cycle so
	// civilware's GetGetInfoDetails returns real chain metrics.
	bucketGetInfo = []byte("getinfo_snapshot")

	// Miniblock store (civilware miniblock-index compat).
	// bucketMBL: "<BE8 block hash>" -> msgpack []*MBLInfo for one block.
	// bucketMBLCount: addr -> BE8 uint64 miniblock count.
	bucketMBL      = []byte("miniblocks")
	bucketMBLCount = []byte("miniblockcount")

	// Route B (DESIGN.md §3) buckets.
	bucketBlockHash   = []byte("blockhashes")  // BE8 height -> 64-hex block hash
	bucketInstalls    = []byte("installs")     // "<BE8:h>|<scid>" -> InstallRecord typed v1; legacy msgpack reads via decodeInstallRecord
	bucketClassIdx    = []byte("class_scid")   // scid -> ClassMeta (typed v1 tag 0x04; legacy msgpack reads via decodeClassMeta; O(1) lookup)
	bucketAddrSCIDs   = []byte("addr_scids")   // parent; per-addr sub-bucket created on demand
	bucketTELAContent = []byte("tela_content") // scid|path -> {body, mime, sha256}
	bucketSCCode      = []byte("sccode")       // scid -> SCCodeEntry (install-time DVM code)
	bucketOwnerSCIDs  = []byte("owner_scids")  // "<owner>|<scid>" -> "" (prefix-scan owner → [scid])
)

// namedBuckets is the complete list of statically-named top-level buckets.
// NewBboltStore creates them and ResetIndex recreates them; it is the single
// source of truth so the two paths can't drift. NOT exhaustive of top-level
// buckets on disk: per-SCID invocation buckets (StoreInvokeDetails and the
// FlushBatch invocation loop, named by raw 64-hex SCID) are dynamic —
// ResetIndex discovers those by enumeration.
var namedBuckets = [][]byte{
	bucketStats, bucketOwners, bucketHeaders,
	bucketClass, bucketTags, bucketHeight,
	bucketScVars, bucketScVarsLatest, bucketNormTx, bucketInvalid,
	bucketNormTxSCID, bucketGetInfo, bucketMBL, bucketMBLCount,
	// Route B M0
	bucketBlockHash, bucketInstalls, bucketClassIdx,
	bucketAddrSCIDs, bucketTELAContent, bucketSCCode,
	bucketOwnerSCIDs,
}

// Key layout helpers: keep binary big-endian so bbolt's byte-order cursor
// walk matches numeric order. Height 0..2^63-1 packs into 8 bytes.

// encHeight BE-encodes a uint64 height into an 8-byte slice.
func encHeight(h int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(h)) // #nosec G115 -- chain heights are 0..2^62 (doc above); negative heights never reach key encoding.
	return b[:]
}

// decHeight reverses encHeight.
func decHeight(b []byte) int64 {
	if len(b) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(b)) // #nosec G115 -- see encHeight: heights are 0..2^62.
}

// installKey packs <BE8:h>|<scid> for prefix scans by height.
func installKey(h int64, scid string) []byte {
	k := make([]byte, 0, 8+1+len(scid))
	k = append(k, encHeight(h)...)
	k = append(k, '|')
	k = append(k, scid...)
	return k
}

// classKey packs <class>|<BE8:h>|<scid> for prefix scans by class.
func classKey(class string, h int64, scid string) []byte {
	k := make([]byte, 0, len(class)+1+8+1+len(scid))
	k = append(k, class...)
	k = append(k, '|')
	k = append(k, encHeight(h)...)
	k = append(k, '|')
	k = append(k, scid...)
	return k
}

// appendHeightKey packs <scid>:<BE8:h> into dst. ':' never occurs in a
// 64-hex SCID, so composite keys can't collide with legacy whole-SCID keys.
func appendHeightKey(dst []byte, scid string, h int64) []byte {
	dst = append(dst, scid...)
	dst = append(dst, ':')
	return append(dst, encHeight(h)...)
}

// appendNormTxKey packs <addr>:<BE8:h>:<txid>:<scid> into dst. ':' never
// occurs in a DERO address, a hex txid, or a hex scid. The SCID is part of
// the key because one multi-asset TX emits one record per (payload SCID,
// ring member) sharing the same (addr, height, txid) — omitting it made the
// later Put silently overwrite the earlier record's SCID association.
// Exact-duplicate records (same addr/height/txid/scid, e.g. two payloads of
// one asset) intentionally coalesce: they are byte-identical.
func appendNormTxKey(dst []byte, addr string, h int64, txid, scid string) []byte {
	dst = append(dst, addr...)
	dst = append(dst, ':')
	dst = append(dst, encHeight(h)...)
	dst = append(dst, ':')
	dst = append(dst, txid...)
	dst = append(dst, ':')
	return append(dst, scid...)
}

// appendNormTxSCIDKey builds the bucketNormTxSCID key:
// "<scid>|<addr>:<BE8:h>:<txid>". The scid comes first so the reverse index is
// a simple prefix scan per SCID. The trailing components mirror
// appendNormTxKey so a record is reconstructable from either index.
func appendNormTxSCIDKey(dst []byte, scid, addr string, h int64, txid string) []byte {
	dst = append(dst, scid...)
	dst = append(dst, '|')
	dst = append(dst, addr...)
	dst = append(dst, ':')
	dst = append(dst, encHeight(h)...)
	dst = append(dst, ':')
	return append(dst, txid...)
}

// sortedBatchKeys returns a map's keys in ascending order. Go map iteration
// is deliberately randomized, but bbolt B+tree inserts are cheaper in key
// order (sequential page fill instead of random splits), so the large
// FlushBatch loops iterate sorted. Measured on the classify-probe flush
// shape (2k snapshots + class metas in one txn, fresh store,
// BenchmarkFlushBatch_VarSnapshotBurst): 19.8 ms random vs 17.5 ms sorted
// (-12%); the sort itself is microseconds.
func sortedBatchKeys[M ~map[string]V, V any](m M) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

// DecodeHeightEntry interprets one KV pair from the "height" bucket in
// either layout and returns the SCID plus the heights it contributes
// (composite uvarint counts expand to repeated heights, matching the legacy
// list semantics). Exposed for the segment-merge path.
func DecodeHeightEntry(k, v []byte) (string, []int64, error) {
	if sep := bytes.IndexByte(k, ':'); sep >= 0 && len(k) == sep+1+8 {
		h := decHeight(k[sep+1:])
		n := uint64(1)
		if len(v) > 0 {
			if cnt, read := binary.Uvarint(v); read > 0 && cnt > 0 {
				n = cnt
			}
		}
		// The store runs NoSync during initial sync, so a torn value after a
		// crash is in-model; bound the count before allocating so one corrupt
		// byte hits the callers' warn-and-skip handling instead of an
		// out-of-memory panic. Same pattern as UnmarshalSCIDVariablesTyped.
		const maxHeightCount = 1 << 20
		if n > maxHeightCount {
			return string(k[:sep]), nil, fmt.Errorf("height count %d exceeds sanity bound %d", n, maxHeightCount)
		}
		heights := make([]int64, n)
		for i := range heights {
			heights[i] = h
		}
		return string(k[:sep]), heights, nil
	}
	var heights []int64
	if err := msgpack.Unmarshal(v, &heights); err != nil {
		return string(k), nil, err
	}
	return string(k), heights, nil
}

// DecodeNormalTxEntry interprets one KV pair from the "normaltxwithscid"
// bucket in either layout and returns the address plus its records.
// Exposed for the segment-merge path. Composite detection only needs the
// first two separators — the full record lives in the value — so the
// <addr>:<BE8:h>:<txid>:<scid> form and the briefly-used scid-less form
// decode identically.
func DecodeNormalTxEntry(k, v []byte) (string, []*structures.NormalTXWithSCIDParse, error) {
	if sep := bytes.IndexByte(k, ':'); sep >= 0 && len(k) > sep+1+8 && k[sep+1+8] == ':' {
		rec := &structures.NormalTXWithSCIDParse{}
		if structures.IsNormalTxTyped(v) {
			// Typed v1 record (current FlushBatch writer).
			if err := rec.UnmarshalTyped(v); err != nil {
				return string(k[:sep]), nil, err
			}
		} else if err := msgpack.Unmarshal(v, rec); err != nil {
			// Legacy single-record msgpack: on-disk records written by the
			// pre-typed FlushBatch and by StoreNormalTxWithSCIDByAddr (which
			// intentionally still emits msgpack). Kept for backward-compat.
			return string(k[:sep]), nil, err
		}
		return string(k[:sep]), []*structures.NormalTXWithSCIDParse{rec}, nil
	}
	var txs []*structures.NormalTXWithSCIDParse
	if err := msgpack.Unmarshal(v, &txs); err != nil {
		return string(k), nil, err
	}
	return string(k), txs, nil
}

// BboltStore implements Storage backed by BoltDB.
//
// No external mutex: bbolt is already single-writer internally (db.rwlock in
// bolt.Tx.WriteBatch path serializes Update calls). Wrapping Update in another
// Lock/Unlock is pure overhead.
type BboltStore struct {
	DB   *bolt.DB
	Path string
}

// NewBboltStore opens or creates a BoltDB database with the 256 MiB mmap
// reservation appropriate for the long-lived main store.
func NewBboltStore(dbDir string, searchFilter string) (*BboltStore, error) {
	return NewBboltStoreWithMmap(dbDir, searchFilter, mainStoreMmapSize)
}

// mainStoreMmapSize reserves the main store's mmap up front. Without it,
// bbolt re-mmaps each time the file doubles — on Windows that tears down and
// recreates the file mapping with transactions quiesced, which stalled write
// bursts: the classify-probe variable flush measured 4.1-4.3 s against a
// growing DB vs 0.41-0.44 s with the reservation (live mainnet daemon, June
// 2026). 256 MiB covers today's ~131 MiB mainnet DB with headroom; bbolt
// ignores the option once the file is larger. Trade-off: on Windows the file
// mapping requires the file itself to be extended, so the DB occupies
// 256 MiB on disk from the start (documented in DOCS/FLAGS.md). On Unix only
// address space is reserved.
const mainStoreMmapSize = 256 << 20

// mainStoreFile is the bbolt filename inside the db dir. Kept as a const so the
// open path and the compact subcommand (CompactBbolt) cannot drift apart.
const mainStoreFile = "HYPERGNOMON.db"

// MainStoreFilePath returns the on-disk path of the bbolt database inside a
// dbDir. Exported so tooling outside this package (e.g. the gnomes compat
// storage layer's ValidateStore) opens the exact same file the store uses
// without duplicating the filename constant.
func MainStoreFilePath(dbDir string) string {
	return filepath.Join(dbDir, mainStoreFile)
}

// NewBboltStoreWithMmap opens or creates a BoltDB database with an explicit
// initial mmap reservation. Short-lived bounded stores — the per-segment
// temp DBs hold ~10k blocks each and many coexist before the merge — pass 0
// so they don't each pre-extend their file on Windows.
func NewBboltStoreWithMmap(dbDir string, searchFilter string, initialMmapSize int) (*BboltStore, error) {
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return nil, fmt.Errorf("create db dir %s: %w", dbDir, err)
	}
	dbPath := filepath.Join(dbDir, mainStoreFile)

	opts := &bolt.Options{
		Timeout:    0,    // 0 = wait indefinitely for lock
		NoGrowSync: true, // skip fsync on db file growth — safe because NoSync already skips fsync
		NoSync:     true, // skip fsync during initial sync for speed; call EnableSync() at chain tip
		// Skip serializing the freelist into every commit; it is rebuilt by
		// scanning the file on open. Removes a per-FlushBatch cost that grows
		// with DB size during initial sync (same trade etcd ships with).
		NoFreelistSync: true,
		FreelistType:   bolt.FreelistMapType,
	}
	if strconv.IntSize == 64 && initialMmapSize > 0 {
		opts.InitialMmapSize = initialMmapSize
	}
	db, err := bolt.Open(dbPath, 0600, opts)
	if err != nil {
		return nil, fmt.Errorf("bbolt open %s: %w", dbPath, err)
	}

	store := &BboltStore{DB: db, Path: dbPath}

	// Create all buckets
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range namedBuckets {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close() // best-effort: the open error is the one worth surfacing
		return nil, fmt.Errorf("bbolt init buckets: %w", err)
	}

	logger.Debugf("BoltDB opened: %s", dbPath)
	return store, nil
}

// EnableSync re-enables fsync after initial sync is complete (caught up to chain tip).
//
// The flip happens inside an empty Update txn: bbolt serializes Update calls
// on its writer mutex and every commit reads db.NoSync, so a bare assignment
// would race with concurrent committers (probeTELA flushes, IndexSingleSCID)
// right at the caught-up transition. The empty txn gives the write a
// happens-before edge with every other commit.
func (s *BboltStore) EnableSync() {
	_ = s.DB.Update(func(*bolt.Tx) error {
		s.DB.NoSync = false
		return nil
	})
	if err := s.DB.Sync(); err != nil {
		logger.Warnf("EnableSync: bbolt Sync returned: %v", err)
	}
	logger.Info("BoltDB sync enabled (caught up to chain tip)")
}

// DisableSync disables fsync for bulk initial sync performance.
// See EnableSync for why the flip rides an empty write txn.
func (s *BboltStore) DisableSync() {
	_ = s.DB.Update(func(*bolt.Tx) error {
		s.DB.NoSync = true
		return nil
	})
	logger.Info("BoltDB sync disabled (initial sync mode)")
}

func (s *BboltStore) Close() error {
	return s.DB.Close()
}

func (s *BboltStore) GetLastIndexHeight() (int64, error) {
	var height int64
	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketStats)
		v := b.Get([]byte("lastindexedheight"))
		if v == nil {
			return nil
		}
		var err error
		height, err = strconv.ParseInt(string(v), 10, 64)
		return err
	})
	return height, err
}

func (s *BboltStore) StoreLastIndexHeight(height int64) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketStats).Put(
			[]byte("lastindexedheight"),
			[]byte(strconv.FormatInt(height, 10)),
		)
	})
}

func (s *BboltStore) StoreOwner(scid, owner string) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		owners := tx.Bucket(bucketOwners)
		isNew := owners.Get([]byte(scid)) == nil
		if err := owners.Put([]byte(scid), []byte(owner)); err != nil {
			return err
		}
		if owner != "" {
			if err := tx.Bucket(bucketOwnerSCIDs).Put(ownerSCIDKey(owner, scid), nil); err != nil {
				return err
			}
		}
		if isNew {
			return addSCIDCountStat(tx, 1)
		}
		return nil
	})
}

func (s *BboltStore) GetOwner(scid string) (string, error) {
	var owner string
	err := s.DB.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketOwners).Get([]byte(scid))
		if v != nil {
			owner = string(v)
		}
		return nil
	})
	return owner, err
}

func (s *BboltStore) StoreInvokeDetails(scid, sender, entrypoint string, height int64, details *structures.SCTXParse) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(scid))
		if err != nil {
			return err
		}
		// Full Txid: 8-char prefix only gives 32 bits of entropy and collides at scale.
		key := fmt.Sprintf("%s:%s:%d:%s", sender, details.Txid, height, entrypoint)
		val, err := msgpack.Marshal(details)
		if err != nil {
			return err
		}
		return b.Put([]byte(key), val)
	})
}

func (s *BboltStore) GetInvokeDetailsBySCID(scid string) ([]*structures.SCTXParse, error) {
	var results []*structures.SCTXParse
	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(scid))
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			detail, err := DecodeSCTXParse(v)
			if err != nil {
				return err
			}
			if detail == nil {
				// Empty stored value — skip rather than append a nil entry
				// (mirrors the segment-merge guard).
				return nil
			}
			results = append(results, detail)
			return nil
		})
	})
	return results, err
}

func (s *BboltStore) StoreSCIDVariableDetails(scid string, vars []*structures.SCIDVariable, height int64) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketScVars)
		key := fmt.Sprintf("%s:%d", scid, height)
		val, err := msgpack.Marshal(vars)
		if err != nil {
			return err
		}
		if err := b.Put([]byte(key), val); err != nil {
			return err
		}
		_, err = putLatestSCVarsHeight(tx.Bucket(bucketScVarsLatest), nil, scid, height)
		return err
	})
}

func (s *BboltStore) GetSCIDVariableDetailsAtHeight(scid string, height int64) ([]*structures.SCIDVariable, error) {
	var vars []*structures.SCIDVariable
	err := s.DB.View(func(tx *bolt.Tx) error {
		var err error
		vars, err = getSCIDVariablesAtHeightTx(tx, scid, height)
		return err
	})
	return vars, err
}

func getSCIDVariablesAtHeightTx(tx *bolt.Tx, scid string, height int64) ([]*structures.SCIDVariable, error) {
	if scid == "" || height <= 0 {
		return nil, nil
	}
	key := fmt.Sprintf("%s:%d", scid, height)
	v := tx.Bucket(bucketScVars).Get([]byte(key))
	if v == nil {
		return nil, nil
	}
	return decodeSCIDVariables(v)
}

func decodeSCIDVariables(v []byte) ([]*structures.SCIDVariable, error) {
	if structures.IsSCIDVariablesTyped(v) {
		return structures.UnmarshalSCIDVariablesTyped(v)
	}
	var vars []*structures.SCIDVariable
	if err := msgpack.Unmarshal(v, &vars); err != nil {
		return nil, err
	}
	return vars, nil
}

func latestSCVarsHeightTx(tx *bolt.Tx, scid string) int64 {
	if scid == "" {
		return 0
	}
	if v := tx.Bucket(bucketScVarsLatest).Get([]byte(scid)); len(v) >= 8 {
		return decHeight(v)
	}
	prefix := []byte(scid + ":")
	var maxH int64
	c := tx.Bucket(bucketScVars).Cursor()
	for k, _ := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, _ = c.Next() {
		h, err := strconv.ParseInt(string(k[len(prefix):]), 10, 64)
		if err != nil {
			continue
		}
		if h > maxH {
			maxH = h
		}
	}
	return maxH
}

// putLatestSCVarsHeight upserts the latest-seen height for scid. keyScratch is a
// reusable key buffer the caller threads across calls: bbolt clones keys on Put
// and never retains the Get key, so one backing slice is safe for both. Returns
// the possibly-grown scratch. The value stays a fresh per-call 8-byte slice for
// simplicity — one call per distinct (scid, height) is far below the volume of
// the arena-backed loops in FlushBatch (an append-only shared arena would also
// be safe, as heightValArena there shows).
func putLatestSCVarsHeight(b *bolt.Bucket, keyScratch []byte, scid string, height int64) ([]byte, error) {
	if scid == "" || height <= 0 {
		return keyScratch, nil
	}
	keyScratch = append(keyScratch[:0], scid...)
	if existing := b.Get(keyScratch); len(existing) >= 8 && decHeight(existing) >= height {
		return keyScratch, nil
	}
	val := make([]byte, 8)
	copy(val, encHeight(height))
	return keyScratch, b.Put(keyScratch, val)
}

func (s *BboltStore) StoreSCIDInteractionHeight(scid string, height int64) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketHeight)
		key := appendHeightKey(make([]byte, 0, len(scid)+1+8), scid, height)
		count := uint64(1)
		if existing := b.Get(key); len(existing) > 0 {
			if prev, read := binary.Uvarint(existing); read > 0 {
				count += prev
			}
		}
		val := make([]byte, binary.MaxVarintLen64)
		val = val[:binary.PutUvarint(val, count)]
		return b.Put(key, val)
	})
}

func (s *BboltStore) GetSCIDInteractionHeights(scid string) ([]int64, error) {
	var heights []int64
	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketHeight)
		// Legacy whole-history blob first (pre-composite writes), then the
		// composite keys in ascending-height cursor order.
		if v := b.Get([]byte(scid)); v != nil {
			if err := msgpack.Unmarshal(v, &heights); err != nil {
				logger.Warnf("heights legacy decode for %s: %v (skipping blob)", scid, err)
				heights = nil
			}
		}
		prefix := append([]byte(scid), ':')
		c := b.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			_, hs, err := DecodeHeightEntry(k, v)
			if err != nil {
				logger.Warnf("heights composite decode for %s: %v", scid, err)
				continue
			}
			heights = append(heights, hs...)
		}
		return nil
	})
	return heights, err
}

func (s *BboltStore) StoreNormalTxWithSCIDByAddr(addr string, ntx *structures.NormalTXWithSCIDParse) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketNormTx)
		key := appendNormTxKey(make([]byte, 0, len(addr)+1+8+1+len(ntx.Txid)+1+len(ntx.Scid)), addr, ntx.Height, ntx.Txid, ntx.Scid)
		val, err := msgpack.Marshal(ntx)
		if err != nil {
			return err
		}
		return b.Put(key, val)
	})
}

func (s *BboltStore) StoreInvalidSCIDDeploys(scid string, fees uint64) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketInvalid).Put([]byte(scid), []byte(strconv.FormatUint(fees, 10)))
	})
}

func (s *BboltStore) StoreTxCounts(reg, burn, norm int64) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketStats)
		// Atomic increment: read existing + add new
		addCount := func(key string, delta int64) error {
			existing := b.Get([]byte(key))
			var current int64
			if existing != nil {
				current, _ = strconv.ParseInt(string(existing), 10, 64)
			}
			return b.Put([]byte(key), []byte(strconv.FormatInt(current+delta, 10)))
		}
		if err := addCount("regtxcount", reg); err != nil {
			return err
		}
		if err := addCount("burntxcount", burn); err != nil {
			return err
		}
		return addCount("normtxcount", norm)
	})
}

func addSCIDCountStat(tx *bolt.Tx, delta int64) error {
	if delta == 0 {
		return nil
	}
	stats := tx.Bucket(bucketStats)
	key := []byte("sc_count")
	if existing := stats.Get(key); existing != nil {
		n, _ := strconv.ParseInt(string(existing), 10, 64)
		return stats.Put(key, []byte(strconv.FormatInt(n+delta, 10)))
	}
	var total int64
	if err := tx.Bucket(bucketOwners).ForEach(func(_, _ []byte) error {
		total++
		return nil
	}); err != nil {
		return err
	}
	return stats.Put(key, []byte(strconv.FormatInt(total, 10)))
}

func (s *BboltStore) GetAllSCIDs() ([]string, error) {
	var scids []string
	err := s.DB.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketOwners).ForEach(func(k, v []byte) error {
			scids = append(scids, string(k))
			return nil
		})
	})
	return scids, err
}

func (s *BboltStore) GetAllOwnersAndSCIDs() (map[string]string, error) {
	result := make(map[string]string)
	err := s.DB.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketOwners).ForEach(func(k, v []byte) error {
			result[string(k)] = string(v)
			return nil
		})
	})
	return result, err
}

func (s *BboltStore) GetSCIDCount() (int64, error) {
	var count int64
	err := s.DB.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(bucketStats).Get([]byte("sc_count")); v != nil {
			n, err := strconv.ParseInt(string(v), 10, 64)
			if err == nil {
				count = n
				return nil
			}
		}
		var n int64
		if err := tx.Bucket(bucketOwners).ForEach(func(_, _ []byte) error {
			n++
			return nil
		}); err != nil {
			return err
		}
		count = n
		return nil
	})
	return count, err
}

func (s *BboltStore) GetOwnersForSCIDs(scids []string) (map[string]string, error) {
	out := make(map[string]string, len(scids))
	if len(scids) == 0 {
		return out, nil
	}
	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketOwners)
		// Hoist one cursor and reuse it for every lookup: bbolt's Bucket.Get
		// creates a fresh Cursor (and grows its descent stack) on each call, so
		// N lookups cost N cursor allocs. One reused cursor resets its stack
		// (stack[:0]) per Seek and keeps the backing array, cutting ~3
		// allocs/lookup. bucketOwners is Put-only (never CreateBucket), so
		// Seek + bytes.Equal reproduces Get's exact-match semantics exactly.
		c := b.Cursor()
		// Reuse one key-scratch buffer across lookups: Seek/bytes.Equal only
		// read the key (bbolt never retains the slice past the call), so
		// append(keyScratch[:0], scid...) avoids the per-lookup []byte(scid)
		// allocation. Only the result string(v) copy remains per hit.
		var keyScratch []byte
		for _, scid := range scids {
			if scid == "" {
				continue
			}
			keyScratch = append(keyScratch[:0], scid...)
			if k, v := c.Seek(keyScratch); k != nil && bytes.Equal(k, keyScratch) {
				out[scid] = string(v)
			}
		}
		return nil
	})
	return out, err
}

// FlushBatch atomically writes all accumulated data in a single BoltDB transaction.
// This is the arena-pattern payoff: one lock acquisition for potentially thousands of records.
func (s *BboltStore) FlushBatch(batch *WriteBatch) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		// Store owners
		ownerBucket := tx.Bucket(bucketOwners)
		var newOwners int64
		// Reused scid key-scratch (Get + Put): bbolt clones keys on Put and never
		// retains the Get key. The owner value stays a fresh per-Put []byte (bbolt
		// holds value headers until commit, so values cannot share a buffer).
		var ownerKeyBuf []byte
		// Sorted keys: 50k registry-fastsync owners inserted in map-random
		// order maximize B+tree page splits (see sortedBatchKeys).
		for _, scid := range sortedBatchKeys(batch.Owners) {
			owner := batch.Owners[scid]
			ownerKeyBuf = append(ownerKeyBuf[:0], scid...)
			if ownerBucket.Get(ownerKeyBuf) == nil {
				newOwners++
			}
			if err := ownerBucket.Put(ownerKeyBuf, []byte(owner)); err != nil {
				return fmt.Errorf("batch owner %s: %w", scid, err)
			}
		}

		// keyBuf is reused across both the invocation and variable loops below.
		// bbolt.Bucket.Put copies the key internally (see bolt.Bucket.Put → node.put
		// which copies into its own arena), so reusing a single backing slice is safe.
		// Pre-size for the worst common case: 64-char addr + ':' + 64-char txid +
		// ':' + up to 20-char int + ':' + ~32-char entrypoint ≈ 184 bytes.
		keyBuf := make([]byte, 0, 192)

		// Store invocations.
		// Old path used fmt.Sprintf("%s:%s:%d:%s", ...) which allocates a fresh string
		// per invocation. Replaced with append + strconv.AppendInt to produce the
		// exact same key bytes without any per-iteration allocation.
		// Invocations cluster by SCID (per-block ordering), so memoizing the
		// last bucket handle skips the CreateBucketIfNotExists lookup and the
		// []byte(scid) copy for consecutive same-SCID records.
		var lastInvScid string
		var lastInvBucket *bolt.Bucket
		// Reused scid bucket-name key: bbolt's CreateBucket clones the key
		// (newKey := cloneBytes(key)) and seek never retains it, so one backing
		// slice is safe. Each unique scid otherwise costs a fresh []byte(scid).
		var invBucketKeyBuf []byte
		for _, inv := range batch.Invocations {
			if inv.Scid == "" {
				// Defense in depth: an empty bucket name makes bbolt error
				// and FlushBatch is all-or-nothing — one malformed record
				// must not drop the whole batch.
				logger.Warnf("batch invoke with empty scid skipped (txid=%s height=%d)", inv.Details.Txid, inv.Height)
				continue
			}
			b := lastInvBucket
			if b == nil || inv.Scid != lastInvScid {
				var err error
				invBucketKeyBuf = append(invBucketKeyBuf[:0], inv.Scid...)
				b, err = tx.CreateBucketIfNotExists(invBucketKeyBuf)
				if err != nil {
					return fmt.Errorf("batch invoke bucket %s: %w", inv.Scid, err)
				}
				lastInvScid, lastInvBucket = inv.Scid, b
			}
			keyBuf = keyBuf[:0]
			keyBuf = append(keyBuf, inv.Sender...)
			keyBuf = append(keyBuf, ':')
			keyBuf = append(keyBuf, inv.Details.Txid...)
			keyBuf = append(keyBuf, ':')
			keyBuf = strconv.AppendInt(keyBuf, inv.Height, 10)
			keyBuf = append(keyBuf, ':')
			keyBuf = append(keyBuf, inv.Entrypoint...)
			var val []byte
			if inv.Details.CanMarshalTurboTyped() {
				val = inv.Details.MarshalTurboTyped()
			} else {
				var err error
				val, err = msgpack.Marshal(inv.Details)
				if err != nil {
					return fmt.Errorf("batch invoke marshal: %w", err)
				}
			}
			if err := b.Put(keyBuf, val); err != nil {
				return err
			}
		}

		// Store variables.
		// Same treatment: fmt.Sprintf("%s:%d", scid, height) → append + AppendInt.
		// Value is now v1 typed (C7 optimization): 6→0 marshal allocs per snapshot.
		// Reader (GetSCIDVariableDetailsAtHeight) dispatches on byte[0] so legacy
		// msgpack-encoded values remain readable until overwritten.
		//
		// CRITICAL: each Put needs its OWN value buffer. bbolt stores the
		// slice header during Put and only copies bytes at commit time, so
		// reusing a single backing array across Puts corrupts all but the
		// last write (earlier nodes end up pointing at mutated bytes).
		// Keys are safe to reuse because bolt's bucket.Put copies keys
		// eagerly (node.put clones the key into its own arena).
		varBucket := tx.Bucket(bucketScVars)
		varLatestBucket := tx.Bucket(bucketScVarsLatest)
		var latestKeyBuf []byte // reused across putLatestSCVarsHeight calls
		// Flat (scid, height) map: sort keys by (scid, height) for deterministic
		// write order and bbolt page locality. The nested layout sorted scids but
		// walked each scid's heights in random map order.
		varKeys := make([]VarKey, 0, len(batch.Variables))
		for k := range batch.Variables {
			varKeys = append(varKeys, k)
		}
		slices.SortFunc(varKeys, func(a, b VarKey) int {
			if c := strings.Compare(a.Scid, b.Scid); c != 0 {
				return c
			}
			return cmp.Compare(a.Height, b.Height)
		})
		for i, k := range varKeys {
			vars := batch.Variables[k]
			keyBuf = keyBuf[:0]
			keyBuf = append(keyBuf, k.Scid...)
			keyBuf = append(keyBuf, ':')
			keyBuf = strconv.AppendInt(keyBuf, k.Height, 10)
			// Exact-size single allocation; the nil-append pattern paid
			// log2(size) realloc+copies per snapshot.
			val := structures.MarshalSCIDVariablesTyped(vars)
			if err := varBucket.Put(keyBuf, val); err != nil {
				return err
			}
			// Latest-height upsert once per scid run, not per snapshot: keys
			// are sorted by (scid, height) ascending, so within a run every
			// successive height beats the previous — the helper's
			// existing>=height skip could never fire (a rw-tx Get sees the
			// tx's own Puts). The run's LAST key holds the batch max; the
			// helper's guard still protects against a higher height already
			// in the DB from an earlier flush.
			if i+1 == len(varKeys) || varKeys[i+1].Scid != k.Scid {
				var phErr error
				latestKeyBuf, phErr = putLatestSCVarsHeight(varLatestBucket, latestKeyBuf, k.Scid, k.Height)
				if phErr != nil {
					return phErr
				}
			}
		}

		// Store interaction heights. Composite keys (<scid>:<BE8:h> -> uvarint
		// count) make this O(batch delta): the legacy one-blob-per-SCID layout
		// re-decoded and rewrote the SCID's entire history every flush, which
		// is quadratic over the life of an active contract during initial sync
		// (measured: 173µs -> 5.7ms per identical flush as history grows
		// 1k -> 100k). Only same-height duplicates read back a (tiny) count.
		heightBucket := tx.Bucket(bucketHeight)
		// Value arena: each record's value is a small uvarint count. One growable
		// arena replaces one make() per *distinct* record. The distinct count
		// isn't known up front (same-height runs collapse below), so grow from a
		// small cap. bbolt keeps each Put'd sub-slice valid until commit; a
		// realloc leaves earlier sub-slices pointing at their still-live old
		// backing array, and appends never mutate an earlier sub-slice's bytes.
		heightValArena := make([]byte, 0, 64)
		for _, scid := range sortedBatchKeys(batch.Heights) {
			heights := batch.Heights[scid]
			// heights is batch-owned scratch (Reset after flush) and arrives
			// near-sorted (ascending block order), so sorting in place to
			// group same-height duplicates is effectively free.
			slices.Sort(heights)
			for i := 0; i < len(heights); {
				h := heights[i]
				count := uint64(1)
				j := i + 1
				for j < len(heights) && heights[j] == h {
					j++
					count++
				}
				i = j
				keyBuf = appendHeightKey(keyBuf[:0], scid, h)
				if existing := heightBucket.Get(keyBuf); len(existing) > 0 {
					if prev, read := binary.Uvarint(existing); read > 0 {
						count += prev
					}
				}
				hStart := len(heightValArena)
				heightValArena = binary.AppendUvarint(heightValArena, count)
				if err := heightBucket.Put(keyBuf, heightValArena[hStart:]); err != nil {
					return err
				}
			}
		}

		// Store normal txs: one composite key per record (append-only), same
		// O(total^2) -> O(delta) reasoning as heights above.
		normBucket := tx.Bucket(bucketNormTx)
		// Sort the flat (addr, tx) records by addr for deterministic write order
		// and bbolt page locality (0-alloc in-place sort).
		slices.SortFunc(batch.NormalTxs, func(a, b NormalTxRef) int {
			return strings.Compare(a.Addr, b.Addr)
		})
		// Value arena: one growable buffer replaces one MarshalTyped make() per
		// record. Same safety argument as heightValArena above: bbolt keeps each
		// Put'd sub-slice valid until commit, a realloc leaves earlier sub-slices
		// on their still-live old backing array, and appends never mutate an
		// earlier sub-slice's bytes.
		normTxValArena := make([]byte, 0, 1024)
		// Reverse index (bucketNormTxSCID): one extra logical Put per record
		// reusing the SAME value bytes. Key is "<scid>|<addr>:<BE8:h>:<txid>"
		// so a per-SCID prefix scan returns every participant record.
		normTxSCIDBucket := tx.Bucket(bucketNormTxSCID)
		for i := range batch.NormalTxs {
			e := &batch.NormalTxs[i]
			keyBuf = appendNormTxKey(keyBuf[:0], e.Addr, e.Tx.Height, e.Tx.Txid, e.Tx.Scid)
			// Typed v1 (tag 0x08): arena-carved value vs msgpack's ~5 allocs.
			// DecodeNormalTxEntry dispatches typed-vs-legacy by byte[0].
			vStart := len(normTxValArena)
			normTxValArena = e.Tx.MarshalTypedAppend(normTxValArena)
			val := normTxValArena[vStart:]
			if err := normBucket.Put(keyBuf, val); err != nil {
				return err
			}
			if err := normTxSCIDBucket.Put(appendNormTxSCIDKey(nil, e.Tx.Scid, e.Addr, e.Tx.Height, e.Tx.Txid), val); err != nil {
				return err
			}
		}

		// Store miniblocks: group consecutive same-block-hash refs into one
		// msgpack []*MBLInfo per blid. Idempotent — a blid already present is
		// left untouched (blocks are mined exactly once per indexer; a reorg's
		// truncate+replay re-writes superseded records via TruncateToHeight).
		if mbB := tx.Bucket(bucketMBL); mbB != nil && len(batch.Miniblocks) > 0 {
			slices.SortFunc(batch.Miniblocks, func(a, b MiniblockRef) int {
				return strings.Compare(a.BlockHash, b.BlockHash)
			})
			var curBlid string
			var group []*structures.MBLInfo
			flushGroup := func() error {
				if curBlid == "" || len(group) == 0 {
					return nil
				}
				if mbB.Get([]byte(curBlid)) != nil {
					return nil // already stored — idempotent overwrite guard
				}
				val, err := msgpack.Marshal(group)
				if err != nil {
					return err
				}
				return mbB.Put([]byte(curBlid), val)
			}
			for i := range batch.Miniblocks {
				r := &batch.Miniblocks[i]
				if r.BlockHash != curBlid {
					if err := flushGroup(); err != nil {
						return err
					}
					curBlid, group = r.BlockHash, group[:0]
				}
				m := r.MBL
				group = append(group, &m)
			}
			if err := flushGroup(); err != nil {
				return err
			}
		}

		// Store invalid SCIDs
		invalidBucket := tx.Bucket(bucketInvalid)
		for scid, fees := range batch.InvalidSCIDs {
			if err := invalidBucket.Put([]byte(scid), []byte(strconv.FormatUint(fees, 10))); err != nil {
				return err
			}
		}

		// Store TX counts (atomic increment)
		statsBucket := tx.Bucket(bucketStats)
		addCount := func(key string, delta int64) error {
			if delta == 0 {
				return nil
			}
			existing := statsBucket.Get([]byte(key))
			var current int64
			if existing != nil {
				current, _ = strconv.ParseInt(string(existing), 10, 64)
			}
			return statsBucket.Put([]byte(key), []byte(strconv.FormatInt(current+delta, 10)))
		}
		if err := addCount("regtxcount", batch.RegTxCount); err != nil {
			return err
		}
		if err := addCount("burntxcount", batch.BurnTxCount); err != nil {
			return err
		}
		if err := addCount("normtxcount", batch.NormTxCount); err != nil {
			return err
		}
		if err := addSCIDCountStat(tx, newOwners); err != nil {
			return err
		}

		// === Route B (DESIGN.md §3) — new buckets, same atomic txn ===

		// Block hashes for reorg detection.
		if len(batch.BlockHashes) > 0 {
			bhBucket := tx.Bucket(bucketBlockHash)
			for h, hash := range batch.BlockHashes {
				if err := bhBucket.Put(encHeight(h), []byte(hash)); err != nil {
					return fmt.Errorf("batch blockhash h=%d: %w", h, err)
				}
			}
		}

		// Installs: height-prefixed, range-scannable. Sort by the BUCKET key
		// order (height, then scid) — installKey is height-prefixed, so the
		// map key's scid-first order would still insert map-randomly.
		if len(batch.Installs) > 0 {
			insBucket := tx.Bucket(bucketInstalls)
			type instRef struct {
				mapKey string
				scid   string
				h      int64
			}
			instKeys := make([]instRef, 0, len(batch.Installs))
			for mapKey := range batch.Installs {
				scid, h, ok := parseSCIDHeightKey(mapKey)
				if !ok {
					continue
				}
				instKeys = append(instKeys, instRef{mapKey: mapKey, scid: scid, h: h})
			}
			slices.SortFunc(instKeys, func(a, b instRef) int {
				if c := cmp.Compare(a.h, b.h); c != 0 {
					return c
				}
				return strings.Compare(a.scid, b.scid)
			})
			for _, k := range instKeys {
				rec := batch.Installs[k.mapKey]
				if err := insBucket.Put(installKey(k.h, k.scid), rec.MarshalTyped()); err != nil {
					return err
				}
			}
		}

		// Class metadata: two writes per SCID.
		//   1. classIdx:  scid -> ClassMeta  (O(1) lookup)
		//   2. class:     <class>|<BE8 h>|<scid> -> ClassMeta  (prefix-scan by class)
		//
		// classPrefix is keyed by height, so re-probing the same SCID at a
		// new chain height would leave a stale entry at the old height and
		// make GetClassInstalls return that SCID twice. Before writing the
		// new key, look up the current classIdx record for this SCID: if its
		// old (class, installHeight) differs from the new one we're about to
		// write, delete the stale classPrefix key so each SCID stays
		// represented exactly once in the class bucket.
		if len(batch.Classes) > 0 {
			classIdx := tx.Bucket(bucketClassIdx)
			classPrefix := tx.Bucket(bucketClass)
			for _, scid := range sortedBatchKeys(batch.Classes) {
				meta := batch.Classes[scid]
				if meta == nil || meta.Class == "" {
					continue
				}
				// Preflight: if we're overwriting a record at a different
				// (class, InstallHeight), delete the stale classPrefix row
				// so each SCID appears exactly once in a class scan.
				// decodeClassMeta transparently handles both typed v1 and
				// legacy msgpack records during the transition window.
				// All class keys reuse the shared FlushBatch keyBuf scratch.
				// bbolt clones keys on Put and never retains the key handed to
				// Get/Delete, so a single backing slice is safe — provided we
				// rebuild keyBuf[:0] before EACH use, issue the stale Delete
				// (built from oldMeta) BEFORE the two Puts, and keep the value on
				// its own MarshalTyped buffer (never keyBuf). The inline
				// class-key build matches classKey()/encHeight byte-for-byte.
				keyBuf = append(keyBuf[:0], scid...)
				if prev := classIdx.Get(keyBuf); prev != nil {
					if oldMeta, err := decodeClassMeta(prev); err == nil && oldMeta != nil {
						if oldMeta.Class != meta.Class || oldMeta.InstallHeight != meta.InstallHeight {
							keyBuf = append(keyBuf[:0], oldMeta.Class...)
							keyBuf = append(keyBuf, '|')
							keyBuf = binary.BigEndian.AppendUint64(keyBuf, uint64(oldMeta.InstallHeight)) // #nosec G115 -- chain heights 0..2^62; matches classKey/encHeight.
							keyBuf = append(keyBuf, '|')
							keyBuf = append(keyBuf, scid...)
							_ = classPrefix.Delete(keyBuf)
						}
					}
				}
				// Typed v1 write — 1 alloc for the buffer, no msgpack
				// reflection. Same slice serves both Puts (bbolt needs the
				// bytes valid until commit, not unique per Put).
				val := meta.MarshalTyped()
				keyBuf = append(keyBuf[:0], scid...)
				if err := classIdx.Put(keyBuf, val); err != nil {
					return err
				}
				keyBuf = append(keyBuf[:0], meta.Class...)
				keyBuf = append(keyBuf, '|')
				keyBuf = binary.BigEndian.AppendUint64(keyBuf, uint64(meta.InstallHeight)) // #nosec G115 -- see above.
				keyBuf = append(keyBuf, '|')
				keyBuf = append(keyBuf, scid...)
				if err := classPrefix.Put(keyBuf, val); err != nil {
					return err
				}
			}
		}

		// SC code snapshots: scid -> SCCodeEntry. Write-once semantics
		// (DERO code is immutable), so we just Put unconditionally — any
		// overwrite is a no-op byte-wise. Typed v1 encoding: 1 alloc
		// per entry (the marshal buffer), zero framing overhead. Msgpack
		// baseline paid 3 allocs + ~16% header bloat.
		//
		// bbolt.Put requires the supplied value to stay valid for the
		// life of the transaction, so we can't reuse a single scratch
		// buffer across iterations — fresh allocation matches the Classes
		// / Installs loops above.
		if len(batch.SCCodes) > 0 {
			codeBucket := tx.Bucket(bucketSCCode)
			for _, scid := range sortedBatchKeys(batch.SCCodes) {
				entry := batch.SCCodes[scid]
				if entry == nil || entry.Code == "" {
					continue
				}
				if err := codeBucket.Put([]byte(scid), entry.MarshalTyped()); err != nil {
					return err
				}
			}
		}

		// Owner → SCIDs reverse index: populated from batch.OwnerSCIDs so
		// listsc_byowner / getscidlist_byaddr don't need a full-owners scan.
		// Value is empty (key-only); the key layout is "<owner>|<scid>" so
		// a prefix scan returns every scid for the given owner.
		if len(batch.OwnerSCIDs) > 0 {
			osBucket := tx.Bucket(bucketOwnerSCIDs)
			// Sorted outer+inner so inserts land in bucket-key order
			// (owner-prefixed composite keys); same page-split reasoning as
			// the Owners loop above.
			for _, owner := range sortedBatchKeys(batch.OwnerSCIDs) {
				if owner == "" {
					continue
				}
				for _, scid := range sortedBatchKeys(batch.OwnerSCIDs[owner]) {
					if err := osBucket.Put(ownerSCIDKey(owner, scid), nil); err != nil {
						return err
					}
				}
			}
		}

		// Address reverse index: nested bucket per address, scid as key.
		// Merge with any existing entry (min/max/sum).
		//
		// v1 typed encoding (DESIGN.md §3 optimization C6): 25-byte fixed
		// layout, tag byte 0x01. Legacy msgpack records (tag 0x80-0x8f
		// fixmap) are still readable — we dispatch on byte[0]. Writes are
		// always v1, so legacy blobs get upgraded on next touch.
		if len(batch.AddrSCIDs) > 0 {
			parent := tx.Bucket(bucketAddrSCIDs)
			// Reused scid key-scratch (Get + Put): bbolt.Bucket.Put clones the
			// key (key = cloneBytes(key)) and Get never retains it, so one
			// backing slice serves both across every pair.
			var scidKeyBuf []byte
			// One exactly-sized value arena for all entries: each encodes to a
			// fixed EncodedAddrSCIDEntrySize, so the pair count gives the precise
			// capacity — a single allocation, no growth, replacing one 25-byte
			// slice per Put. bbolt keeps each Put'd sub-slice valid until commit
			// (the backing array stays alive via its stored headers; a later
			// append never mutates an earlier sub-slice's bytes).
			// Flat (addr, scid) map: sort keys by addr (then scid) so every pair
			// for one addr is contiguous and we CreateBucketIfNotExists once per
			// addr. The sorted-key slice is the loop's one added flush allocation,
			// repaid many times over by the per-addr inner maps this flat layout
			// removes on the build side (AddAddrSCID).
			addrKeys := make([]addrSCIDKey, 0, len(batch.AddrSCIDs))
			for k := range batch.AddrSCIDs {
				addrKeys = append(addrKeys, k)
			}
			slices.SortFunc(addrKeys, func(a, b addrSCIDKey) int {
				if c := strings.Compare(a.addr, b.addr); c != 0 {
					return c
				}
				return strings.Compare(a.scid, b.scid)
			})
			// One exactly-sized value arena for all entries: pair count == map len.
			addrValArena := make([]byte, 0, len(batch.AddrSCIDs)*structures.EncodedAddrSCIDEntrySize)
			var addrBucketKeyBuf []byte // reused addr bucket-name key (CreateBucket clones)
			var subBucket *bolt.Bucket
			var lastAddr string
			for _, k := range addrKeys {
				if subBucket == nil || k.addr != lastAddr {
					addrBucketKeyBuf = append(addrBucketKeyBuf[:0], k.addr...)
					var err error
					subBucket, err = parent.CreateBucketIfNotExists(addrBucketKeyBuf)
					if err != nil {
						return fmt.Errorf("batch addr bucket %s: %w", k.addr, err)
					}
					lastAddr = k.addr
				}
				delta := batch.AddrSCIDs[k]
				scidKeyBuf = append(scidKeyBuf[:0], k.scid...)
				existing := subBucket.Get(scidKeyBuf)
				merged := delta
				if existing != nil {
					var cur structures.AddrSCIDEntry
					var decErr error
					if structures.IsAddrSCIDEntryTyped(existing) {
						decErr = cur.UnmarshalTyped(existing)
					} else {
						decErr = msgpack.Unmarshal(existing, &cur)
					}
					if decErr != nil {
						logger.Warnf("addr_scids decode %s/%s: %v (overwriting)", k.addr, k.scid, decErr)
					} else {
						merged = &structures.AddrSCIDEntry{
							FirstHeight: minInt64(cur.FirstHeight, delta.FirstHeight),
							LastHeight:  maxInt64(cur.LastHeight, delta.LastHeight),
							Count:       cur.Count + delta.Count,
						}
					}
				}
				vStart := len(addrValArena)
				addrValArena = merged.MarshalTypedAppend(addrValArena)
				if err := subBucket.Put(scidKeyBuf, addrValArena[vStart:]); err != nil {
					return err
				}
			}
		}

		// Store last indexed height (crash recovery point). Keeping decimal
		// for backward compat with existing DBs; binary encoding will come in
		// the v1→v2 migration.
		if batch.LastHeight > 0 {
			if err := statsBucket.Put(
				[]byte("lastindexedheight"),
				[]byte(strconv.FormatInt(batch.LastHeight, 10)),
			); err != nil {
				return err
			}
		}

		return nil
	})
}

// parseSCIDHeightKey inverts scidHeightKey. Returns (scid, height, ok).
func parseSCIDHeightKey(k string) (string, int64, bool) {
	at := -1
	for i := len(k) - 1; i >= 0; i-- {
		if k[i] == '@' {
			at = i
			break
		}
	}
	if at < 0 {
		return "", 0, false
	}
	h, err := strconv.ParseInt(k[at+1:], 10, 64)
	if err != nil {
		return "", 0, false
	}
	return k[:at], h, true
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// ========================================================================
// Route B — storage.Storage interface additions
// ========================================================================

// StoreBlockHash writes a block hash outside a FlushBatch. Prefer
// WriteBatch.AddBlockHash for the normal path — this method exists for
// recovery/truncate tools.
func (s *BboltStore) StoreBlockHash(height int64, hash string) error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketBlockHash).Put(encHeight(height), []byte(hash))
	})
}

// GetBlockHash returns the hash stored at the given height, or "" if absent.
func (s *BboltStore) GetBlockHash(height int64) (string, error) {
	var hash string
	err := s.DB.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketBlockHash).Get(encHeight(height))
		if v != nil {
			hash = string(v)
		}
		return nil
	})
	return hash, err
}

// decodeClassMeta dispatches on the leading tag byte to pick between the
// v1 typed decoder and the legacy msgpack decoder. Legacy records upgrade
// in-place on next write (FlushBatch always emits v1). Returned meta is a
// fresh allocation — callers own it.
func decodeClassMeta(v []byte) (*structures.ClassMeta, error) {
	if len(v) == 0 {
		return nil, nil
	}
	var m structures.ClassMeta
	if structures.IsClassMetaTyped(v) {
		if err := m.UnmarshalTyped(v); err != nil {
			return nil, err
		}
		return &m, nil
	}
	if err := msgpack.Unmarshal(v, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func decodeInstallRecord(v []byte) (*structures.InstallRecord, error) {
	if len(v) == 0 {
		return nil, nil
	}
	var r structures.InstallRecord
	if structures.IsInstallRecordTyped(v) {
		if err := r.UnmarshalTyped(v); err != nil {
			return nil, err
		}
		return &r, nil
	}
	if err := msgpack.Unmarshal(v, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func DecodeSCTXParse(v []byte) (*structures.SCTXParse, error) {
	if len(v) == 0 {
		return nil, nil
	}
	var detail structures.SCTXParse
	if structures.IsSCTXParseTurboTyped(v) {
		if err := detail.UnmarshalTurboTyped(v); err != nil {
			return nil, err
		}
		return &detail, nil
	}
	if err := msgpack.Unmarshal(v, &detail); err != nil {
		return nil, err
	}
	return &detail, nil
}

// GetClassInstalls returns classified SCIDs for a given class, one entry
// per unique SCID (the highest-height entry wins). Ordered by install
// height ascending. limit<=0 means unlimited.
//
// Dedup note: classPrefix is keyed by (class, height, scid), so a SCID
// re-indexed at a later chain height used to leak two+ rows. FlushBatch
// now deletes stale rows before writing new ones, but legacy DBs can
// still contain duplicates — we collapse them here at read time so every
// caller (GetTELAApps, GetMyDOCs, OmniSearch, …) sees a clean view.
func (s *BboltStore) GetClassInstalls(class string, limit int) ([]structures.ClassInstall, error) {
	if class == "" {
		return nil, nil
	}
	prefix := append([]byte(class), '|')
	latest := make(map[string]structures.ClassInstall)
	err := s.DB.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketClass).Cursor()
		for k, v := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, v = c.Next() {
			// k = "<class>|<BE8 h>|<scid>"
			rem := k[len(prefix):]
			if len(rem) < 8+1 {
				continue
			}
			h := decHeight(rem[:8])
			scid := string(rem[9:])
			meta, err := decodeClassMeta(v)
			if err != nil {
				logger.Warnf("GetClassInstalls decode %s/%d/%s: %v", class, h, scid, err)
				continue
			}
			if prev, seen := latest[scid]; !seen || h > prev.InstallHeight {
				latest[scid] = structures.ClassInstall{
					SCID:          scid,
					InstallHeight: h,
					Meta:          meta,
				}
			}
		}
		return nil
	})
	// Emit in install-height-ascending order so callers can treat limit as
	// "oldest N" just like before the dedup.
	out := make([]structures.ClassInstall, 0, len(latest))
	for _, inst := range latest {
		out = append(out, inst)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].InstallHeight < out[j].InstallHeight
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, err
}

// GetSCIDClass returns the classifier's stored metadata for a SCID, or nil.
// Dispatches on the leading tag byte so legacy msgpack records still decode
// until they're rewritten as typed v1 on the next class update.
func (s *BboltStore) GetSCIDClass(scid string) (*structures.ClassMeta, error) {
	var meta *structures.ClassMeta
	err := s.DB.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketClassIdx).Get([]byte(scid))
		if v == nil {
			return nil
		}
		m, err := decodeClassMeta(v)
		if err != nil {
			return fmt.Errorf("class_scid decode %s: %w", scid, err)
		}
		meta = m
		return nil
	})
	return meta, err
}

// GetInstallsInRange returns installs with height in [fromHeight, toHeight).
// limit<=0 means unlimited.
func (s *BboltStore) GetInstallsInRange(fromHeight, toHeight int64, limit int) ([]structures.ClassInstall, error) {
	if toHeight <= fromHeight {
		return nil, nil
	}
	start := encHeight(fromHeight)
	var out []structures.ClassInstall
	err := s.DB.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketInstalls).Cursor()
		classBucket := tx.Bucket(bucketClassIdx)
		for k, v := c.Seek(start); k != nil; k, v = c.Next() {
			if len(k) < 8+1 {
				continue
			}
			h := decHeight(k[:8])
			if h >= toHeight {
				break
			}
			scid := string(k[9:])
			if _, err := decodeInstallRecord(v); err != nil {
				logger.Warnf("GetInstallsInRange decode h=%d scid=%s: %v", h, scid, err)
				continue
			}
			// Pull class metadata if classified (typed or legacy).
			var meta *structures.ClassMeta
			if cv := classBucket.Get([]byte(scid)); cv != nil {
				if m, err := decodeClassMeta(cv); err == nil {
					meta = m
				}
			}
			out = append(out, structures.ClassInstall{
				SCID:          scid,
				InstallHeight: h,
				Meta:          meta,
			})
			if limit > 0 && len(out) >= limit {
				return nil
			}
		}
		return nil
	})
	return out, err
}

// GetAddressSCIDs returns the SCIDs an address has interacted with.
// Map value holds first/last interaction heights + count.
func (s *BboltStore) GetAddressSCIDs(addr string) (map[string]*structures.AddrSCIDEntry, error) {
	if addr == "" {
		return nil, nil
	}
	out := make(map[string]*structures.AddrSCIDEntry)
	err := s.DB.View(func(tx *bolt.Tx) error {
		parent := tx.Bucket(bucketAddrSCIDs)
		sub := parent.Bucket([]byte(addr))
		if sub == nil {
			return nil
		}
		return sub.ForEach(func(k, v []byte) error {
			var e structures.AddrSCIDEntry
			var decErr error
			// Tag-byte dispatch: 0x01 is typed v1, 0x80-0x8f is legacy
			// msgpack fixmap. New writes always land as v1.
			if structures.IsAddrSCIDEntryTyped(v) {
				decErr = e.UnmarshalTyped(v)
			} else {
				decErr = msgpack.Unmarshal(v, &e)
			}
			if decErr != nil {
				logger.Warnf("addr_scids decode %s/%s: %v", addr, string(k), decErr)
				return nil
			}
			out[string(k)] = &e
			return nil
		})
	})
	return out, err
}

// GetRatingsForSCID returns the TELA ratings stored as variables on scid at
// the given snapshot height. height <= 0 picks the latest snapshot.
//
// Per the canonical TELA spec (github.com/civilware/tela):
//   - The `Rate(r uint64)` entrypoint stores each rater's entry under a
//     STORE key that **is** the rater's wallet address (e.g. "deto1qy…"),
//     not a "rating_<addr>" prefix.
//   - The value is hex-encoded `"<score>_<height>"`, e.g. "35_6937500" →
//     hex "33355f36393337353030".
//   - Score is 0–99; >=50 → like, <50 → dislike (aggregate keys `likes`
//     and `dislikes` count these separately).
//   - There is no `ratingMsg_<addr>` companion key; ratings carry no
//     message payload.
//
// Returns ([], nil) when the SCID has no ratings, or an error on a DB issue.
func (s *BboltStore) GetRatingsForSCID(scid string, height int64) ([]structures.Rating, error) {
	ratings, _, err := s.GetRatingsAndSummaryForSCID(scid, height)
	return ratings, err
}

// GetRatingSummary reads the `likes` and `dislikes` aggregate STORE keys
// that the TELA `Rate()` entrypoint maintains alongside the per-rater
// entries. Returns zero values if neither key is present.
func (s *BboltStore) GetRatingSummary(scid string, height int64) (*structures.RatingSummary, error) {
	_, summary, err := s.GetRatingsAndSummaryForSCID(scid, height)
	return summary, err
}

func (s *BboltStore) GetRatingsAndSummaryForSCID(scid string, height int64) ([]structures.Rating, *structures.RatingSummary, error) {
	if scid == "" {
		return nil, nil, nil
	}

	var ratings []structures.Rating
	var summary *structures.RatingSummary
	err := s.DB.View(func(tx *bolt.Tx) error {
		resolvedHeight := height
		if resolvedHeight <= 0 {
			resolvedHeight = latestSCVarsHeightTx(tx, scid)
		}
		if resolvedHeight <= 0 {
			summary = &structures.RatingSummary{Height: 0}
			return nil
		}
		vars, err := getSCIDVariablesAtHeightTx(tx, scid, resolvedHeight)
		if err != nil {
			return err
		}
		summary = &structures.RatingSummary{Height: resolvedHeight}
		ratings = ratingsFromVars(vars, resolvedHeight, summary)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return ratings, summary, nil
}

func ratingsFromVars(vars []*structures.SCIDVariable, height int64, sum *structures.RatingSummary) []structures.Rating {
	if len(vars) == 0 {
		return nil
	}
	out := make([]structures.Rating, 0, 4)
	// hexScratch backs the per-rater hex decode, reused across the loop so the
	// decode costs ~0 allocs/rater instead of make([]byte)+string(out). The
	// decoded value is transient (parseTELARatingValue feeds it straight to
	// ParseFloat/ParseInt and never retains it), so reuse across raters is safe.
	var hexScratch []byte
	for _, v := range vars {
		keyStr, ok := v.Key.(string)
		if !ok {
			continue
		}
		switch keyStr {
		case "likes":
			if sum != nil {
				sum.Likes = uint64(ratingScore(v.Value))
			}
			continue
		case "dislikes":
			if sum != nil {
				sum.Dislikes = uint64(ratingScore(v.Value))
			}
			continue
		}
		if !looksLikeDEROAddress(keyStr) {
			continue
		}
		score, h, ok := parseTELARatingValue(v.Value, &hexScratch)
		if !ok {
			continue
		}
		if h == 0 {
			h = height
		}
		out = append(out, structures.Rating{
			Rater:  keyStr,
			Score:  score,
			Height: h,
		})
	}
	if len(out) == 0 {
		return nil
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Rater < out[j].Rater })
	return out
}

// looksLikeDEROAddress is a cheap shape check: DERO mainnet addresses
// start with "dero1" and are ~66 chars Base58 (testnet "deto1"). We just
// prefix-check + length-gate here; callers don't need strict checksum
// validation, only "not a regular English variable name."
func looksLikeDEROAddress(s string) bool {
	if len(s) < 40 || len(s) > 128 {
		return false
	}
	if strings.HasPrefix(s, "dero1") || strings.HasPrefix(s, "deto1") || strings.HasPrefix(s, "deroi1") {
		return true
	}
	return false
}

// parseTELARatingValue decodes the TELA rating value format: the stored
// value is hex-encoded `"<score>_<height>"`. Returns (score, height, ok).
// ok=false means the value doesn't match the rating format at all.
func parseTELARatingValue(raw interface{}, scratch *[]byte) (float64, int64, bool) {
	s, ok := raw.(string)
	if !ok {
		return 0, 0, false
	}
	// Best-effort hex decode into the caller's reused scratch (viewed via
	// unsafe.String); TELA stores the value as hex bytes of the ASCII
	// "<score>_<height>" string. Legacy records may already be raw ASCII, so
	// fall through if hex-decode fails. `decoded` is consumed by the
	// ParseFloat/ParseInt below and never retained, so scratch reuse is safe.
	decoded := hexDecodeIfHex(s, scratch)
	// IndexByte + slicing instead of SplitN: the two substrings are zero-copy
	// slices into decoded either way, but SplitN also allocates a 2-element
	// []string header per call — dropped here (−1 alloc/rater). Same semantics:
	// no "_" → reject; split on the first "_" only.
	i := strings.IndexByte(decoded, '_')
	if i < 0 {
		return 0, 0, false
	}
	scoreF, err := strconv.ParseFloat(decoded[:i], 64)
	if err != nil {
		return 0, 0, false
	}
	if scoreF < 0 || scoreF >= 100 {
		return 0, 0, false
	}
	height, err := strconv.ParseInt(decoded[i+1:], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	return scoreF, height, true
}

// hexDecodeIfHex returns the hex-decoded form of s when s is valid even-length
// hex, otherwise s unchanged. The decode is written into the caller-owned
// *scratch buffer (grown once, then reused across the rater loop) and returned
// as an unsafe.String VIEW of that buffer — so after the first sizing there is
// no per-rater allocation. The returned string is TRANSIENT: the sole caller
// (parseTELARatingValue) feeds it straight to ParseFloat/ParseInt and never
// retains it, so the next rater may safely overwrite the scratch. The TELA
// Rate() entrypoint stores rating values as hex-encoded ASCII; the fallback is
// defensive for fixtures that pre-decode.
func hexDecodeIfHex(s string, scratch *[]byte) string {
	if len(s)%2 != 0 {
		return s
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return s
		}
	}
	n := len(s) / 2
	if n == 0 {
		return ""
	}
	if cap(*scratch) < n {
		*scratch = make([]byte, n) // grow once; subsequent raters reuse the backing array
	}
	buf := (*scratch)[:n]
	for i := 0; i < n; i++ {
		buf[i] = hexNibble(s[i*2])<<4 | hexNibble(s[i*2+1])
	}
	// View the owned scratch — never aliases s. Safe because the result is
	// consumed before the next hexDecodeIfHex call overwrites buf.
	return unsafe.String(&buf[0], n)
}

func hexNibble(c byte) byte {
	switch {
	case c >= '0' && c <= '9':
		return c - '0'
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10
	}
	return 0
}

// ResetIndex drops every data bucket then recreates the named ones empty. The
// block hash bucket is preserved so reorg-detection has history to compare
// against when the next scan reaches those heights again. Drops are discovered
// by enumeration rather than a fixed list so the dynamic per-SCID invocation
// buckets (named by raw 64-hex SCID) go too — a fixed list silently leaked
// every one of them across a resync. Safe to call on an open DB; callers
// should ensure no scan is running.
func (s *BboltStore) ResetIndex() error {
	return s.DB.Update(func(tx *bolt.Tx) error {
		// Collect first: deleting while ForEach iterates is undefined in bbolt.
		var toDrop [][]byte
		err := tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			if !bytes.Equal(name, bucketBlockHash) {
				toDrop = append(toDrop, append([]byte(nil), name...))
			}
			return nil
		})
		if err != nil {
			return err
		}
		for _, name := range toDrop {
			if err := tx.DeleteBucket(name); err != nil {
				return fmt.Errorf("drop bucket %s: %w", name, err)
			}
		}
		// IfNotExists: blockhashes was kept, and the enumeration above may
		// legitimately have found nothing on a fresh store.
		for _, name := range namedBuckets {
			if _, err := tx.CreateBucketIfNotExists(name); err != nil {
				return fmt.Errorf("recreate bucket %s: %w", name, err)
			}
		}
		return nil
	})
}

// GetSCIDsByOwner returns every SCID installed by the given owner address.
// Prefix-scans the owner_scids bucket; linear in the count of SCIDs for
// this owner (not the total — bbolt's cursor advances inside a shared page
// as long as keys share the prefix).
//
// Ordering is by SCID bytewise ascending (bbolt's natural key order).
// Returns ([], nil) on unknown owner.
func (s *BboltStore) GetSCIDsByOwner(owner string) ([]string, error) {
	if owner == "" {
		return nil, nil
	}
	prefix := append([]byte(owner), '|')
	var out []string
	err := s.DB.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketOwnerSCIDs).Cursor()
		for k, _ := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, _ = c.Next() {
			scid := string(k[len(prefix):])
			if scid != "" {
				out = append(out, scid)
			}
		}
		return nil
	})
	return out, err
}

// ownerSCIDKey packs owner + scid for the flat bucket layout.
func ownerSCIDKey(owner, scid string) []byte {
	k := make([]byte, 0, len(owner)+1+len(scid))
	k = append(k, owner...)
	k = append(k, '|')
	k = append(k, scid...)
	return k
}

// GetSCCode returns the install-time code snapshot for scid. (nil, nil) on
// miss — caller (indexer.GetSCCode) handles lazy backfill from the daemon.
// Uses the v1 typed encoding — one string allocation for the Code, zero
// header allocations. The msgpack baseline was 3 allocs + ~16% framing
// overhead on 2 KB contracts.
func (s *BboltStore) GetSCCode(scid string) (*structures.SCCodeEntry, error) {
	if scid == "" {
		return nil, nil
	}
	var out *structures.SCCodeEntry
	err := s.DB.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketSCCode).Get([]byte(scid))
		if v == nil {
			return nil
		}
		var e structures.SCCodeEntry
		if err := e.UnmarshalTyped(v); err != nil {
			return err
		}
		out = &e
		return nil
	})
	return out, err
}

// PutSCCode writes a code snapshot. Overwrites any previous entry for scid.
// Normal install/lazy-backfill path is WriteBatch.AddSCCode + FlushBatch —
// this direct method exists for tests and the lazy-backfill cache-fill path
// in indexer.GetSCCode where a one-shot write is cheaper than a pooled batch.
func (s *BboltStore) PutSCCode(scid string, entry *structures.SCCodeEntry) error {
	if scid == "" || entry == nil {
		return nil
	}
	val := entry.MarshalTyped()
	return s.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketSCCode).Put([]byte(scid), val)
	})
}

// telaContentKey packs (scid, path) into a single bucket key. '|' is a safe
// separator because SCIDs are 64-hex and TELA paths never contain '|'.
func telaContentKey(scid, path string) []byte {
	k := make([]byte, 0, len(scid)+1+len(path))
	k = append(k, scid...)
	k = append(k, '|')
	k = append(k, path...)
	return k
}

// GetTELAContent reads a cached TELA content entry. Returns (nil, nil) on miss.
func (s *BboltStore) GetTELAContent(scid, path string) (*structures.TELAContentEntry, error) {
	if scid == "" {
		return nil, nil
	}
	var out *structures.TELAContentEntry
	err := s.DB.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketTELAContent).Get(telaContentKey(scid, path))
		if v == nil {
			return nil
		}
		var e structures.TELAContentEntry
		if structures.IsTELAContentTyped(v) {
			if err := e.UnmarshalTyped(v); err != nil {
				return err
			}
		} else if err := msgpack.Unmarshal(v, &e); err != nil {
			return err
		}
		out = &e
		return nil
	})
	return out, err
}

// PutTELAContent writes a content entry. Overwrites any previous entry for
// the same (scid, path). Caller should have already computed ETag.
func (s *BboltStore) PutTELAContent(scid, path string, entry *structures.TELAContentEntry) error {
	if scid == "" || entry == nil {
		return nil
	}
	val := entry.MarshalTyped()
	return s.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketTELAContent).Put(telaContentKey(scid, path), val)
	})
}

// DeleteTELAContentForSCID removes every cached entry for the given SCID.
// Called from the bus invalidator when EventInstall / EventVarChange fires
// on a TELA app — the content may have been updated on-chain.
func (s *BboltStore) DeleteTELAContentForSCID(scid string) error {
	if scid == "" {
		return nil
	}
	prefix := append([]byte(scid), '|')
	return s.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketTELAContent)
		c := b.Cursor()
		var toDelete [][]byte
		for k, _ := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, _ = c.Next() {
			// Copy — bbolt key slices are only valid inside the callback.
			kc := make([]byte, len(k))
			copy(kc, k)
			toDelete = append(toDelete, kc)
		}
		for _, k := range toDelete {
			if err := b.Delete(k); err != nil {
				return err
			}
		}
		return nil
	})
}

// ratingScore converts a rating variable value to float64. TELA contracts
// store ratings as uint64 / int / string depending on SDK version — tolerate
// all three and return 0 for unparseable values (caller can filter).
func ratingScore(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int64:
		return float64(t)
	case uint64:
		return float64(t)
	case int:
		return float64(t)
	case string:
		f, _ := strconv.ParseFloat(t, 64)
		return f
	}
	return 0
}

// hasPrefix is a 2-operand []byte prefix test; avoids bytes.HasPrefix import
// in a module that already imports it indirectly via bbolt.
func hasPrefix(b, prefix []byte) bool {
	if len(b) < len(prefix) {
		return false
	}
	for i, p := range prefix {
		if b[i] != p {
			return false
		}
	}
	return true
}

func (s *BboltStore) GetInvalidSCIDDeploys() (map[string]uint64, error) {
	result := make(map[string]uint64)
	err := s.DB.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketInvalid).ForEach(func(k, v []byte) error {
			fees, _ := strconv.ParseUint(string(v), 10, 64)
			result[string(k)] = fees
			return nil
		})
	})
	return result, err
}

func (s *BboltStore) GetTxCounts() (reg, burn, norm int64, err error) {
	err = s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketStats)
		parse := func(key string) int64 {
			v := b.Get([]byte(key))
			if v == nil {
				return 0
			}
			n, _ := strconv.ParseInt(string(v), 10, 64)
			return n
		}
		reg = parse("regtxcount")
		burn = parse("burntxcount")
		norm = parse("normtxcount")
		return nil
	})
	return
}

func (s *BboltStore) GetNormalTxWithSCIDByAddr(addr string) ([]*structures.NormalTXWithSCIDParse, error) {
	var txs []*structures.NormalTXWithSCIDParse
	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketNormTx)
		// Legacy whole-history blob first, then composite keys (ascending
		// height in cursor order).
		if v := b.Get([]byte(addr)); v != nil {
			if err := msgpack.Unmarshal(v, &txs); err != nil {
				logger.Warnf("normaltx legacy decode for %s: %v (skipping blob)", addr, err)
				txs = nil
			}
		}
		prefix := append([]byte(addr), ':')
		c := b.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			_, recs, err := DecodeNormalTxEntry(k, v)
			if err != nil {
				logger.Warnf("normaltx composite decode for %s: %v", addr, err)
				continue
			}
			txs = append(txs, recs...)
		}
		return nil
	})
	return txs, err
}

// GetNormalTxWithSCIDBySCID returns every normal-TX record referencing `scid`,
// regardless of participant address. Backed by the append-only reverse index
// bucketNormTxSCID ("<scid>|<addr>:<BE8:h>:<txid>" -> typed record). Order is
// bbolt cursor order (addr, then height).
func (s *BboltStore) GetNormalTxWithSCIDBySCID(scid string) ([]*structures.NormalTXWithSCIDParse, error) {
	if scid == "" {
		return nil, nil
	}
	var txs []*structures.NormalTXWithSCIDParse
	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketNormTxSCID)
		if b == nil {
			return nil
		}
		prefix := append([]byte(scid), '|')
		c := b.Cursor()
		for k, v := c.Seek(prefix); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			rec := &structures.NormalTXWithSCIDParse{}
			if structures.IsNormalTxTyped(v) {
				if err := rec.UnmarshalTyped(v); err != nil {
					logger.Warnf("normaltx scid-index typed decode: %v", err)
					continue
				}
			} else if err := msgpack.Unmarshal(v, rec); err != nil {
				logger.Warnf("normaltx scid-index decode: %v", err)
				continue
			}
			txs = append(txs, rec)
		}
		return nil
	})
	return txs, err
}

// StoreGetInfo persists the most recent daemon DERO.GetInfo result under the
// fixed snapshot key. Overwrites the previous snapshot each poll cycle.
func (s *BboltStore) StoreGetInfo(info *rpcc.GetInfo_Result) error {
	if info == nil {
		return fmt.Errorf("StoreGetInfo: nil info")
	}
	val, err := msgpack.Marshal(info)
	if err != nil {
		return fmt.Errorf("StoreGetInfo marshal: %w", err)
	}
	return s.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketGetInfo).Put([]byte("latest"), val)
	})
}

// GetStoredGetInfo returns the last persisted DERO.GetInfo result, or nil when
// the indexer has never polled (fresh DB or no daemon contact yet).
func (s *BboltStore) GetStoredGetInfo() (*rpcc.GetInfo_Result, error) {
	var info *rpcc.GetInfo_Result
	err := s.DB.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketGetInfo).Get([]byte("latest"))
		if v == nil {
			return nil
		}
		var out rpcc.GetInfo_Result
		if err := msgpack.Unmarshal(v, &out); err != nil {
			return fmt.Errorf("GetStoredGetInfo unmarshal: %w", err)
		}
		info = &out
		return nil
	})
	return info, err
}

// StoreMiniblockDetails stores the miniblock records for the block identified
// by `blid` (the block's 64-hex hash — civilware's key shape). Overwrites any
// prior record for the same blid.
func (s *BboltStore) StoreMiniblockDetails(blid string, mbls []*structures.MBLInfo) error {
	if blid == "" {
		return fmt.Errorf("StoreMiniblockDetails: empty blid")
	}
	val, err := msgpack.Marshal(mbls)
	if err != nil {
		return fmt.Errorf("StoreMiniblockDetails marshal: %w", err)
	}
	return s.DB.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketMBL).Put([]byte(blid), val)
	})
}

// GetAllMiniblockDetails returns every stored miniblock record keyed by its
// parent block hash. Empty map for a fresh store.
func (s *BboltStore) GetAllMiniblockDetails() (map[string][]*structures.MBLInfo, error) {
	result := make(map[string][]*structures.MBLInfo)
	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMBL)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			var mbls []*structures.MBLInfo
			if err := msgpack.Unmarshal(v, &mbls); err != nil {
				logger.Warnf("miniblock decode for %s: %v", string(k), err)
				return nil
			}
			result[string(k)] = mbls
			return nil
		})
	})
	return result, err
}

// GetMiniblockDetailsByHash returns the miniblock records for one block hash,
// or nil when the block has no stored miniblock records.
func (s *BboltStore) GetMiniblockDetailsByHash(blid string) ([]*structures.MBLInfo, error) {
	var mbls []*structures.MBLInfo
	err := s.DB.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketMBL).Get([]byte(blid))
		if v == nil {
			return nil
		}
		if err := msgpack.Unmarshal(v, &mbls); err != nil {
			return fmt.Errorf("miniblock decode for %s: %w", blid, err)
		}
		return nil
	})
	return mbls, err
}

// GetMiniblockCountByAddress counts stored miniblock records whose miner
// resolves to `addr`. Scans the miniblock bucket (counts are derived, so
// reorg truncate cannot drift them). Only final miniblocks carry a resolvable
// miner (see structures.MBLInfo), so this effectively counts final
// miniblocks mined by addr.
func (s *BboltStore) GetMiniblockCountByAddress(addr string) (int64, error) {
	if addr == "" {
		return 0, nil
	}
	var count int64
	err := s.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMBL)
		if b == nil {
			return nil
		}
		return b.ForEach(func(k, v []byte) error {
			var mbls []*structures.MBLInfo
			if err := msgpack.Unmarshal(v, &mbls); err != nil {
				return nil
			}
			for _, m := range mbls {
				if m != nil && m.Miner == addr {
					count++
				}
			}
			return nil
		})
	})
	return count, err
}
