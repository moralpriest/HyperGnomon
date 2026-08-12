package storage

import (
	"bytes"
	"strconv"

	bolt "go.etcd.io/bbolt"

	"github.com/hypergnomon/hypergnomon/structures"
)

// TruncateToHeight rolls the index back to a prefix-consistent snapshot as of
// height h in a single atomic transaction. See the Storage interface for the
// correctness contract (which state is restored exactly vs left for replay).
//
// The pass order matters: aggregates are recomputed from the height-keyed detail
// BEFORE that detail is deleted.
//
//	Step 0  build the affected-scid set (interaction heights > h, scvars
//	        snapshots > h, installs > h) — this bounds the per-SCID work.
//	Step 1  recompute addr_scids from surviving (<=h) invocation records.
//	Step 2  delete height-keyed detail > h across every bucket.
//	Step 3  drop latest-wins/derived state for scids first installed > h, lower
//	        the scvars_latest pointers, and recompute sc_count + lastindexedheight.
//
// Bucket coverage — every bucket in the store's bucket list is accounted for:
//
//	blockhashes, installs               deleted > h (BE8 seek)
//	height, scvars, per-SCID invokes    deleted > h (affected-scid bounded)
//	normaltxwithscid                    deleted > h (full scan; addr-keyed)
//	class prefix, class_scid, owners,
//	  sccode, owner_scids               dropped for scids first installed > h
//	tela_content                        dropped for all affected scids (cache)
//	addr_scids                          recomputed from surviving <= h invokes
//	scvars_latest                       lowered to the max surviving <= h
//	stats: sc_count, lastindexedheight  decremented / pinned to h
//	headers, tags                       inert (never written)
//
// KNOWN LIMITATIONS (not restored by truncate alone — see the interface doc):
//   - stats reg/burn/normtxcount: counted-and-discarded, no per-height source →
//     left for replay to recount.
//   - invalidscidinvokes: a failed deploy stores scid->fees with NO height (it
//     returns before AddInstall), so a > h invalid deploy cannot be located and
//     is left in place; replay re-adds it idempotently if still on the new chain.
//   - owners / class_scid re-mutated above the fork (owner transfer, class-meta
//     bump on a <= h scid) keep their > h value pending replay.
//   - UPGRADED DBs: legacy whole-SCID height blobs and whole-addr normtx blobs
//     (splitHeightKey / normTxHeight return not-ok / 0 for them) are invisible
//     to affected-scid discovery, so their > h state survives truncate. Fresh
//     DBs never write these layouts; if reorg wiring is ever pointed at an
//     upgraded DB, migrate the blobs first or discovery misses exactly the SCs
//     with the longest history.
//
// COST MODEL (measured; BenchmarkTruncateToHeight, medians of -count=6): the
// dominant terms scale with TOTAL DB SIZE, not reorg depth — discovery and
// rollback full-scan height, scvars, normtx, installs, addr_scids (every addr
// sub-bucket), and owner_scids, because all but installs are entity-major and
// carry no height-ordered access path. allocs/op fits
//
//	~3.0·(total SCs) + ~12·(distinct addrs) + ~92·(affected SCs) + ~4·(depth)
//
// so a 1-block reorg pays within ~13% of a 1000-block one (8k-SC fixture:
// 30.6k vs 34.7k allocs). Deliberate tradeoff: reorgs are rare and even the
// 32k-SC fixture truncates in ~30 ms — orders of magnitude cheaper than a
// resync — and the alternatives were rejected: a height->scids index taxes
// the alloc-tuned flush path for a cold operation, and a max-height watermark
// early-return would skip the unconditional lastindexedheight rollback below
// (permanent index gap under the fastsync default). Bounding discovery to
// O(reorg depth) is the M2.3 orchestrator's job: it re-fetches the orphaned
// blocks and can pass the touched-scid set in, skipping discovery entirely.
// NOTE the scvars DECIMAL height suffix ("<scid>:<decimal h>") is NOT a
// correctness issue — every consumer parses numerically (splitScVarsKey,
// maxSurvivingScVarsHeight, latestSCVarsHeightTx); it merely forecloses
// seek-by-height there.
func (s *BboltStore) TruncateToHeight(h int64) error {
	if h < 0 {
		h = 0
	}
	return s.DB.Update(func(tx *bolt.Tx) error {
		// ---- Step 0: affected-scid set + scvars keys to delete ----
		affected := map[string]struct{}{}

		// (a) scids with an interaction height > h.
		if hb := tx.Bucket(bucketHeight); hb != nil {
			c := hb.Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				if scid, hh, ok := splitHeightKey(k); ok && hh > h {
					affected[scid] = struct{}{}
				}
			}
		}

		// (b) scids with a scvars snapshot > h. This covers TELA-refresher /
		//     fastsync-probe writes that add a snapshot at chainHeight without an
		//     interaction height, which (a) alone would miss. The delete keys are
		//     collected in the same pass.
		var scvarsToDel [][]byte
		if vb := tx.Bucket(bucketScVars); vb != nil {
			c := vb.Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				if scid, hh, ok := splitScVarsKey(k); ok && hh > h {
					affected[scid] = struct{}{}
					scvarsToDel = append(scvarsToDel, cloneKey(k))
				}
			}
		}

		// (c) scids first installed > h — their entire footprint must go.
		installedAbove := map[string]struct{}{}
		if ib := tx.Bucket(bucketInstalls); ib != nil {
			c := ib.Cursor()
			for k, _ := c.Seek(encHeight(h + 1)); k != nil; k, _ = c.Next() {
				if scid, ok := installKeyScid(k); ok {
					installedAbove[scid] = struct{}{}
					affected[scid] = struct{}{}
				}
			}
		}

		// ---- Step 1: recompute addr_scids from surviving (<=h) detail ----
		// An addr_scids entry can originate from THREE pipeline sources, and the
		// recompute must union all of them or legitimate ≤h edges get destroyed:
		//   (1) invocation records (install/invoke paths pair AddInvocation with
		//       AddAddrSCID);
		//   (2) normal-tx ring membership (processNormalTx pairs AddNormalTx with
		//       AddAddrSCID — NO invocation record);
		//   (3) install-owner edges with no invocation at all (turbo fastsync and
		//       the on-demand refresh path).
		survivors := map[addrSCIDKey]*structures.AddrSCIDEntry{}
		// (1) invocation records of affected scids.
		for scid := range affected {
			invB := tx.Bucket([]byte(scid))
			if invB == nil {
				continue
			}
			if err := invB.ForEach(func(k, _ []byte) error {
				if sender, hh, ok := parseInvocationKey(k); ok && hh <= h {
					mergeAddrTouch(survivors, sender, scid, hh)
				}
				return nil
			}); err != nil {
				return err
			}
		}
		// (2) surviving normal-tx records of affected scids. Folded into the SAME
		// single pass that collects the > h keys Step 2 deletes, so the bucket is
		// full-scanned once, not twice; the affected lookup stays allocation-free
		// (map access via string(bytes) doesn't materialize) so unaffected keys
		// cost nothing extra.
		var normTxToDel [][]byte
		if nb := tx.Bucket(bucketNormTx); nb != nil {
			if err := nb.ForEach(func(k, _ []byte) error {
				if normTxHeight(k) > h {
					normTxToDel = append(normTxToDel, cloneKey(k))
					return nil
				}
				if addr, scid, hh, ok := splitNormTxKey(k); ok && hh <= h {
					if _, aff := affected[string(scid)]; aff {
						mergeAddrTouch(survivors, string(addr), string(scid), hh)
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
		// (3) surviving installs of affected scids: fastsync/refresh write the
		// owner edge without any invocation record. Added only when (1)+(2) left
		// nothing for (owner, scid), so pipeline installs — whose install
		// invocation already contributed — are not double-counted. Best-effort:
		// a fastsync edge later re-touched by real invocations recomputes from
		// those invocations alone (fastsync state is not fresh-sync-identical
		// anyway, see the class InstallHeight caveat on the interface).
		if ib := tx.Bucket(bucketInstalls); ib != nil {
			stop := encHeight(h + 1)
			c := ib.Cursor()
			for k, v := c.First(); k != nil; k, v = c.Next() {
				if len(k) < 9 || k[8] != '|' { // not a <BE8 h>|<scid> key
					continue
				}
				if bytes.Compare(k[:8], stop) >= 0 {
					break // BE8-prefixed keys are height-sorted; rest are > h
				}
				// affected lookup before materializing the scid string — the
				// string(bytes) map access doesn't allocate, so the ≤h walk is
				// free for unaffected scids.
				if _, aff := affected[string(k[9:])]; !aff {
					continue
				}
				rec, err := decodeInstallRecord(v)
				if err != nil || rec == nil || rec.Owner == "" {
					continue
				}
				scid := string(k[9:])
				key := addrSCIDKey{addr: rec.Owner, scid: scid}
				if survivors[key] == nil {
					mergeAddrTouch(survivors, rec.Owner, scid, decHeight(k[:8]))
				}
			}
		}
		if err := applyAddrSCIDRollback(tx, affected, survivors); err != nil {
			return err
		}

		// ---- Step 2: delete height-keyed detail > h ----

		// blockhashes + installs: BE8-height-prefixed, so seek + delete to end.
		// Collect the removed block hashes first so the height-derived miniblock
		// records (keyed by block hash, not height) can be removed in lockstep.
		var mblToDel [][]byte
		if bh := tx.Bucket(bucketBlockHash); bh != nil {
			c := bh.Cursor()
			for k, v := c.Seek(encHeight(h + 1)); k != nil; k, v = c.Next() {
				mblToDel = append(mblToDel, v)
			}
		}
		if err := deleteFromSeek(tx.Bucket(bucketBlockHash), encHeight(h+1)); err != nil {
			return err
		}
		if err := deleteFromSeek(tx.Bucket(bucketInstalls), encHeight(h+1)); err != nil {
			return err
		}

		// height + invocation buckets: bounded to affected scids by prefix/bucket.
		hb := tx.Bucket(bucketHeight)
		for scid := range affected {
			if hb != nil {
				prefix := append([]byte(scid), ':')
				if err := deletePrefixWhere(hb, prefix, func(k []byte) bool {
					_, hh, ok := splitHeightKey(k)
					return ok && hh > h
				}); err != nil {
					return err
				}
			}
			if invB := tx.Bucket([]byte(scid)); invB != nil {
				if err := deleteWhere(invB, func(k []byte) bool {
					_, hh, ok := parseInvocationKey(k)
					return ok && hh > h
				}); err != nil {
					return err
				}
			}
		}

		// scvars: delete the keys collected in Step 0(b).
		if vb := tx.Bucket(bucketScVars); vb != nil {
			for _, k := range scvarsToDel {
				if err := vb.Delete(k); err != nil {
					return err
				}
			}
		}

		// normaltxwithscid: the height is embedded mid-key, so it needs a full
		// scan on the decoded height — already done in Step 1(2), which collected
		// the > h keys; just delete them here.
		if nb := tx.Bucket(bucketNormTx); nb != nil {
			for _, k := range normTxToDel {
				if err := nb.Delete(k); err != nil {
					return err
				}
			}
		}

		// normaltxwithscid_scid: same record set as normaltxwithscid, keyed by
		// scid. Delete the matching keys (> h data only).
		if snb := tx.Bucket(bucketNormTxSCID); snb != nil && len(normTxToDel) > 0 {
			delKeys := make([][]byte, 0, len(normTxToDel))
			for _, k := range normTxToDel {
				if inv, ok := normTxKeyToSCIDKey(k); ok {
					delKeys = append(delKeys, inv)
				}
			}
			for _, k := range delKeys {
				if err := snb.Delete(k); err != nil {
					return err
				}
			}
		}

		// miniblocks: keyed by block hash; delete the hashes removed above.
		if mbB := tx.Bucket(bucketMBL); mbB != nil {
			for _, blid := range mblToDel {
				if err := mbB.Delete(blid); err != nil {
					return err
				}
			}
		}

		// ---- Step 3: latest-wins / derived + aggregate stats ----

		// Entities first created > h: nothing at <=h refers to them. The class
		// prefix row is deleted targeted (via its classIdx meta) instead of by a
		// full class-bucket scan — every class key with BE8 height > h belongs to
		// an installed-above scid (re-classification never raises InstallHeight).
		classPrefix := tx.Bucket(bucketClass)
		classIdx := tx.Bucket(bucketClassIdx)
		ownersB := tx.Bucket(bucketOwners)
		scidKeyedBuckets := [][]byte{bucketOwners, bucketClassIdx, bucketSCCode, bucketInvalid}
		var ownersDeleted int64
		for scid := range installedAbove {
			key := []byte(scid)
			// The refresh path can install with owner=="" and no owners entry, so
			// count what the owners bucket actually loses for the sc_count fixup.
			if ownersB != nil && ownersB.Get(key) != nil {
				ownersDeleted++
			}
			if classPrefix != nil && classIdx != nil {
				if v := classIdx.Get(key); v != nil {
					if meta, err := decodeClassMeta(v); err == nil && meta != nil {
						if err := classPrefix.Delete(classKey(meta.Class, meta.InstallHeight, scid)); err != nil {
							return err
						}
					}
				}
			}
			for _, name := range scidKeyedBuckets {
				if b := tx.Bucket(name); b != nil {
					if err := b.Delete(key); err != nil {
						return err
					}
				}
			}
		}
		// owner_scids: "<owner>|<scid>" — drop any whose scid was installed > h.
		if err := deleteWhere(tx.Bucket(bucketOwnerSCIDs), func(k []byte) bool {
			if i := bytes.LastIndexByte(k, '|'); i >= 0 {
				_, ok := installedAbove[string(k[i+1:])]
				return ok
			}
			return false
		}); err != nil {
			return err
		}

		// tela_content: a durable serve-cache keyed <scid>|<path>. On-chain
		// content for an affected scid may have changed above the fork, so drop
		// its cached bodies (they re-cache on the next serve). Bounded to affected
		// scids (O(reorg depth)), unlike a full-bucket scrub.
		if tc := tx.Bucket(bucketTELAContent); tc != nil {
			for scid := range affected {
				prefix := append([]byte(scid), '|')
				if err := deletePrefixWhere(tc, prefix, func([]byte) bool { return true }); err != nil {
					return err
				}
			}
		}

		// scvars_latest: lower each affected pointer to the max surviving (<=h)
		// snapshot, or drop it entirely. The monotonic putLatestSCVarsHeight
		// helper refuses to lower, so we write the raw value here.
		latest := tx.Bucket(bucketScVarsLatest)
		vb := tx.Bucket(bucketScVars)
		for scid := range affected {
			if latest == nil {
				break
			}
			maxH := maxSurvivingScVarsHeight(vb, scid, h)
			if maxH == 0 {
				if err := latest.Delete([]byte(scid)); err != nil {
					return err
				}
				continue
			}
			val := make([]byte, 8)
			copy(val, encHeight(maxH))
			if err := latest.Put([]byte(scid), val); err != nil {
				return err
			}
		}

		// stats: recompute sc_count from the surviving owners; pin
		// lastindexedheight to h; leave the counted-and-discarded tx-count stats
		// (reg/burn/norm) for replay to recount.
		stats := tx.Bucket(bucketStats)
		if stats == nil {
			return nil
		}
		// sc_count: decrement by the owners entries actually removed (NOT by
		// len(installedAbove) — ownerless refresh-path installs never bumped the
		// count on the way in) rather than rescan the whole bucket.
		if n := ownersDeleted; n > 0 {
			if v := stats.Get([]byte("sc_count")); v != nil {
				if cur, err := strconv.ParseInt(string(v), 10, 64); err == nil {
					nv := cur - n
					if nv < 0 {
						nv = 0
					}
					if err := stats.Put([]byte("sc_count"), []byte(strconv.FormatInt(nv, 10))); err != nil {
						return err
					}
				}
			}
		}
		// lastindexedheight: truncate only rolls back, so never raise it above
		// the current value (truncating to a height >= tip is a no-op here).
		newLast := h
		if v := stats.Get([]byte("lastindexedheight")); v != nil {
			if cur, err := strconv.ParseInt(string(v), 10, 64); err == nil && cur < h {
				newLast = cur
			}
		}
		return stats.Put([]byte("lastindexedheight"), []byte(strconv.FormatInt(newLast, 10)))
	})
}

// applyAddrSCIDRollback replaces each affected (addr, scid) addr_scids entry with
// its recomputed <=h survivor, or deletes it when no <=h interaction survived.
func applyAddrSCIDRollback(tx *bolt.Tx, affected map[string]struct{}, survivors map[addrSCIDKey]*structures.AddrSCIDEntry) error {
	parent := tx.Bucket(bucketAddrSCIDs)
	if parent == nil {
		return nil
	}
	var addrs [][]byte
	if err := parent.ForEach(func(name, v []byte) error {
		if v == nil { // sub-bucket
			addrs = append(addrs, cloneKey(name))
		}
		return nil
	}); err != nil {
		return err
	}
	for _, addrName := range addrs {
		sub := parent.Bucket(addrName)
		if sub == nil {
			continue
		}
		addr := string(addrName)
		type op struct {
			scid string
			val  []byte // nil => delete
		}
		var ops []op
		if err := sub.ForEach(func(scidK, _ []byte) error {
			scid := string(scidK)
			if _, aff := affected[scid]; !aff {
				return nil
			}
			if e := survivors[addrSCIDKey{addr: addr, scid: scid}]; e != nil {
				ops = append(ops, op{scid: scid, val: e.MarshalTypedAppend(nil)})
			} else {
				ops = append(ops, op{scid: scid})
			}
			return nil
		}); err != nil {
			return err
		}
		for _, o := range ops {
			if o.val == nil {
				if err := sub.Delete([]byte(o.scid)); err != nil {
					return err
				}
			} else if err := sub.Put([]byte(o.scid), o.val); err != nil {
				return err
			}
		}
	}
	return nil
}

// deleteFromSeek deletes every key at or after `from` (two-phase: collect then
// delete, since bbolt forbids delete under a live cursor).
func deleteFromSeek(b *bolt.Bucket, from []byte) error {
	if b == nil {
		return nil
	}
	var toDel [][]byte
	c := b.Cursor()
	for k, _ := c.Seek(from); k != nil; k, _ = c.Next() {
		toDel = append(toDel, cloneKey(k))
	}
	for _, k := range toDel {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// deletePrefixWhere deletes keys sharing `prefix` for which pred is true.
func deletePrefixWhere(b *bolt.Bucket, prefix []byte, pred func(k []byte) bool) error {
	if b == nil {
		return nil
	}
	var toDel [][]byte
	c := b.Cursor()
	for k, _ := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, _ = c.Next() {
		if pred(k) {
			toDel = append(toDel, cloneKey(k))
		}
	}
	for _, k := range toDel {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// deleteWhere full-scans b and deletes keys for which pred is true.
func deleteWhere(b *bolt.Bucket, pred func(k []byte) bool) error {
	if b == nil {
		return nil
	}
	var toDel [][]byte
	c := b.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if v == nil { // sub-bucket: not a data key
			continue
		}
		if pred(k) {
			toDel = append(toDel, cloneKey(k))
		}
	}
	for _, k := range toDel {
		if err := b.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// maxSurvivingScVarsHeight returns the greatest scvars snapshot height <= h for
// scid, or 0 if none survive.
func maxSurvivingScVarsHeight(vb *bolt.Bucket, scid string, h int64) int64 {
	if vb == nil {
		return 0
	}
	prefix := append([]byte(scid), ':')
	var maxH int64
	c := vb.Cursor()
	for k, _ := c.Seek(prefix); k != nil && hasPrefix(k, prefix); k, _ = c.Next() {
		hh, err := strconv.ParseInt(string(k[len(prefix):]), 10, 64)
		if err != nil {
			continue
		}
		if hh <= h && hh > maxH {
			maxH = hh
		}
	}
	return maxH
}

func cloneKey(k []byte) []byte {
	c := make([]byte, len(k))
	copy(c, k)
	return c
}

// --- key decoders (height extraction for truncation predicates) ---

// splitHeightKey parses a "<scid>:<BE8 h>" interaction-height key. Legacy
// whole-SCID msgpack keys (no ':' + 8-byte tail) return ok=false.
func splitHeightKey(k []byte) (scid string, height int64, ok bool) {
	sep := bytes.IndexByte(k, ':')
	if sep < 0 || len(k) != sep+1+8 {
		return "", 0, false
	}
	return string(k[:sep]), decHeight(k[sep+1:]), true
}

// splitScVarsKey parses a "<scid>:<decimal h>" scvars key.
func splitScVarsKey(k []byte) (scid string, height int64, ok bool) {
	sep := bytes.IndexByte(k, ':')
	if sep < 0 {
		return "", 0, false
	}
	hh, err := strconv.ParseInt(string(k[sep+1:]), 10, 64)
	if err != nil {
		return "", 0, false
	}
	return string(k[:sep]), hh, true
}

// installKeyScid parses the scid out of a "<BE8 h>|<scid>" install key.
func installKeyScid(k []byte) (string, bool) {
	if len(k) < 9 || k[8] != '|' {
		return "", false
	}
	return string(k[9:]), true
}

// parseInvocationKey parses "<sender>:<txid>:<decimal h>:<entrypoint>".
func parseInvocationKey(k []byte) (sender string, height int64, ok bool) {
	i1 := bytes.IndexByte(k, ':')
	if i1 < 0 {
		return "", 0, false
	}
	rest := k[i1+1:]
	i2 := bytes.IndexByte(rest, ':')
	if i2 < 0 {
		return "", 0, false
	}
	hpart := rest[i2+1:]
	i3 := bytes.IndexByte(hpart, ':')
	if i3 < 0 {
		return "", 0, false
	}
	hh, err := strconv.ParseInt(string(hpart[:i3]), 10, 64)
	if err != nil {
		return "", 0, false
	}
	return string(k[:i1]), hh, true
}

// normTxHeight extracts the BE8 height from "<addr>:<BE8 h>:<txid>:<scid>".
// Legacy whole-addr blob keys (no ':') return 0 (left in place).
func normTxHeight(k []byte) int64 {
	i1 := bytes.IndexByte(k, ':')
	if i1 < 0 || len(k) < i1+1+8 {
		return 0
	}
	return decHeight(k[i1+1 : i1+9])
}

// splitNormTxKey fully parses a "<addr>:<BE8 h>:<txid>:<scid>" key. The scid is
// located from the RIGHT (a ':' byte can legitimately occur inside the BE8
// height) and validated as a 64-char tail. The briefly-used scid-less composite
// form and legacy whole-addr blobs return ok=false — they cannot be attributed
// to a scid, matching normTxHeight's legacy tolerance. addr/scid are returned
// as sub-slices of k (no allocation); callers copy only when they keep them.
func splitNormTxKey(k []byte) (addr, scid []byte, height int64, ok bool) {
	sep := bytes.IndexByte(k, ':')
	if sep < 0 || len(k) <= sep+9 || k[sep+9] != ':' {
		return nil, nil, 0, false
	}
	last := bytes.LastIndexByte(k, ':')
	if last < sep+10 || len(k)-last-1 != 64 {
		return nil, nil, 0, false
	}
	return k[:sep], k[last+1:], decHeight(k[sep+1 : sep+9]), true
}

// normTxKeyToSCIDKey maps a "<addr>:<BE8 h>:<txid>:<scid>" normaltxwithscid
// key to its inverse "<scid>|<addr>:<BE8 h>:<txid>" bucketNormTxSCID form.
// Same right-anchored, BE8-colon-tolerant parsing as splitNormTxKey. Returns
// ok=false for scid-less composite or legacy blob keys.
func normTxKeyToSCIDKey(k []byte) ([]byte, bool) {
	sep := bytes.IndexByte(k, ':')
	if sep < 0 || len(k) <= sep+9 || k[sep+9] != ':' {
		return nil, false
	}
	last := bytes.LastIndexByte(k, ':')
	if last < sep+10 || len(k)-last-1 != 64 {
		return nil, false
	}
	out := make([]byte, 0, len(k)+1)
	out = append(out, k[last+1:]...) // scid
	out = append(out, '|')
	out = append(out, k[:sep]...) // addr
	out = append(out, ':')
	out = append(out, k[sep+1:last]...) // BE8 h ':' txid
	return out, true
}

// mergeAddrTouch folds one (addr, scid, height) interaction into the survivors
// map with the same min/max/sum semantics FlushBatch uses to merge addr_scids
// entries, so a recomputed entry is byte-identical to a fresh-sync one.
func mergeAddrTouch(survivors map[addrSCIDKey]*structures.AddrSCIDEntry, addr, scid string, hh int64) {
	key := addrSCIDKey{addr: addr, scid: scid}
	if e := survivors[key]; e != nil {
		if hh < e.FirstHeight {
			e.FirstHeight = hh
		}
		if hh > e.LastHeight {
			e.LastHeight = hh
		}
		e.Count++
		return
	}
	survivors[key] = &structures.AddrSCIDEntry{FirstHeight: hh, LastHeight: hh, Count: 1}
}
