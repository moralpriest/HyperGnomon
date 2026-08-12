package storage

import (
	"errors"
	"sync"

	"github.com/hypergnomon/hypergnomon/structures"

	compatstructures "github.com/hypergnomon/hypergnomon/pkg/gnomes/structures"
)

// ErrGravDBNotSupported is the legacy sentinel for the graviton backend.
// HyperGnomon is bbolt-only by design — graviton iterates in hash byte-sorted
// order with no prefix/range queries, so it cannot serve the key-ordered
// Route B scans HyperGnomon is built on, and civilware/Gnomon #24 (graviton
// bloat) has been open 2+ years.
//
// Deprecated: NewGravDB no longer returns this — it warns and hands back a
// vestigial store so a civilware consumer that defaults to dbType="gravdb"
// (e.g. HOLOGRAM) does not fail at pre-open. The kept symbol lets source that
// references it compile, and a BARE GravitonStore's GetLastIndexHeight still
// returns it. See NewGravDB / NewGravDBDelegate.
var ErrGravDBNotSupported = errors.New("hypergnomon: graviton backend is not supported; use the bbolt store instead")

// GravitonStore is a civilware-shape store handle. HyperGnomon has no graviton
// engine, so a GravitonStore is one of two things:
//
//   - DELEGATING (bolt != nil): forwards every read to the bbolt store the
//     indexer actually runs on. The compat indexer wires GravDBBackend to one of
//     these (via NewGravDBDelegate) so a civilware consumer (HOLOGRAM) that reads
//     through GravDBBackend gets REAL data — this matters because HOLOGRAM
//     defaults to dbType="gravdb" and at least one read
//     (LatestInteractionHeight → GravDBBackend.GetSCIDInteractionHeight) is
//     HARDCODED to GravDBBackend with no DBType check.
//   - BARE (bolt == nil): the vestigial pre-open handle returned by NewGravDB.
//     Reads return empties; GetLastIndexHeight returns ErrGravDBNotSupported.
//     The compat indexer discards the bare handle the caller passes and wires a
//     delegating one instead.
type GravitonStore struct {
	bolt *BboltStore
}

// NewGravDBDelegate wraps a bbolt store as a civilware-shape GravitonStore that
// forwards all reads to bbolt. HyperGnomon-internal (not part of the civilware
// API); used by the compat indexer to populate GravDBBackend so both
// gravdb-dispatched reads and hardcoded-GravDBBackend reads land on the real
// store even though HyperGnomon runs on bbolt.
func NewGravDBDelegate(b *BboltStore) *GravitonStore { return &GravitonStore{bolt: b} }

var gravWarnOnce sync.Once

// NewGravDB no longer fails. Graviton is unsupported, but erroring here breaks
// drop-in for civilware consumers that default to dbType="gravdb" (HOLOGRAM):
// they check the error and abort before the indexer ever gets a chance to fall
// back to bbolt. Instead we warn once and return a BARE (non-delegating)
// vestigial store; the real work happens on the bbolt store the consumer also
// opens, which the compat indexer uses (and wires GravDBBackend to, as a
// delegate) regardless of dbType. It does NOT open a store at `path` — that
// would lock-conflict with the consumer's own NewBBoltDB(path).
func NewGravDB(path, interval string) (*GravitonStore, error) {
	gravWarnOnce.Do(func() {
		structures.Logger.Warnf("[gnomes] graviton backend unsupported in HyperGnomon; the indexer will run on bbolt (dbType=\"gravdb\" is accepted but maps to bbolt)")
	})
	return &GravitonStore{}, nil
}

// Close is a no-op so defer g.Close() compiles and runs without panic.
func (g *GravitonStore) Close() error { return nil }

// GetLastIndexHeight delegates to bbolt when this is a delegating handle;
// a bare handle returns (0, ErrGravDBNotSupported) as it always has.
func (g *GravitonStore) GetLastIndexHeight() (int64, error) {
	if g != nil && g.bolt != nil {
		return g.bolt.GetLastIndexHeight()
	}
	return 0, ErrGravDBNotSupported
}

// GetAllOwnersAndSCIDs delegates to bbolt (delegating handle) or returns an
// empty map (bare handle). Nil-receiver safe.
func (g *GravitonStore) GetAllOwnersAndSCIDs() map[string]string {
	if g != nil && g.bolt != nil {
		return g.bolt.GetAllOwnersAndSCIDs()
	}
	return map[string]string{}
}

// GetAllSCIDVariableDetails delegates to bbolt or returns nil. Return type is
// []*SCIDVariable to match civilware (was []interface{} — a drop-in drift).
func (g *GravitonStore) GetAllSCIDVariableDetails(scid string) []*compatstructures.SCIDVariable {
	if g != nil && g.bolt != nil {
		return g.bolt.GetAllSCIDVariableDetails(scid)
	}
	return nil
}

// GetSCIDValuesByKey delegates to bbolt or returns empty slices. Signature
// matches civilware (key interface{}, []uint64 heights).
func (g *GravitonStore) GetSCIDValuesByKey(scid string, key interface{}, height int64, matchAny bool) ([]string, []uint64) {
	if g != nil && g.bolt != nil {
		return g.bolt.GetSCIDValuesByKey(scid, key, height, matchAny)
	}
	return nil, nil
}

// GetSCIDKeysByValue delegates to bbolt or returns empty slices.
func (g *GravitonStore) GetSCIDKeysByValue(scid string, value interface{}, height int64, matchAny bool) ([]string, []uint64) {
	if g != nil && g.bolt != nil {
		return g.bolt.GetSCIDKeysByValue(scid, value, height, matchAny)
	}
	return nil, nil
}

// GetSCIDInteractionHeight delegates to bbolt or returns nil. This is the read
// HOLOGRAM hardcodes to GravDBBackend regardless of DBType, so delegation here
// is what makes the gravdb-default drop-in actually return data.
func (g *GravitonStore) GetSCIDInteractionHeight(scid string) []int64 {
	if g != nil && g.bolt != nil {
		return g.bolt.GetSCIDInteractionHeight(scid)
	}
	return nil
}

// GetOwner delegates to bbolt or returns "".
func (g *GravitonStore) GetOwner(scid string) string {
	if g != nil && g.bolt != nil {
		return g.bolt.GetOwner(scid)
	}
	return ""
}

// GetTxCount delegates to bbolt or returns 0.
func (g *GravitonStore) GetTxCount(txType string) int64 {
	if g != nil && g.bolt != nil {
		return g.bolt.GetTxCount(txType)
	}
	return 0
}

// GetAllNormalTxWithSCIDByAddr delegates to bbolt or returns nil.
func (g *GravitonStore) GetAllNormalTxWithSCIDByAddr(addr string) []*compatstructures.NormalTXWithSCIDParse {
	if g != nil && g.bolt != nil {
		return g.bolt.GetAllNormalTxWithSCIDByAddr(addr)
	}
	return nil
}

// GetAllNormalTxWithSCIDBySCID delegates to bbolt or returns nil.
func (g *GravitonStore) GetAllNormalTxWithSCIDBySCID(scid string) []*compatstructures.NormalTXWithSCIDParse {
	if g != nil && g.bolt != nil {
		return g.bolt.GetAllNormalTxWithSCIDBySCID(scid)
	}
	return nil
}

// GetAllSCIDInvokeDetails delegates to bbolt or returns nil.
func (g *GravitonStore) GetAllSCIDInvokeDetails(scid string) []*compatstructures.SCTXParse {
	if g != nil && g.bolt != nil {
		return g.bolt.GetAllSCIDInvokeDetails(scid)
	}
	return nil
}

// GetAllSCIDInvokeDetailsByEntrypoint delegates to bbolt or returns nil.
func (g *GravitonStore) GetAllSCIDInvokeDetailsByEntrypoint(scid, entrypoint string) []*compatstructures.SCTXParse {
	if g != nil && g.bolt != nil {
		return g.bolt.GetAllSCIDInvokeDetailsByEntrypoint(scid, entrypoint)
	}
	return nil
}

// GetAllSCIDInvokeDetailsBySigner delegates to bbolt or returns nil.
func (g *GravitonStore) GetAllSCIDInvokeDetailsBySigner(scid, signer string) []*compatstructures.SCTXParse {
	if g != nil && g.bolt != nil {
		return g.bolt.GetAllSCIDInvokeDetailsBySigner(scid, signer)
	}
	return nil
}

// GetGetInfoDetails delegates to bbolt or returns nil.
func (g *GravitonStore) GetGetInfoDetails() *compatstructures.GetInfo {
	if g != nil && g.bolt != nil {
		return g.bolt.GetGetInfoDetails()
	}
	return nil
}

// GetSCIDVariableDetailsAtTopoheight delegates to bbolt or returns nil.
func (g *GravitonStore) GetSCIDVariableDetailsAtTopoheight(scid string, topoheight int64) []*compatstructures.SCIDVariable {
	if g != nil && g.bolt != nil {
		return g.bolt.GetSCIDVariableDetailsAtTopoheight(scid, topoheight)
	}
	return nil
}

// GetInteractionIndex delegates to bbolt or returns 0.
func (g *GravitonStore) GetInteractionIndex(topoheight int64, heights []int64, rmax bool) int64 {
	if g != nil && g.bolt != nil {
		return g.bolt.GetInteractionIndex(topoheight, heights, rmax)
	}
	return 0
}

// GetInvalidSCIDDeploys delegates to bbolt or returns an empty map.
func (g *GravitonStore) GetInvalidSCIDDeploys() map[string]uint64 {
	if g != nil && g.bolt != nil {
		return g.bolt.GetInvalidSCIDDeploys()
	}
	return map[string]uint64{}
}

// GetAllMiniblockDetails delegates to bbolt or returns an empty map.
func (g *GravitonStore) GetAllMiniblockDetails() map[string][]*compatstructures.MBLInfo {
	if g != nil && g.bolt != nil {
		return g.bolt.GetAllMiniblockDetails()
	}
	return map[string][]*compatstructures.MBLInfo{}
}

// GetMiniblockDetailsByHash delegates to bbolt or returns nil.
func (g *GravitonStore) GetMiniblockDetailsByHash(blid string) []*compatstructures.MBLInfo {
	if g != nil && g.bolt != nil {
		return g.bolt.GetMiniblockDetailsByHash(blid)
	}
	return nil
}

// GetMiniblockCountByAddress delegates to bbolt or returns 0.
func (g *GravitonStore) GetMiniblockCountByAddress(addr string) int64 {
	if g != nil && g.bolt != nil {
		return g.bolt.GetMiniblockCountByAddress(addr)
	}
	return 0
}

// GetSCIDInteractionByAddr delegates to bbolt or returns nil.
func (g *GravitonStore) GetSCIDInteractionByAddr(addr string) []string {
	if g != nil && g.bolt != nil {
		return g.bolt.GetSCIDInteractionByAddr(addr)
	}
	return nil
}

// StoreTelaCandidate delegates to bbolt or returns nil.
func (g *GravitonStore) StoreTelaCandidate(scid string, status string) error {
	if g != nil && g.bolt != nil {
		return g.bolt.StoreTelaCandidate(scid, status)
	}
	return nil
}

// GetTelaCandidate delegates to bbolt or returns "".
func (g *GravitonStore) GetTelaCandidate(scid string) string {
	if g != nil && g.bolt != nil {
		return g.bolt.GetTelaCandidate(scid)
	}
	return ""
}

// GetAllTelaCandidates delegates to bbolt or returns an empty map.
func (g *GravitonStore) GetAllTelaCandidates() map[string]string {
	if g != nil && g.bolt != nil {
		return g.bolt.GetAllTelaCandidates()
	}
	return map[string]string{}
}
