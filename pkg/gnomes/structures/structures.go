// Package structures is the civilware/Gnomon compat surface for shared
// types. Consumers who previously imported
// `github.com/civilware/Gnomon/structures` can rewrite to
// `github.com/hypergnomon/hypergnomon/pkg/gnomes/structures` with no
// additional source changes.
//
// Each exported type either aliases an internal HyperGnomon type (when
// the wire shape matches) or declares a fresh struct with the exact
// civilware shape and provides conversion helpers. Consumers see the
// civilware-documented types; HyperGnomon's internals retain their own
// arena-oriented shape.
//
// v1.0 scope: covers the surface HOLOGRAM imports. Expand as other
// embedders surface missing types.
package structures

import (
	"github.com/deroproject/derohe/rpc"
	"github.com/deroproject/derohe/transaction"

	hgstructures "github.com/hypergnomon/hypergnomon/structures"
)

// GnomonSC registry SCIDs, mirroring civilware's structures package so a
// consumer that hardcodes these (e.g. Engram's name-registry reads) compiles
// unchanged after the import rewrite.
const (
	MAINNET_GNOMON_SCID = "a05395bb0cf77adc850928b0db00eb5ca7a9ccbafd9a38d021c8d299ad5ce1a4"
	TESTNET_GNOMON_SCID = "df3a698af94afb46e7f6de40bbb628df2e10f29f79900928524d97f30a1928a2"
)

// SCTXParse is the civilware wire shape for a single smart-contract
// interaction. HyperGnomon's internal SCTXParse carries the same data but with
// a uint8 Method and ScArgs naming; this declared type keeps the exact
// civilware field set (Scid_hex []byte, Method string, Sc_args rpc.Arguments)
// so consumers compile unchanged. Convert with FromHGSCTXParse.
type SCTXParse struct {
	Txid       string
	Scid       string
	Scid_hex   []byte
	Entrypoint string
	Method     string
	Sc_args    rpc.Arguments
	Sender     string
	Payloads   []transaction.AssetPayload
	Fees       uint64
	Height     int64
}

// FromHGSCTXParse converts an internal HyperGnomon SCTXParse into the
// civilware shape. Maps HyperGnomon's uint8 Method (1=install, 2=invoke) onto
// civilware's strings ("installsc"/"scinvoke") and fills Scid_hex from Scid.
func FromHGSCTXParse(in *hgstructures.SCTXParse) *SCTXParse {
	if in == nil {
		return nil
	}
	method := "scinvoke"
	if in.Method == hgstructures.MethodInstallSC {
		method = "installsc"
	}
	return &SCTXParse{
		Txid:       in.Txid,
		Scid:       in.Scid,
		Scid_hex:   []byte(in.Scid),
		Entrypoint: in.Entrypoint,
		Method:     method,
		Sc_args:    in.ScArgs,
		Sender:     in.Sender,
		Payloads:   in.Payloads,
		Fees:       in.Fees,
		Height:     in.Height,
	}
}

// NormalTXWithSCIDParse is a normal transaction whose payload references a
// SCID. The field set (Txid, Scid, Fees, Height) is identical between
// civilware and HyperGnomon, so this is a type alias to the internal struct
// (no conversion needed).
type NormalTXWithSCIDParse = hgstructures.NormalTXWithSCIDParse

// MBLInfo describes one miniblock of a DERO block: the block's hash and, when
// resolvable without the daemon's balance tree, its miner address. Matches
// civilware's structures.MBLInfo byte-for-byte.
type MBLInfo struct {
	Hash  string
	Miner string
}

// GetInfo mirrors civilware's `type GetInfo rpc.GetInfo_Result`. Aliased to
// the derohe RPC struct so JSON field names and assignment work identically.
type GetInfo = rpc.GetInfo_Result

// SCIDVariable is a single key/value pair read from a smart contract's
// STORE. The shape matches civilware's exactly — both fields are
// `interface{}` because DVM STORE values can be either a DERO `Uint64`
// or a `String` (byte sequence, often hex-encoded over RPC).
//
// Type-aliased to hypergnomon/structures.SCIDVariable so a facade call
// path can return the exact same pointer a civilware caller would
// expect. Alias (not new-type) because changing the struct identity
// would break `interface{}` assertions downstream consumers may have
// baked into their code.
type SCIDVariable = hgstructures.SCIDVariable

// FastSyncConfig controls how a newly-created Indexer bootstraps its
// state from the on-chain GnomonSC registry. Mirrors civilware's
// `structures.FastSyncConfig` byte-for-byte so consumers can construct
// one and hand it to `NewIndexer` unchanged.
//
// Field semantics follow civilware's documentation; HyperGnomon's
// internal fastsync path translates to its own Config when NewIndexer
// is called.
type FastSyncConfig struct {
	// Enabled toggles the whole fastsync phase. Off means the indexer
	// scans from height 0 (or StartHeight).
	Enabled bool
	// SkipFSRecheck: when true, skip the per-SCID GetSC revalidation
	// civilware normally runs after registry enumeration. Trusts the
	// GnomonSC registry's data integrity, per civilware/Gnomon #16.
	SkipFSRecheck bool
	// ForceFastSync: runs fastsync even if the indexer already has
	// LastIndexedHeight close to chain tip. Civilware uses this to
	// re-probe for newly-registered SCIDs on request.
	ForceFastSync bool
	// ForceFastSyncDiff: minimum height delta from tip at which
	// fastsync activates. Default 100 per civilware convention.
	ForceFastSyncDiff int64
	// NoCode: when true, the fastsync code probe is skipped and SCs
	// are registered without classification. Civilware wallet+asset
	// runmodes use this to avoid the probe cost.
	NoCode bool
}

// FastSyncImport is one entry handed to (*indexer.Indexer).AddSCIDToIndex to
// inject a specific SCID into the index (civilware's manual-add path, used by
// HOLOGRAM to index a SCID fastsync missed). Mirrors civilware's
// structures.FastSyncImport byte-for-byte so a consumer can build the
// map[string]*FastSyncImport unchanged. HyperGnomon's IndexSingleSCID
// re-extracts the owner itself, so Owner/Height/Headers are advisory here.
type FastSyncImport struct {
	Owner   string
	Height  uint64
	Headers string
}
