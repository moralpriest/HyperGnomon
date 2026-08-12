package structures

import (
	"github.com/deroproject/derohe/rpc"
	"github.com/deroproject/derohe/transaction"
)

// Method constants -- uint8 instead of string to avoid allocations
const (
	MethodInstallSC uint8 = 1
	MethodInvokeSC  uint8 = 2
)

// SCTXParse represents a parsed smart contract transaction.
// Fixed-size arrays for SCID/TXID reduce GC scanner work.
type SCTXParse struct {
	Txid       string
	Scid       string
	Entrypoint string
	Method     uint8
	ScArgs     rpc.Arguments
	Sender     string
	Payloads   []transaction.AssetPayload
	Fees       uint64
	Height     int64
}

// Reset zeroes all fields for sync.Pool reuse (arena-style recycling).
func (s *SCTXParse) Reset() {
	s.Txid = ""
	s.Scid = ""
	s.Entrypoint = ""
	s.Method = 0
	s.ScArgs = nil
	s.Sender = ""
	s.Payloads = s.Payloads[:0]
	s.Fees = 0
	s.Height = 0
}

// BlockTxns holds the transaction hashes for a single block.
type BlockTxns struct {
	Topoheight int64
	TxHashes   []string
}

func (b *BlockTxns) Reset() {
	b.Topoheight = 0
	b.TxHashes = b.TxHashes[:0]
}

// SCIDVariable represents a single key-value pair from a smart contract.
type SCIDVariable struct {
	Key   interface{}
	Value interface{}
}

// NormalTXWithSCIDParse represents a normal transaction with a SCID in its payload.
type NormalTXWithSCIDParse struct {
	Txid   string
	Scid   string
	Fees   uint64
	Height int64
}

// InvokeRecord stores SC invocation details for batch writing.
type InvokeRecord struct {
	Scid       string
	Sender     string
	Entrypoint string
	Height     int64
	Details    *SCTXParse
}

// SCIDInfo stores indexed smart contract metadata.
type SCIDInfo struct {
	Owner  string
	Code   string
	Height int64
}

// GetInfoResult caches daemon getinfo response.
type GetInfoResult struct {
	Height       int64
	TopoHeight   int64
	StableHeight int64
	Status       string
}

// MBLInfo describes one miniblock of a DERO block: the miniblock's hash and,
// when resolvable without the daemon's balance tree, its miner address.
// Matches civilware/Gnomon's structures.MBLInfo so the compat surface can
// return it directly. Non-final miniblocks have KeyHash pointers into the
// daemon balance tree, which HyperGnomon cannot resolve, so their Miner is
// left empty; the final miniblock's miner decodes from Miner_TX.MinerAddress.
type MBLInfo struct {
	Hash  string
	Miner string
}

// WorkItem flows through the pipeline stages. Recycled via sync.Pool.
type WorkItem struct {
	Height int64
	// BlockHash is the 64-hex block hash for Height, captured in the fetch
	// stage (also the key under which miniblocks are stored).
	BlockHash string
	BlockTxns *BlockTxns
	SCTxs     []SCTXParse
	RegCount  int64
	BurnCount int64
	NormCount int64
	NormalTxs []NormalTXWithSCIDParse
	// Miniblocks for the block at Height, harvested in the fetch stage where
	// the full block.Block is available. Flushed via WriteBatch.AddMiniblocks.
	Miniblocks []MBLInfo
	Err        error
}

func (w *WorkItem) Reset() {
	w.Height = 0
	w.BlockHash = ""
	w.BlockTxns = nil
	w.SCTxs = w.SCTxs[:0]
	w.RegCount = 0
	w.BurnCount = 0
	w.NormCount = 0
	w.NormalTxs = w.NormalTxs[:0]
	w.Miniblocks = w.Miniblocks[:0]
	w.Err = nil
}

// ----- Route B (DESIGN.md §3) additions -----

// ClassMeta is stored in the class bucket under key
// "<class>:<BE8 install_height>:<scid>" and also on a per-SCID lookup bucket.
// Populated at index time by ClassifySC.
type ClassMeta struct {
	Class         string   `msgpack:"class"`
	Tags          []string `msgpack:"tags"`
	Name          string   `msgpack:"name,omitempty"`
	Desc          string   `msgpack:"desc,omitempty"`
	IconURL       string   `msgpack:"icon,omitempty"`
	DURL          string   `msgpack:"durl,omitempty"`
	Version       string   `msgpack:"version,omitempty"`
	InstallHeight int64    `msgpack:"install_h"`
	LastHeight    int64    `msgpack:"last_h"`

	// Media URLs lifted from the G45 `metadata` JSON blob. G45 assets do not
	// use the `icon` key IconURL was built for — not once across the 45,651-SC
	// mainnet corpus under indexer/testdata — so without these the only
	// media-ish field on /api/assets is empty for every G45 asset.
	//
	// These are URLs only (overwhelmingly `ipfs://`); HyperGnomon never fetches
	// the bytes behind them. Resolving them is the consumer's job.
	//
	// The msgpack tags are load-bearing, not decoration: the classify seed
	// cache still round-trips ClassMeta through msgpack even though the
	// class/class_scid buckets are on the typed v1 encoding.
	Image      string `msgpack:"image,omitempty"`     // `image`, else `backdropImage`
	AltImage   string `msgpack:"alt_image,omitempty"` // `alt-image`, else `alt-backdropImage`
	Audio      string `msgpack:"audio,omitempty"`
	Video      string `msgpack:"video,omitempty"`
	ImagesJSON string `msgpack:"images,omitempty"` // `images` object, verbatim on-chain JSON
}

// InstallRecord is stored in the installs bucket under key
// "<BE8 height>:<scid>". Enables "SCs installed in [h1,h2)" in a prefix scan.
type InstallRecord struct {
	Owner      string `msgpack:"owner"`
	Entrypoint string `msgpack:"entrypoint,omitempty"`
	Fees       uint64 `msgpack:"fees"`
}

// AddrSCIDEntry is the value of a nested addr_scids/<addr>/<scid> record.
// Tracks first/last interaction and count; doubles as a per-address activity
// rollup for /api/address/{addr}/scs.
type AddrSCIDEntry struct {
	FirstHeight int64 `msgpack:"first"`
	LastHeight  int64 `msgpack:"last"`
	Count       int64 `msgpack:"count"`
}

// ClassInstall pairs a classified SCID with its install height. Returned by
// GetClassInstalls for the class index prefix scan.
type ClassInstall struct {
	SCID          string
	InstallHeight int64
	Meta          *ClassMeta
}

// SCCodeEntry is the install-time smart contract code snapshot, keyed by
// scid. DERO SC code is immutable once installed, so this is a write-once
// record. Populated on install (four sites in the indexer) and lazily
// backfilled on first read for SCIDs indexed before the sccode bucket
// existed.
type SCCodeEntry struct {
	Code          string `msgpack:"code"`
	InstallHeight int64  `msgpack:"h"`
}

// TELAContentEntry is the durable cache value for one (scid, path) pair. The
// ETag is the sha256 hex of Body — precomputed so every cache hit is a
// zero-alloc 304 comparison.
type TELAContentEntry struct {
	Body   []byte `msgpack:"body"`
	MIME   string `msgpack:"mime,omitempty"`
	ETag   string `msgpack:"etag,omitempty"`
	Height int64  `msgpack:"h"`
}

// Rating (spec reference above) — this older godoc block is kept purely
// so that grep finds the historical comment for future archeology. The
// actual type is declared immediately below.
// Rating is one rater's score on a TELA INDEX/DOC contract.
//
// Per the canonical TELA spec (github.com/civilware/tela), the STORE key
// is the rater's wallet address; the value is hex-encoded `"<score>_<height>"`.
// Score is 0-99. No comment field: ratings do not carry a message.
type Rating struct {
	Rater  string  `json:"rater"`
	Score  float64 `json:"score"`
	Height int64   `json:"height"`
}

// RatingSummary aggregates the `likes` / `dislikes` counters that the TELA
// Rate() entrypoint maintains as separate STORE keys. Height is the snapshot
// height the counters were read at.
type RatingSummary struct {
	Height   int64  `json:"height"`
	Likes    uint64 `json:"likes"`
	Dislikes uint64 `json:"dislikes"`
}
