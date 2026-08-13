// Package gnomes_test is the civilware compat-surface integration
// test. It mirrors HOLOGRAM's exact import paths + call shape (per
// Agent F's teardown of github.com/DHEBP/HOLOGRAM/gnomon.go) so a
// maintainer can see at a glance that the migration is a three-sed
// operation, not a rewrite.
//
// This test lives at the pkg/gnomes root rather than per-subpackage
// because its entire purpose is to exercise all three packages
// together in realistic consumer order.
package gnomes_test

import (
	"os"
	"testing"

	compatindexer "github.com/hypergnomon/hypergnomon/pkg/gnomes/indexer"
	compatstorage "github.com/hypergnomon/hypergnomon/pkg/gnomes/storage"
	compatstructures "github.com/hypergnomon/hypergnomon/pkg/gnomes/structures"
)

// TestCompatSurface_CompilesLikeCivilware is the cheapest guard: it
// uses every type and symbol in a HOLOGRAM-shaped call sequence so
// any API change that breaks consumer code fails here immediately.
// It does NOT connect to a daemon — that's the example in
// pkg/gnomes/example.
func TestCompatSurface_CompilesLikeCivilware(t *testing.T) {
	// 1. Construct the FastSyncConfig as HOLOGRAM does.
	cfg := &compatstructures.FastSyncConfig{
		Enabled:           true,
		SkipFSRecheck:     false,
		ForceFastSync:     false,
		ForceFastSyncDiff: 100,
		NoCode:            false,
	}
	if cfg.ForceFastSyncDiff != 100 {
		t.Fatalf("FastSyncConfig field wiring broken")
	}

	// 2. SCIDVariable type must be usable as map value + typed pointer.
	// Civilware consumers build these at runtime from parsed RPC
	// responses; HOLOGRAM treats it as a plain struct with Key/Value.
	v := &compatstructures.SCIDVariable{Key: "hello", Value: "world"}
	if v.Key != "hello" || v.Value != "world" {
		t.Fatalf("SCIDVariable field access broken")
	}

	// 3. NewIndexer with a nil boltDB returns a typed-but-dead indexer
	// (civilware-shape has no error return, so we signal via DBType=="").
	// gravdb is no longer rejected at the dbType — it warns and falls back to
	// bbolt — so the dead signal here comes from the missing pre-opened store.
	dead := compatindexer.NewIndexer(
		nil, nil, // no pre-opened gravDB/boltDB (concrete-typed nils)
		"gravdb",                              // accepted (warns), but there's no store to run on
		[]string{"telaVersion", "docVersion"}, // civilware ships filter as []string
		0,
		"http://127.0.0.1:10102",
		"daemon",
		false, false, // mbllookup, closeOnDisconnect
		cfg,
		[]string{},
		false, // storeIntegrators
	)
	if dead == nil {
		t.Fatalf("NewIndexer returned nil — civilware shape must return a non-nil pointer")
	}
	if dead.DBType != "" {
		t.Fatalf("nil-boltDB path must return dead indexer with DBType=\"\", got %q", dead.DBType)
	}

	// 3b. FastSyncImport + AddSCIDToIndex are the civilware manual-add path
	// HOLOGRAM v1.0.7 calls. Must compile with the exact map shape and run
	// without panicking (a dead indexer returns an error, which is fine here).
	imports := map[string]*compatstructures.FastSyncImport{
		"a05395bb0cf77adc850928b0db00eb5ca7a9ccbafd9a38d021c8d299ad5ce1a4": {
			Owner:   "dero1qy",
			Height:  1,
			Headers: "",
		},
	}
	if err := dead.AddSCIDToIndex(imports, true, false); err == nil {
		t.Fatalf("AddSCIDToIndex on a dead indexer should error, got nil")
	}

	// Close on a dead indexer is idempotent no-op.
	dead.Close()
	dead.Close()

	// 4. NewIndexerWithDBDir is the HyperGnomon-native entry point
	// for callers who can't use civilware's (gravDB, boltDB, dbType, ...)
	// shape. We DON'T actually call it here because it would try to
	// open an RPC pool; see pkg/gnomes/example for the real usage.
	_ = compatindexer.NewIndexerWithDBDir

	// 5. InitLog is a no-op; must accept any writer.
	compatindexer.InitLog("test", nil)

	// 6. The storage types are importable and have the civilware-
	// shape methods. We don't actually open a store here — just
	// confirm the symbols exist.
	_ = (*compatstorage.BboltStore)(nil)
	_ = (*compatstorage.GravitonStore)(nil)
	_ = compatstorage.NewBBoltDB
	_ = compatstorage.NewGravDB
	_ = compatstorage.ErrGravDBNotSupported //nolint:staticcheck // kept deliberately: this test proves the compat symbol still exists
}

// TestNewGravDB_WarnFallback confirms NewGravDB no longer errors: a civilware
// consumer that defaults to dbType="gravdb" (e.g. HOLOGRAM) must get past its
// own pre-open error check so the indexer can fall back to bbolt. It returns a
// vestigial non-nil store and warns; the legacy sentinel stays exported.
func TestNewGravDB_WarnFallback(t *testing.T) {
	g, err := compatstorage.NewGravDB(t.TempDir(), "25ms")
	if err != nil {
		t.Fatalf("NewGravDB now warn-falls-back to bbolt; want nil error, got %v", err)
	}
	if g == nil {
		t.Fatalf("NewGravDB returned nil store")
	}
	_ = g.Close()

	// Typed-nil receiver methods still don't panic.
	var nilGrav *compatstorage.GravitonStore
	_ = nilGrav.Close()
}

// TestNewIndexer_GravdbBorrowsStore exercises HOLOGRAM's exact drop-in shape
// without a daemon: dbType="gravdb" with a pre-opened boltDB and an unreachable
// endpoint. gravdb is accepted (warn+fallback) and the store is injected, so
// New gets past store resolution and fails only at the RPC pool (dead indexer).
// Crucially the caller's boltDB stays open — the indexer borrows, never closes,
// an injected store.
func TestNewIndexer_GravdbBorrowsStore(t *testing.T) {
	bolt, err := compatstorage.NewBBoltDB(t.TempDir(), "")
	if err != nil {
		t.Fatalf("NewBBoltDB: %v", err)
	}
	defer bolt.Close()

	idx := compatindexer.NewIndexer(
		nil, bolt, "gravdb", nil, 0,
		"127.0.0.1:1", // unreachable → RPC pool fails after store injection
		"daemon", false, false, nil, nil, false,
	)
	if idx == nil {
		t.Fatal("NewIndexer returned nil")
	}
	idx.Close() // dead indexer; must not close the caller's bolt

	if _, err := bolt.GetLastIndexHeight(); err != nil {
		t.Fatalf("NewIndexer closed the caller's boltDB (%v); it must borrow, not own", err)
	}
}

// TestNewIndexer_LiveDropIn is the full HOLOGRAM drop-in against a real daemon.
// Gated on HG_TEST_DAEMON (host:port) so CI without a node skips it. Proves the
// gravdb-default path produces a LIVE bbolt indexer (DBType=="boltdb").
func TestNewIndexer_LiveDropIn(t *testing.T) {
	endpoint := os.Getenv("HG_TEST_DAEMON")
	if endpoint == "" {
		t.Skip("set HG_TEST_DAEMON=host:port to run the live drop-in test")
	}
	bolt, err := compatstorage.NewBBoltDB(t.TempDir(), "")
	if err != nil {
		t.Fatalf("NewBBoltDB: %v", err)
	}
	idx := compatindexer.NewIndexer(
		nil, bolt, "gravdb", nil, 0, endpoint, "daemon", false, false, nil, nil, false,
	)
	if idx == nil || idx.DBType != "boltdb" {
		t.Fatalf("gravdb default must yield a LIVE bbolt indexer; got DBType=%q", idx.DBType)
	}
	// HOLOGRAM (dbType="gravdb") reads through GravDBBackend, so the live facade
	// must wire it (delegating to bbolt), not leave it nil.
	if idx.GravDBBackend == nil {
		t.Fatal("live NewIndexer must wire a delegating GravDBBackend for gravdb-default consumers")
	}
	idx.Close() // closes the borrowed bolt
}

// TestGravDBBackend_DelegatesToBolt is the RUNTIME drop-in guard (the compile
// guard above only proves shapes match). HOLOGRAM defaults to dbType="gravdb"
// and reads through g.Indexer.GravDBBackend — including LatestInteractionHeight,
// which is HARDCODED to GravDBBackend.GetSCIDInteractionHeight with no DBType
// check. HyperGnomon runs on bbolt, so GravDBBackend must DELEGATE to the bbolt
// store and return real data, not the vestigial empties a bare GravitonStore
// returns. Daemon-free: seeds the store directly and reads back.
func TestGravDBBackend_DelegatesToBolt(t *testing.T) {
	const (
		scid  = "a05395bb0cf77adc850928b0db00eb5ca7a9ccbafd9a38d021c8d299ad5ce1a4"
		owner = "dero1qyownerseedaddress"
		h     = int64(6927000)
	)
	bolt, err := compatstorage.NewBBoltDB(t.TempDir(), "")
	if err != nil {
		t.Fatalf("NewBBoltDB: %v", err)
	}
	defer bolt.Close()

	// Seed the bbolt store directly via the inner write API.
	if err := bolt.Inner().StoreOwner(scid, owner); err != nil {
		t.Fatalf("StoreOwner: %v", err)
	}
	if err := bolt.Inner().StoreSCIDInteractionHeight(scid, h); err != nil {
		t.Fatalf("StoreSCIDInteractionHeight: %v", err)
	}

	// A DELEGATING GravitonStore (what NewIndexer wires into GravDBBackend) must
	// return the seeded data — this is the read path HOLOGRAM hardcodes.
	grav := compatstorage.NewGravDBDelegate(bolt)
	if got := grav.GetOwner(scid); got != owner {
		t.Fatalf("delegating GravDBBackend.GetOwner = %q, want %q (must delegate to bbolt)", got, owner)
	}
	hs := grav.GetSCIDInteractionHeight(scid)
	if len(hs) == 0 || hs[len(hs)-1] != h {
		t.Fatalf("delegating GravDBBackend.GetSCIDInteractionHeight = %v, want to contain %d", hs, h)
	}

	// DISCRIMINATOR: a BARE GravitonStore (NewGravDB path, no delegate) returns
	// empties — proving this test would catch the nil/vestigial regression.
	bare, err := compatstorage.NewGravDB(t.TempDir(), "25ms")
	if err != nil {
		t.Fatalf("NewGravDB: %v", err)
	}
	if got := bare.GetOwner(scid); got != "" {
		t.Fatalf("bare GravitonStore.GetOwner = %q, want empty (no delegate)", got)
	}
	if hs := bare.GetSCIDInteractionHeight(scid); len(hs) != 0 {
		t.Fatalf("bare GravitonStore.GetSCIDInteractionHeight = %v, want empty (no delegate)", hs)
	}
}

// TestBboltStore_OpenClose verifies the bbolt facade opens + closes a
// fresh store and returns no error. Uses t.TempDir so the store is
// cleaned up automatically.
func TestBboltStore_OpenClose(t *testing.T) {
	store, err := compatstorage.NewBBoltDB(t.TempDir(), "testfilter")
	if err != nil {
		t.Fatalf("NewBBoltDB: %v", err)
	}
	if store == nil {
		t.Fatalf("NewBBoltDB returned nil store without error")
	}
	// Civilware query methods return empty / zero on a fresh store.
	if h, err := store.GetLastIndexHeight(); err != nil || h != 0 {
		t.Fatalf("GetLastIndexHeight fresh: (%d, %v), want (0, nil)", h, err)
	}
	if m := store.GetAllOwnersAndSCIDs(); len(m) != 0 {
		t.Fatalf("GetAllOwnersAndSCIDs fresh: len=%d, want 0", len(m))
	}
	// GetOwner is civilware-shape (string, no error); empty for an unknown SCID.
	if o := store.GetOwner("a05395bb0cf77adc850928b0db00eb5ca7a9ccbafd9a38d021c8d299ad5ce1a4"); o != "" {
		t.Fatalf("GetOwner fresh: got %q, want empty", o)
	}
	// Civilware query methods accept interface{} keys and return []uint64 heights.
	if vals, hs := store.GetSCIDValuesByKey("scid", "k", 0, false); len(vals) != 0 || len(hs) != 0 {
		t.Fatalf("GetSCIDValuesByKey fresh: got %d vals / %d heights, want 0/0", len(vals), len(hs))
	}
	if store.Inner() == nil {
		t.Fatalf("Inner() returned nil on live store")
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
