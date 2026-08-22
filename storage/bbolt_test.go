package storage

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/hypergnomon/hypergnomon/structures"
)

// --- helpers ---

// randHex returns a random hex string of the given byte length (output is 2*n chars).
func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// fakeSCID returns a realistic 64-char hex SCID.
func fakeSCID() string { return randHex(32) }

// fakeAddr returns a DERO-style address (dero1q prefix + 60 hex chars).
func fakeAddr() string { return "dero1q" + randHex(30) }

// fakeTxid returns a 64-char hex transaction ID.
func fakeTxid() string { return randHex(32) }

// openTestStore creates a BboltStore in a temp directory.
func openTestStore(tb testing.TB) *BboltStore {
	tb.Helper()
	store, err := NewBboltStore(tb.TempDir(), "")
	if err != nil {
		tb.Fatalf("NewBboltStore: %v", err)
	}
	tb.Cleanup(func() { store.Close() })
	return store
}

// makeBatch builds a WriteBatch with the given number of owners, invocations,
// and variable snapshots spread across distinct SCIDs.
func makeBatch(nOwners, nInvocations, nVarSnapshots int) *WriteBatch {
	batch := NewWriteBatch()

	// Pre-generate SCIDs so invocations and variables can reference real ones.
	scids := make([]string, nOwners)
	for i := range scids {
		scids[i] = fakeSCID()
		batch.AddOwner(scids[i], fakeAddr())
	}

	for i := 0; i < nInvocations; i++ {
		scid := scids[i%len(scids)]
		txid := fakeTxid()
		batch.AddInvocation(structures.InvokeRecord{
			Scid:       scid,
			Sender:     fakeAddr(),
			Entrypoint: "Transfer",
			Height:     int64(1000 + i),
			Details: &structures.SCTXParse{
				Txid:       txid,
				Scid:       scid,
				Entrypoint: "Transfer",
				Method:     structures.MethodInvokeSC,
				Sender:     fakeAddr(),
				Fees:       uint64(100 + i),
				Height:     int64(1000 + i),
			},
		})
	}

	for i := 0; i < nVarSnapshots; i++ {
		scid := scids[i%len(scids)]
		height := int64(2000 + i)
		vars := []*structures.SCIDVariable{
			{Key: "balance", Value: uint64(i * 1000)},
			{Key: "owner", Value: fakeAddr()},
		}
		batch.AddVariables(scid, height, vars)
		batch.AddInteractionHeight(scid, height)
	}

	batch.LastHeight = int64(2000 + nVarSnapshots)
	batch.RegTxCount = int64(nOwners)
	batch.BurnTxCount = int64(nInvocations / 10)
	batch.NormTxCount = int64(nInvocations)

	return batch
}

// --- functional tests ---

func TestBboltStore_BasicOps(t *testing.T) {
	store := openTestStore(t)

	// Default last index height should be 0.
	h, err := store.GetLastIndexHeight()
	if err != nil {
		t.Fatalf("GetLastIndexHeight (initial): %v", err)
	}
	if h != 0 {
		t.Fatalf("expected initial height 0, got %d", h)
	}

	// Store and retrieve last index height.
	if err := store.StoreLastIndexHeight(42000); err != nil {
		t.Fatalf("StoreLastIndexHeight: %v", err)
	}
	h, err = store.GetLastIndexHeight()
	if err != nil {
		t.Fatalf("GetLastIndexHeight: %v", err)
	}
	if h != 42000 {
		t.Fatalf("expected height 42000, got %d", h)
	}

	// Store and retrieve owner.
	scid := fakeSCID()
	owner := fakeAddr()
	if err := store.StoreOwner(scid, owner); err != nil {
		t.Fatalf("StoreOwner: %v", err)
	}
	got, err := store.GetOwner(scid)
	if err != nil {
		t.Fatalf("GetOwner: %v", err)
	}
	if got != owner {
		t.Fatalf("owner mismatch: got %q, want %q", got, owner)
	}

	// Non-existent SCID returns empty string, no error.
	got, err = store.GetOwner(fakeSCID())
	if err != nil {
		t.Fatalf("GetOwner (missing): %v", err)
	}
	if got != "" {
		t.Fatalf("expected empty owner for missing SCID, got %q", got)
	}

	// GetAllSCIDs should contain our SCID.
	scids, err := store.GetAllSCIDs()
	if err != nil {
		t.Fatalf("GetAllSCIDs: %v", err)
	}
	found := false
	for _, s := range scids {
		if s == scid {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("GetAllSCIDs did not contain %s", scid)
	}
}

func TestBboltStore_SCIDCountAndBulkOwners(t *testing.T) {
	store := openTestStore(t)
	scidA := fakeSCID()
	scidB := fakeSCID()
	ownerA := fakeAddr()
	ownerB := fakeAddr()

	if err := store.StoreOwner(scidA, ownerA); err != nil {
		t.Fatalf("StoreOwner A: %v", err)
	}
	batch := NewWriteBatch()
	batch.AddOwner(scidB, ownerB)
	batch.LastHeight = 10
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(batch)

	count, err := store.GetSCIDCount()
	if err != nil {
		t.Fatalf("GetSCIDCount: %v", err)
	}
	if count != 2 {
		t.Fatalf("GetSCIDCount = %d, want 2", count)
	}
	owners, err := store.GetOwnersForSCIDs([]string{scidA, scidB, fakeSCID()})
	if err != nil {
		t.Fatalf("GetOwnersForSCIDs: %v", err)
	}
	if owners[scidA] != ownerA || owners[scidB] != ownerB {
		t.Fatalf("owners = %+v, want %s/%s", owners, ownerA, ownerB)
	}
	if len(owners) != 2 {
		t.Fatalf("owners len = %d, want 2", len(owners))
	}
}

func TestBboltStore_FlushBatch(t *testing.T) {
	store := openTestStore(t)

	const (
		numOwners      = 100
		numInvocations = 500
		numVarSnaps    = 200
	)

	batch := makeBatch(numOwners, numInvocations, numVarSnaps)

	// Capture SCIDs before flush (map iteration order is random, so snapshot).
	ownerSnapshot := make(map[string]string, len(batch.Owners))
	for k, v := range batch.Owners {
		ownerSnapshot[k] = v
	}

	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}

	// Verify owner count.
	allOwners, err := store.GetAllOwnersAndSCIDs()
	if err != nil {
		t.Fatalf("GetAllOwnersAndSCIDs: %v", err)
	}
	if len(allOwners) != numOwners {
		t.Fatalf("expected %d owners, got %d", numOwners, len(allOwners))
	}

	// Spot-check every owner.
	for scid, wantOwner := range ownerSnapshot {
		gotOwner, err := store.GetOwner(scid)
		if err != nil {
			t.Fatalf("GetOwner(%s): %v", scid[:16], err)
		}
		if gotOwner != wantOwner {
			t.Fatalf("owner mismatch for %s: got %q, want %q", scid[:16], gotOwner, wantOwner)
		}
	}

	// Verify invocation count for a sample SCID.
	// Each SCID gets roughly numInvocations/numOwners invocations.
	sampleSCID := batch.Invocations[0].Scid
	invokes, err := store.GetInvokeDetailsBySCID(sampleSCID)
	if err != nil {
		t.Fatalf("GetInvokeDetailsBySCID: %v", err)
	}
	if len(invokes) == 0 {
		t.Fatalf("expected invocations for SCID %s, got none", sampleSCID[:16])
	}

	// Verify variable snapshots.
	for k, wantVars := range batch.Variables {
		gotVars, err := store.GetSCIDVariableDetailsAtHeight(k.Scid, k.Height)
		if err != nil {
			t.Fatalf("GetSCIDVariableDetailsAtHeight(%s, %d): %v", k.Scid[:16], k.Height, err)
		}
		if len(gotVars) != len(wantVars) {
			t.Fatalf("var count mismatch at %s:%d: got %d, want %d",
				k.Scid[:16], k.Height, len(gotVars), len(wantVars))
		}
	}

	// Verify interaction heights.
	for scid, wantHeights := range batch.Heights {
		gotHeights, err := store.GetSCIDInteractionHeights(scid)
		if err != nil {
			t.Fatalf("GetSCIDInteractionHeights(%s): %v", scid[:16], err)
		}
		if len(gotHeights) != len(wantHeights) {
			t.Fatalf("height count mismatch for %s: got %d, want %d",
				scid[:16], len(gotHeights), len(wantHeights))
		}
	}

	// Verify TX counts.
	reg, burn, norm, err := store.GetTxCounts()
	if err != nil {
		t.Fatalf("GetTxCounts: %v", err)
	}
	if reg != batch.RegTxCount || burn != batch.BurnTxCount || norm != batch.NormTxCount {
		t.Fatalf("tx counts mismatch: got (%d,%d,%d), want (%d,%d,%d)",
			reg, burn, norm, batch.RegTxCount, batch.BurnTxCount, batch.NormTxCount)
	}

	// Verify last indexed height.
	h, err := store.GetLastIndexHeight()
	if err != nil {
		t.Fatalf("GetLastIndexHeight: %v", err)
	}
	if h != batch.LastHeight {
		t.Fatalf("last height mismatch: got %d, want %d", h, batch.LastHeight)
	}
}

func TestBboltStore_BatchReset(t *testing.T) {
	batch := makeBatch(50, 200, 100)

	if len(batch.Owners) == 0 || len(batch.Invocations) == 0 || len(batch.Variables) == 0 {
		t.Fatal("batch was not populated before reset")
	}

	batch.Reset()

	if len(batch.Owners) != 0 {
		t.Fatalf("Owners not cleared: %d entries remain", len(batch.Owners))
	}
	if len(batch.Invocations) != 0 {
		t.Fatalf("Invocations not cleared: %d entries remain", len(batch.Invocations))
	}
	if len(batch.Variables) != 0 {
		t.Fatalf("Variables not cleared: %d entries remain", len(batch.Variables))
	}
	if len(batch.Heights) != 0 {
		t.Fatalf("Heights not cleared: %d entries remain", len(batch.Heights))
	}
	if batch.RegTxCount != 0 || batch.BurnTxCount != 0 || batch.NormTxCount != 0 {
		t.Fatalf("TX counts not cleared: (%d, %d, %d)",
			batch.RegTxCount, batch.BurnTxCount, batch.NormTxCount)
	}
	if batch.LastHeight != 0 {
		t.Fatalf("LastHeight not cleared: %d", batch.LastHeight)
	}

	// Reuse: add new data after reset.
	scid := fakeSCID()
	batch.AddOwner(scid, fakeAddr())
	if len(batch.Owners) != 1 {
		t.Fatalf("batch not reusable after reset: expected 1 owner, got %d", len(batch.Owners))
	}

	// Flush the reused batch to a real store.
	store := openTestStore(t)
	txid := fakeTxid()
	batch.AddInvocation(structures.InvokeRecord{
		Scid:       scid,
		Sender:     fakeAddr(),
		Entrypoint: "Initialize",
		Height:     5000,
		Details: &structures.SCTXParse{
			Txid:       txid,
			Scid:       scid,
			Entrypoint: "Initialize",
			Method:     structures.MethodInstallSC,
			Sender:     fakeAddr(),
			Fees:       200,
			Height:     5000,
		},
	})
	batch.LastHeight = 5000

	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch after reset+reuse: %v", err)
	}
	h, err := store.GetLastIndexHeight()
	if err != nil {
		t.Fatalf("GetLastIndexHeight after reuse: %v", err)
	}
	if h != 5000 {
		t.Fatalf("expected height 5000 after reuse, got %d", h)
	}
}

// --- benchmarks ---
// The key comparison: FlushBatch (one transaction for N records) vs
// individual StoreOwner calls (N transactions for N records).
// This proves the arena-style batch write optimization.

func benchmarkFlushBatch(b *testing.B, n int) {
	store := openTestStore(b)

	// Pre-build batch data outside the timer.
	scids := make([]string, n)
	addrs := make([]string, n)
	txids := make([]string, n)
	for i := 0; i < n; i++ {
		scids[i] = fakeSCID()
		addrs[i] = fakeAddr()
		txids[i] = fakeTxid()
	}

	batch := NewWriteBatch()

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		batch.Reset()

		for i := 0; i < n; i++ {
			batch.AddOwner(scids[i], addrs[i])
			batch.AddInvocation(structures.InvokeRecord{
				Scid:       scids[i],
				Sender:     addrs[i],
				Entrypoint: "Transfer",
				Height:     int64(i),
				Details: &structures.SCTXParse{
					Txid:       txids[i],
					Scid:       scids[i],
					Entrypoint: "Transfer",
					Method:     structures.MethodInvokeSC,
					Sender:     addrs[i],
					Fees:       uint64(100 + i),
					Height:     int64(i),
				},
			})
			batch.AddVariables(scids[i], int64(i), []*structures.SCIDVariable{
				{Key: "balance", Value: uint64(i * 1000)},
			})
			batch.AddInteractionHeight(scids[i], int64(i))
		}

		batch.LastHeight = int64(n)
		batch.RegTxCount = int64(n)

		if err := store.FlushBatch(batch); err != nil {
			b.Fatalf("FlushBatch: %v", err)
		}
	}

	b.ReportMetric(float64(n), "records/flush")
}

func BenchmarkFlushBatch_100(b *testing.B)   { benchmarkFlushBatch(b, 100) }
func BenchmarkFlushBatch_1000(b *testing.B)  { benchmarkFlushBatch(b, 1000) }
func BenchmarkFlushBatch_10000(b *testing.B) { benchmarkFlushBatch(b, 10000) }

func BenchmarkGetSCIDCount(b *testing.B) {
	store := openTestStore(b)
	batch := NewWriteBatch()
	for i := 0; i < 10000; i++ {
		batch.AddOwner(fakeSCID(), fakeAddr())
	}
	batch.LastHeight = 1
	if err := store.FlushBatch(batch); err != nil {
		b.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(batch)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		n, err := store.GetSCIDCount()
		if err != nil {
			b.Fatal(err)
		}
		if n != 10000 {
			b.Fatalf("count = %d, want 10000", n)
		}
	}
}

func BenchmarkGetOwnersForSCIDs(b *testing.B) {
	store := openTestStore(b)
	batch := NewWriteBatch()
	scids := make([]string, 0, 10000)
	for i := 0; i < 10000; i++ {
		scid := fakeSCID()
		scids = append(scids, scid)
		batch.AddOwner(scid, fakeAddr())
	}
	batch.LastHeight = 1
	if err := store.FlushBatch(batch); err != nil {
		b.Fatalf("FlushBatch: %v", err)
	}
	PutWriteBatch(batch)
	window := scids[:100]

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		owners, err := store.GetOwnersForSCIDs(window)
		if err != nil {
			b.Fatal(err)
		}
		if len(owners) != len(window) {
			b.Fatalf("owners = %d, want %d", len(owners), len(window))
		}
	}
}

// BenchmarkIndividualWrites does the same work as BenchmarkFlushBatch but uses
// per-record StoreOwner + StoreInvokeDetails + StoreSCIDVariableDetails calls.
// Each call opens its own BoltDB transaction. Compare against FlushBatch to
// measure the batch optimization speedup.
func BenchmarkIndividualWrites(b *testing.B) {
	const n = 100 // Use 100 to keep wall-clock reasonable; compare vs FlushBatch_100.

	store := openTestStore(b)

	scids := make([]string, n)
	addrs := make([]string, n)
	txids := make([]string, n)
	for i := 0; i < n; i++ {
		scids[i] = fakeSCID()
		addrs[i] = fakeAddr()
		txids[i] = fakeTxid()
	}

	b.ResetTimer()
	b.ReportAllocs()

	for range b.N {
		for i := 0; i < n; i++ {
			if err := store.StoreOwner(scids[i], addrs[i]); err != nil {
				b.Fatalf("StoreOwner: %v", err)
			}

			detail := &structures.SCTXParse{
				Txid:       txids[i],
				Scid:       scids[i],
				Entrypoint: "Transfer",
				Method:     structures.MethodInvokeSC,
				Sender:     addrs[i],
				Fees:       uint64(100 + i),
				Height:     int64(i),
			}
			if err := store.StoreInvokeDetails(scids[i], addrs[i], "Transfer", int64(i), detail); err != nil {
				b.Fatalf("StoreInvokeDetails: %v", err)
			}

			vars := []*structures.SCIDVariable{
				{Key: "balance", Value: uint64(i * 1000)},
			}
			if err := store.StoreSCIDVariableDetails(scids[i], vars, int64(i)); err != nil {
				b.Fatalf("StoreSCIDVariableDetails: %v", err)
			}

			if err := store.StoreSCIDInteractionHeight(scids[i], int64(i)); err != nil {
				b.Fatalf("StoreSCIDInteractionHeight: %v", err)
			}
		}

		if err := store.StoreLastIndexHeight(int64(n)); err != nil {
			b.Fatalf("StoreLastIndexHeight: %v", err)
		}
	}

	b.ReportMetric(float64(n), "records/iter")
}

// TestFlushBatch_VariableLengthSnapshots pins down the bbolt-Put buffer-reuse
// bug we hit in probeTELA: bolt stores the value slice header and copies at
// commit time, so sharing a single backing array across multiple Puts
// corrupts every write except the last. The earlier FlushBatch reused a
// single varValBuf across all scvars Puts; when successive SCIDs had
// variable-length snapshots (e.g., TELA INDEX with 24 vars vs TELA DOC with
// 14 vars), the larger record ended up with later, shorter bytes in its
// tail — the reader saw a valid tag + count but stale/truncated payload.
//
// Guard the fix by writing three distinct snapshots of clearly different
// sizes in one batch and asserting every one round-trips verbatim.
func TestFlushBatch_VariableLengthSnapshots(t *testing.T) {
	store := openTestStore(t)

	scids := []string{fakeSCID(), fakeSCID(), fakeSCID()}
	heights := []int64{1001, 1002, 1003}

	// Deliberately vary counts and string lengths so encoded sizes differ.
	snapshots := [][]*structures.SCIDVariable{
		// Tiny: 2 vars, short values
		{
			{Key: "a", Value: "x"},
			{Key: "b", Value: uint64(1)},
		},
		// Medium: 8 vars, 64-byte string values
		func() []*structures.SCIDVariable {
			s := make([]*structures.SCIDVariable, 8)
			for i := range s {
				s[i] = &structures.SCIDVariable{
					Key:   fmt.Sprintf("med_%d", i),
					Value: randHex(32), // 64 hex chars
				}
			}
			return s
		}(),
		// Large: 30 vars, 256-byte string values
		func() []*structures.SCIDVariable {
			s := make([]*structures.SCIDVariable, 30)
			for i := range s {
				s[i] = &structures.SCIDVariable{
					Key:   fmt.Sprintf("large_%d_%s", i, randHex(4)),
					Value: randHex(128),
				}
			}
			return s
		}(),
	}

	batch := NewWriteBatch()
	for i, snap := range snapshots {
		batch.AddVariables(scids[i], heights[i], snap)
	}
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}

	for i, want := range snapshots {
		got, err := store.GetSCIDVariableDetailsAtHeight(scids[i], heights[i])
		if err != nil {
			t.Fatalf("snapshot %d read: %v", i, err)
		}
		if len(got) != len(want) {
			t.Fatalf("snapshot %d: count=%d, want %d", i, len(got), len(want))
		}
		// Order is iteration-dependent across runs, but we wrote unique
		// keys per snapshot, so a map compare is sufficient.
		wantMap := make(map[string]any, len(want))
		for _, v := range want {
			wantMap[fmt.Sprintf("%v", v.Key)] = v.Value
		}
		for _, v := range got {
			k := fmt.Sprintf("%v", v.Key)
			wv, ok := wantMap[k]
			if !ok {
				t.Errorf("snapshot %d: unexpected key %q", i, k)
				continue
			}
			if fmt.Sprintf("%v", v.Value) != fmt.Sprintf("%v", wv) {
				t.Errorf("snapshot %d key %q: got %v, want %v", i, k, v.Value, wv)
			}
		}
	}
}

// TestFlushBatch_AddrSCIDs_MultiEntry covers the sibling bug on the
// addr_scids bucket: the old FlushBatch reused a single 25-byte valBuf
// across every Put, so after commit all entries shared the last-written
// record. Three entries with distinct FirstHeight/LastHeight/Count make
// any silent sharing visible.
func TestFlushBatch_AddrSCIDs_MultiEntry(t *testing.T) {
	store := openTestStore(t)
	addr := fakeAddr()

	type want struct {
		scid               string
		first, last, count int64
	}
	wants := []want{
		{scid: fakeSCID(), first: 100, last: 100, count: 1},
		{scid: fakeSCID(), first: 200, last: 250, count: 5},
		{scid: fakeSCID(), first: 400, last: 999, count: 42},
	}

	batch := NewWriteBatch()
	for _, w := range wants {
		// AddAddrSCID's signature lets us supply only one (addr, scid,
		// height); for count>1 we call it once per increment so FirstHeight
		// / LastHeight get set correctly via the min/max merge.
		batch.AddAddrSCID(addr, w.scid, w.first)
		for h := w.first + 1; h <= w.last && h <= w.first+w.count-1; h++ {
			batch.AddAddrSCID(addr, w.scid, h)
		}
		// Ensure LastHeight hits w.last even when count < (last-first+1).
		batch.AddAddrSCID(addr, w.scid, w.last)
	}
	if err := store.FlushBatch(batch); err != nil {
		t.Fatalf("FlushBatch: %v", err)
	}

	for _, w := range wants {
		got, err := store.GetAddressSCIDs(addr)
		if err != nil {
			t.Fatalf("GetAddressSCIDs: %v", err)
		}
		entry, ok := got[w.scid]
		if !ok {
			t.Fatalf("missing entry for scid %s", w.scid[:16])
		}
		if entry.FirstHeight != w.first || entry.LastHeight != w.last {
			t.Errorf("scid %s heights: got first=%d last=%d, want first=%d last=%d",
				w.scid[:16], entry.FirstHeight, entry.LastHeight, w.first, w.last)
		}
	}
}

// BenchmarkFlushBatch_Scaling runs all batch sizes as sub-benchmarks for
// convenient comparison with `go test -bench=Scaling -benchmem`.
func BenchmarkFlushBatch_Scaling(b *testing.B) {
	for _, size := range []int{100, 500, 1000, 5000, 10000} {
		b.Run(fmt.Sprintf("n=%d", size), func(b *testing.B) {
			benchmarkFlushBatch(b, size)
		})
	}
}

// BenchmarkBatchAlloc measures only the cost of acquiring + discarding a
// WriteBatch, which is exactly the per-flush churn in processorLoop.
// Pool_Pair uses the new batchPool; Direct_New bypasses it.
func BenchmarkBatchAlloc(b *testing.B) {
	b.Run("Pool_Pair", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			batch := NewWriteBatch()
			PutWriteBatch(batch)
		}
	})
	b.Run("Direct_New", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			_ = newEmptyBatch()
		}
	})
}

// TestGetSCIDVariableDetailsAtHeight_LatestOnZero pins the civilware compat
// contract: GetSCIDVariableDetailsAtHeight(scid, 0) must return the LATEST
// snapshot, not nil. pkg/gnomes/storage.GetAllSCIDVariableDetails passes 0
// verbatim ("0 means latest"), but the reader used to reject height <= 0,
// so every XSWD Gnomon.GetAllSCIDVariableDetails query returned empty on
// freshly indexed contracts (e.g. DeroBeats' song registry via Engram's
// AutoIndexDependentSCIDs).
func TestGetSCIDVariableDetailsAtHeight_LatestOnZero(t *testing.T) {
	store := openTestStore(t)
	scid := fakeSCID()

	v100 := []*structures.SCIDVariable{{Key: "song", Value: "old"}}
	v200 := []*structures.SCIDVariable{
		{Key: "song", Value: "new"},
		{Key: "count", Value: uint64(2)},
	}
	if err := store.StoreSCIDVariableDetails(scid, v100, 100); err != nil {
		t.Fatalf("StoreSCIDVariableDetails(100): %v", err)
	}
	if err := store.StoreSCIDVariableDetails(scid, v200, 200); err != nil {
		t.Fatalf("StoreSCIDVariableDetails(200): %v", err)
	}

	// Explicit heights keep exact-snapshot semantics.
	for _, tc := range []struct {
		height int64
		want   string
	}{
		{100, "old"},
		{200, "new"},
	} {
		vars, err := store.GetSCIDVariableDetailsAtHeight(scid, tc.height)
		if err != nil {
			t.Fatalf("AtHeight(%d): %v", tc.height, err)
		}
		if len(vars) == 0 || vars[0].Value != tc.want {
			t.Fatalf("AtHeight(%d): want song=%q, got %+v", tc.height, tc.want, vars)
		}
	}

	// 0 resolves to the latest stored snapshot.
	vars, err := store.GetSCIDVariableDetailsAtHeight(scid, 0)
	if err != nil {
		t.Fatalf("AtHeight(0): %v", err)
	}
	if len(vars) != 2 || vars[0].Value != "new" {
		t.Fatalf("AtHeight(0): want latest snapshot (2 vars, song=new), got %+v", vars)
	}

	// Negative heights behave the same as 0.
	if vars, err = store.GetSCIDVariableDetailsAtHeight(scid, -1); err != nil || len(vars) != 2 {
		t.Fatalf("AtHeight(-1): want latest snapshot, got %+v (err %v)", vars, err)
	}

	// Unknown SCID: nil, no error.
	if vars, err = store.GetSCIDVariableDetailsAtHeight(fakeSCID(), 0); err != nil || vars != nil {
		t.Fatalf("unknown scid: want nil,nil, got %+v (err %v)", vars, err)
	}

	// Empty scid: nil, no error.
	if vars, err = store.GetSCIDVariableDetailsAtHeight("", 0); err != nil || vars != nil {
		t.Fatalf("empty scid: want nil,nil, got %+v (err %v)", vars, err)
	}
}
