# HyperGnomon — Route B Design

**Realtime query layer for DERO client applications.**

Status: **partially implemented**. v0.8 and v0.9 shipped M0 + M1 + M5 and partial M3. M2 truncate-replay is deferred to v1.x — see §14 for per-milestone status. See [README.md](README.md) for the current shipped feature surface and [CHANGELOG.md](CHANGELOG.md) for what landed when.

## 0. Why Route B

civilware/Gnomon already occupies the batch-indexer niche. HyperGnomon's 14 ms cached TELA load is not "same tool faster" — it is a different capability: something a UI can call on every screen. Route B leans into that difference: HG becomes the **realtime query + content layer** that Commando-1000, PureWolf Mobile, TELA bracket, Captains Quarters, and atomic swaps all need but currently reinvent in-app.

## 1. Goals & non-goals

**Goals.**
- Push, not poll. One WS subscription endpoint with typed events.
- Finality-aware. Separate `tip_height` from `safe_height`; emit `reorg` on head-change.
- Query-time filtering (code match, height range, class, address) — not just index-time.
- Self-contained TELA content server. Browsers hit HG directly; zero daemon round-trips.
- Mempool speculation. New SCs visible to subscribers within ~5 s of broadcast.
- Embeddable as a Go library, not just a binary.

**Non-goals.**
- Not a chain node. No P2P, no consensus, no mempool mutation.
- Not a SQL engine. Fixed method set, not a user-defined query language.
- Not a long-term code archive. Store hashes, fetch code on demand.
- Not an auth provider. TLS + optional bearer token; no user accounts.

## 2. Architecture overview

```
            +--------+       +-----------+      +---------+
  daemon -->|Fetcher |------>| Processor |----->| Flusher |---> BoltDB
  (RPC)     +--------+       +-----------+      +---------+
                 ^                 |                 |
                 |                 v                 |
            Mempool tail      Classifier         EventBus <-- Publish(event)
                 |                                   |
                 +------------+ speculative +--------+
                              v                      |
                         speculative                 v
                         bucket              Subscription router
                                                     |
                                                     v
                                              WS connections
```

Existing 3-stage pipeline stays. The two new components are the **EventBus** (in-process pub/sub) and the **speculative** path (mempool tail). Every write that changes durable state publishes an event; every write from mempool goes into a separate bucket and publishes a `speculative=true` event.

## 3. Data model changes

Add six buckets, deprecate two keying schemes. bbolt stays; if write amplification bites past 20 M rows, migrate the list-valued buckets to Pebble (§15).

| Bucket | Key | Value | Replaces |
|---|---|---|---|
| `blockhashes` | `height_be8` (8 B BE) | 32-byte block hash | — |
| `installs` | `height_be8 \| scid` | `msgpack({owner, entrypoint, fees})` | derived from invocations |
| `class` | `class \| height_be8 \| scid` | `msgpack({tags, name, desc, icon})` | derives unused `ClassifySC` |
| `addr_scids/<addr>/` (nested) | `scid` | `msgpack({first, last, count})` | — |
| `scvars_v2/<scid>/` (nested) | `height_be8` | `msgpack([]SCIDVariable)` | flat `scid:height` string key |
| `heights_v2/<scid>/` (nested) | `height_be8` | empty | O(n²) re-encoded `[]int64` list |
| `speculative/<scid>/` (nested) | `txid` | `msgpack(SCTXParse)` | — |
| `tela_content` | `scid \| path` | `{body, mime, sha256}` | — |

**Binary big-endian height keys** enable `cursor.Seek(start)` for range scans — critical for "installs in block range X..Y" and "vars at or before height H" in O(log n).

**Migration.** Read v1 keys, write v2, keep v1 readable during transition. Drop v1 after a release cycle. `hypergnomon migrate --from=v1 --to=v2` command.

## 4. Subscription protocol

WS JSON-RPC 2.0 at `/ws` (already exists, extended). Three new methods: `subscribe`, `unsubscribe`, and one new notification type `event`.

### Subscribe

```json
{ "jsonrpc":"2.0", "id":1, "method":"subscribe", "params":{
    "events":  ["install","invoke","var_change","reorg","safe","class_match"],
    "filters": {
      "scid":          "optional exact SCID (hex)",
      "owner":         "optional exact address",
      "sender":        "optional address (invoke only)",
      "class":         "optional, e.g. TELA-INDEX-1 or G45-NFT",
      "code_contains": "optional substring (subject to cost cap)",
      "from_height":   0
    },
    "include_speculative": false
}}

// ← {"jsonrpc":"2.0","id":1,"result":{"subscription_id":"s_a1b2","safe_height":6927300}}
```

### Event notification

```json
{"jsonrpc":"2.0","method":"event","params":{
  "subscription_id":"s_a1b2",
  "type":"invoke",
  "height":6927301,
  "safe_height":6927291,
  "speculative": false,
  "scid":"...",
  "sender":"dero1q...",
  "entrypoint":"InputStr",
  "class":"TELA-INDEX-1",
  "payload":{ /* type-specific */ }
}}
```

### Unsubscribe

```json
{"jsonrpc":"2.0","id":2,"method":"unsubscribe","params":{"subscription_id":"s_a1b2"}}
```

### Semantics

- **Backfill.** `from_height > 0` replays historical events from that height, then transitions atomically to live. The server buffers new events during replay to avoid gaps.
- **Cursor-based resumption.** Each event carries `(height, event_seq_in_block)`. On reconnect, client sends `from_height=<last_seen>`.
- **Per-connection limits.** Max 32 subs per connection, max 512 connections per process (config). Events dropped for slow consumers after 1 s backpressure; connection closed with RPC error `-32099 {"reason":"slow_consumer"}`.
- **Reorg event** fires once per reorg with `{old_tip, new_tip, affected_scids:[...]}`. Subscribers with open subscriptions on affected SCIDs receive it regardless of their filters.
- **Safe-height event** fires when `safe_height` advances. Cheap poll replacement for apps that only care about finality.

## 5. Event bus (internal)

```go
type EventType uint8
const (
    EventInstall EventType = iota
    EventInvoke
    EventVarChange
    EventReorg
    EventSafe
    EventClassMatch
    EventMempoolInstall  // speculative
)

type Event struct {
    Type       EventType
    Height     int64
    SafeHeight int64
    SCID       string
    Class      string
    Tags       []string
    Owner      string
    Sender     string
    Entrypoint string
    Speculative bool
    Payload    any          // SCTXParse | []SCIDVariable | ReorgInfo | ...
}

type Bus struct {
    in       chan Event               // publish → fan-out goroutine
    subs     map[string]*sub          // sub_id → sub
    mu       sync.RWMutex
}

func (b *Bus) Publish(e Event)                         // non-blocking (1024 buffer)
func (b *Bus) Subscribe(f Filter) (id string, out <-chan Event, cancel func())
```

Publishing order: Flusher calls `Publish(...)` *after* `FlushBatch` commit succeeds (so subscribers never see events that are not durable). Mempool path publishes with `Speculative=true`.

Fan-out is single goroutine reading `in`, evaluating filters, copying to per-sub channels. Filter evaluation is cheap: exact-match first, `code_contains` last (only if pre-filters passed). Cost-capped per §11.

## 6. Reorg handling

**Store** `blockhashes[h] = hash` inside the same `FlushBatch` transaction that writes that height. Strong consistency.

**Detect.** Each Fetcher batch compares `result.Block.Prev_Hash` against `blockhashes[h-1]`. Mismatch → reorg.

**Truncate.**
```
findForkPoint()           // walk back, compare hashes until match
truncateFrom(forkHeight)  // delete entries > forkHeight in:
                          //   blockhashes, installs, class,
                          //   per-scid nested buckets (use cursor.Seek + Delete)
                          //   addr_scids (decrement counts, drop if 0)
                          //   speculative (drop — was pending anyway)
publish Reorg{old, new, affected_scids}
replay from forkHeight    // normal pipeline
```

**safe_height** = `max(tip - FinalityDepth, 0)`. DERO stabilizes in 8 blocks per user's operational knowledge, so `FinalityDepth = 10` default. Published in every RPC envelope so clients can gate on it.

**Testing.** Build a fake-daemon harness (`testdaemon` package) that can serve an arbitrary block sequence and inject reorgs on command. Target: 3-block and 20-block reorgs both converge.

## 7. Classifier wire-up

Today `indexer/classify.go` defines `ClassifySC` and nothing calls it. Wire it into `handleInstallSC` and `handleInvokeSC`:

```go
sc := ClassifySC(scid, code, vars)
if sc.Class != "UNKNOWN" {
    batch.AddClass(sc.Class, height, scid, ClassMeta{
        Tags: sc.Tags, Name: sc.Name, Desc: sc.Desc, IconURL: sc.IconURL,
    })
}
```

**Effect.** `/api/tela` drops from `N × 3` disk reads to a single prefix scan of `class/TELA-INDEX-1/*`. Same for `NFA`, `G45-NFT`, `NAMESERVICE`. `listsc_codematch` becomes superfluous for the common cases — it only matters for never-classified custom SCs.

**Pattern scan.** For query-time `code_contains`, cache per-SCID code hashes + a Bloom filter of substrings (Aho-Corasick too heavy for dynamic patterns). First pass eliminates ~90% of SCIDs before the expensive `strings.Contains`.

## 8. Address reverse index

`addr_scids/<addr>/<scid>` = `msgpack({first_height, last_height, count})`.

- Populated on every SC invoke and every `NormalTxWithSCIDParse`.
- `GET /api/address/:addr/scs` returns `[{scid, first, last, count}]` in one prefix scan.
- Cardinality concern: whale addresses may touch thousands of SCs. Nested bucket keeps this O(log n); not a single growing blob.
- Enables subscription filter `{"sender": "dero1q..."}` and `subscribe` by address of interest.

## 9. Mempool-aware speculation

DERO daemon exposes `DERO.GetTransactionPool`. Poll every 500 ms (confirmed via derohe SDK; extend to WS push if daemon gains it).

**Pipeline.**
```
mempool_tail:
  seen := LRU<txid>(10k)
  every 500ms:
    pool := c.GetTransactionPool()
    for tx in pool not in seen:
      seen.Add(tx.Txid)
      if SC_ACTION == 1:
        parse code from tx.SCDATA (no GetSC needed — code is in the tx itself)
        cls := ClassifySC(tx.Txid, code, nil)
        store in speculative/<scid>/
        publish Event{Type: EventMempoolInstall, Speculative: true, Height: 0}
```

**Reconciliation.** Every flushed height, diff confirmed installs against speculative. Matches are promoted (delete from `speculative`, already present in main). Stale speculative entries older than 200 blocks are dropped; their subscribers receive `Event{Type: EventMempoolDropped}`.

**Client opt-in.** Default `include_speculative=false` for subscribers. Apps that want them (TELA live-browser) pass `true`. Payload always carries `speculative=true|false` so the UI can render "pending" differently.

## 10. TELA content server

Fold in the piece that lives on Commando-1000 port 8082.

**Route.** `GET /tela/{scid}/{path...}`

**Resolution algorithm.**
```
index := getSCVariables(scid)        // TELA-INDEX-1 variables
if index["dURL"] does not exist: 404
entry := index.find(path)            // match path against INDEX's route table
docScid := entry.scid                // INDEX references DOC by SCID hash
body := telaContentBucket[docScid, path]
if miss: fetch DOC variables, decode, cache
return body with:
  Content-Type: inferred from path extension
  Content-Security-Policy: default-src 'self' 'unsafe-inline'; frame-ancestors 'none'
  X-Content-Type-Options: nosniff
  ETag: sha256 of body
  Integrity-SRI: sha256-... (for clients that check)
```

**Cache.** 2-tier: LRU in-memory (hot, 128 MB cap), tela_content bucket (durable). Invalidation on `EventInstall` or `EventVarChange` for the same SCID.

## 11. Query cost accounting

Each WS method declares a cost function:

```go
type Cost struct {
    EstRowsRead int64
    EstBytesIO  int64
    ScansAll    bool
}

func (m *Method) Cost(params json.RawMessage) (Cost, error)
```

**Token bucket per connection.** Tokens = milli-rows-read credits. Refill rate config (default 10_000/s). Submissions deduct `max(1, EstRowsRead/1000)` tokens. Over-budget: reject with `-32099 {"reason":"rate_limited","retry_after_ms":X}`.

**`explain` method** returns `{method, params, cost}` without executing. Power tool for debugging and clients that want to batch.

Why not per-IP? Per-connection is good enough for an indexer with dozens of clients. Per-IP needs a reverse-proxy layer, out of scope.

## 12. Embedded / library mode

Move from "binary only" to "library + binary."

- Remove package-global `logger`. `indexer.New(Config)` takes `Logger logr.Logger` and `Metrics prometheus.Registerer`.
- `idx.Run(ctx context.Context) error` — clean cancellation. All goroutines propagate ctx. `Close()` stays as a convenience wrapper around a CancelFunc.
- No `os.Exit` in any non-`main` package.
- Zero hardcoded paths.
- Subscribe usable in-process: `idx.Subscribe(filter)` returns a channel, same as the WS path.

**Use cases.** PureWolf via gomobile embeds for offline-first TELA browsing. Tests use a real indexer against a fake daemon instead of mocks. Commando-1000 can embed instead of shelling to the binary.

**Trade-off.** Binary footprint grows (exposed API surface). Document the public API in `doc.go`; use `internal/` for what stays private.

## 13. civilware ports (absorbed into this design)

These land as normal WS methods alongside subscribe/unsubscribe:

| Method | Semantics | Cost class |
|---|---|---|
| `addscid_toindex` | triggers in-band index of one SCID, optional `varsonly`, `skipfsrecheck` | medium (1 GetSC) |
| `listsc` | paginated all SCIDs + owner + class | high (full scan) |
| `listsc_byheight` | SCIDs installed in [h1,h2) via `installs` bucket | low (prefix scan) |
| `listsc_codematch` | code substring filter with Bloom pre-scan | high, cost-gated |
| `listsc_variables` | vars at height (or latest) | low |
| `listsc_hardcoded` | the known-SCID list (GnomonSC, NameService, etc.) | trivial |

Multi-prefix daemon connect (`http/https/ws/wss`) is a small change in `rpc/client.go:Connect` — strip scheme, dial appropriately. Required for public-node testing.

## 14. Milestones

Each milestone is shippable on its own. Rough estimates assume one developer.

| # | Milestone | Status | Notes |
|---|---|---|---|
| M0 | Wire `ClassifySC`, block-hash bucket, `safe_height`, addr reverse index, inject logger | **done** (v0.8) | foundation for everything |
| M1 | Subscription API: event bus, WS methods, filter engine, backfill, disconnect cleanup | **done** (v0.8/v0.9) | realtime push |
| M2 | Reorg detection + truncate-replay, `reorg` events, fake-daemon test harness | **partial / deferred** | Detection (`SafeHeight` atomic, `CheckReorgAt` stub) shipped in v0.9; truncate-replay deferred to v1.x. Measured (July 2026, `cmd/reorgwatch`, DOCS/BENCHMARKS.md): reorgs are shallow (depth ≤ 2, `STABLE_LIMIT=8` bound) but ROUTINE — ~1 per 100 blocks (≈1.8/h), invisible to the canonical DAG (zero sideblocks in 23 days). Truncate-replay is therefore an hourly path, not an exceptional one; civilware/Gnomon ships zero reorg handling, operators use manual `pop`. Our `resync` subcommand covers the operator path. |
| M3 | civilware ports (addscid_toindex, listsc_*, multi-prefix daemon) | **partial** | core list methods + multi-prefix in v0.9; HOLOGRAM extras (SCIDTagStore, `resolvedurl`, `gettelaapps`) tracked for v1.x |
| M4 | Query cost estimator, token bucket, `explain` method | pending | public-facing viability |
| M5 | TELA content server + 2-tier cache | **done** (v1.0) | canonical-spec compliant: base64+gunzip `.gz`, DocShard dispatch, `X-TELA-Verify` header (signature presence in v1.0/v1.1; crypto check in v1.2) |
| M6 | Mempool speculative path + reconcile loop | pending | sub-10 s new-SC visibility |
| M7 | Embedded mode: logger injection, ctx cancel, snapshot export/import | **partial** (v1.0) | `pkg/gnomes` civilware-shape drop-in shipped; external-store injection + snapshot import/export in v1.1 |
| M8 | Ops polish: metrics, health, config file, grafana dash | pending | production polish |

**Shipped to date (v1.0 RC):** M0, M1, M5 complete; M2, M3, M7 partial. The §3-§4 design is normative for what has shipped; §6 + §8 remain forward-looking.

## 15. Risks & open questions

- **bbolt write amplification** on nested buckets with millions of entries. Needs a bench pass on M0. The `storage.Storage` interface is already abstracted so a future backend could be slotted in, but no engine swap is warranted today: bbolt wins the measured read paths (PointRead 625 ns, RangeScan 4.8 µs; `storage/dbbench/RESULTS.md`) and the merge-on-write read-back inside `FlushBatch` is cheaper on a B+tree than on any LSM peer.
- **Daemon mempool semantics.** `GetTransactionPool` returns IDs; we need the full TX to parse SC_INSTALL. Call `GetTransaction` on each novel TXID. Cost = 1 RPC per new mempool tx. Acceptable (DERO mempool is small).
- **Filter expressivity creep.** Resist adding `OR`, `NOT`, `glob`. When that itch arises, add a new method instead.
- **Event ordering guarantees.** Within a block, order by (tx position, payload index). Document this; clients cannot assume cross-block ordering under reorg.
- **Embedded logger flavor.** `logr.Logger` vs `slog.Logger` — Go 1.26 has `slog` as stdlib. Pick `slog`; provide a `logr→slog` shim for callers that insist.
- **Snapshot format versioning.** Every snapshot carries a version + data-model hash. Import refuses mismatched versions unless `--force-reindex-from=H` is given.

## 16. File layout changes

New files:
```
eventbus/bus.go                 // publisher, subscriber registry, fan-out
eventbus/filter.go              // filter eval, Bloom pre-scan for code_contains
indexer/classify_wire.go        // call sites in install/invoke paths
indexer/reorg.go                // hash-chain detect, truncateFrom, replay
indexer/mempool.go              // daemon mempool tail, speculate, reconcile
storage/nested.go               // generic nested-bucket helpers
storage/migrate.go              // v1 → v2 key migration
storage/buckets.go              // new bucket definitions (blockhashes, class, etc.)
api/subscribe.go                // WS handshake, sub lifecycle, backfill
api/cost.go                     // per-method cost fns, token bucket
api/tela.go                     // /tela/{scid}/{path} handler + cache
api/listsc.go                   // civilware-parity methods
testdaemon/harness.go           // fake daemon, reorg injection
cmd/hypergnomon/config.go       // TOML + flag merging
```

Modified:
```
storage/bbolt.go     — add new buckets, drop legacy key formats after migration
storage/storage.go   — new interface methods for class/installs/addr lookups
indexer/indexer.go   — publish events after flush, integrate classifier, ctx cancel
indexer/fastsync.go  — publish install events during probe
rpc/client.go        — multi-prefix connect, mempool fetch
api/http.go          — serve /tela/*, /healthz, /metrics, /debug/pprof behind flags
api/ws.go            — dispatch table extended; no breaking changes to existing methods
cmd/hypergnomon/main.go — ctx lifecycle, config file, graceful shutdown errgroup
structures/types.go — Event, ClassMeta, ReorgInfo types
```

---

## Appendix A — Example subscription flows

**TELA browser watching a specific INDEX SCID:**
```json
subscribe {
  "events": ["invoke","var_change"],
  "filters": {"scid":"a05395bb..."}
}
```

**Wallet watching an address for any activity:**
```json
subscribe {
  "events": ["invoke"],
  "filters": {"sender":"dero1qyjj..."}
}
```

**Atomic swap waiting for finality of a specific height:**
```json
subscribe {
  "events": ["safe"],
  "filters": {"from_height": 6927350}
}
// one event when safe_height crosses 6927350, then client unsubscribes
```

**Sports-pool watcher for any new TELA install:**
```json
subscribe {
  "events": ["install"],
  "filters": {"class":"TELA-INDEX-1"},
  "include_speculative": true
}
```

## Appendix B — Non-obvious design choices

1. **bbolt stays.** LSM trees (Pebble) are faster for append-heavy workloads but fsync-heavy on commit. bbolt's B-tree fits "batch every 100 blocks" better. Reassess after M6 if `class`+`speculative` buckets push total db size past 50 GB.
2. **Single event bus, not per-type topics.** Fan-out evaluates filters centrally. Simpler than maintaining N topic channels; CPU cost is negligible for < 1 k subscribers.
3. **No protobuf yet.** JSON over WS is ~2× bandwidth but one less codegen dependency. Revisit if mobile bandwidth becomes a constraint.
4. **No Kafka.** The bus is in-process. If you need fan-out to external systems, `hypergnomon hook --stdout` streams events to stdout; pipe that into whatever.
5. **`safe_height` not `confirmed_height`.** "Safe" is weaker than "confirmed" but clearer to app developers. DERO has no strict finality; 10 blocks is a pragmatic choice, not a theorem.
