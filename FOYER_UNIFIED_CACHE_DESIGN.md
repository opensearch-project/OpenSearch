# Foyer-backed unified cache (experiment branch `foyer-unified-cache`)

**Goal:** back all four DataFusion caches with a single foyer in-memory cache wrapper, replacing
the bespoke eviction machinery (our hand-written S3-FIFO `CachePolicy`, #22254's
`BoundedCache`/`DashMap + ScopedEvictionPolicy`, and the metadata/statistics ad-hoc stores).

**Branch isolation:** this is on `foyer-unified-cache` (cut from PR #22254's head). It does **not**
touch PR #22146 (`lock-opt` / `origin/metadata-fix`). Pure experiment to evaluate the diff.

## Why foyer
`foyer-memory = 0.22.3` is already vendored (used by `block-cache-foyer`). It provides, out of the box:
- **All eviction algorithms we care about**: `fifo, lru, lfu, s3fifo, sieve` (so we don't hand-write/maintain S3-FIFO at all).
- **Byte-bounded** capacity via a `with_weighter(|k,v| bytes)` — exactly our size-limit model.
- **Sharded, lock-free-ish concurrency** (`with_shards(n)`) — no global `Mutex<policy>` on the read path (the contention our micro-bench A/B flagged; foyer solves it natively).
- Clean API: `insert / get / remove / contains / usage() / clear / evict_all`.

```rust
foyer::CacheBuilder::new(capacity_bytes)
    .with_shards(n)
    .with_eviction_config(S3FifoConfig::default())   // runtime-selectable
    .with_weighter(|_k, v: &V| v.byte_size())
    .build()
```

## The four caches (on #22254 base)
| Cache | Key | Value | Today's backing |
|---|---|---|---|
| Metadata (footer) | `Path` | `CachedFileMetadataEntry` | `Mutex<DefaultFilesMetadataCache>` (hardcoded LRU) |
| Statistics | `Path` | `CachedFileMetadata` | `DashMap + Mutex policy` (CustomStatisticsCache) |
| ColumnIndex (page) | `CiCellKey` | `ColumnIndexMetaData` | `BoundedCache` (DashMap + Fifo policy) |
| OffsetIndex (page) | `OiCellKey` | `OiColumn` | `BoundedCache` (DashMap + Fifo policy) |

## Design: `FoyerBackedCache<K, V>`
A generic wrapper exposing the **union of the existing call sites' methods** so migration is mechanical:
- `with_eviction(limit_bytes, CacheEvictionPolicy, weighter)` — maps our enum → foyer `EvictionConfig`
  (`Lru→LruConfig, Lfu→LfuConfig, Fifo→FifoConfig, S3Fifo→S3FifoConfig`).
- `get(&K) -> Option<V>`, `insert(K, V, size)` (size→weighter), `insert_batch`, `contains`, `remove`.
- `set_limit(bytes)` (foyer `resize`), `usage()`/`stats()` (hits/misses/usage), `clear`/`clear_keep_limit`.
- `evict_by_prefix(prefix)` — page-index caches evict all cells for a segment; foyer has no native
  prefix scan, so keep a side index (or iterate) — **the one method needing care** (see Open issues).

Then:
- `CustomStatisticsCache` → wraps `FoyerBackedCache<Path, CachedFileMetadata>`.
- `BoundedCache` (CI/OI) → becomes a thin alias over `FoyerBackedCache`, default `S3Fifo` per the
  "S3-FIFO on page-index" decision.
- Metadata footer cache: per the standing decision we keep #22254's `Mutex<DefaultFilesMetadataCache>`
  by default, but this wrapper lets us optionally back it with foyer-S3Fifo too (flag).

## Open issues to resolve during impl
1. **`evict_by_prefix`** — foyer keys aren't prefix-scannable. Options: (a) maintain a
   `DashMap<segment_prefix, HashSet<K>>` side index updated on insert/evict; (b) store keys so a
   cheap `clear`+rebuild works; (c) foyer entry properties/tags. Likely (a).
2. **`is_valid_for`** (metadata) — staleness check stays caller-side on the returned entry; unchanged.
3. **CacheEntry lifetime** — foyer `get` returns a `CacheEntry` guard; we `.value().clone()` to match
   the existing `-> Option<V>` clone semantics.
4. **Stats parity** — foyer exposes its own stats; map to the existing `ScopedCacheStats`
   (hit/miss/eviction/used_bytes/limit) shape the Java stats endpoint reads.
5. **Weighter must equal the old `size`** so byte accounting / size-limit behavior is unchanged.

## foyer API (confirmed from source clone `/Users/abandeji/Public/workplace/foyer-src` @ v0.22.3)
```rust
use foyer::{Cache, CacheBuilder, CacheEntry, EvictionConfig,
            FifoConfig, LfuConfig, LruConfig, S3FifoConfig};
let cache: Cache<K,V> = CacheBuilder::new(capacity_bytes)
    .with_shards(n)
    .with_eviction_config(S3FifoConfig { small_queue_capacity_ratio: 0.25,
                                         ghost_queue_capacity_ratio: 0.0,   // ghost-off
                                         small_to_main_freq_threshold: <default> })
    .with_weighter(|_k, v: &V| v.byte_size())
    .build();
cache.insert(k, v) -> CacheEntry;          // CacheEntry: Deref<Target=V> + Clone; .value()/.weight()
cache.get(&k) -> Option<CacheEntry>;       // .value().clone() for Option<V> parity
cache.resize(new_cap) -> Result<()>;       // dynamic size-limit (our set_limit)
cache.usage() -> usize;  cache.clear();
```
**Our S3-FIFO tuning maps 1:1 to foyer's `S3FifoConfig`** (per `project_s3fifo_tuning_findings`):
`small_queue_capacity_ratio = 0.25` + `ghost_queue_capacity_ratio = 0.0` (ghost-off) = our empirically-best config. So foyer's production S3-FIFO replaces our hand-written policy with no loss of tuning — and far less code to maintain.

## Validation plan
Build + `cargo test --lib cache` on the node; confirm hit/miss/usage stats and the page-index
prefix-eviction tests pass. Then (optionally) the ClickBench hit-rate A/B to compare foyer-S3Fifo
vs the current Fifo on the page-index caches.
