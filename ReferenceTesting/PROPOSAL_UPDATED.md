# Proposal: Retroactive Star Tree Index Building via Per-Segment Upgrade

## Problem Statement

Today, star tree indexes can only be created at index creation time by setting `index.composite_index: true`. Existing indices with historical data cannot benefit from star tree acceleration without re-indexing all data into a new index. For large indices (hundreds of GBs or TBs), re-indexing is expensive, time-consuming, and operationally risky.

We need a way to retroactively add star tree indexes to existing indices without re-indexing.

## Proposed Solution

A per-segment upgrade approach that builds star tree data directly from existing doc values, rewrites segment metadata to declare the new codec, and reopens the engine. The upgrade is a one-time operation after which new segments automatically include star tree data.

## How It Works

The upgrade is a 3-phase operation:

### Phase 1: Mapping Update (cluster-wide, once)

Adds the star tree field configuration to the index mapping under the composite section. This requires bypassing two restrictions:

- `index.composite_index` is a Final setting that cannot be changed after index creation. The upgrade sets a bypass flag (`allowCompositeFieldWithoutSettings`) on the mapping parser context so `StarTreeMapper.Builder` skips this check.
- `CompositeIndexValidator` normally blocks adding new composite fields during mapping updates. A new `MergeReason.STAR_TREE_UPGRADE` tells the validator to skip this restriction while still validating that dimensions and metrics reference real fields with compatible types.

The mapping update is submitted as a cluster state task and waits for all nodes to acknowledge, ensuring every node's `MapperService` has the composite field types before the per-shard work begins. Additionally, both `index.append_only.enabled` and `index.composite_index` are force-set to `true` in the cluster state update — the former to protect star tree correctness (no deletes/updates after upgrade), the latter to ensure all future `CodecService` instances include the composite codec without needing runtime overrides.

### Phase 2: Per-Segment Star Tree Build (per-shard, parallel)

For each primary shard:

1. Flush all in-memory data to disk
2. Block all write operations on the shard
3. Swap the InternalEngine to a ReadOnlyEngine (reads continue serving from the last committed state)
4. For each segment not already using Composite912Codec (processed in parallel across available CPU cores):
   - Open a SegmentReader using the segment's existing codec
   - Get the DocValuesProducer from the reader
   - Build a live docs bitset from hard deletes (`.liv` file) and soft deletes (`__soft_deletes` field) — only live documents are included in the star tree
   - If deletes exist, wrap the DocValuesProducer in a `LiveDocsFilteredDocValuesProducer` that remaps doc IDs to exclude deleted documents
   - Build the fieldProducerMap for configured dimensions and metrics
   - Build star tree via the existing `StarTreesBuilder.build()` infrastructure
   - Write `.cid`, `.cim`, `.cidvd`, `.cidvm` files to the segment directory

A ReadOnlyEngine serves reads throughout this phase. It holds a point-in-time DirectoryReader on the pre-upgrade `segments_N`. Since it does not hold the write lock, Phase 3 can write to the directory without conflict.

Segments are built in parallel using a thread pool sized to `Math.max(1, Math.min(eligibleSegments, availableProcessors / 2))`. Each segment's build is independent — they read from different segment files and write to different output files.

### Phase 3: SegmentInfos and .si Rewrite (per-shard)

For each upgraded segment, the codec switch depends on whether the segment has soft-delete doc values updates:

**Clean segments** (`docValuesGen == -1`):
- Create a new SegmentInfo with `Composite912Codec` (all other fields copied from original)
- Add the 4 star tree files to the segment's file set
- Copy `fieldInfosFiles` and `docValuesUpdatesFiles` maps
- Delete the old `.si` file and write a new one declaring `Composite912Codec`

**Segments with soft deletes** (`docValuesGen != -1`):
- Keep the original codec (`Lucene104Codec`) — switching codecs on these segments causes field number mismatches that are incompatible with Lucene's `SegmentDocValuesProducer` routing
- Add the 4 star tree files to the segment's file set
- Rewrite the `.si` file to persist the expanded file set (without changing the codec)

Commit `segments_N+1` atomically.

After the commit:
1. Close the ReadOnlyEngine and open a fresh InternalEngine from `segments_N+1`
2. Call `skipTranslogRecovery()` on the new engine (since it was created outside the normal shard recovery flow, `pendingTranslogRecovery` would otherwise block all flushes)
3. Refresh the searcher to warm it up
4. Populate the `StarTreeDirectReader` cache for soft-delete segments (these segments serve star tree data via a direct file reader that bypasses the codec)
5. Unblock operations

### Post-Upgrade Behaviour

Once the upgrade completes:

- New segments from flushes and refreshes are written natively with `Composite104Codec`, including inline star tree data. No additional upgrade steps needed.
- Background merges produce clean native `Composite104Codec` segments, gradually replacing the retroactively upgraded ones. The merge path correctly filters soft-deleted documents when building star tree data for the merged segment.
- The star tree index stays current with all new data automatically.
- Documents are ingested with the same mapping as before. Star tree is built transparently at flush time from the doc values of the configured dimension and metric fields.

## Read Path Compatibility

Three compatibility mechanisms handle upgraded segments:

### 1. Per-field doc values naming

Upgraded segments have doc values files with per-field naming (e.g., `_0_Lucene90_0.dvd`) because they were originally written by `Lucene912Codec`'s `PerFieldDocValuesFormat`. `Composite912DocValuesFormat.fieldsProducer()` detects `PerFieldDocValuesFormat.format` and `PerFieldDocValuesFormat.suffix` attributes in FieldInfos. It then checks whether the suffixed file actually exists on disk (`perFieldSuffixedFileExists()`) — if yes, it creates a `SegmentReadState` with the correct suffix so the delegate opens the right files. If the suffixed file does not exist (as happens after a merge produces a new segment with stale attributes), it falls back to empty suffix. This detection is generic and not hardcoded to any specific Lucene version.

### 2. Compound file segments

For `.cfs` segments, star tree files are written outside the compound file (since `.cfs` is read-only after creation). `Composite912DocValuesReader` tries `readState.directory` first (CompoundDirectory), and if `.cim` is not found, falls back to `readState.segmentInfo.dir` (parent directory).

### 3. StarTreeDirectReader cache (soft-delete segments)

Segments where the codec was NOT switched (`docValuesGen != -1`) cannot serve star tree data through the normal `Composite912DocValuesReader` path. Instead, after the engine restarts, `IndexShard.populateStarTreeDirectReaderCache()` creates a `StarTreeDirectReader` for each such segment and stores it in a `ConcurrentHashMap<String, StarTreeDirectReader>` keyed by segment name.

Each `StarTreeDirectReader` holds:
- An open `IndexInput` on the `.cid` (star tree data) file
- Sliced `IndexInput` instances per star tree field (for tree traversal)
- Parsed `StarTreeMetadata` from the `.cim` file (dimensions, metrics, data offsets)
- A `DocValuesProducer` opened on the `.cidvd`/`.cidvm` files (for reading pre-aggregated metric values)
- A synthetic `FieldInfos` and `SegmentReadState` constructed from the star tree metadata

The reader implements `CompositeIndexReader`, so it can be used interchangeably with `Composite912DocValuesReader` in the query path. `StarTreeQueryHelper.getStarTreeValues()` checks this cache as a fallback when the segment's `DocValuesReader` is not a `CompositeIndexReader`.

Cache entries are evicted when a segment is merged away — detected via a merge cleanup callback wired to `InternalEngine.afterMerge()`. The callback compares cached segment names against the current `SegmentInfos` and closes/removes stale entries.

## Delete Support

The upgrade fully supports indices with prior document deletions:

### How deletes are handled during upgrade (Phase 2)

When building star tree data for a segment with deletes:
1. `buildLiveDocsBitset()` constructs a `FixedBitSet` by combining hard deletes (from the `.liv` file via `segmentReader.getLiveDocs()`) and soft deletes (from the `__soft_deletes` numeric doc values field via `segmentReader.getNumericDocValues()`)
2. The `DocValuesProducer` is wrapped in `LiveDocsFilteredDocValuesProducer` (remap mode) which remaps contiguous doc IDs to only live documents
3. `SegmentWriteState` is created with `numLiveDocs` (not `maxDoc`), so the star tree is built from exactly the live document set

### How deletes are handled during merge (post-upgrade)

When background merges combine upgraded segments:
1. Source segments with `Lucene104Codec` (soft-delete segments) don't have a `CompositeIndexReader`, triggering the merge fallback path in `Composite912DocValuesWriter`
2. During `super.merge()`, the `__soft_deletes` producer is captured when `addNumericField("__soft_deletes", ...)` is called
3. `buildSoftDeleteLiveDocsBitset()` iterates the captured producer to identify deleted docs
4. Each merge field producer is wrapped in `LiveDocsFilteredDocValuesProducer` (skip-only mode) which skips deleted docs during sequential iteration
5. The merged star tree contains only live documents

**Why `mergeState.liveDocs` doesn't work**: Lucene's `mergeState.liveDocs[i]` only reflects hard deletes. Soft-deleted documents appear "live" in this array. The `__soft_deletes` field captured during the merge is the only source of truth for soft delete state.

### Why codec switch is incompatible with `docValuesGen != -1`

When soft deletes are applied to a segment, Lucene creates generation-based update files (`_0_1.fnm`, `_0_1_Lucene90_0.dvd/dvm`) with UPDATED field numbers. The base doc values inside `.cfs` retain ORIGINAL field numbers. Switching the codec to `Composite912Codec` causes `Lucene90DocValuesProducer` to be initialized with the updated field infos, but it reads from the base `.dvm` which has different field numbers — resulting in `CorruptIndexException`. Further, `PendingSoftDeletes` reads `__soft_deletes` through the codec reader, and the field doesn't exist in the original field infos, causing `softDeleteCount` assertion failures.

The hybrid approach (skip codec switch + direct reader cache) avoids this entirely.

## Known Limitations and Risks

### Shard write unavailability during upgrade

Writes are blocked for the duration of the upgrade. Reads remain available throughout the star tree build and SegmentInfos rewrite phases via the ReadOnlyEngine bridge. During the final engine swap (ReadOnlyEngine → InternalEngine), there is a brief window (typically <100ms) where reads may return empty results (HTTP 200 with 0 hits) because the searcher momentarily has no segments visible. For 1M docs the write-blocked window is approximately 8-10 seconds (with parallel build). For 100k docs it is approximately 1.8 seconds.

### `index.composite_index` setting is force-set to true

During the Phase 1 mapping update, `index.composite_index` is force-set to `true` in the cluster state alongside `index.append_only.enabled`. Although this is normally a `Final` setting that cannot be changed after index creation, the `StarTreeUpgradeMappingExecutor` bypasses this restriction by directly writing the setting into `IndexMetadata.Builder.settings()` during the cluster state update (the Final check only applies to the Settings Update API, not to direct cluster state manipulation). This ensures that subsequent `CodecService` instances (created during shard recovery, node restart, or replica allocation) automatically include `Composite912Codec` without needing the volatile `codecServiceOverride` mechanism.

### `index.append_only.enabled` is force-set to true

After upgrade, the index becomes append-only (required for composite indices). Deletes BEFORE upgrade are supported and correctly filtered. Deletes AFTER upgrade are blocked by this setting.

### Direct Lucene file manipulation

The approach rewrites `.si` files and `SegmentInfos` directly, bypassing `IndexWriter`. This relies on Lucene internal behaviours:

- `.si` file format and `SegmentInfos.commit()` semantics
- `PerFieldDocValuesFormat.format` and `PerFieldDocValuesFormat.suffix` FieldInfo attributes
- `Lucene90DocValuesFormat` as the delegate for reading doc values

These are not part of Lucene's public API contract and could change in future Lucene versions. Each Lucene upgrade requires verification that these assumptions still hold.

### Concurrent upgrade rejection

Only one upgrade can run per shard at a time. A second concurrent request is rejected via an `AtomicBoolean` guard (`starTreeUpgradeInProgress`).

## Test Results

### 5-doc test (basic functionality)

- Index: 1 shard, 1 segment, 5 docs
- Upgrade: successful, `_shards.successful: 1`
- Doc count preserved: 5
- Star tree files (`.cid`, `.cim`, `.cidvd`, `.cidvm`) present
- Aggregation works with `terminated_early: true` (star tree optimization active)

### 100k-doc test (ecommerce schema)

- Index: 1 shard, 5 segments, 100,000 docs
- Dimensions: customer_gender, currency, day_of_week, order_date
- Metrics: taxful_total_price, taxless_total_price, total_quantity, total_unique_products (sum, avg, min, max, value_count)
- Upgrade time: 1.8 seconds (parallel, 6 threads)
- Doc count preserved: 100,000
- Star tree files present for all 5 segments
- Aggregation latency: 14ms before upgrade, 1-2ms after upgrade

### 1M-doc test (ecommerce schema, parallel build)

- Index: 1 shard, 10 segments, 1,000,000 docs
- Dimensions: customer_gender, currency, day_of_week, order_date
- Metrics: taxful_total_price, taxless_total_price, total_quantity, total_unique_products
- Upgrade time: 8-10 seconds (parallel build across available cores)
- Doc count preserved: 1,000,000
- Cold aggregation latency: 40-100ms before upgrade, 2-4ms after upgrade (10-20x speedup)
- Star tree files present for all 10 segments
- Reads available throughout upgrade (0 wrong values during build phases, 2/16 reads during the engine swap window returned aggregation value of 0/null because the shard had no active searcher momentarily)

### 1M-doc test with deletes

- Index: 1 shard, 7 segments, 1,000,000 docs + 50,000 soft deletes
- Upgrade time: 7.5 seconds (parallel)
- Both paths exercised: 7 segments via direct reader cache, remaining via native codec
- Aggregation: ALL VALUES MATCH (before == after)
- `terminated_early: true` — star tree active on all segments
- Doc count: 950,000 (correct, deleted docs excluded)
- 0 wrong values during concurrent reads

### 1M-doc test with deletes + force merge

- Index: 1 shard, 6 segments, 1,000,000 docs + ~100,000 soft deletes
- Upgrade time: 23.3 seconds
- Force merge time: 18.2 seconds
- Live docs after delete: 899,796
- Aggregation after upgrade: exact match
- Aggregation after merge: exact match
- Merged segment: single segment, `docs=899796, deleted=0`
- Star tree active: `terminated_early: true`
- Merged codec: `Composite104Codec` (native)

### 181M-doc test (http_logs benchmark)

- Upgrade time: approximately 6 minutes (no force merge)
- Reindexing the same 181M docs takes 24.8 hours

## Why Not Force Merge?

`forceMerge(1)` rewrites ALL segment data (stored fields, postings, norms, point values, doc values) into one new segment. For 1M docs with around 20 fields, the star tree build is roughly 20-30% of the total time. The other 70-80% is Lucene rewriting data unrelated to star tree. Per-segment build only reads the dimension and metric doc values columns and writes star tree files, making it significantly faster.

## Why Not IndexUpgrader?

`IndexUpgrader` opens an `IndexWriter` internally, which:
- Acquires a write lock on the directory, conflicting with the ReadOnlyEngine's open readers
- Reads the committed SegmentInfos file set — it has no way to discover the star tree files written in Phase 2 (they're not in any committed file set yet)
- Upgrades ALL segments to the new codec, including segments that failed in Phase 2 (creating corrupt state)
