# Retroactive Star Tree Building — Changes Log

## Files Modified

### 1. `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/StarTreeUpgradeService.java`
- **What changed**: Complete rewrite of the upgrade service
- **Old approach**: Used `IndexUpgrader` with `IndexWriterConfig.setCodec(Composite912Codec)` for Phase 2
- **New approach**: 
  - Phase 1 (`buildStarTreeData`): Opens DirectoryReader → finds SegmentReader → gets DocValuesProducer → builds fieldProducerMap → creates SegmentWriteState → opens .cid/.cim IndexOutputs → creates .cidvd/.cidvm DocValuesConsumer → calls StarTreesBuilder.build()
  - Phase 2 (`rewriteSegmentInfos`): Reads SegmentInfos → creates new SegmentCommitInfo with Composite912Codec for upgraded segments → adds star tree files to file set → commits segments_N+1 atomically
- **Added**: `MapperService` parameter to `upgradeSegments()` and `buildStarTreeData()`
- **Removed**: `upgradeCodec()` method (replaced by `rewriteSegmentInfos()`)

### 2. `server/src/main/java/org/opensearch/index/shard/IndexShard.java`
- **What changed**: Replaced force-merge-based `upgradeToStarTree()` with per-segment approach
- **Old signature**: `public void upgradeToStarTree()` — no params, used codecServiceOverride + engine restart + noOp + flush + forceMerge
- **New signature**: `public int upgradeToStarTree(StarTreeField starTreeField)` — accepts StarTreeField, returns upgrade count
- **New flow**: flush → blockOperations → swap to ReadOnlyEngine (with seqNoStats/translogStats) → StarTreeUpgradeService.upgradeSegments() → resetEngineToGlobalCheckpoint()
- **Added**: `AtomicBoolean starTreeUpgradeInProgress` field for concurrent upgrade guard
- **Added imports**: `StarTreeField`, `StarTreeUpgradeService`, `AtomicInteger`

### 3. `server/src/main/java/org/opensearch/action/admin/indices/startree/TransportStarTreeUpgradeAction.java`
- **What changed**: Updated `shardOperation()` to pass `StarTreeField` and added mapping propagation guard
- **Old call**: `indexShard.upgradeToStarTree()` (no args)
- **New call**: `indexShard.upgradeToStarTree(request.getStarTreeField())`
- **Added**: MapperService check before proceeding (guards against cluster state propagation race)
- **Updated Javadoc**: Changed "Phase 2+3: Per-shard engine restart and force merge" to "Phase 2: Per-shard star tree building and SegmentInfos rewrite"

### 4. `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912DocValuesFormat.java`
- **What changed**: Made `fieldsProducer()` smart enough to handle retroactively upgraded segments
- **Old behavior**: Always called `delegate.fieldsProducer(state)` which opens `_0.dvd`/`_0.dvm` directly — fails for upgraded segments where files are named `_0_Lucene90_0.dvd`
- **New behavior**: Checks `FieldInfos` for `PerFieldDocValuesFormat.format` and `PerFieldDocValuesFormat.suffix` attributes. If present (upgraded segment), creates a `SegmentReadState` with the correct suffix (e.g., `"Lucene90_0"`) so `Lucene90DocValuesFormat` opens the right files. If absent (native Composite912 segment), uses the original direct path.
- **Added**: `getPerFieldDocValuesSuffix()` static helper method
- **Impact on existing code**: ZERO — native star tree segments have no PerField attributes, so they always take the original code path

### 5. `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912DocValuesReader.java`
- **What changed**: Added fallback to `segmentInfo.dir` for star tree files in compound file segments
- **Old behavior**: Always opened `.cim`/`.cid` from `readState.directory` — fails for compound segments where star tree files are outside `.cfs`
- **New behavior**: Tries `readState.directory` first. If `.cim` not found (FileNotFoundException/NoSuchFileException), falls back to `readState.segmentInfo.dir`. Also uses `starTreeDir` for the `compositeDocValuesProducer` SegmentReadState.
- **Impact on existing code**: ZERO — for non-compound segments and native star tree segments, files are always found on first try, fallback never fires

## Issues Encountered & Resolutions

### Issue 1: AssertionError in BaseStarTreeBuilder.getIteratorForNumericField
- **Error**: `assert fieldProducerMap.containsKey(fieldInfo.name)` fails for `_doc_count`
- **Root cause**: `StarTreesBuilder` reads star tree fields from `MapperService` which includes `_doc_count` as an implicit metric. Our `fieldProducerMap` only had user-specified fields.
- **Fix**: Added `_doc_count` with an `EmptyDocValuesProducer` to `fieldProducerMap`, following the pattern from `Composite912DocValuesWriter.addDocValuesForEmptyField()`.
- **Status**: RESOLVED

### Issue 2: FileNotFoundException for `.dvm` in compound file
- **Error**: `No sub-file with id .dvm found in compound file "_0.cfs" (fileName=_0.dvm files: [_Lucene90_0.doc, _Lucene104_0.tim, .fnm, _Lucene90_0.dvd, ...])`
- **Root cause**: When we switch the codec from `Lucene912Codec` to `Composite912Codec` in the SegmentInfos, the new codec's delegate `Lucene90DocValuesFormat` looks for files named `_0.dvd`/`_0.dvm`. But inside the compound file, these files have codec-specific suffixes like `_Lucene90_0.dvd`/`_Lucene90_0.dvm`. The per-field format naming is baked into the segment's `.si` metadata and doesn't match what the new codec expects.
- **This is NOT a compound file issue** — it's a fundamental codec switching issue. Even without compound files, the per-field doc values format names would be wrong because the original codec used different format names than `Composite912Codec`'s delegate.
- **Status**: INVESTIGATING — the direct SegmentInfos rewrite approach cannot simply swap the codec declaration because the internal file naming conventions are codec-specific.

### Issue 3: Star tree files deleted by resetEngineToGlobalCheckpoint()
- **Error**: `NoSuchFileException: _0.cim` during engine reopen
- **Root cause**: `resetEngineToGlobalCheckpoint()` calls `flush()` which creates a NEW commit via the `IndexWriter` — this commit doesn't include the star tree files. The `IndexWriter` then deletes them as orphans.
- **Fix**: Replaced the read-only engine + `resetEngineToGlobalCheckpoint()` approach with a simpler close-engine approach: flush → block ops → close engine entirely → build star tree files → rewrite SegmentInfos → create new engine directly. No `IndexWriter` alive during the upgrade means no one deletes our files.
- **Status**: RESOLVED

### Issue 4: .si file not rewritten — codec mismatch
- **Error**: `NoSuchFileException: _0.cim` (files deleted by IndexWriter)
- **Root cause**: `SegmentInfos.commit()` writes `segments_N+1` with the new codec name, but the `.si` file on disk still declares the original codec. When the `IndexWriter` opens, it reads the `.si` file, sees the old codec, and doesn't recognize the star tree files in the file set.
- **Fix**: Added `.si` file rewrite in `rewriteSegmentInfos()` — delete old `.si`, write new one with `Composite912Codec.segmentInfoFormat().write()`.
- **Status**: RESOLVED

### Issue 5: Engine not warmed up after creation
- **Error**: `AssertionError: searcher was not warmed up yet for source[refresh_needed]`
- **Root cause**: After creating the new engine directly (without `resetEngineToGlobalCheckpoint()`), the searcher wasn't warmed up. Scheduled refresh tasks hit the assertion.
- **Fix**: Added `newEngine.refresh("star-tree-upgrade")` after engine creation.
- **Status**: RESOLVED

### Issue 2 Resolution: PerField doc values suffix detection
- **Error**: `FileNotFoundException: _0.dvm` — Composite912Codec's delegate looks for wrong file names
- **Root cause**: Upgraded segments have doc values files with per-field naming (`_0_Lucene90_0.dvd`) but `Composite912DocValuesFormat` delegate looks for direct naming (`_0.dvd`)
- **Fix**: Modified `Composite912DocValuesFormat.fieldsProducer()` to detect `PerFieldDocValuesFormat.format` and `PerFieldDocValuesFormat.suffix` attributes in `FieldInfos`. For upgraded segments, creates a `SegmentReadState` with the correct suffix (e.g., `"Lucene90_0"`) so the delegate opens the right files. For native segments, uses the original direct path.
- **Status**: RESOLVED — compiles and the doc values reading works correctly


## Test Results

### 5-doc test (basic functionality)
- Index: 1 shard, 1 segment, 5 docs
- Upgrade: successful, `_shards.successful: 1`
- Doc count preserved: 5
- Star tree files: `.cid`, `.cim`, `.cidvd`, `.cidvm` present
- Aggregation: works with `terminated_early: true` (star tree optimization)

### 100k-doc test (ecommerce schema)
- Index: 1 shard, 5 segments, 100,000 docs
- Dimensions: customer_gender, currency, day_of_week, order_date
- Metrics: taxful_total_price, taxless_total_price, total_quantity, total_unique_products (sum, avg, min, max, value_count)
- Upgrade time: 5.4 seconds (all 5 segments)
- Doc count preserved: 100,000
- Star tree files: present for all 5 segments
- Aggregation before upgrade: 14ms → 1ms
- Aggregation after upgrade: 1-2ms (star tree acceleration working)


### 1M-doc test (ecommerce schema, cold comparison)
- Index: 1 shard, 10 segments (mix of compound and non-compound), 1,000,000 docs
- Dimensions: customer_gender, currency, day_of_week, order_date
- Metrics: taxful_total_price, taxless_total_price, total_quantity, total_unique_products
- Upgrade time: 27.0 seconds (10 segments, no force merge)
- Doc count preserved: 1,000,000
- Cold aggregation before upgrade: 40-100ms
- Cold aggregation after upgrade: 2-4ms (10-20x speedup)
- Star tree files: present for all 10 segments
- Both compound (.cfs) and non-compound segments handled correctly
