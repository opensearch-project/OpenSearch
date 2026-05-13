# Star Tree Per-Segment Upgrade — Full Architecture & Method Reference

## Overview

This document traces every file, class, method, return type, and inheritance chain involved in the per-segment star tree upgrade feature. It covers what each component did before our changes, what we added/modified, and the complete execution flow.

---

## File 1: TransportStarTreeUpgradeAction.java

**Path**: `server/src/main/java/org/opensearch/action/admin/indices/startree/TransportStarTreeUpgradeAction.java`

**Status**: NEW FILE (did not exist before our changes)

**Extends**: `TransportBroadcastByNodeAction<StarTreeUpgradeRequest, StarTreeUpgradeResponse, ShardStarTreeUpgradeResult>`

### What TransportBroadcastByNodeAction Does

`TransportBroadcastByNodeAction` is an abstract base class for actions that execute on every shard of one or more indices. It:
- Groups target shards by the node they live on
- Sends one transport request per node (not per shard)
- Each node executes `shardOperation()` on its local shards sequentially
- Collects `ShardResult` objects from all nodes
- Calls `newResponse()` to build the final response from all shard results
- Handles shard failures via `DefaultShardOperationFailedException`

**Type parameters**:
- `Request` = `StarTreeUpgradeRequest` — the client request
- `Response` = `StarTreeUpgradeResponse` — the aggregated response
- `ShardResult` = `ShardStarTreeUpgradeResult` — per-shard result

### Methods

#### `doExecute(Task task, StarTreeUpgradeRequest request, ActionListener<StarTreeUpgradeResponse> listener)`
- **Override of**: `TransportBroadcastByNodeAction.doExecute()`
- **Called by**: Transport framework when the action is invoked
- **What it does**:
  1. Resolves concrete index names from the request via `indexNameExpressionResolver.concreteIndexNames()`
  2. Checks idempotency: if ALL indices already have star tree config → skips mapping update, calls `super.doExecute()` directly
  3. Validates no index has a DIFFERENT star tree config (would be a conflict)
  4. Calls `submitMappingUpdate()` to add star tree field to cluster state
  5. On mapping ack → calls `super.doExecute()` which triggers the broadcast
- **Returns**: void (async via listener)
- **When it returns**: After the mapping update is acknowledged AND all shard operations complete (or fail)

#### `shardOperation(StarTreeUpgradeRequest request, ShardRouting shardRouting)`
- **Override of**: `TransportBroadcastByNodeAction.shardOperation()`
- **Called by**: The broadcast framework on each node, once per local shard
- **What it does**:
  1. Gets `IndexShard` from `IndicesService` via `indicesService.indexServiceSafe(shardRouting.shardId().getIndex()).getShard(shardRouting.shardId().id())`
  2. Checks mapping propagation: `indexShard.mapperService().getCompositeFieldTypes().isEmpty()` — if empty, throws IOException asking for retry (cluster state hasn't propagated yet)
  3. Calls `indexShard.upgradeToStarTree(request.getStarTreeField())`
  4. Catches `InterruptedException` → re-interrupts thread + wraps in IOException
  5. Catches `TimeoutException` → wraps in IOException
- **Returns**: `ShardStarTreeUpgradeResult(shardRouting.shardId(), shardRouting.primary())`
- **When it returns**: After the shard upgrade completes (Phase 1 + Phase 2 + engine restart)
- **Throws**: `IOException` on mapping not propagated, interrupt, timeout, or upgrade failure

#### `shards(ClusterState clusterState, StarTreeUpgradeRequest request, String[] concreteIndices)`
- **Override of**: `TransportBroadcastByNodeAction.shards()`
- **Called by**: The broadcast framework to determine which shards to operate on
- **What it does**:
  1. Gets `ShardsIterator` via `clusterState.routingTable().allShards(concreteIndices)` — this returns ALL shards (primary + replica) for the given indices
  2. Checks all primaries are active via `indicesWithMissingPrimaries()`
  3. If any primary is missing → throws `PrimaryMissingActionException`
- **Returns**: `ShardsIterator` containing all shard routings (primary + replica)
- **When it returns**: Immediately (synchronous)
- **NOTE**: Currently broadcasts to ALL shards including replicas. Should be filtered to primary only.

#### `newResponse(StarTreeUpgradeRequest, int totalShards, int successfulShards, int failedShards, List<ShardStarTreeUpgradeResult>, List<DefaultShardOperationFailedException>, ClusterState)`
- **Override of**: `TransportBroadcastByNodeAction.newResponse()`
- **Called by**: The broadcast framework after all shard operations complete
- **Returns**: `new StarTreeUpgradeResponse(totalShards, successfulShards, failedShards, shardFailures)`
- **When it returns**: Immediately (synchronous constructor call)

#### `submitMappingUpdate(StarTreeUpgradeRequest request, String[] concreteIndices, ActionListener<Boolean> listener)`
- **Called by**: `doExecute()` after validation
- **What it does**:
  1. Creates `StarTreeUpgradeMappingTask` with request + concrete indices
  2. Submits to `clusterService.submitStateUpdateTask()` with `Priority.HIGH`
  3. Uses `StarTreeUpgradeMappingExecutor` as the executor
  4. Uses `AckedClusterStateTaskListener` to wait for all nodes to acknowledge
  5. `onAllNodesAcked()` → calls `listener.onResponse(true)` if no exception
  6. `onAckTimeout()` → calls `listener.onResponse(false)` (1 minute timeout)
  7. `onFailure()` → calls `listener.onFailure(e)`
- **Returns**: void (async via listener)
- **When it returns**: After all nodes acknowledge the cluster state update (or timeout)

#### `buildCompleteMappingSource(StarTreeUpgradeRequest request, IndexMetadata indexMetadata)`
- **Static method**
- **Called by**: `applyStarTreeMapping()` in the executor
- **What it does**:
  1. Gets existing mapping from `indexMetadata.mapping().sourceAsMap()`
  2. Copies all top-level fields except "composite"
  3. Serializes `StarTreeField` to JSON via `starTreeField.toXContent()`
  4. Removes "name" and dimension "type" fields from config (StarTreeMapper.Builder doesn't expect them)
  5. Builds composite section: `{ "composite": { "my_star_tree": { "type": "star_tree", "config": {...} } } }`
  6. Wraps in `_doc` type: `{ "_doc": { "properties": {...}, "composite": {...} } }`
- **Returns**: `CompressedXContent` — the complete mapping JSON
- **When it returns**: Immediately (synchronous)
- **Why complete mapping**: `StarTreeMapper.Builder.getDimension()` validates dimension fields by looking them up in `objbuilder.mappersBuilders`, which only contains fields from the current mapping source being parsed. Without existing properties, dimension validation fails.

#### `hasExistingStarTreeConfig(IndexMetadata indexMetadata)`
- **Static, private**
- **Returns**: `boolean` — true if the index mapping already has a non-empty "composite" section
- **When it returns**: Immediately

#### `indicesWithMissingPrimaries(ClusterState, String[])`
- **Private**
- **Returns**: `Set<String>` — index names where not all primaries are active
- **When it returns**: Immediately

### Inner Class: StarTreeUpgradeMappingExecutor

**Implements**: `ClusterStateTaskExecutor<StarTreeUpgradeMappingTask>`

`ClusterStateTaskExecutor` is the interface for cluster state update tasks. The master node calls `execute()` with the current cluster state and a batch of tasks. The executor returns a new cluster state.

#### `execute(ClusterState currentState, List<StarTreeUpgradeMappingTask> tasks)`
- **Called by**: Cluster state task framework on the master node
- **What it does**: Iterates tasks, calls `applyStarTreeMapping()` for each, builds `ClusterTasksResult`
- **Returns**: `ClusterTasksResult<StarTreeUpgradeMappingTask>` with updated cluster state
- **When it returns**: After all tasks are processed

#### `applyStarTreeMapping(ClusterState, StarTreeUpgradeMappingTask, Map<Index, MapperService>)`
- **Called by**: `execute()`
- **What it does** (per index):
  1. Creates `MapperService` via `indicesService.createIndexMapperService(indexMetadata)`
  2. Sets `mapperService.documentMapperParser().setAllowCompositeFieldWithoutSettings(true)` — bypasses `IS_COMPOSITE_INDEX_SETTING` check in `ObjectMapper.parseCompositeField()`
  3. Merges existing mappings: `mapperService.merge(indexMetadata, MergeReason.MAPPING_RECOVERY)` — loads current field types
  4. Builds complete mapping source via `buildCompleteMappingSource()`
  5. Merges star tree mapping: `mapperService.merge(SINGLE_MAPPING_NAME, mappingUpdateSource, MergeReason.STAR_TREE_UPGRADE)` — this triggers `StarTreeMapper.Builder` parsing and validation
  6. Validates: `CompositeIndexValidator.validate(mapperService, compositeIndexSettings, indexSettings, isCompositeFieldPresent, MergeReason.STAR_TREE_UPGRADE)` — bypasses "no new composite fields" restriction for upgrade merge reason
  7. Force-sets `index.append_only.enabled = true` in settings (bypasses Final restriction)
  8. Updates `IndexMetadata` with new mapping + settings + incremented versions
- **Returns**: Updated `ClusterState`
- **When it returns**: After all indices are processed

---

## File 2: IndexShard.java

**Path**: `server/src/main/java/org/opensearch/index/shard/IndexShard.java`

**Status**: MODIFIED (existing file, we added methods and fields)

**Extends**: `AbstractIndexShardComponent` implements `IndicesClusterStateService.Shard`

### What IndexShard Does

`IndexShard` is the core shard abstraction. Each shard of an index has one `IndexShard` instance on the node that hosts it. It:
- Manages the engine lifecycle (create, close, swap)
- Handles indexing operations (index, delete, bulk)
- Handles search operations (acquireSearcher)
- Manages the translog
- Coordinates with the replication tracker for sequence numbers and global checkpoints
- Handles flush, refresh, force merge
- Manages recovery (peer recovery, snapshot recovery)

### Key Fields (Relevant to Upgrade)

| Field | Type | Before Us | Our Change |
|-------|------|-----------|------------|
| `currentEngineReference` | `AtomicReference<Engine>` | Existed | Used for engine swaps |
| `engineMutex` | `Object` | Existed | Used for synchronized engine swaps |
| `indexShardOperationPermits` | `IndexShardOperationPermits` | Existed | Used to block writes |
| `codecService` | `CodecService` (final) | Existed | Not modified |
| `codecServiceOverride` | `volatile CodecService` | **NEW** | Set during upgrade so new engine uses Composite912Codec |
| `starTreeUpgradeInProgress` | `AtomicBoolean` | **NEW** | Prevents concurrent upgrades on same shard |
| `engineFactory` | `EngineFactory` | Existed | `newReadWriteEngine()` creates InternalEngine |
| `engineConfigFactory` | `EngineConfigFactory` | Existed | `newDefaultCodecService()` creates codec |
| `active` | `AtomicBoolean` | Existed | Set to true after engine restart |
| `mapperService` | `MapperService` | Existed | Passed to StarTreeUpgradeService |

### Methods We Added

#### `upgradeToStarTree(StarTreeField starTreeField)`
- **Called by**: `TransportStarTreeUpgradeAction.shardOperation()`
- **What it does**:
  1. `verifyActive()` — checks shard state is STARTED,
 throws `IllegalIndexShardStateException` if not
  2. `starTreeUpgradeInProgress.compareAndSet(false, true)` — atomic guard, throws `IllegalStateException` if already in progress
  3. `flush(new FlushRequest().force(true))` — first flush, commits in-memory indexing buffer to disk segments
  4. `indexShardOperationPermits.blockOperations(30, TimeUnit.MINUTES, lambda)` — blocks all write operations (index, delete, bulk). The lambda runs with writes blocked. Searches/reads are NOT blocked.
  5. Inside lambda: `flush(new FlushRequest().waitIfOngoing(true))` — second flush to catch writes between first flush and block
  6. `synchronized (engineMutex) { IOUtils.close(currentEngineReference.getAndSet(null)); }` — closes InternalEngine, releases IndexWriter, write lock, all file handles
  7. `StarTreeUpgradeService.upgradeSegments(store().directory(), starTreeField, mapperService)` — runs Phase 1 + Phase 2
  8. `codecServiceOverride = engineConfigFactory.newDefaultCodecService(indexSettings, mapperService, logger)` — creates new CodecService that includes Composite912Codec (because mapperService now has composite field types after mapping update)
  9. `synchronized (engineMutex) { Engine newEngine = engineFactory.newReadWriteEngine(newEngineConfig(replicationTracker)); ... }` — creates new InternalEngine from upgraded segments_N+1 commit
  10. `onNewEngine(newEngine)` — registers refresh listeners, translog sync, etc.
  11. `newEngine.refresh("star-tree-upgrade")` — warms the searcher so scheduled refreshes don't hit assertion errors
  12. `active.set(true)` — marks shard as active
  13. `codecServiceOverride = null` — clears override, future engine configs will be created fresh
- **Returns**: `int` — number of segments upgraded
- **When it returns**: After the full upgrade completes (Phase 1 + Phase 2 + engine restart)
- **Throws**: `IOException`, `InterruptedException`, `TimeoutException`

### Existing Methods We Use

#### `flush(FlushRequest request)`
- **What it does**: Commits the in-memory indexing buffer (Lucene's IndexWriter buffer) to disk as segments. Also commits the translog.
- **Calls**: `getEngine().flush(request)` → `InternalEngine.flush()`
- **Returns**: void
- **When it returns**: After the flush completes and data is on disk

#### `store()`
- **Returns**: `Store` — wrapper around Lucene's `Directory`
- `store().directory()` returns the `Directory` used by StarTreeUpgradeService to read/write segment files

#### `mapperService()`
- **Returns**: `MapperService` — holds all field type information for the index
- Used by `StarTreeUpgradeService` → `StarTreesBuilder` → `BaseStarTreeBuilder.generateMetricAggregatorInfos()` to resolve `FieldValueConverter` for each metric field

#### `newEngineConfig(ReplicationTracker replicationTracker)`
- **Returns**: `EngineConfig` — configuration for creating a new engine
- **Key behavior**: If `codecServiceOverride != null`, uses that instead of `codecService`. This is how we inject Composite912Codec into the new engine after upgrade.
- The `EngineConfig` includes: directory, index settings, translog config, merge policy, codec service, etc.

#### `onNewEngine(Engine engine)`
- **What it does**: Registers refresh listeners, translog sync processor, and other engine lifecycle hooks
- **Called after**: Engine creation, before the engine is used for reads/writes

#### `seqNoStats()` / `translogStats()`
- **Returns**: `SeqNoStats` / `TranslogStats` — current sequence number and translog statistics
- **Used for**: ReadOnlyEngine construction (Option B) — captures stats before closing InternalEngine

#### `verifyActive()`
- **What it does**: Checks `state == IndexShardState.STARTED`, throws `IllegalIndexShardStateException` if not
- **Returns**: void

### IndexShardOperationPermits (used by blockOperations)

`IndexShardOperationPermits` manages concurrent access to the shard. It uses a semaphore-like mechanism:
- `acquire()` — called by indexing operations to get a permit
- `blockOperations(timeout, unit, runnable)` — blocks all new permits, waits for existing permits to drain, then runs the runnable. After the runnable completes, permits are unblocked.
- Searches do NOT go through this permit system — they use `acquireSearcher()` which goes directly to the engine's `SearcherManager`.

---

## File 3: StarTreeUpgradeService.java

**Path**: `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/StarTreeUpgradeService.java`

**Status**: NEW FILE (did not exist before our changes)

**Extends**: Nothing. Utility class with static methods and private constructor.

### Methods

#### `upgradeSegments(Directory directory, StarTreeField starTreeField, MapperService mapperService)`
- **Called by**: `IndexShard.upgradeToStarTree()`
- **What it does**:
  1. `SegmentInfos.readLatestCommit(directory)` — reads the current commit point (segments_N file) to get the list of all segments
  2. Iterates each `SegmentCommitInfo`:
     - Checks `commitInfo.info.getCodec().getName()` — skips if already `Composite912Codec`
     - Calls `buildStarTreeData()` for eligible segments
     - Tracks upgraded segment names in a `Set<String>`
     - Catches and logs failures per segment (doesn't abort on single segment failure)
  3. If any segments were upgraded: calls `rewriteSegmentInfos(directory, upgradedSegmentNames)`
- **Returns**: `int` — number of segments upgraded
- **When it returns**: After Phase 1 + Phase 2 complete for all segments

#### `buildStarTreeData(Directory directory, SegmentCommitInfo commitInfo, StarTreeField starTreeField, MapperService mapperService)`
- **Visibility**: package-private (static)
- **Called by**: `upgradeSegments()` for each eligible segment
- **What it does** (Phase 1 for one segment):
  1. `DirectoryReader.open(directory)` — opens a Lucene DirectoryReader on the index directory. This creates a point-in-time snapshot of all segments.
  2. Iterates `directoryReader.leaves()` to find the `SegmentReader` matching `commitInfo.info.name`:
     - `Lucene.segmentReader(leafContext.reader())` — unwraps FilterLeafReader to get the underlying SegmentReader
     - Compares `candidate.getSegmentName()` with target segment name
  3. `segmentReader.getDocValuesReader()` — returns `DocValuesProducer` (Lucene interface). This is the reader that provides access to doc values columns (numeric, sorted, sorted-numeric, etc.)
  4. Builds `Map<String, DocValuesProducer> fieldProducerMap`:
     - Maps each dimension field name → docValuesProducer
     - Maps each metric field name → docValuesProducer
     - Maps `_doc_count` → `EmptyDocValuesProducer` (anonymous subclass that returns `DocValues.emptyNumeric()`)
  5. Creates `SegmentWriteState`:
     - Uses raw directory (not compound) for writing
     - Uses segment's actual `maxDoc`, `FieldInfos`, segment name, segment ID
  6. Opens `IndexOutput` for `.cid` file (star tree data):
     - `directory.createOutput(segmentFileName(segName, "", DATA_EXTENSION), IOContext.DEFAULT)`
     - Writes `CodecUtil.writeIndexHeader()` with codec name, version, segment ID
  7. Opens `IndexOutput` for `.cim` file (star tree metadata):
     - Same pattern as `.cid`
  8. Creates `DocValuesConsumer` for `.cidvd` and `.cidvm` files:
     - `LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(consumerWriteState, ...)` — creates a Lucene90-compatible doc values writer for the aggregated star tree values
     - Uses a special `SegmentInfo` with `maxDoc = DocIdSetIterator.NO_MORE_DOCS` for sparse doc values
  9. Builds star tree:
     - `new StarTreesBuilder(state, mapperService, new AtomicInteger())` — creates the builder
     - `starTreesBuilder.build(metaOut, dataOut, fieldProducerMap, compositeDocValuesConsumer)` — reads doc values, sorts, builds tree, writes output
  10. Writes EOF marker (`metaOut.writeLong(-1)`) and `CodecUtil.writeFooter()` on both outputs
- **Returns**: void
- **When it returns**: After all star tree files are written for this segment
- **Throws**: `IOException` on any I/O error
- **Resource cleanup**: Finally block closes compositeDocValuesConsumer, metaOut, dataOut, directoryReader (in that order)

#### `rewriteSegmentInfos(Directory directory, Set<String> upgradedSegmentNames)`
- **Visibility**: package-private (static)
- **Called by**: `upgradeSegments()` after Phase 1 completes
- **What it does** (Phase 2):
  1. `SegmentInfos.readLatestCommit(directory)` — reads current commit again (same segments as Phase 1 since no IndexWriter is running)
  2. Creates `newSegmentInfos = originalInfos.clone()` then `newSegmentInfos.clear()` — empty clone with same generation
  3. For each `SegmentCommitInfo` in original:
     - If segment name is in `upgradedSegmentNames`:
       a. Creates new `SegmentInfo` with `new Composite912Codec()` — copies all fields (dir, version, minVersion, name, maxDoc, useCompoundFile, hasBlocks, diagnostics, id, attributes, indexSort) but replaces codec
       b. Builds new file set: `new HashSet<>(oldInfo.files())` + adds `.cid`, `.cim`, `.cidvd`, `.cidvm` via `IndexFileNames.segmentFileName(segName, "", extension)`
       c. `newInfo.setFiles(files)` — sets the complete file set
       d. Creates new `SegmentCommitInfo(newInfo, delCount, softDelCount, delGen, fieldInfosGen, docValuesGen, id)` — preserves all commit metadata
       e. Deletes old `.si` file: `directory.deleteFile(IndexFileNames.segmentFileName(segName, "", "si"))`
       f. Writes new `.si` file: `new Composite912Codec().segmentInfoFormat().write(directory, newInfo, IOContext.DEFAULT)` — this writes the segment info with Composite912Codec declared
     - If segment NOT in upgraded set: adds original `SegmentCommitInfo` unchanged
  4. `newSegmentInfos.setUserData(originalInfos.getUserData(), false)` — preserves commit user data (local_checkpoint, max_seq_no, translog_uuid, etc.)
  5. `newSegmentInfos.commit(directory)` — writes `segments_N+1` atomically (Lucene writes to temp file, fsyncs, renames)
  6. `directory.sync(newSegmentInfos.files(true))` — fsyncs all referenced files
  7. `directory.syncMetaData()` — fsyncs directory metadata
- **Returns**: void
- **When it returns**: After segments_N+1 is committed and synced to disk
- **Throws**: `IOException`

### Lucene Classes Used by StarTreeUpgradeService

| Class | What It Does |
|-------|-------------|
| `SegmentInfos` | Represents the commit point (segments_N file). Lists all segments and their metadata. `readLatestCommit()` reads the latest. `commit()` writes a new one. |
| `SegmentCommitInfo` | Wraps `SegmentInfo` + per-commit metadata (delete count, field infos gen, doc values gen). One per segment in `SegmentInfos`. |
| `SegmentInfo` | Core segment metadata: name, maxDoc, codec, files, directory, version, ID, sort order. Stored in `.si` file. |
| `DirectoryReader` | Reads all segments in a directory. `open(directory)` creates a reader from the latest commit. |
| `SegmentReader` | Reads one segment. Provides access to postings, doc values, stored fields, etc. |
| `DocValuesProducer` | Abstract class. Reads doc values columns (numeric, sorted, sorted-numeric, etc.) from a segment. |
| `EmptyDocValuesProducer` | Abstract class with default implementations that throw. We override `getNumeric()` to return empty. |
| `DocValuesConsumer` | Abstract class. Writes doc values columns. Used for `.cidvd`/`.cidvm` files. |
| `SegmentWriteState` | Context for writing: directory, segment info, field infos, IOContext. |
| `IndexOutput` | Lucene's output stream for writing files to a directory. |
| `CodecUtil` | Utility for writing/reading codec headers and footers (magic number, codec name, version, segment ID, CRC). |
| `IndexFileNames` | Utility for generating segment file names: `segmentFileName(segName, suffix, extension)` → e.g., `_0.cid`. |

---

## File 4: Composite912DocValuesFormat.java

**Path**: `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912DocValuesFormat.java`

**Status**: MODIFIED (existing file)

**Extends**: `DocValuesFormat` (Lucene abstract class)

### What DocValuesFormat Does

`DocValuesFormat` is Lucene's SPI (Service Provider Interface) for doc values. Each codec has a `DocValuesFormat` that provides:
- `fieldsConsumer(SegmentWriteState)` → returns `DocValuesConsumer` for writing
- `fieldsProducer(SegmentReadState)` → returns `DocValuesProducer` for reading

### Before Our Changes

`fieldsProducer()` assumed all segments were natively created with Composite912Codec. Doc values files used direct naming (`_0.dvd`, `_0.dvm`). It created a `SegmentReadState` with empty suffix and passed it to the delegate `Lucene90DocValuesFormat`.

### What We Changed

#### `fieldsProducer(SegmentReadState state)` — Modified
- **Called by**: Lucene when opening a segment for reading (via Composite912Codec)
- **What we added**:
  1. Calls `getPerFieldDocValuesSuffix(state.fieldInfos)` to detect PerField attributes
  2. If suffix found (upgraded segment): creates new `SegmentReadState` with the correct suffix (e.g., "Lucene90_0") so the delegate opens `_0_Lucene90_0.dvd` instead of `_0.dvd`
  3. If no suffix (native segment): uses original path with empty suffix
- **Returns**: `Composite912DocValuesReader` (which extends `DocValuesProducer`)
- **When it returns**: During segment open (engine creation, refresh, etc.)
- **Why this is needed**: Upgraded segments were originally written by `Lucene912Codec` which uses `PerFieldDocValuesFormat`. This format adds a suffix to doc values file names (e.g., `_0_Lucene90_0.dvd`). When we switch the codec to Composite912Codec, we need to tell the delegate reader about this suffix so it opens the right files.

#### `getPerFieldDocValuesSuffix(FieldInfos fieldInfos)` — NEW
- **Visibility**: private static
- **Called by**: `fieldsProducer()`
- **What it does**: Iterates `FieldInfo` objects, looks for `PerFieldDocValuesFormat.PER_FIELD_FORMAT_KEY` and `PerFieldDocValuesFormat.PER_FIELD_SUFFIX_KEY` attributes
- **Returns**: `String` — the suffix (e.g., "Lucene90_0") or `null` if not found
- **When it returns**: Immediately

### Constants Defined Here (Used by StarTreeUpgradeService)

| Constant | Value | Used For |
|----------|-------|----------|
| `DATA_EXTENSION` | `"cid"` | Star tree data file |
| `META_EXTENSION` | `"cim"` | Star tree metadata file |
| `DATA_DOC_VALUES_EXTENSION` | `"cidvd"` | Star tree doc values data |
| `META_DOC_VALUES_EXTENSION` | `"cidvm"` | Star tree doc values metadata |
| `DATA_CODEC_NAME` | `"Composite912FormatData"` | CodecUtil header for .cid |
| `META_CODEC_NAME` | `"Composite912FormatMeta"` | CodecUtil header for .cim |
| `DATA_DOC_VALUES_CODEC` | `"Composite912DocValuesData"` | CodecUtil header for .cidvd |
| `META_DOC_VALUES_CODEC` | `"Composite912DocValuesMeta"` | CodecUtil header for .cidvm |

---

## File 5: Composite912DocValuesReader.java

**Path**: `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912DocValuesReader.java`

**Status**: MODIFIED (existing file)

**Extends**: `DocValuesProducer` (Lucene abstract class)

### What DocValuesProducer Does

Abstract class that provides read access to doc values columns for a segment:
- `getNumeric(FieldInfo)` → `NumericDocValues`
- `getSorted(FieldInfo)` → `SortedDocValues`
- `getSortedNumeric(FieldInfo)` → `SortedNumericDocValues`
- `getSortedSet(FieldInfo)` → `SortedSetDocValues`
- `getBinary(FieldInfo)` → `BinaryDocValues`

### Before Our Changes

The reader opened star tree files (`.cid`, `.cim`) from `readState.directory`. For native star tree segments, this works because star tree files are inside the compound file (`.cfs`) and `readState.directory` is the `CompoundDirectory` that sees them.

### What We Changed — `starTreeDir` Fallback

- **In the constructor**:
  1. Tries `readState.directory` first (works for native segments where star tree files are inside `.cfs`)
  2. If `.cim` file is not found in `readState.directory`, falls back to `readState.segmentInfo.dir` (the parent directory)
  3. Sets `starTreeDir` to whichever directory has the `.cim` file
- **Why**: For upgraded segments, star tree files are written outside `.cfs` by our Phase 1. The `CompoundDirectory` only sees files inside `.cfs`. We need to look in the parent directory.
- **Also uses `starTreeDir`** for the `compositeDocValuesProducer` `SegmentReadState` — so the delegate reader also finds its files in the right directory.

---

## File 6: Composite912Codec.java

**Path**: `server/src/main/java/org/opensearch/index/codec/composite/composite912/Composite912Codec.java`

**Status**: NOT MODIFIED (but critical to the flow)

**Extends**: `FilterCodec` extends `Codec` (Lucene abstract class)

### What Codec Does

`Codec` is Lucene's top-level SPI. It bundles all format implementations:
- `postingsFormat()` → how postings (inverted index) are stored
- `docValuesFormat()` → how doc values are stored (this is where star tree lives)
- `storedFieldsFormat()` → how stored fields are stored
- `segmentInfoFormat()` → how `.si` files are read/written
- etc.

When Lucene opens a segment, it reads the codec name from the `.si` file and uses Java's `ServiceLoader` to find the `Codec` implementation.

### How It's Used in the Upgrade

1. `rewriteSegmentInfos()` creates `new Composite912Codec()` and passes it to the new `SegmentInfo`
2. `new Composite912Codec().segmentInfoFormat().write(directory, newInfo, IOContext.DEFAULT)` — writes the `.si` file declaring this codec
3. When the new engine opens, Lucene reads `.si`, finds "Composite912Codec", loads this class
4. `Composite912Codec.docValuesFormat()` returns `Composite912DocValuesFormat`
5. `Composite912DocValuesFormat.fieldsProducer()` creates `Composite912DocValuesReader` which reads star tree data

### Constant

`COMPOSITE_INDEX_CODEC_NAME = "Composite912Codec"` — used by `StarTreeUpgradeService` to check if a segment already uses this codec (skip if so).

---

## File 7: ReadOnlyEngine.java (Used in Option B)

**Path**: `server/src/main/java/org/opensearch/index/engine/ReadOnlyEngine.java`

**Status**: NOT MODIFIED (but will be used by Option B)

**Extends**: `Engine` (OpenSearch abstract class)

### What Engine Does

`Engine` is the abstract base for all engine implementations. It provides:
- `index()`, `delete()` — write operations
- `acquireSearcher()` — get a searcher for reads
- `flush()`, `refresh()`, `forceMerge()` — maintenance operations
- `close()` — shutdown

### What ReadOnlyEngine Does

A read-only engine that serves searches without an `IndexWriter`. Used during:
- Shard relocation (source shard becomes read-only)
- `resetEngineToGlobalCheckpoint()` (temporary read-only during engine reset)
- Closed/remote indices

### Constructor: `ReadOnlyEngine(EngineConfig, SeqNoStats, TranslogStats, boolean obtainLock, Function<DirectoryReader, DirectoryReader> readerWrapperFunction, boolean requireCompleteHistory)`

- `obtainLock` — if true, grabs `IndexWriter.WRITE_LOCK_NAME`. For our use case: `false` (Phase 2 needs to write to directory)
- `requireCompleteHistory` — if true, validates translog completeness. For our use case: `false`
- **What it does**:
  1. `store.incRef()` — increments store reference count
  2. If `obtainLock`: `directory.obtainLock(IndexWriter.WRITE_LOCK_NAME)`
  3. `Lucene.readSegmentInfos(directory)` — reads latest commit
  4. Builds `SeqNoStats` if not provided
  5. `Lucene.getIndexCommit(lastCommittedSegmentInfos, directory)` — gets IndexCommit
  6. `open(indexCommit)` → `DirectoryReader.openIfChanged()` or `DirectoryReader.open(commit)` — opens a reader on the commit
  7. `wrapReader(reader, readerWrapperFunction)` — wraps in `OpenSearchDirectoryReader`
  8. `new OpenSearchReaderManager(reader)` — creates a reader manager for searcher acquisition
  9. Creates `NoOpTranslogManager` — no translog operations in read-only mode

### Key Methods

- `acquireSearcher(String source)` — returns a searcher from the `OpenSearchReaderManager`. This is how reads work during the upgrade.
- `index()`, `delete()` — throw `UnsupportedOperationException`
- `flush()`, `forceMerge()` — no-op
- `close()` — closes reader manager, releases write lock (if held), decrements store ref count

### Why It Works for Option B

- Opens a point-in-time snapshot of the pre-upgrade segments
- Serves reads via `acquireSearcher()` while Phase 1 + Phase 2 modify files on disk
- `obtainLock=false` means Phase 2 can write to the directory without lock conflict
- The reader holds open file descriptors to segment data files — even if Phase 2 deletes/rewrites `.si` files, the reader's open FDs keep the data accessible (POSIX semantics)
- Phase 2 only modifies `.si` files and writes `segments_N+1` — it doesn't touch the actual segment data files (`.dvd`, `.doc`, `.pos`, `.cfs`) that the reader has open

---

## File 8: StarTreesBuilder.java (Existing, Not Modified)

**Path**: `server/src/main/java/org/opensearch/index/compositeindex/datacube/startree/builder/StarTreesBuilder.java`

**Status**: NOT MODIFIED (existing infrastructure we call)

**Implements**: `Closeable`

### What It Does

Orchestrates building star tree data for one or more star tree fields. During native index creation, it's called by `Composite912DocValuesWriter` during flush. During upgrade, we call it from `StarTreeUpgradeService.buildStarTreeData()`.

### Constructor: `StarTreesBuilder(SegmentWriteState state, MapperService mapperService, AtomicInteger fieldNumberAcquirer)`

### Method: `build(IndexOutput metaOut, IndexOutput dataOut, Map<String, DocValuesProducer> fieldProducerMap, DocValuesConsumer compositeDocValuesConsumer)`

- **Called by**: `StarTreeUpgradeService.buildStarTreeData()` (upgrade) or `Composite912DocValuesWriter` (native flush)
- **What it does**:
  1. Gets star tree field configs from `mapperService.getCompositeFieldTypes()`
  2. For each star tree field: creates a `BaseStarTreeBuilder` (OnHeap or OffHeap based on config)
  3. `builder.build()` — the actual tree construction:
     a. Reads all doc values for dimensions + metrics via `fieldProducerMap`
     b. Sorts documents by dimension values
     c. Builds the star tree node structure (recursive aggregation at each level)
     d. Writes tree structure to `metaOut`/`dataOut` (`.cim`/`.cid`)
     e. Writes aggregated doc values to `compositeDocValuesConsumer` (`.cidvd`/`.cidvm`)
  4. Uses `mapperService` → `generateMetricAggregatorInfos()` to resolve `FieldValueConverter` for each metric field (needed to know how to aggregate: sum, min, max, count, etc.)
- **Returns**: void
- **When it returns**: After all star tree fields are built for the segment

---

## Complete Execution Flow (Current Implementation)

```
Client: POST /_star_tree_upgrade { "index": "my_index", "star_tree_field": {...} }
  │
  ▼
TransportStarTreeUpgradeAction.doExecute()
  │
  ├─ indexNameExpressionResolver.concreteIndexNames() → ["my_index"]
  ├─ hasExistingStarTreeConfig() → false (no star tree yet)
  ├─ submitMappingUpdate()
  │    │
  │    ▼
  │  StarTreeUpgradeMappingExecutor.execute() [on master node]
  │    │
  │    ├─ indicesService.createIndexMapperService(indexMetadata) → MapperService
  │    ├─ mapperService.documentMapperParser().setAllowCompositeFieldWithoutSettings(true)
  │    ├─ mapperService.merge(indexMetadata, MergeReason.MAPPING_RECOVERY) → loads existing fields
  │    ├─ buildCompleteMappingSource(request, indexMetadata) → CompressedXContent
  │    ├─ mapperService.merge("_doc", mappingSource, MergeReason.STAR_TREE_UPGRADE) → DocumentMapper
  │    ├─ CompositeIndexValidator.validate(..., MergeReason.STAR_TREE_UPGRADE)
  │    ├─ Settings.builder().put("index.append_only.enabled", true) → force-set
  │    └─ Returns updated ClusterState with new mapping + settings
  │
  ├─ [Cluster state propagates to all nodes]
  │
  ├─ super.doExecute() → triggers broadcast to all shards
  │    │
  │    ▼
  │  [For each shard on each node:]
  │  shardOperation(request, shardRouting)
  │    │
  │    ├─ indicesService.indexServiceSafe().getShard() → IndexShard
  │    ├─ indexShard.mapperService().getCompositeFieldTypes().isEmpty() → false (mapping propagated)
  │    ├─ indexShard.upgradeToStarTree(request.getStarTreeField())
  │    │    │
  │    │    ▼
  │    │  IndexShard.upgradeToStarTree(starTreeField)
  │    │    │
  │    │    ├─ verifyActive() → OK
  │    │    ├─ starTreeUpgradeInProgress.compareAndSet(false, true) → OK
  │    │    ├─ flush(force=true) → commits in-memory data
  │    │    ├─ blockOperations(30 min, lambda)
  │    │    │    │
  │    │    │    ├─ flush(waitIfOngoing=true) → catches stragglers
  │    │    │    ├─ IOUtils.close(currentEngineReference.getAndSet(null)) → engine closed
  │    │    │    │
  │    │    │    ├─ StarTreeUpgradeService.upgradeSegments(directory, starTreeField, mapperService)
  │    │    │    │    │
  │    │    │    │    ├─ SegmentInfos.readLatestCommit(directory) → [seg0, seg1, seg2]
  │    │    │    │    │
  │    │    │    │    ├─ [Phase 1: For each segment]
  │    │    │    │    │    ├─ buildStarTreeData(directory, seg0, starTreeField, mapperService)
  │    │    │    │    │    │    ├─ DirectoryReader.open(directory) → reader
  │    │    │    │    │    │    ├─ Find SegmentReader for seg0
  │    │    │    │    │    │    ├─ segmentReader.getDocValuesReader() → DocValuesProducer
  │    │    │    │    │    │    ├─ Build fieldProducerMap (dims + metrics + _doc_count)
  │    │    │    │    │    │    ├─ Create SegmentWriteState
  │    │    │    │    │    │    ├─ Open IndexOutput for _0.cid, _0.cim
  │    │    │    │    │    │    ├─ Create DocValuesConsumer for _0.cidvd, _0.cidvm
  │    │    │    │    │    │    ├─ StarTreesBuilder.build(metaOut, dataOut, fieldProducerMap, consumer)
  │    │    │    │    │    │    │    ├─ Read doc values for all dims/metrics
  │    │    │    │    │    │    │    ├─ Sort by dimension values
  │    │    │    │    │    │    │    ├─ Build star tree nodes (recursive aggregation)
  │    │    │    │    │    │    │    ├─ Write tree to .cid/.cim
  │    │    │    │    │    │    │    └─ Write aggregated values to .cidvd/.cidvm
  │    │    │    │    │    │    ├─ Write EOF + CodecUtil footer
  │    │    │    │    │    │    └─ Close reader, outputs, consumer
  │    │    │    │    │    ├─ buildStarTreeData(directory, seg1, ...) → same flow
  │    │    │    │    │    └─ buildStarTreeData(directory, seg2, ...) → same flow
  │    │    │    │    │
  │    │    │    │    ├─ [Phase 2: rewriteSegmentInfos]
  │    │    │    │    │    ├─ SegmentInfos.readLatestCommit(directory) → same segments
  │    │    │    │    │    ├─ For each upgraded segment:
  │    │    │    │    │    │    ├─ New SegmentInfo with Composite912Codec
  │    │    │    │    │    │    ├─ Add .cid/.cim/.cidvd/.cidvm to file set
  │    │    │    │    │    │    ├─ New SegmentCommitInfo (preserve metadata)
  │    │    │    │    │    │    ├─ Delete old .si file
  │    │    │    │    │    │    └─ Write new .si with Composite912Codec
  │    │    │    │    │    ├─ setUserData(originalInfos.getUserData())
  │    │    │    │    │    ├─ newSegmentInfos.commit(directory) → segments_N+1
  │    │    │    │    │    └─ directory.sync() + syncMetaData()
  │    │    │    │    │
  │    │    │    │    └─ Returns: 3 (segments upgraded)
  │    │    │    │
  │    │    │    ├─ codecServiceOverride = new CodecService (with Composite912Codec)
  │    │    │    ├─ newReadWriteEngine(newEngineConfig()) → InternalEngine from segments_N+1
  │    │    │    ├─ onNewEngine(newEngine)
  │    │    │    ├─ newEngine.refresh("star-tree-upgrade")
  │    │    │    ├─ active.set(true)
  │    │    │    └─ codecServiceOverride = null
  │    │    │
  │    │    ├─ starTreeUpgradeInProgress.set(false)
  │    │    └─ Returns: 3
  │    │
  │    └─ Returns: ShardStarTreeUpgradeResult(shardId, isPrimary)
  │
  └─ newResponse() → StarTreeUpgradeResponse(totalShards, successful, failed, failures)
       │
       ▼
Client: { "_shards": { "total": 6, "successful": 6, "failed": 0 } }
```

---

## Post-Upgrade Read Flow (How Star Tree Data Is Read)

```
Search query arrives → IndexShard.acquireSearcher()
  │
  ▼
Engine.acquireSearcher() → OpenSearchDirectoryReader
  │
  ▼
For each segment in the reader:
  │
  ├─ Lucene reads .si file → codec = "Composite912Codec"
  ├─ ServiceLoader finds Composite912Codec class
  ├─ Composite912Codec.docValuesFormat() → Composite912DocValuesFormat
  ├─ Composite912DocValuesFormat.fieldsProducer(readState)
  │    │
  │    ├─ getPerFieldDocValuesSuffix(fieldInfos)
  │    │    ├─ Upgraded segment: returns "Lucene90_0" (PerField attributes found)
  │    │    └─ Native segment: returns null (no PerField attributes)
  │    │
  │    ├─ If suffix found: create SegmentReadState with suffix "Lucene90_0"
  │    │    → delegate opens _0_Lucene90_0.dvd (correct file for upgraded segment)
  │    ├─ If no suffix: create SegmentReadState with empty suffix
  │    │    → delegate opens _0.dvd (correct file for native segment)
  │    │
  │    └─ Returns: Composite912DocValuesReader
  │         │
  │         ├─ Constructor:
  │         │    ├─ Try readState.directory for .cim file
  │         │    ├─ If not found: fallback to readState.segmentInfo.dir
  │         │    │    (star tree files outside .cfs for upgraded segments)
  │         │    ├─ Open .cid + .cim from starTreeDir
  │         │    └─ Open .cidvd + .cidvm via compositeDocValuesProducer
  │         │
  │         └─ Provides star tree data to query engine
  │              → Star tree acceleration (terminated_early=true)
```

---

## ReadOnlyEngine Bridge for Read Availability (New)

### Upgrade Flow (Updated)

The upgrade now uses a ReadOnlyEngine bridge to keep reads available:

```
1. Flush + block writes
2. Swap 1 (atomic, inside engineMutex):
   a. Create ReadOnlyEngine(obtainLock=false)
   b. Set currentEngineReference to ROE
   c. Close old InternalEngine
   d. Set codecServiceOverride
3. Phase 1: buildStarTreeDataForSegments() — ROE serves reads
4. Phase 2: rewriteSegmentInfos() (with write lock) — ROE serves reads
5. Swap 2 (atomic, inside engineMutex):
   a. Create new InternalEngine(newEngineConfig with codecServiceOverride)
   b. onNewEngine()
   c. Set currentEngineReference to new IE
   d. Close ROE
6. Refresh (outside mutex, non-fatal if fails)
7. Unblock writes
```

### New Methods in StarTreeUpgradeService

| Method | Returns | Purpose |
|--------|---------|---------|
| `getCandidateSegmentNames(Directory)` | `Set<String>` | Segment names not using Composite912Codec |
| `buildStarTreeDataForSegments(Directory, StarTreeField, MapperService)` | `Set<String>` | Phase 1: build star tree files, return upgraded segment names |
| `rewriteSegmentInfos(Directory, Set<String>)` | void | Phase 2: rewrite .si + commit (now public, with write lock) |
| `cleanupStarTreeFiles(Directory, Set<String>)` | void | Delete orphaned .cid/.cim/.cidvd/.cidvm files |

### codecServiceOverride Lifecycle

- Set inside Swap 1 mutex, after old engine close
- NOT cleared after upgrade (needed for resetEngineToGlobalCheckpoint)
- Nulled in resetEngineToGlobalCheckpoint when mapperService.isCompositeIndexPresent() is true
- On node restart: new IndexShard constructor creates fresh codecService with Composite912Codec (because index.composite_index=true in cluster state)

### Star Tree Query Null Safety

During the ReadOnlyEngine phase, MapperService has composite field types (mapping updated) but segments don't have star tree data (old codec). The query optimizer tries star tree → getStarTreeValues() returns null → NPE.

Fix: `precomputeLeafUsingStarTree()` changed from void to boolean. Returns false when null. Callers check return value — if false, `tryPrecomputeAggregationForLeaf` returns false → normal doc values collection runs → correct results.

Bucket aggregators: `getStarTreeBucketCollector()` returns null → callers check null → return false from tryPrecomputeAggregationForLeaf.
