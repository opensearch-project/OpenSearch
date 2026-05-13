# Star Tree Per-Segment Upgrade — Deep Dive

## Starting Point: The Client Request

A user sends: `POST /my_index/_star_tree_upgrade { ... }`
This hits the REST layer, which routes it to `TransportStarTreeUpgradeAction`.

## What is a Transport Action?

Every API endpoint maps to a Transport Action — the internal RPC mechanism.
REST request → transport request → routed to handler → executed → response.

---

## TransportStarTreeUpgradeAction.java (NEW FILE)

```
class TransportStarTreeUpgradeAction
    extends TransportBroadcastByNodeAction<
        StarTreeUpgradeRequest,
        StarTreeUpgradeResponse,
        ShardStarTreeUpgradeResult>
```

### What is TransportBroadcastByNodeAction?

Abstract base for actions that run on every shard of an index.
Examples: force merge, flush, refresh.

How it works:
1. Calls `shards()` to determine target shards
2. Groups shards by node
3. Sends ONE transport request per node
4. Each node calls `shardOperation()` on its local shards
5. Collects `ShardResult` objects from all nodes
6. Calls `newResponse()` to build the final response

We must implement: `shardOperation()`, `shards()`, `newResponse()`,
`readShardResult()`, `readRequestFrom()`, `checkGlobalBlock()`, `checkRequestBlock()`.

### Constructor

```java
@Inject  // Guice dependency injection — OpenSearch wires this up at node startup
public TransportStarTreeUpgradeAction(
    ClusterService clusterService,       // access to cluster state + submit updates
    TransportService transportService,   // send/receive transport messages
    IndicesService indicesService,        // access to local shards + create MapperService
    ActionFilters actionFilters,          // pre/post action filters
    IndexNameExpressionResolver resolver  // resolve wildcards/aliases to concrete indices
)
```

Super constructor uses `ThreadPool.Names.FORCE_MERGE` — shard operations run on a
single-threaded pool, so only one upgrade runs at a time per node.

---

### doExecute() — THE ENTRY POINT

```java
void doExecute(Task task, StarTreeUpgradeRequest request,
               ActionListener<StarTreeUpgradeResponse> listener)
```

#### What is ActionListener?

Callback interface for async operations:
- `onResponse(T response)` — called on success
- `onFailure(Exception e)` — called on failure

Almost everything in OpenSearch is async. Methods take a listener instead of returning.

#### What doExecute does:

**Step 1**: Resolve concrete indices
```java
ClusterState clusterState = clusterService.state();  // immutable snapshot
String[] concreteIndices = resolver.concreteIndexNames(clusterState, request);
// "my_index*" → ["my_index"]
```

**Step 2**: Check idempotency — if ALL indices already have star tree, skip mapping
update and go straight to `super.doExecute()` (the broadcast). Handles retry case.

**Step 3**: Validate no conflicting config — if SOME indices have star tree, error.

**Step 4**: Submit mapping update, THEN broadcast
```java
submitMappingUpdate(request, concreteIndices, ActionListener.wrap(
    ack -> {
        if (ack) super.doExecute(task, request, listener);  // broadcast
        else listener.onFailure(new IOException("not acknowledged"));
    },
    listener::onFailure
));
```

**Why mapping first?** `shardOperation()` → `upgradeToStarTree()` → `StarTreesBuilder`
reads star tree config from `MapperService.getCompositeFieldTypes()`. Without the
mapping update, that returns empty.

**What is super.doExecute()?** The parent class `TransportBroadcastByNodeAction.doExecute()`.
It does the actual broadcast: calls `shards()`, groups by node, sends transport requests,
each node calls `shardOperation()`, collects results, calls `newResponse()`.

---

### submitMappingUpdate() — CLUSTER STATE UPDATE

```java
private void submitMappingUpdate(StarTreeUpgradeRequest request,
    String[] concreteIndices, ActionListener<Boolean> listener)
```

#### What is a Cluster State Update?

Cluster state = single source of truth for the cluster. Contains:
- Metadata for every index (mappings, settings, aliases)
- Routing table (which shard on which node)
- Node info, cluster settings

Only the MASTER node can modify it. Process:
1. Any node submits a task
2. Task forwarded to master if needed
3. Master has single-threaded task queue
4. Master calls `executor.execute(currentState, [tasks])`
5. Executor returns new ClusterState
6. Master publishes new state to all nodes
7. Each node applies it locally (updates MapperService, settings, etc.)
8. Each node sends ack to master
9. When all ack, listener notified

#### What we submit:

```java
clusterService.submitStateUpdateTask(
    "star-tree-upgrade-mapping",                         // name (logging)
    new StarTreeUpgradeMappingTask(request, indices),    // task data
    ClusterStateTaskConfig.build(Priority.HIGH),         // high priority
    mappingExecutor,                                     // StarTreeUpgradeMappingExecutor
    new AckedClusterStateTaskListener() {                // wait for ALL nodes to ack
        mustAck(node) → true                             // every node must ack
        onAllNodesAcked(e) → listener.onResponse(e==null)
        onAckTimeout() → listener.onResponse(false)      // 1 min timeout
        onFailure(src, e) → listener.onFailure(e)
        ackTimeout() → 1 minute
    }
);
```

`StarTreeUpgradeMappingTask` — just a data holder: request + concreteIndices.

---

### StarTreeUpgradeMappingExecutor — THE MAPPING LOGIC

```java
class StarTreeUpgradeMappingExecutor
    implements ClusterStateTaskExecutor<StarTreeUpgradeMappingTask>
```

#### What is ClusterStateTaskExecutor?

Interface for modifying cluster state. One method:
```java
ClusterTasksResult<T> execute(ClusterState currentState, List<T> tasks)
```
Master calls this with current state + batch of tasks. Returns new state.

#### execute() method:

Iterates tasks, calls `applyStarTreeMapping()` for each. Marks each as
success/failure. Returns accumulated state changes.

#### applyStarTreeMapping() — THE CORE LOGIC (runs on master node)

For each index:

**Step 1: Create temporary MapperService**
```java
MapperService mapperService = indicesService.createIndexMapperService(indexMetadata);
```

##### What is MapperService?
Central class for managing an index's mapping. It:
- Parses mapping JSON into DocumentMapper objects
- Holds all field type info (which fields exist, what types)
- Validates mapping changes
- Provides `getCompositeFieldTypes()` for star tree configs

This creates a TEMPORARY MapperService (not the live one on shards).
Used for parsing/validation only, then discarded.

**Step 2: Set bypass flag**
```java
mapperService.documentMapperParser().setAllowCompositeFieldWithoutSettings(true);
```

##### What is DocumentMapperParser?
Parses mapping JSON into DocumentMapper. When it encounters "composite",
calls `ObjectMapper.parseCompositeField()`.

##### What does the bypass flag do?
`ObjectMapper.parseCompositeField()` normally checks `index.composite_index`
setting (a Final setting — can only be set at index creation). Our index
wasn't created with it, so the check would fail. The flag skips it.

**Step 3: Load existing mappings**
```java
mapperService.merge(indexMetadata, MergeReason.MAPPING_RECOVERY);
```

##### What is merge()?
How mappings are applied to MapperService. Parses JSON, registers field types.
Overloads:
- `merge(IndexMetadata, MergeReason)` — load all mappings from metadata
- `merge(String type, CompressedXContent, MergeReason)` — merge one source

##### What is MergeReason?
Enum controlling validation:
- `MAPPING_RECOVERY` — loading existing, lenient
- `MAPPING_UPDATE` — normal update, strict
- `STAR_TREE_UPGRADE` — our custom, allows adding composite to existing index

After this, MapperService knows all existing fields (status=keyword, price=long).

**Step 4: Build complete mapping source**
```java
CompressedXContent source = buildCompleteMappingSource(request, indexMetadata);
```

Creates JSON with existing properties + new composite section.
Why include properties? `StarTreeMapper.Builder` validates dimensions by
looking them up in the mapper builders from the CURRENT source being parsed.

**Step 5: Merge star tree mapping**
```java
DocumentMapper merged = mapperService.merge("_doc", source,
    MergeReason.STAR_TREE_UPGRADE);
```

Parse chain:
1. `DocumentMapperParser.parse()` parses JSON
2. Finds "composite" section
3. `ObjectMapper.parseCompositeField()` → `StarTreeMapper.Builder`
4. Builder validates dims exist, have compatible types, metrics are numeric
5. `STAR_TREE_UPGRADE` allows adding composite (normally blocked)
6. Returns DocumentMapper with star tree field

##### What is DocumentMapper?
Compiled representation of an index's mapping. Holds root object mapper,
all field mappers, composite field types, source config, etc.

**Step 6: Validate constraints**
```java
CompositeIndexValidator.validate(mapperService, settings, indexSettings,
    isCompositePresent, MergeReason.STAR_TREE_UPGRADE);
```
Checks max dims, max metrics, supported types. STAR_TREE_UPGRADE bypasses
"can't add composite to existing index" but enforces structural constraints.

**Step 7: Force-set append_only**
```java
Settings updated = Settings.builder()
    .put(indexMetadata.getSettings())
    .put("index.append_only.enabled", true)
    .build();
```
Final setting — can't change via API. We bypass by directly building state.
After this, index rejects updates/deletes. Protects star tree integrity.

**Step 8: Build updated IndexMetadata**
New IndexMetadata with updated mapping + settings + incremented versions.
Goes into Metadata → ClusterState → published to all nodes.

---

### shardOperation() — PER-SHARD EXECUTION

```java
ShardStarTreeUpgradeResult shardOperation(
    StarTreeUpgradeRequest request, ShardRouting shardRouting)
```

#### What is ShardRouting?
Routing info for one shard copy: shardId, primary/replica, nodeId, state.

**Step 1**: Get IndexShard
```java
IndexShard indexShard = indicesService
    .indexServiceSafe(shardRouting.shardId().getIndex())
    .getShard(shardRouting.shardId().id());
```

**Step 2**: Check mapping propagated
```java
if (indexShard.mapperService().getCompositeFieldTypes().isEmpty())
    throw new IOException("Star tree field not yet available...");
```

**Step 3**: Call upgrade
```java
indexShard.upgradeToStarTree(request.getStarTreeField());
```
→ This enters IndexShard (next section)

**Step 4**: Return result
```java
return new ShardStarTreeUpgradeResult(shardRouting.shardId(), shardRouting.primary());
```

### shards() — WHICH SHARDS TO TARGET

```java
ShardsIterator shards(ClusterState state, request, String[] indices)
```

Returns `state.routingTable().allShards(indices)` — ALL shards (primary + replica).
Validates all primaries active, throws `PrimaryMissingActionException` if not.

NOTE: Should be filtered to primary only (known issue).

### newResponse() — BUILD FINAL RESPONSE

Constructs `StarTreeUpgradeResponse(total, successful, failed, failures)`.
Client sees: `{ "_shards": { "total": 6, "successful": 6, "failed": 0 } }`

---

## IndexShard.java (MODIFIED)

```
class IndexShard extends AbstractIndexShardComponent
    implements IndicesClusterStateService.Shard
```

### What is IndexShard?

Core shard abstraction. Each shard has one IndexShard on its host node. Manages:
- Engine lifecycle (create, close, swap)
- Indexing (index, delete, bulk)
- Search (acquireSearcher)
- Translog, flush, refresh, force merge
- Recovery, replication

### Fields We Added

| Field | Type | Purpose |
|-------|------|---------|
| `codecServiceOverride` | `volatile CodecService` | Injected during upgrade so new engine uses Composite912Codec |
| `starTreeUpgradeInProgress` | `AtomicBoolean` | Prevents concurrent upgrades on same shard |

### Key Existing Fields

| Field | Type | What It Does |
|-------|------|-------------|
| `currentEngineReference` | `AtomicReference<Engine>` | The live engine serving reads/writes |
| `engineMutex` | `Object` | Lock for engine swaps |
| `indexShardOperationPermits` | `IndexShardOperationPermits` | Controls concurrent write access |
| `engineFactory` | `EngineFactory` | Creates engines (`newReadWriteEngine()` → InternalEngine) |
| `engineConfigFactory` | `EngineConfigFactory` | Creates EngineConfig (`newDefaultCodecService()` → codec) |
| `mapperService` | `MapperService` | Field type info for the index |
| `active` | `AtomicBoolean` | Whether shard has recent activity |

### upgradeToStarTree(StarTreeField) — THE SHARD-LEVEL UPGRADE

**Step 1**: Guards
```java
verifyActive();  // checks state == STARTED
starTreeUpgradeInProgress.compareAndSet(false, true);  // atomic guard
```

**Step 2**: First flush
```java
flush(new FlushRequest().force(true));
```

#### What is flush()?
Commits the in-memory indexing buffer (Lucene IndexWriter buffer) to disk
as segments. Also commits the translog. After flush, all indexed data is
on disk in segment files.

**Step 3**: Block writes + second flush
```java
indexShardOperationPermits.blockOperations(30, TimeUnit.MINUTES, () -> {
    flush(new FlushRequest().waitIfOngoing(true));
```

#### What is IndexShardOperationPermits?
Manages concurrent access to the shard via a semaphore-like mechanism:
- `acquire()` — indexing operations get a permit before writing
- `blockOperations(timeout, unit, runnable)` — blocks new permits, waits
  for existing to drain, runs the runnable, then unblocks

IMPORTANT: Searches do NOT use this permit system. They go through
`acquireSearcher()` → engine's SearcherManager. So reads continue
even when operations are blocked.

**Step 4**: Close engine
```java
synchronized (engineMutex) {
    IOUtils.close(currentEngineReference.getAndSet(null));
}
```

#### What is Engine?
Abstract base for all engine implementations:
- `InternalEngine` — normal read-write engine with IndexWriter
- `ReadOnlyEngine` — read-only, no IndexWriter
- `NRTReplicationEngine` — for segment replication

`currentEngineReference` holds the live engine. Setting it to null means
no engine — searches will fail until a new engine is set.

`engineMutex` — all engine swaps happen under this lock. Prevents races
between engine close and acquireSearcher.

**Step 5**: Run the upgrade
```java
int count = StarTreeUpgradeService.upgradeSegments(
    store().directory(), starTreeField, mapperService);
```

#### What is store().directory()?
`Store` wraps Lucene's `Directory`. `Directory` is Lucene's abstraction
for file I/O — it provides `createOutput()`, `openInput()`, `deleteFile()`,
`listAll()`, etc. On disk, this is typically an `MMapDirectory` or
`NIOFSDirectory` pointing to the shard's index directory.

→ This enters StarTreeUpgradeService (next section)

**Step 6**: Set codec override
```java
codecServiceOverride = engineConfigFactory.newDefaultCodecService(
    indexSettings, mapperService, logger);
```

#### What is CodecService?
Provides the Lucene `Codec` for the engine. The codec determines how
segments are written (postings format, doc values format, etc.).

`newDefaultCodecService()` creates a CodecService that includes
Composite912Codec — because `mapperService` now has composite field types
(after the mapping update). The original `codecService` was created before
the mapping update and doesn't include the composite codec.

#### Why codecServiceOverride?
`codecService` is final — can't be changed. `newEngineConfig()` checks:
if `codecServiceOverride != null`, use that; else use `codecService`.
This is how we inject Composite912Codec into the new engine.

**Step 7**: Create new engine
```java
synchronized (engineMutex) {
    Engine newEngine = engineFactory.newReadWriteEngine(
        newEngineConfig(replicationTracker));
    onNewEngine(newEngine);
    currentEngineReference.set(newEngine);
    newEngine.refresh("star-tree-upgrade");
    active.set(true);
}
```

`newReadWriteEngine()` creates an `InternalEngine` from the upgraded
`segments_N+1` commit. The engine opens the directory, reads the commit,
creates an IndexWriter, and builds a searcher.

`onNewEngine()` registers refresh listeners, translog sync, etc.

`refresh()` warms the searcher so scheduled refreshes don't hit assertions.

**Step 8**: Clear override
```java
codecServiceOverride = null;
```
Future engine configs will be created fresh by the engine config factory,
which will see the updated mapping with star tree fields.

---

## StarTreeUpgradeService.java (NEW FILE)

Utility class with static methods. No inheritance.

### upgradeSegments(Directory, StarTreeField, MapperService) → int

**Step 1**: Read current commit
```java
SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
```

#### What is SegmentInfos?
Represents the commit point — the `segments_N` file on disk. Lists all
segments and their metadata. Each entry is a `SegmentCommitInfo`.

`readLatestCommit()` finds the highest-generation `segments_N` file in
the directory and reads it.

**Step 2**: Phase 1 — for each segment
```java
for (SegmentCommitInfo commitInfo : segmentInfos) {
    String codecName = commitInfo.info.getCodec().getName();
    if ("Composite912Codec".equals(codecName)) { skip; continue; }
    buildStarTreeData(directory, commitInfo, starTreeField, mapperService);
    upgradedSegmentNames.add(commitInfo.info.name);
}
```

#### What is SegmentCommitInfo?
Wraps `SegmentInfo` + per-commit metadata:
- `getDelCount()` — number of deleted docs
- `getSoftDelCount()` — soft deletes
- `getDelGen()` — delete generation
- `getFieldInfosGen()` — field infos generation
- `getDocValuesGen()` — doc values generation
- `getId()` — unique segment commit ID

#### What is SegmentInfo?
Core segment metadata stored in `.si` file:
- `name` — segment name (e.g., "_0", "_1")
- `maxDoc()` — number of documents
- `getCodec()` — which Codec wrote this segment
- `files()` — set of files belonging to this segment
- `dir` — the Directory containing the segment
- `getVersion()` — Lucene version
- `getId()` — unique segment ID (byte array)
- `getUseCompoundFile()` — whether segment uses .cfs
- `getIndexSort()` — sort order (if any)
- `getDiagnostics()` — diagnostic info (OS, Java version, etc.)
- `getAttributes()` — custom key-value attributes

**Step 3**: Phase 2 — rewrite SegmentInfos
```java
if (upgradedSegmentNames.isEmpty() == false) {
    rewriteSegmentInfos(directory, upgradedSegmentNames);
}
```

**Returns**: `int` — number of segments upgraded

---

### buildStarTreeData() — PHASE 1 (per segment)

```java
static void buildStarTreeData(Directory directory,
    SegmentCommitInfo commitInfo, StarTreeField starTreeField,
    MapperService mapperService)
```

**Step 1**: Open DirectoryReader, find SegmentReader
```java
DirectoryReader directoryReader = DirectoryReader.open(directory);
```

#### What is DirectoryReader?
Lucene's top-level reader. Opens all segments in a directory from the
latest commit. Provides access to each segment via `leaves()`.

```java
for (LeafReaderContext leafContext : directoryReader.leaves()) {
    SegmentReader candidate = Lucene.segmentReader(leafContext.reader());
    if (candidate.getSegmentName().equals(segmentName)) {
        segmentReader = candidate;
        break;
    }
}
```

#### What is LeafReaderContext?
Wrapper around a LeafReader (one segment). `reader()` returns the LeafReader.

#### What is SegmentReader?
Reads one segment. Provides access to:
- `getDocValuesReader()` → DocValuesProducer
- `getFieldInfos()` → FieldInfos
- `getSegmentName()` → segment name
- `getSegmentInfo()` → SegmentInfo

`Lucene.segmentReader()` unwraps any FilterLeafReader wrappers to get
the underlying SegmentReader.

**Step 2**: Get DocValuesProducer
```java
DocValuesProducer docValuesProducer = segmentReader.getDocValuesReader();
```

#### What is DocValuesProducer?
Lucene abstract class for reading doc values columns:
- `getNumeric(FieldInfo)` → NumericDocValues (long per doc)
- `getSorted(FieldInfo)` → SortedDocValues (ordinal per doc)
- `getSortedNumeric(FieldInfo)` → SortedNumericDocValues (multi-valued)
- `getSortedSet(FieldInfo)` → SortedSetDocValues (multi-valued ordinals)
- `getBinary(FieldInfo)` → BinaryDocValues (byte array per doc)

Doc values are columnar storage — one column per field, one value per doc.
This is what star tree reads to get dimension/metric values.

**Step 3**: Build fieldProducerMap
```java
Map<String, DocValuesProducer> fieldProducerMap = new HashMap<>();
for (Dimension dim : starTreeField.getDimensionsOrder())
    fieldProducerMap.put(dim.getField(), docValuesProducer);
for (Metric metric : starTreeField.getMetrics())
    fieldProducerMap.put(metric.getField(), docValuesProducer);
fieldProducerMap.put("_doc_count", new EmptyDocValuesProducer() {
    public NumericDocValues getNumeric(FieldInfo f) {
        return DocValues.emptyNumeric();
    }
});
```

Maps each field name to its DocValuesProducer. All fields use the same
producer (from the same segment). `_doc_count` uses an empty producer
because it's an implicit metric that StarTreesBuilder always includes.

**Step 4**: Create SegmentWriteState
```java
SegmentWriteState state = new SegmentWriteState(
    null, directory, writeSegInfo, fieldInfos, null, IOContext.DEFAULT, "");
```

#### What is SegmentWriteState?
Lucene's context for writing segment data:
- `directory` — where to write files
- `segmentInfo` — segment metadata (name, ID, etc.)
- `fieldInfos` — field definitions
- `segmentSuffix` — suffix for file names (empty for us)

**Step 5**: Open outputs for .cid and .cim
```java
dataOut = directory.createOutput("_0.cid", IOContext.DEFAULT);
CodecUtil.writeIndexHeader(dataOut, "Composite912FormatData", VERSION, segId, "");

metaOut = directory.createOutput("_0.cim", IOContext.DEFAULT);
CodecUtil.writeIndexHeader(metaOut, "Composite912FormatMeta", VERSION, segId, "");
```

#### What is IndexOutput?
Lucene's output stream for writing to a Directory. Like OutputStream but
with Lucene-specific methods (writeLong, writeInt, etc.).

#### What is CodecUtil.writeIndexHeader()?
Writes a standard header: magic number + codec name + version + segment ID
+ suffix. Used for file integrity verification on read.

**Step 6**: Create DocValuesConsumer for .cidvd and .cidvm
```java
compositeDocValuesConsumer = LuceneDocValuesConsumerFactory
    .getDocValuesConsumerForCompositeCodec(consumerWriteState, ...);
```

#### What is DocValuesConsumer?
Lucene abstract class for writing doc values columns. The inverse of
DocValuesProducer. Writes numeric, sorted, sorted-numeric, etc.

**Step 7**: Build star tree
```java
try (StarTreesBuilder builder = new StarTreesBuilder(state, mapperService, counter)) {
    builder.build(metaOut, dataOut, fieldProducerMap, compositeDocValuesConsumer);
}
```

#### What does StarTreesBuilder.build() do?
1. Gets star tree field configs from `mapperService.getCompositeFieldTypes()`
2. For each field: creates BaseStarTreeBuilder (OnHeap or OffHeap)
3. `builder.build()`:
   a. Reads all doc values for dims + metrics via fieldProducerMap
   b. Sorts documents by dimension values
   c. Builds star tree nodes (recursive aggregation at each level)
   d. Writes tree structure to .cid/.cim
   e. Writes aggregated values to .cidvd/.cidvm via DocValuesConsumer
4. Uses `mapperService` → `generateMetricAggregatorInfos()` to resolve
   FieldValueConverter for each metric (sum, min, max, count, etc.)

**Step 8**: Write footer
```java
metaOut.writeLong(-1);  // EOF marker
CodecUtil.writeFooter(metaOut);  // CRC checksum
CodecUtil.writeFooter(dataOut);
```

**Returns**: void. Files are written to disk.

---

### rewriteSegmentInfos() — PHASE 2

```java
static void rewriteSegmentInfos(Directory directory,
    Set<String> upgradedSegmentNames)
```

**Step 1**: Read current commit again
```java
SegmentInfos originalInfos = SegmentInfos.readLatestCommit(directory);
SegmentInfos newSegmentInfos = originalInfos.clone();
newSegmentInfos.clear();
```

**Step 2**: For each segment in original commit:

If segment was upgraded:

a. Create new SegmentInfo with Composite912Codec
```java
SegmentInfo newInfo = new SegmentInfo(
    oldInfo.dir, oldInfo.getVersion(), oldInfo.getMinVersion(),
    oldInfo.name, oldInfo.maxDoc(), oldInfo.getUseCompoundFile(),
    oldInfo.getHasBlocks(), new Composite912Codec(),  // ← NEW CODEC
    oldInfo.getDiagnostics(), oldInfo.getId(),
    oldInfo.getAttributes(), oldInfo.getIndexSort());
```

b. Add star tree files to file set
```java
Set<String> files = new HashSet<>(oldInfo.files());
files.add("_0.cid");   // star tree data
files.add("_0.cim");   // star tree metadata
files.add("_0.cidvd"); // star tree doc values data
files.add("_0.cidvm"); // star tree doc values metadata
newInfo.setFiles(files);
```

c. Create new SegmentCommitInfo preserving all metadata
```java
SegmentCommitInfo newCommitInfo = new SegmentCommitInfo(
    newInfo, commitInfo.getDelCount(), commitInfo.getSoftDelCount(),
    commitInfo.getDelGen(), commitInfo.getFieldInfosGen(),
    commitInfo.getDocValuesGen(), commitInfo.getId());
```

d. Delete old .si file, write new one with Composite912Codec
```java
directory.deleteFile("_0.si");
new Composite912Codec().segmentInfoFormat().write(directory, newInfo, IOContext.DEFAULT);
```

#### Why rewrite .si?
`SegmentInfos.commit()` writes `segments_N+1` with the new codec name,
but the `.si` file on disk still declares the original codec. When Lucene
opens a segment, it reads the codec from `.si`, NOT from `segments_N`.
Without rewriting `.si`, Lucene would use the old codec and never invoke
Composite912DocValuesReader for star tree data.

If segment was NOT upgraded: add original SegmentCommitInfo unchanged.

**Step 3**: Preserve commit user data + commit
```java
newSegmentInfos.setUserData(originalInfos.getUserData(), false);
// User data contains: local_checkpoint, max_seq_no, translog_uuid, etc.

newSegmentInfos.commit(directory);  // writes segments_N+1 atomically
directory.sync(newSegmentInfos.files(true));  // fsync all files
directory.syncMetaData();  // fsync directory metadata
```

#### How does commit() work atomically?
Lucene writes to a temp file, fsyncs it, then renames to `segments_N+1`.
Rename is atomic on most filesystems. If crash happens before rename,
the old `segments_N` is still valid.

**Returns**: void. `segments_N+1` is on disk.

---

## Post-Upgrade: How Star Tree Data Is Read

### Composite912DocValuesFormat.java (MODIFIED)

```
class Composite912DocValuesFormat extends DocValuesFormat
```

#### What is DocValuesFormat?
Lucene SPI for doc values. Provides:
- `fieldsConsumer(SegmentWriteState)` → DocValuesConsumer (writer)
- `fieldsProducer(SegmentReadState)` → DocValuesProducer (reader)

#### What we changed in fieldsProducer():

Before: assumed all segments were native (direct file naming `_0.dvd`).

After: detects upgraded segments via PerField attributes.

```java
String suffix = getPerFieldDocValuesSuffix(state.fieldInfos);
if (suffix != null) {
    // Upgraded segment: files named _0_Lucene90_0.dvd
    // Create SegmentReadState with suffix "Lucene90_0"
} else {
    // Native segment: files named _0.dvd
    // Use empty suffix
}
return new Composite912DocValuesReader(readState, ...);
```

#### Why PerField suffix?
Upgraded segments were originally written by Lucene912Codec which uses
`PerFieldDocValuesFormat`. This format adds a suffix to file names:
`_0_Lucene90_0.dvd` instead of `_0.dvd`. When we switch codec to
Composite912Codec, we need to tell the delegate reader about this suffix.

`getPerFieldDocValuesSuffix()` reads `PerFieldDocValuesFormat.format` and
`PerFieldDocValuesFormat.suffix` attributes from FieldInfo objects.

### Composite912DocValuesReader.java (MODIFIED)

```
class Composite912DocValuesReader extends DocValuesProducer
```

#### What we changed: starTreeDir fallback

Before: opened star tree files from `readState.directory`.

After: tries `readState.directory` first, falls back to `readState.segmentInfo.dir`.

#### Why?
For compound file segments (.cfs), `readState.directory` is a CompoundDirectory
that only sees files INSIDE .cfs. Star tree files (.cid, .cim) are OUTSIDE
.cfs (written by our Phase 1). We need to look in the parent directory.

For native star tree segments, files are inside .cfs (written during flush),
so `readState.directory` works fine.

### Composite912Codec.java (NOT MODIFIED)

```
class Composite912Codec extends FilterCodec
```

#### What is Codec?
Lucene's top-level SPI. Bundles all format implementations:
- `postingsFormat()` — inverted index
- `docValuesFormat()` — doc values (star tree lives here)
- `storedFieldsFormat()` — stored fields
- `segmentInfoFormat()` — .si files

When Lucene opens a segment, it reads codec name from .si, uses ServiceLoader
to find the Codec class.

#### How it's used:
1. `rewriteSegmentInfos()` writes "Composite912Codec" into .si
2. Engine opens → Lucene reads .si → finds Composite912Codec
3. `Composite912Codec.docValuesFormat()` → Composite912DocValuesFormat
4. `fieldsProducer()` → Composite912DocValuesReader → reads star tree data

Constant: `COMPOSITE_INDEX_CODEC_NAME = "Composite912Codec"` — used by
StarTreeUpgradeService to skip already-upgraded segments.

---

## ReadOnlyEngine.java (NOT MODIFIED — used in Option B)

```
class ReadOnlyEngine extends Engine
```

#### What is Engine?
Abstract base for all engine implementations:
- `index()`, `delete()` — write operations
- `acquireSearcher()` — get searcher for reads
- `flush()`, `refresh()`, `forceMerge()` — maintenance
- `close()` — shutdown

#### What ReadOnlyEngine does:
Read-only engine without IndexWriter. Used during shard relocation,
engine reset, closed indices.

Constructor:
```java
ReadOnlyEngine(EngineConfig config, SeqNoStats seqNoStats,
    TranslogStats translogStats, boolean obtainLock,
    Function<DirectoryReader, DirectoryReader> readerWrapper,
    boolean requireCompleteHistory)
```

- `obtainLock` — if true, grabs IndexWriter write lock. For Option B: false
  (Phase 2 needs to write to directory)
- Opens DirectoryReader on latest commit
- Creates OpenSearchReaderManager for searcher acquisition
- Creates NoOpTranslogManager (no translog in read-only mode)

Key methods:
- `acquireSearcher()` — returns searcher from ReaderManager (reads work)
- `index()`, `delete()` — throw UnsupportedOperationException
- `flush()`, `forceMerge()` — no-op
- `close()` — closes reader, releases lock, decrements store ref

#### Why it works for Option B:
- Opens point-in-time snapshot of pre-upgrade segments
- Serves reads while Phase 1 + Phase 2 modify files on disk
- `obtainLock=false` means Phase 2 can write without lock conflict
- Reader holds open file descriptors — even if Phase 2 deletes .si files,
  reader's FDs keep data accessible (POSIX semantics)
- Phase 2 only modifies .si files + writes segments_N+1 — doesn't touch
  actual segment data files (.dvd, .doc, .cfs) that reader has open

---

## ReadOnlyEngine Bridge — Enabling Reads During Upgrade

### The Problem

The original upgradeToStarTree() closed the InternalEngine entirely:
```
close engine → [NO ENGINE, no reads] → Phase 1 → Phase 2 → open engine
```
For 1M docs this was ~27 seconds of zero read availability.

### The Solution

Swap to a ReadOnlyEngine instead of nulling the engine:
```
Swap 1: close InternalEngine + open ReadOnlyEngine → [reads via ROE] → Phase 1 → Phase 2 → Swap 2: close ROE + open InternalEngine
```

### Why ReadOnlyEngine Works Here

ReadOnlyEngine has no IndexWriter, no write lock, no merge threads. It opens a
DirectoryReader snapshot of the pre-upgrade segments. Phase 1 only creates new
files (.cid/.cim/.cidvd/.cidvm) — doesn't touch existing files. Phase 2 deletes
old .si files and writes new ones — the ROE's reader doesn't hold .si files open
(they're read once during SegmentInfos construction and closed). Phase 2 acquires
the write lock explicitly — ROE was opened with obtainLock=false so no conflict.

### The NPE Problem and Fix

After the mapping update, MapperService.getCompositeFieldTypes() returns the star
tree field. The query optimizer sees this and tries to use star tree acceleration.
But the ReadOnlyEngine's segments don't have star tree data (old codec). 
getStarTreeValues() returns null → aggregators NPE.

Fix: precomputeLeafUsingStarTree() changed from void to boolean. Returns false
when starTreeValues is null. tryPrecomputeAggregationForLeaf checks the return —
if false, normal doc values collection runs instead. Same fix for bucket
aggregators: getStarTreeBucketCollector() returns null → callers return false.

This doesn't affect normal star tree operation because getStarTreeValues() only
returns null when a segment doesn't have star tree data. For native star tree
indices, all segments have the data, so the null check is never triggered.

### Swap 1 Details (IndexShard.upgradeToStarTree)

```java
synchronized (engineMutex) {
    // 1. Create ROE first — if this throws, old engine is still current
    ReadOnlyEngine roEngine = new ReadOnlyEngine(
        newEngineConfig(replicationTracker),  // stale codec — fine, ROE only reads
        null, null, false, Function.identity(), false
    );
    // 2. Swap reference — reads now go to ROE
    currentEngineReference.set(roEngine);
    // 3. Close old engine — releases IndexWriter, write lock
    IOUtils.close(oldEngine);
    // 4. Set codec override — inside mutex, after close, prevents race
    codecServiceOverride = engineConfigFactory.newDefaultCodecService(...);
}
```

### Swap 2 Details (with error recovery)

```java
try {
    synchronized (engineMutex) {
        newEngine = engineFactory.newReadWriteEngine(newEngineConfig(replicationTracker));
        onNewEngine(newEngine);
        currentEngineReference.set(newEngine);
        IOUtils.close(roEngine);
    }
} catch (Exception e) {
    // Recovery: try with original codec
    codecServiceOverride = null;
    synchronized (engineMutex) {
        newEngine = engineFactory.newReadWriteEngine(newEngineConfig(replicationTracker));
        onNewEngine(newEngine);
        currentEngineReference.set(newEngine);
        IOUtils.close(roEngine);
    }
    // If recovery also fails: failShard()
}
// Refresh outside mutex — non-fatal if fails
newEngine.refresh("star-tree-upgrade");
```

### codecServiceOverride Lifecycle

The codecService field is final — created at shard init before the mapping update.
It doesn't include Composite912Codec. The override bridges this gap.

- Set inside Swap 1 mutex (after old engine close)
- NOT cleared after upgrade — needed if resetEngineToGlobalCheckpoint() runs
- Nulled in resetEngineToGlobalCheckpoint() when mapperService.isCompositeIndexPresent()
  is true — at that point the fresh CodecService already has Composite912Codec
- On node restart: new IndexShard gets fresh codecService with Composite912Codec
  because index.composite_index=true is in cluster state

### index.composite_index=true in Cluster State

Set in applyStarTreeMapping() alongside append_only=true. Both are Final settings
bypassed by directly writing to IndexMetadata.Builder in the cluster state executor.
This ensures subsequent shard constructions (node restart, relocation) get
Composite912Codec natively without needing the volatile codecServiceOverride.
