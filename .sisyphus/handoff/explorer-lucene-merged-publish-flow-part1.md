# Lucene `publishMergedSegment(SegmentCommitInfo)` — Exhaustive Flow Map

> Authoritative specification for the DFA parallel refactor. Every guard, assertion, lock, null check, feature flag, setting, exception, log line, and replica-side hop is documented.

---

## A. Warmer Entry Point (MergedSegmentWarmer)

**File:** `server/src/main/java/org/opensearch/index/engine/MergedSegmentWarmer.java`

### Class Declaration (line 31)
```java
public class MergedSegmentWarmer implements IndexReaderWarmer {
    private final TransportService transportService;
    private final RecoverySettings recoverySettings;
    private final ClusterService clusterService;
    private final IndexShard indexShard;
    private final MergedSegmentTransferTracker mergedSegmentTransferTracker;
    private final Logger logger;
}
```

### Constructor (line 39)
```java
public MergedSegmentWarmer(
    TransportService transportService,
    RecoverySettings recoverySettings,
    ClusterService clusterService,
    IndexShard indexShard
)
```
- `mergedSegmentTransferTracker` = `indexShard.mergedSegmentTransferTracker()`
- `logger` = `Loggers.getLogger(getClass(), indexShard.shardId())`

### `warm(LeafReader)` — line 52
```java
@Override
public void warm(LeafReader leafReader) throws IOException {
    long startTime = System.currentTimeMillis();
    long elapsedTime = -1;
    boolean shouldWarm = false;
    try {
        SegmentCommitInfo segmentCommitInfo = segmentCommitInfo(leafReader);
        shouldWarm = shouldWarm(segmentCommitInfo);
        if (shouldWarm == false) { return; }
        mergedSegmentTransferTracker.incrementTotalWarmInvocationsCount();
        mergedSegmentTransferTracker.incrementOngoingWarms();
        logger.trace(() -> new ParameterizedMessage("Warming segment: {}", segmentCommitInfo));
        indexShard.publishMergedSegment(segmentCommitInfo);
        elapsedTime = System.currentTimeMillis() - startTime;
        long finalElapsedTime = elapsedTime;
        logger.trace(() -> {
            long segmentSize = -1;
            try { segmentSize = segmentCommitInfo.sizeInBytes(); } catch (IOException ignored) {}
            return new ParameterizedMessage(
                "Completed segment warming for {}. Size: {}B, Timing: {}ms",
                segmentCommitInfo.info.name, segmentSize, finalElapsedTime);
        });
    } catch (Throwable t) {
        logger.warn(() -> new ParameterizedMessage("Failed to warm segment. Continuing. {}", segmentCommitInfo(leafReader)), t);
        mergedSegmentTransferTracker.incrementTotalWarmFailureCount();
    } finally {
        if (shouldWarm == true) {
            if (elapsedTime == -1) { elapsedTime = System.currentTimeMillis() - startTime; }
            mergedSegmentTransferTracker.addTotalWarmTimeMillis(elapsedTime);
            mergedSegmentTransferTracker.decrementOngoingWarms();
        }
    }
}
```

### `segmentCommitInfo(LeafReader)` — line 89 (package-private)
```java
SegmentCommitInfo segmentCommitInfo(LeafReader leafReader) {
    assert leafReader instanceof SegmentReader;
    assert indexShard.indexSettings().isSegRepLocalEnabled() || indexShard.indexSettings().isRemoteStoreEnabled();
    try {
        return ((SegmentReader) leafReader).getSegmentInfo();
    } catch (Throwable e) {
        logger.warn("Unable to get segment info from leafReader. Continuing.", e);
    }
    return null;
}
```

### `shouldWarm(SegmentCommitInfo)` — line 101 (package-private)
Guards (in order):
1. **Min node version check** (line ~103): `Version.V_3_4_0.compareTo(minNodeVersion) > 0` → return false
2. **Feature toggle** (line ~108): `indexShard.getRecoverySettings().isMergedSegmentReplicationWarmerEnabled() == false` → return false
   - Setting: `indices.replication.merges.warmer.enabled` (default: **false**, dynamic, node-scope)
3. **Null checks** (line ~113): `segmentCommitInfo.info == null || segmentCommitInfo.info.dir == null` → return false
4. **Size threshold** (line ~117): `segmentSize < threshold` → return false
   - Setting: `indices.replication.merges.warmer.min_segment_size_threshold` (default: **500MB**, dynamic, node-scope)
   - Log on skip: `"Skipping warm for segment {}. SegmentSize {}B is less than the configured threshold {}B."`

### Wiring into IndexWriter
- **`MergedSegmentWarmerFactory`** (`server/src/main/java/org/opensearch/index/engine/MergedSegmentWarmerFactory.java`)
  - `get(IndexShard shard)`:
    - If `shard.indexSettings().isDocumentReplication()` → returns **null**
    - If `isSegRepLocalEnabled() || isRemoteStoreEnabled()` → returns `new MergedSegmentWarmer(...)`
    - Otherwise → throws `IllegalStateException`
- **Called at:** `IndexShard.java:4657` inside `newEngineConfig()`: `mergedSegmentWarmerFactory.get(this)`
- **Set on IndexWriterConfig:** `NativeLuceneIndexWriterFactory.java:222`:
  ```java
  if (indexSettings.isDocumentReplication() == false
      && (indexSettings.isSegRepLocalEnabled() || indexSettings.isRemoteStoreEnabled())) {
      assert null != engineConfig.getIndexReaderWarmer();
      iwc.setMergedSegmentWarmer(engineConfig.getIndexReaderWarmer());
  }
  ```
- **Lucene calls it from:** `IndexWriter#mergeMiddle` after merge completes, before the merged segment is visible.


---

## B. `publishMergedSegment(SegmentCommitInfo)` Call Site

**File:** `server/src/main/java/org/opensearch/index/shard/IndexShard.java:2184`

```java
public void publishMergedSegment(SegmentCommitInfo segmentCommitInfo) throws IOException {
    assert mergedSegmentPublisher != null;
    mergedSegmentPublisher.publish(this, computeMergeSegmentCheckpoint(segmentCommitInfo));
}
```

### `mergedSegmentPublisher` field
- **Declaration:** line 413: `private final MergedSegmentPublisher mergedSegmentPublisher;`
- **Constructor param:** line 463: `@Nullable final MergedSegmentPublisher mergedSegmentPublisher,`
- **Assignment:** line 613: `this.mergedSegmentPublisher = mergedSegmentPublisher;`

### Nullability semantics
- The field is `@Nullable` in the constructor signature.
- The `assert mergedSegmentPublisher != null` in `publishMergedSegment` means it MUST be non-null at call time.
- It is always non-null when the warmer is active because:
  - The warmer is only created for seg-rep/remote-store indices.
  - `MergedSegmentPublisher` is bound as eager singleton in `Node.java:1725`.
  - `IndicesClusterStateService` passes it through to `IndicesService.createShard()` → `IndexService.createShard()` → `IndexShard` constructor.

### When is it null?
- In test frameworks that don't wire up the full DI (e.g., `EngineTestCase`).
- Never null in production for seg-rep/remote-store indices.


---

## C. `computeMergeSegmentCheckpoint(SegmentCommitInfo)`

**File:** `server/src/main/java/org/opensearch/index/shard/IndexShard.java:2197`

```java
public MergedSegmentCheckpoint computeMergeSegmentCheckpoint(SegmentCommitInfo segmentCommitInfo) throws IOException {
    // Only need to get the file metadata information in segmentCommitInfo and reuse Store#getSegmentMetadataMap.
    try (GatedCloseable<SegmentInfos> segmentInfosGatedCloseable = getSegmentInfosSnapshot()) {
        SegmentInfos segmentInfos = new SegmentInfos(Version.LATEST.major);
        segmentInfos.add(segmentCommitInfo);
        Map<String, StoreFileMetadata> segmentMetadataMap = store.getSegmentMetadataMap(segmentInfos);
        return new MergedSegmentCheckpoint(
            shardId,
            getOperationPrimaryTerm(),
            segmentInfosGatedCloseable.get().getVersion(),
            segmentMetadataMap.values().stream().mapToLong(StoreFileMetadata::length).sum(),
            getIndexer().config().getCodec().getName(),
            segmentMetadataMap,
            segmentCommitInfo.info.name
        );
    }
}
```

### `getSegmentInfosSnapshot()` — line 6094
```java
@Deprecated
public GatedCloseable<SegmentInfos> getSegmentInfosSnapshot() {
    if (getIndexer() instanceof EngineBackedIndexer indexer) {
        return indexer.getEngine().getSegmentInfosSnapshot();
    }
    throw new IllegalStateException("Cannot request SegmentInfos directly on IndexShard");
}
```

### `InternalEngine.getSegmentInfosSnapshot()` — line 1911
```java
public GatedCloseable<SegmentInfos> getSegmentInfosSnapshot() {
    final OpenSearchDirectoryReader reader;
    try {
        reader = internalReaderManager.acquire();
        return new GatedCloseable<>(
            ((StandardDirectoryReader) reader.getDelegate()).getSegmentInfos(),
            () -> {
                try { internalReaderManager.release(reader); }
                catch (AlreadyClosedException e) { logger.warn("Engine is already closed.", e); }
            }
        );
    } catch (IOException e) {
        throw new EngineException(shardId, e.getMessage(), e);
    }
}
```
- **Lock:** Acquires a reader reference via `internalReaderManager.acquire()` (reference-counted).
- **Closeable lifetime:** The `GatedCloseable` holds the reader ref. Closing it releases the reader.
- **MUST happen inside closeable:** Reading `segmentInfosGatedCloseable.get().getVersion()` and `store.getSegmentMetadataMap(segmentInfos)`.

### `Store.getSegmentMetadataMap(SegmentInfos)` — `Store.java:429`
```java
public Map<String, StoreFileMetadata> getSegmentMetadataMap(SegmentInfos segmentInfos) throws IOException {
    assert indexSettings.isSegRepEnabledOrRemoteNode();
    failIfCorrupted();
    try {
        return loadMetadata(segmentInfos, directory, logger, true).fileMetadata;
    } catch (NoSuchFileException | CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
        markStoreCorrupted(ex);
        throw ex;
    }
}
```
- **Locks:** No explicit lock; relies on the directory's thread safety.
- **Can throw:** `NoSuchFileException`, `CorruptIndexException`, `IndexFormatTooOldException`, `IndexFormatTooNewException`, `IOException`.
- **Side effect on failure:** `markStoreCorrupted(ex)` — marks the store as corrupted.
- `loadMetadata` reads checksums from the Lucene directory for each file in the SegmentInfos.


---

## D. `MergedSegmentCheckpoint` Class

**File:** `server/src/main/java/org/opensearch/indices/replication/checkpoint/MergedSegmentCheckpoint.java:30`

```java
@ExperimentalApi
public class MergedSegmentCheckpoint extends ReplicationCheckpoint {
    private final String segmentName;
    // ...
}
```

### Fields
| Field | Source | Role |
|-------|--------|------|
| `segmentName` | `segmentCommitInfo.info.name` | Lucene segment name (e.g., `_0`) |

### Parent class: `ReplicationCheckpoint` (same package, line 34)
| Field | Type | Role |
|-------|------|------|
| `shardId` | `ShardId` | Identifies the shard |
| `primaryTerm` | `long` | Primary term at checkpoint creation |
| `segmentsGen` | `long` | Set to `SequenceNumbers.NO_OPS_PERFORMED` for merged segments |
| `segmentInfosVersion` | `long` | `SegmentInfos.getVersion()` at snapshot time |
| `length` | `long` | Sum of all file lengths in the segment |
| `codec` | `String` | Codec name from engine config |
| `metadataMap` | `Map<String, StoreFileMetadata>` | File name → checksum/length metadata |
| `createdTimeStamp` | `long` | `DateUtils.toLong(Instant.now())` |

### Serialization (Writeable)
Wire format (write order):
1. `super.writeTo(out)` → `shardId`, `primaryTerm`, `segmentsGen`, `segmentInfosVersion`, then version-gated: `length`+`codec` (≥V_2_7_0), `metadataMap` (≥V_2_10_0), `createdTimeStamp` (≥V_3_0_0)
2. `out.writeString(segmentName)`

Deserialization: `super(in)` then `segmentName = in.readString()`

### Subclass: `RemoteStoreMergedSegmentCheckpoint`
**File:** same package, `RemoteStoreMergedSegmentCheckpoint.java:25`
- Adds: `Map<String, String> localToRemoteSegmentFilenameMap` (local filename → remote store filename)
- Wire: after `super.writeTo`, writes the map; after `super(in)`, reads the map.

### Methods beyond getters
- `equals`: matches on `primaryTerm`, `segmentName`, `shardId`, `codec`
- `hashCode`: `Objects.hash(shardId, primaryTerm, segmentName)`
- `toString`: includes all fields


---

## E. `mergedSegmentPublisher.publish(IndexShard, MergedSegmentCheckpoint)`

### `MergedSegmentPublisher` class
**File:** `server/src/main/java/org/opensearch/indices/replication/checkpoint/MergedSegmentPublisher.java:23`

```java
@ExperimentalApi
public class MergedSegmentPublisher {
    private final PublishAction publishAction;

    @Inject
    public MergedSegmentPublisher(PublishAction publishAction) {
        this.publishAction = Objects.requireNonNull(publishAction);
    }

    public void publish(IndexShard indexShard, MergedSegmentCheckpoint checkpoint) {
        publishAction.publish(indexShard, checkpoint);
    }

    @ExperimentalApi
    public interface PublishAction {
        void publish(IndexShard indexShard, MergedSegmentCheckpoint checkpoint);
    }

    public static final MergedSegmentPublisher EMPTY = new MergedSegmentPublisher((indexShard, checkpoint) -> {});
}
```

### Binding (Node.java:1720-1725)
```java
if (isRemoteDataAttributePresent(settings)) {
    b.bind(MergedSegmentPublisher.PublishAction.class).to(RemoteStorePublishMergedSegmentAction.class).asEagerSingleton();
} else {
    b.bind(MergedSegmentPublisher.PublishAction.class).to(PublishMergedSegmentAction.class).asEagerSingleton();
}
b.bind(MergedSegmentPublisher.class).asEagerSingleton();
```
