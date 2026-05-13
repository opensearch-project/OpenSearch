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
### Implementation 1: `PublishMergedSegmentAction` (local seg-rep)
**File:** `server/src/main/java/org/opensearch/indices/replication/checkpoint/PublishMergedSegmentAction.java`

```java
public final void publish(IndexShard indexShard, MergedSegmentCheckpoint checkpoint) {
    doPublish(
        indexShard,
        checkpoint,
        new PublishMergedSegmentRequest(checkpoint),
        TASK_ACTION_NAME,  // "segrep_publish_merged_segment"
        true,              // waitForCompletion
        indexShard.getRecoverySettings().getMergedSegmentReplicationTimeout(),
        ActionListener.noOp()
    );
}
```
- Action name: `"indices:admin/publish_merged_segment"`
- No retry/backoff — fire-and-wait with timeout.
- Error handling: delegated to `doPublish` (see Section H).

### Implementation 2: `RemoteStorePublishMergedSegmentAction` (remote store)
**File:** `server/src/main/java/org/opensearch/indices/replication/checkpoint/RemoteStorePublishMergedSegmentAction.java`

```java
public final void publish(IndexShard indexShard, MergedSegmentCheckpoint checkpoint) {
    long startTimeMillis = System.currentTimeMillis();
    Map<String, String> localToRemoteStoreFilenames = uploadMergedSegmentsToRemoteStore(indexShard, checkpoint);
    long endTimeMillis = System.currentTimeMillis();
    long elapsedTimeMillis = endTimeMillis - startTimeMillis;
    long timeoutMillis = indexShard.getRecoverySettings().getMergedSegmentReplicationTimeout().millis();
    long timeLeftMillis = Math.max(0, timeoutMillis - elapsedTimeMillis);
    indexShard.mergedSegmentTransferTracker().addTotalSendTimeMillis(elapsedTimeMillis);

    if (timeLeftMillis > 0) {
        RemoteStoreMergedSegmentCheckpoint remoteCheckpoint = new RemoteStoreMergedSegmentCheckpoint(checkpoint, localToRemoteStoreFilenames);
        doPublish(indexShard, remoteCheckpoint, new RemoteStorePublishMergedSegmentRequest(remoteCheckpoint),
            "segrep_remote_publish_merged_segment", true, TimeValue.timeValueMillis(timeLeftMillis),
            new ActionListener<>() {
                @Override public void onResponse(Void unused) {}
                @Override public void onFailure(Exception e) {
                    indexShard.mergedSegmentTransferTracker().incrementTotalWarmFailureCount();
                }
            });
    } else {
        indexShard.mergedSegmentTransferTracker().incrementTotalWarmFailureCount();
        logger.warn(() -> new ParameterizedMessage(
            "Unable to confirm upload of merged segment {} to remote store. Timeout of {}ms exceeded. Skipping pre-copy.",
            checkpoint, TimeValue.timeValueMillis(elapsedTimeMillis).toHumanReadableString(3)));
    }
}
```
- **Upload phase:** `uploadMergedSegmentsToRemoteStore` — uploads segment files to remote store with a `CountDownLatch`, times out at `getMergedSegmentReplicationTimeout().seconds()`.
- **Publish phase:** sends `RemoteStorePublishMergedSegmentRequest` to replicas with remaining time budget.
- Action name: `"indices:admin/remote_publish_merged_segment"`


---

## F. Replica-side Receive/Validation Path

### Transport dispatch
Both `PublishMergedSegmentAction` and `RemoteStorePublishMergedSegmentAction` extend `AbstractPublishCheckpointAction` which extends `TransportReplicationAction`. The replication framework routes the request to replicas via `shardOperationOnReplica` → `doReplicaOperation`.

### `AbstractPublishCheckpointAction.shardOperationOnReplica` (line ~196)
```java
final protected void shardOperationOnReplica(ReplicaRequest shardRequest, IndexShard replica, ActionListener<ReplicaResult> listener) {
    Objects.requireNonNull(shardRequest);
    Objects.requireNonNull(replica);
    ActionListener.completeWith(listener, () -> {
        logger.trace(() -> new ParameterizedMessage("Checkpoint {} received on replica {}", shardRequest, replica.shardId()));
        if (replica.indexSettings().isAssignedOnRemoteNode() == false && replica.indexSettings().isSegRepLocalEnabled() == false) {
            logger.trace("Received segrep checkpoint on a docrep shard copy during an ongoing remote migration. NoOp.");
            return new ReplicaResult();
        }
        doReplicaOperation(shardRequest, replica);
        return new ReplicaResult();
    });
}
```

### Local seg-rep: `PublishMergedSegmentAction.doReplicaOperation` (line ~93)
```java
protected void doReplicaOperation(PublishMergedSegmentRequest request, IndexShard replica) {
    if (request.getMergedSegment().getShardId().equals(replica.shardId())) {
        replicationService.onNewMergedSegmentCheckpoint(request.getMergedSegment(), replica);
    }
}
```

### Remote store: `RemoteStorePublishMergedSegmentAction.doReplicaOperation` (line ~82)
```java
protected void doReplicaOperation(RemoteStorePublishMergedSegmentRequest shardRequest, IndexShard replica) {
    RemoteStoreMergedSegmentCheckpoint checkpoint = shardRequest.getMergedSegment();
    if (checkpoint.getShardId().equals(replica.shardId())) {
        long startTime = System.currentTimeMillis();
        replica.getRemoteDirectory().markMergedSegmentsPendingDownload(checkpoint.getLocalToRemoteSegmentFilenameMap());
        replicationService.onNewMergedSegmentCheckpoint(checkpoint, replica);
        replica.mergedSegmentTransferTracker().addTotalReceiveTimeMillis(System.currentTimeMillis() - startTime);
    } else {
        logger.warn(() -> new ParameterizedMessage(
            "Received merged segment checkpoint for shard {} on replica shard {}, ignoring checkpoint",
            checkpoint.getShardId(), replica.shardId()));
    }
}
```

### `SegmentReplicationTargetService.onNewMergedSegmentCheckpoint` (line 687)
Guards:
1. `replicaShard.state().equals(IndexShardState.CLOSED)` → return (log trace)
2. `replicaShard.state().equals(IndexShardState.STARTED) == true`:
   - Check ongoing targets: cancel if old primary term, skip if duplicate checkpoint
   - `replicaShard.shouldProcessMergedSegmentCheckpoint(receivedCheckpoint)`:
     - `isSegmentReplicationAllowed() && requestCheckpoint.getPrimaryTerm() >= getOperationPrimaryTerm()`
   - If passes: `startMergedSegmentReplication(...)` with a `CountDownLatch` + timeout
3. Else: log trace "shard not started"

### File fetch (replica pulls from primary)
- **Local seg-rep:** `PrimaryShardReplicationSource.getMergedSegmentFiles` sends transport request to `"internal:index/shard/replication/get_merged_segment_files"` on the primary node.
- **Remote store:** `RemoteStoreReplicationSource.getMergedSegmentFiles` downloads from remote directory using `indexShard.getFileDownloader().downloadAsync(...)`.

### Finalization: `MergedSegmentReplicationTarget.finalizeReplication` (line ~70)
```java
protected void finalizeReplication(CheckpointInfoResponse checkpointInfoResponse) throws Exception {
    assert checkpoint instanceof MergedSegmentCheckpoint;
    multiFileWriter.renameAllTempFiles();
    indexShard.addPendingMergeSegmentCheckpoint((MergedSegmentCheckpoint) checkpoint);
}
```
- Renames temp files to final names.
- Adds checkpoint to `pendingMergedSegmentCheckpoints` (ConcurrentHashSet on IndexShard:415).

### Checksum divergence concern
- The primary computes checksums via `Store.getSegmentMetadataMap(SegmentInfos)` → `loadMetadata` which reads Lucene's stored checksums from the directory.
- The replica does NOT recompute checksums — it uses the `metadataMap` from the checkpoint directly (`getCheckpointMetadata` returns `checkpoint.getMetadataMap()`).
- **Risk:** If the DFA path computes checksums differently (e.g., different `FormatChecksumStrategy`), the replica would still trust the primary's metadata map. Divergence would only surface if a subsequent full replication checkpoint comparison fails to match file metadata.


---

## G. Feature Flags / Settings Gating the Entire Flow

### Settings controlling the flow

| Setting | Default | Scope | Controls |
|---------|---------|-------|----------|
| `indices.replication.merges.warmer.enabled` | `false` | Dynamic, NodeScope | Master on/off for the warmer |
| `indices.replication.merges.warmer.min_segment_size_threshold` | `500MB` | Dynamic, NodeScope | Min segment size to trigger warm |
| `indices.replication.merges.warmer.timeout` | `15m` | Dynamic, NodeScope | Timeout for the entire publish+replicate cycle |
| `indices.replication.merges.warmer.max_bytes_per_sec` | `-1` (reuse recovery setting) | Dynamic, NodeScope | Rate limiter for merged segment transfer |

### Version compatibility
- `shouldWarm()` checks: `Version.V_3_4_0.compareTo(clusterService.state().nodes().getMinNodeVersion()) > 0` → skip if any node < 3.4.0
- `ReplicationCheckpoint` wire format is version-gated at V_2_7_0, V_2_10_0, V_3_0_0

### Nullability of `mergedSegmentPublisher`
- Constructor param is `@Nullable`.
- In production, always non-null because `Node.java` binds `MergedSegmentPublisher.class` as eager singleton.
- The `assert mergedSegmentPublisher != null` in `publishMergedSegment` guards against test misconfiguration.

### Index-level gates
- `MergedSegmentWarmerFactory.get()` returns null for document-replication indices → warmer not set on IWC.
- `NativeLuceneIndexWriterFactory` only sets warmer if `!isDocumentReplication() && (isSegRepLocalEnabled() || isRemoteStoreEnabled())`.

### Node-level gate (binding)
- `isRemoteDataAttributePresent(settings)` determines which `PublishAction` impl is bound.


---

## H. Exceptions and Logging

### Catch blocks in the flow

| Location | Catches | Action |
|----------|---------|--------|
| `MergedSegmentWarmer.warm()` :87 | `Throwable t` | log warn + `incrementTotalWarmFailureCount()` |
| `MergedSegmentWarmer.segmentCommitInfo()` :96 | `Throwable e` | log warn "Unable to get segment info from leafReader. Continuing." |
| `InternalEngine.getSegmentInfosSnapshot()` | `IOException e` | throw `EngineException` |
| `Store.getSegmentMetadataMap()` | `NoSuchFileException, CorruptIndexException, IndexFormatTooOldException, IndexFormatTooNewException` | `markStoreCorrupted(ex)` + rethrow |
| `AbstractPublishCheckpointAction.doPublish()` handleException | `TransportException e` | log debug timing, then check for `NodeClosedException/IndexNotFoundException/AlreadyClosedException/IndexShardClosedException/ShardNotInPrimaryModeException` → silent return; otherwise log warn |
| `AbstractPublishCheckpointAction.doPublish()` latch.await | `InterruptedException e` | `notifyOnceListener.onFailure(e)` + log warn |
| `AbstractPublishCheckpointAction.doPublish()` outer try | `Exception e` | `notifyOnceListener.onFailure(e)` |
| `RemoteStorePublishMergedSegmentAction.uploadMergedSegmentsToRemoteStore` latch.await | `InterruptedException e` | log warn |
| `RemoteStorePublishMergedSegmentAction` onFailure listener | `Exception e` | `incrementTotalWarmFailureCount()` |
| `SegmentReplicationTargetService.onNewMergedSegmentCheckpoint` latch.await | `InterruptedException e` | log warn |

### Log lines (verbatim, with level)

| Level | Message | Location |
|-------|---------|----------|
| TRACE | `"Warming segment: {}"` | MergedSegmentWarmer.java:67 |
| TRACE | `"Completed segment warming for {}. Size: {}B, Timing: {}ms"` | MergedSegmentWarmer.java:70 |
| WARN | `"Failed to warm segment. Continuing. {}"` | MergedSegmentWarmer.java:80 |
| WARN | `"Unable to get segment info from leafReader. Continuing."` | MergedSegmentWarmer.java:96 |
| TRACE | `"Skipping warm for segment {}. SegmentSize {}B is less than the configured threshold {}B."` | MergedSegmentWarmer.java:119 |
| TRACE | `"[shardId {}] Publishing replication checkpoint [{}]"` | AbstractPublishCheckpointAction.java (after sendChildRequest) |
| DEBUG | `"[shardId {}] Completed publishing checkpoint [{}], timing: {}"` | AbstractPublishCheckpointAction.java handleResponse |
| DEBUG | `"[shardId {}] Failed to publish checkpoint [{}], timing: {}"` | AbstractPublishCheckpointAction.java handleException |
| WARN | `"{} segment replication checkpoint [{}] publishing failed"` | AbstractPublishCheckpointAction.java handleException (non-benign) |
| WARN | `"Interrupted while waiting for publish checkpoint complete [{}]"` | AbstractPublishCheckpointAction.java latch.await |
| TRACE | `"Checkpoint {} received on replica {}"` | AbstractPublishCheckpointAction.java shardOperationOnReplica |
| TRACE | `"Received segrep checkpoint on a docrep shard copy during an ongoing remote migration. NoOp."` | AbstractPublishCheckpointAction.java |
| DEBUG | `"Replica received new merged segment checkpoint [{}] from primary"` | SegmentReplicationTargetService.java:688 |
| TRACE | `"Ignoring merged segment checkpoint, Shard is closed"` | SegmentReplicationTargetService.java:693 |
| DEBUG | `"Cancelling ongoing merge replication {} from old primary with primary term {}"` | SegmentReplicationTargetService.java:702 |
| DEBUG | `"Ignoring new merge replication checkpoint - shard is currently replicating to checkpoint {}"` | SegmentReplicationTargetService.java:710 |
| DEBUG | `"[shardId {}] [replication id {}] Merge Replication complete to {}, timing data: {}"` | SegmentReplicationTargetService.java:724 |
| WARN | `"Merged segment replication for {} timed out after [{}] seconds"` | SegmentReplicationTargetService.java:744 |
| WARN | `"Interrupted while waiting for pre copy merged segment [{}]"` | SegmentReplicationTargetService.java:750 |
| TRACE | `"Ignoring merged segment checkpoint, shard not started {} {}"` | SegmentReplicationTargetService.java:756 |
| TRACE | `"Successfully uploaded segments {} to remote store"` | RemoteStorePublishMergedSegmentAction.java upload onResponse |
| WARN | `"Failed to upload segments {} to remote store. {}"` | RemoteStorePublishMergedSegmentAction.java upload onFailure |
| WARN | `"Unable to upload segments during merge. Continuing."` | RemoteStorePublishMergedSegmentAction.java per-file onFailure |
| WARN | `"Unable to confirm upload of merged segment {} to remote store. Timeout of {}ms exceeded. Skipping pre-copy."` | RemoteStorePublishMergedSegmentAction.java timeout |
| WARN | `"Timeout exceeded {}s: Could not verify merge segment downloads were completed by replicas. Continuing."` | RemoteStorePublishMergedSegmentAction.java latch timeout |
| WARN | `"Unable to confirm successful merge segment downloads by replicas due to interruption. Continuing."` | RemoteStorePublishMergedSegmentAction.java InterruptedException |
| WARN | `"Received merged segment checkpoint for shard {} on replica shard {}, ignoring checkpoint"` | RemoteStorePublishMergedSegmentAction.java doReplicaOperation mismatch |


---

## I. Call Sequence Diagram

```
PRIMARY NODE (inside IndexWriter#mergeMiddle thread)
═══════════════════════════════════════════════════

1. Lucene IndexWriter#mergeMiddle completes merge
   └─► calls IndexReaderWarmer.warm(LeafReader)

2. MergedSegmentWarmer.warm(leafReader)                    [MergedSegmentWarmer.java:52]
   ├─ segmentCommitInfo(leafReader)                        [MergedSegmentWarmer.java:89]
   │   ├─ assert leafReader instanceof SegmentReader
   │   ├─ assert isSegRepLocalEnabled() || isRemoteStoreEnabled()
   │   └─ return ((SegmentReader) leafReader).getSegmentInfo()
   ├─ shouldWarm(segmentCommitInfo)                        [MergedSegmentWarmer.java:101]
   │   ├─ CHECK: V_3_4_0 <= minNodeVersion                [line ~103]
   │   ├─ CHECK: isMergedSegmentReplicationWarmerEnabled() [line ~108]
   │   ├─ CHECK: info != null && info.dir != null          [line ~113]
   │   └─ CHECK: sizeInBytes() >= threshold               [line ~117]
   ├─ incrementTotalWarmInvocationsCount()
   ├─ incrementOngoingWarms()
   ├─ LOG TRACE: "Warming segment: {}"
   │
   ├─► indexShard.publishMergedSegment(segmentCommitInfo)  [IndexShard.java:2184]
   │   ├─ assert mergedSegmentPublisher != null
   │   │
   │   ├─► computeMergeSegmentCheckpoint(segmentCommitInfo) [IndexShard.java:2197]
   │   │   ├─ getSegmentInfosSnapshot()                    [IndexShard.java:6094]
   │   │   │   └─ InternalEngine.getSegmentInfosSnapshot() [InternalEngine.java:1911]
   │   │   │       └─ acquire reader ref from internalReaderManager
   │   │   ├─ new SegmentInfos(Version.LATEST.major)
   │   │   ├─ segmentInfos.add(segmentCommitInfo)
   │   │   ├─ store.getSegmentMetadataMap(segmentInfos)    [Store.java:429]
   │   │   │   ├─ assert isSegRepEnabledOrRemoteNode()
   │   │   │   ├─ failIfCorrupted()
   │   │   │   └─ loadMetadata(segmentInfos, directory, logger, true)
   │   │   ├─ new MergedSegmentCheckpoint(shardId, primaryTerm, version, totalLength, codec, metadataMap, segmentName)
   │   │   └─ close GatedCloseable (release reader ref)
   │   │
   │   └─► mergedSegmentPublisher.publish(this, checkpoint) [MergedSegmentPublisher.java:30]
   │       └─► publishAction.publish(indexShard, checkpoint)
   │
   │   ┌─── LOCAL SEG-REP PATH ────────────────────────────────────────────────┐
   │   │ PublishMergedSegmentAction.publish()              [PublishMergedSegmentAction.java:74]
   │   │ └─ doPublish(shard, checkpoint, request, "segrep_publish_merged_segment", true, timeout, noOp)
   │   │    └─ AbstractPublishCheckpointAction.doPublish() [AbstractPublishCheckpointAction.java:103]
   │   │       ├─ stash thread context, mark system context
   │   │       ├─ register ReplicationTask
   │   │       ├─ transportService.sendChildRequest(targetNode, transportPrimaryAction, ...)
   │   │       ├─ LOG TRACE: "Publishing replication checkpoint"
   │   │       └─ latch.await(timeout) — blocks until response or timeout
   │   └───────────────────────────────────────────────────────────────────────┘
   │
   │   ┌─── REMOTE STORE PATH ─────────────────────────────────────────────────┐
   │   │ RemoteStorePublishMergedSegmentAction.publish()   [RemoteStorePublishMergedSegmentAction.java:107]
   │   │ ├─ uploadMergedSegmentsToRemoteStore(shard, checkpoint)
   │   │ │   ├─ getRemoteStoreUploaderService(shard).uploadSegments(...)
   │   │ │   ├─ per-file: localToRemoteStoreFilenames.put(file, remoteFilename)
   │   │ │   └─ latch.await(timeout) — blocks until upload completes
   │   │ ├─ compute timeLeftMillis
   │   │ ├─ addTotalSendTimeMillis(elapsed)
   │   │ ├─ if timeLeft > 0:
   │   │ │   ├─ new RemoteStoreMergedSegmentCheckpoint(checkpoint, localToRemoteFilenames)
   │   │ │   └─ doPublish(shard, remoteCheckpoint, request, "segrep_remote_publish_merged_segment", true, timeLeft, failureListener)
   │   │ └─ else: incrementTotalWarmFailureCount() + LOG WARN timeout
   │   └───────────────────────────────────────────────────────────────────────┘
   │
   ├─ LOG TRACE: "Completed segment warming for ..."
   └─ finally: addTotalWarmTimeMillis() + decrementOngoingWarms()


REPLICA NODE (transport thread)
═══════════════════════════════

3. TransportReplicationAction routes to replica
   └─► AbstractPublishCheckpointAction.shardOperationOnReplica()
       ├─ Objects.requireNonNull(shardRequest, replica)
       ├─ LOG TRACE: "Checkpoint {} received on replica {}"
       ├─ CHECK: isAssignedOnRemoteNode() || isSegRepLocalEnabled() (else NoOp)
       └─► doReplicaOperation(request, replica)

4a. [LOCAL] PublishMergedSegmentAction.doReplicaOperation()
    └─ CHECK: shardId matches
       └─► replicationService.onNewMergedSegmentCheckpoint(checkpoint, replica)

4b. [REMOTE] RemoteStorePublishMergedSegmentAction.doReplicaOperation()
    ├─ CHECK: shardId matches
    ├─ replica.getRemoteDirectory().markMergedSegmentsPendingDownload(localToRemoteMap)
    └─► replicationService.onNewMergedSegmentCheckpoint(checkpoint, replica)

5. SegmentReplicationTargetService.onNewMergedSegmentCheckpoint()  [line 687]
   ├─ LOG DEBUG: "Replica received new merged segment checkpoint"
   ├─ CHECK: shard not CLOSED
   ├─ CHECK: shard is STARTED
   ├─ CHECK: no duplicate ongoing target for same checkpoint
   ├─ CHECK: shouldProcessMergedSegmentCheckpoint(checkpoint)
   │   └─ isSegmentReplicationAllowed() && primaryTerm >= local primaryTerm
   └─► startMergedSegmentReplication(shard, checkpoint, listener)

6. MergedSegmentReplicationTarget (extends AbstractSegmentReplicationTarget)
   ├─ getCheckpointMetadata(): returns checkpoint.getMetadataMap() directly (no round-trip)
   ├─ getFilesFromSource():
   │   ├─ [LOCAL] PrimaryShardReplicationSource.getMergedSegmentFiles() → transport to primary
   │   └─ [REMOTE] RemoteStoreReplicationSource.getMergedSegmentFiles() → download from remote dir
   └─ finalizeReplication():
       ├─ assert checkpoint instanceof MergedSegmentCheckpoint
       ├─ multiFileWriter.renameAllTempFiles()
       └─ indexShard.addPendingMergeSegmentCheckpoint(checkpoint)

7. Completion callback:
   ├─ LOG DEBUG: "Merge Replication complete to {}, timing data: {}"
   └─ unmarkPendingDownloadMergedSegments(shard, checkpoint)
       └─ [REMOTE only] shard.getRemoteDirectory().unmarkMergedSegmentsPendingDownload(metadataMap.keySet())
```

---

## Cross-check Verification

Every line in `publishMergedSegment` (IndexShard.java:2184-2186) is accounted for:
- Line 2184: method signature → Step 2 entry
- Line 2185: `assert mergedSegmentPublisher != null` → noted in Step 2
- Line 2186: `mergedSegmentPublisher.publish(this, computeMergeSegmentCheckpoint(segmentCommitInfo))` → Steps 2→computeMergeSegmentCheckpoint→publish

Every line in `computeMergeSegmentCheckpoint` (2197-2213) is accounted for:
- try-with-resources `getSegmentInfosSnapshot()` → Step 2 sub-bullet
- `new SegmentInfos(...)` + `.add(...)` → Step 2 sub-bullet
- `store.getSegmentMetadataMap(segmentInfos)` → Step 2 sub-bullet
- `new MergedSegmentCheckpoint(...)` with all 7 args → Step 2 sub-bullet
- close of GatedCloseable → Step 2 sub-bullet

