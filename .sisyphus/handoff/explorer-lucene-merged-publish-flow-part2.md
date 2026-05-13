# Lucene publishMergedSegment Flow — Part 2 (Sections F–I)

> Continuation of explorer-lucene-merged-publish-flow-part1.md

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

