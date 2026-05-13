# Merged Segment Publishing API — Full Source Code

## 1. `IndexShard.publishMergedSegment(SegmentCommitInfo)`

**File:** `server/src/main/java/org/opensearch/index/shard/IndexShard.java` (line 2177)

```java
public void publishMergedSegment(SegmentCommitInfo segmentCommitInfo) throws IOException {
    assert mergedSegmentPublisher != null;
    mergedSegmentPublisher.publish(this, computeMergeSegmentCheckpoint(segmentCommitInfo));
}
```

## 2. `IndexShard.computeMergeSegmentCheckpoint(SegmentCommitInfo)`

**File:** `server/src/main/java/org/opensearch/index/shard/IndexShard.java` (line 2190)

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

## 3. `MergedSegmentPublisher` — Full Source

**File:** `server/src/main/java/org/opensearch/indices/replication/checkpoint/MergedSegmentPublisher.java`

```java
package org.opensearch.indices.replication.checkpoint;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.inject.Inject;
import org.opensearch.index.shard.IndexShard;

import java.util.Objects;

/**
 * Publish merged segment.
 *
 * @opensearch.api
 */
@ExperimentalApi
public class MergedSegmentPublisher {
    private final PublishAction publishAction;

    // This Component is behind feature flag so we are manually binding this in IndicesModule.
    @Inject
    public MergedSegmentPublisher(PublishAction publishAction) {
        this.publishAction = Objects.requireNonNull(publishAction);
    }

    public void publish(IndexShard indexShard, MergedSegmentCheckpoint checkpoint) {
        publishAction.publish(indexShard, checkpoint);
    }

    /**
     * Represents an action that is invoked to publish merged segment to replica shard
     *
     * @opensearch.api
     */
    @ExperimentalApi
    public interface PublishAction {
        void publish(IndexShard indexShard, MergedSegmentCheckpoint checkpoint);
    }

    /**
     * NoOp Checkpoint publisher
     */
    public static final MergedSegmentPublisher EMPTY = new MergedSegmentPublisher((indexShard, checkpoint) -> {});
}
```

## 4. `MergedSegmentWarmer.warm(LeafReader)` and `shouldWarm()`

**File:** `server/src/main/java/org/opensearch/index/engine/MergedSegmentWarmer.java`

```java
@Override
public void warm(LeafReader leafReader) throws IOException {

    long startTime = System.currentTimeMillis();
    long elapsedTime = -1;
    boolean shouldWarm = false;
    try {
        SegmentCommitInfo segmentCommitInfo = segmentCommitInfo(leafReader);
        // If shouldWarm fails, we increment the warmFailureCount
        // However, the time taken by shouldWarm is not accounted for in the totalWarmTime
        shouldWarm = shouldWarm(segmentCommitInfo);
        if (shouldWarm == false) {
            return;
        }
        mergedSegmentTransferTracker.incrementTotalWarmInvocationsCount();
        mergedSegmentTransferTracker.incrementOngoingWarms();
        logger.trace(() -> new ParameterizedMessage("Warming segment: {}", segmentCommitInfo));
        indexShard.publishMergedSegment(segmentCommitInfo);
        elapsedTime = System.currentTimeMillis() - startTime;
        long finalElapsedTime = elapsedTime;
        logger.trace(() -> {
            long segmentSize = -1;
            try {
                segmentSize = segmentCommitInfo.sizeInBytes();
            } catch (IOException ignored) {}
            return new ParameterizedMessage(
                "Completed segment warming for {}. Size: {}B, Timing: {}ms",
                segmentCommitInfo.info.name,
                segmentSize,
                finalElapsedTime
            );
        });
    } catch (Throwable t) {
        logger.warn(() -> new ParameterizedMessage("Failed to warm segment. Continuing. {}", segmentCommitInfo(leafReader)), t);
        mergedSegmentTransferTracker.incrementTotalWarmFailureCount();
    } finally {
        if (shouldWarm == true) {
            if (elapsedTime == -1) {
                elapsedTime = System.currentTimeMillis() - startTime;
            }
            mergedSegmentTransferTracker.addTotalWarmTimeMillis(elapsedTime);
            mergedSegmentTransferTracker.decrementOngoingWarms();
        }
    }
}

// package-private for tests
boolean shouldWarm(SegmentCommitInfo segmentCommitInfo) throws IOException {
    // Min node version check ensures that we only warm, when all nodes expect it
    Version minNodeVersion = clusterService.state().nodes().getMinNodeVersion();
    if (Version.V_3_4_0.compareTo(minNodeVersion) > 0) {
        return false;
    }

    if (indexShard.getRecoverySettings().isMergedSegmentReplicationWarmerEnabled() == false) {
        return false;
    }

    // in case we are unable to gauge the size of the merged segment segmentCommitInfo.sizeInBytes throws IOException
    // we would not warm the segment
    if (segmentCommitInfo.info == null || segmentCommitInfo.info.dir == null) {
        return false;
    }

    long segmentSize = segmentCommitInfo.sizeInBytes();
    double threshold = indexShard.getRecoverySettings().getMergedSegmentWarmerMinSegmentSizeThreshold().getBytes();
    if (segmentSize < threshold) {
        logger.trace(
            () -> new ParameterizedMessage(
                "Skipping warm for segment {}. SegmentSize {}B is less than the configured threshold {}B.",
                segmentCommitInfo.info.name,
                segmentSize,
                threshold
            )
        );
        return false;
    }

    return true;
}
```

## 5. `RemoteStorePublishMergedSegmentAction.publish()`

**File:** `server/src/main/java/org/opensearch/indices/replication/checkpoint/RemoteStorePublishMergedSegmentAction.java`

```java
@Override
public final void publish(IndexShard indexShard, MergedSegmentCheckpoint checkpoint) {
    long startTimeMillis = System.currentTimeMillis();
    Map<String, String> localToRemoteStoreFilenames = uploadMergedSegmentsToRemoteStore(indexShard, checkpoint);
    long endTimeMillis = System.currentTimeMillis();

    long elapsedTimeMillis = endTimeMillis - startTimeMillis;
    long timeoutMillis = indexShard.getRecoverySettings().getMergedSegmentReplicationTimeout().millis();
    long timeLeftMillis = Math.max(0, timeoutMillis - elapsedTimeMillis);
    indexShard.mergedSegmentTransferTracker().addTotalSendTimeMillis(elapsedTimeMillis);

    if (timeLeftMillis > 0) {
        RemoteStoreMergedSegmentCheckpoint remoteStoreMergedSegmentCheckpoint = new RemoteStoreMergedSegmentCheckpoint(
            checkpoint,
            localToRemoteStoreFilenames
        );
        doPublish(
            indexShard,
            remoteStoreMergedSegmentCheckpoint,
            new RemoteStorePublishMergedSegmentRequest(remoteStoreMergedSegmentCheckpoint),
            "segrep_remote_publish_merged_segment",
            true,
            TimeValue.timeValueMillis(timeLeftMillis),
            new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {}

                @Override
                public void onFailure(Exception e) {
                    indexShard.mergedSegmentTransferTracker().incrementTotalWarmFailureCount();
                }
            }
        );
    } else {
        indexShard.mergedSegmentTransferTracker().incrementTotalWarmFailureCount();
        logger.warn(
            () -> new ParameterizedMessage(
                "Unable to confirm upload of merged segment {} to remote store. Timeout of {}ms exceeded. Skipping pre-copy.",
                checkpoint,
                TimeValue.timeValueMillis(elapsedTimeMillis).toHumanReadableString(3)
            )
        );
    }
}
```

## 6. `RemoteStorePublishMergedSegmentAction.uploadMergedSegmentsToRemoteStore()`

**File:** `server/src/main/java/org/opensearch/indices/replication/checkpoint/RemoteStorePublishMergedSegmentAction.java`

```java
private Map<String, String> uploadMergedSegmentsToRemoteStore(IndexShard indexShard, MergedSegmentCheckpoint checkpoint) {
    Collection<String> segmentsToUpload = checkpoint.getMetadataMap().keySet();
    Map<String, String> localToRemoteStoreFilenames = new ConcurrentHashMap<>();

    Map<String, Long> segmentsSizeMap = checkpoint.getMetadataMap()
        .entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().length()));
    final CountDownLatch latch = new CountDownLatch(1);

    // Extract crypto metadata for merged segment upload
    CryptoMetadata cryptoMetadata = null;
    if (indexShard.indexSettings() != null) {
        IndexMetadata indexMetadata = indexShard.indexSettings().getIndexMetadata();
        if (indexMetadata != null) {
            cryptoMetadata = CryptoMetadata.fromIndexSettings(indexMetadata.getSettings());
        }
    }

    getRemoteStoreUploaderService(indexShard).uploadSegments(segmentsToUpload, segmentsSizeMap, new ActionListener<>() {
        @Override
        public void onResponse(Void unused) {
            logger.trace(() -> new ParameterizedMessage("Successfully uploaded segments {} to remote store", segmentsToUpload));
            latch.countDown();
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn(() -> new ParameterizedMessage("Failed to upload segments {} to remote store. {}", segmentsToUpload, e));
            latch.countDown();
        }
    }, (x) -> new UploadListener() {
        @Override
        public void beforeUpload(String file) {}

        @Override
        public void onSuccess(String file) {
            localToRemoteStoreFilenames.put(file, indexShard.getRemoteDirectory().getExistingRemoteFilename(file));
            indexShard.mergedSegmentTransferTracker().addTotalBytesSent(checkpoint.getMetadataMap().get(file).length());
        }

        @Override
        public void onFailure(String file) {
            logger.warn("Unable to upload segments during merge. Continuing.");
        }
    }, true, cryptoMetadata);
    try {
        long timeout = indexShard.getRecoverySettings().getMergedSegmentReplicationTimeout().seconds();
        if (latch.await(timeout, TimeUnit.SECONDS) == false) {
            logger.warn(
                () -> new ParameterizedMessage(
                    "Timeout exceeded {}s: Could not verify merge segment downloads were completed by replicas. Continuing.",
                    timeout
                )
            );
        }
    } catch (InterruptedException e) {
        logger.warn(
            () -> new ParameterizedMessage(
                "Unable to confirm successful merge segment downloads by replicas due to interruption. Continuing. \nException - {}",
                e
            )
        );
    }

    return localToRemoteStoreFilenames;
}
```

## 7. `PublishMergedSegmentAction.publish()`

**File:** `server/src/main/java/org/opensearch/indices/replication/checkpoint/PublishMergedSegmentAction.java`

```java
/**
 * Publish merged segment request to shard
 */
final public void publish(IndexShard indexShard, MergedSegmentCheckpoint checkpoint) {
    doPublish(
        indexShard,
        checkpoint,
        new PublishMergedSegmentRequest(checkpoint),
        TASK_ACTION_NAME,
        true,
        indexShard.getRecoverySettings().getMergedSegmentReplicationTimeout(),
        ActionListener.noOp()
    );
}
```

## 8. How `mergedSegmentPublisher` is injected into IndexShard

**File:** `server/src/main/java/org/opensearch/index/shard/IndexShard.java`

### Field declaration (line 412):
```java
private final MergedSegmentPublisher mergedSegmentPublisher;
```

### Constructor parameter (line 462):
```java
@Nullable final MergedSegmentPublisher mergedSegmentPublisher,
```

### Constructor assignment (line 612):
```java
this.mergedSegmentPublisher = mergedSegmentPublisher;
```

## 9. `MergedSegmentCheckpoint` Constructor Signature

**File:** `server/src/main/java/org/opensearch/indices/replication/checkpoint/MergedSegmentCheckpoint.java`

```java
public MergedSegmentCheckpoint(
    ShardId shardId,
    long primaryTerm,
    long segmentInfosVersion,
    long length,
    String codec,
    Map<String, StoreFileMetadata> metadataMap,
    String segmentName
) {
    super(shardId, primaryTerm, SequenceNumbers.NO_OPS_PERFORMED, segmentInfosVersion, length, codec, metadataMap);
    this.segmentName = segmentName;
}
```

There is also a deserialization constructor:
```java
public MergedSegmentCheckpoint(StreamInput in) throws IOException {
    super(in);
    segmentName = in.readString();
}
```

---

## Key Architecture Notes

- `MergedSegmentPublisher` is a simple delegator wrapping a `PublishAction` interface.
- Two implementations of `PublishAction`:
  - `PublishMergedSegmentAction` — for node-to-node segment replication (local on-disk)
  - `RemoteStorePublishMergedSegmentAction` — for remote store enabled clusters (uploads to remote store first, then publishes checkpoint)
- The warmer (`MergedSegmentWarmer`) is an `IndexWriter.IndexReaderWarmer` that calls `indexShard.publishMergedSegment()` during Lucene merge.
- `MergedSegmentCheckpoint` extends `ReplicationCheckpoint` and adds a `segmentName` field.
