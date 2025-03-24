/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.opensearch.OpenSearchCorruptionException;
import org.opensearch.common.SetOnce;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.ReplicationStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationCollection;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationListener;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;

/**
 * This class is responsible for managing segment replication events on replicas.
 * It uses a {@link ReplicationCollection} to track ongoing replication events and
 * manages the state of each replication event.
 *
 * @opensearch.internal
 */
public class SegmentReplicator {

    private static final Logger logger = LogManager.getLogger(SegmentReplicator.class);

    private final ReplicationCollection<SegmentReplicationTarget> onGoingReplications;
    private final Map<ShardId, SegmentReplicationState> completedReplications = ConcurrentCollections.newConcurrentMap();
    private final ConcurrentMap<ShardId, ConcurrentNavigableMap<Long, ReplicationCheckpointStats>> replicationCheckpointStats =
        ConcurrentCollections.newConcurrentMap();
    private final ConcurrentMap<ShardId, ReplicationCheckpoint> primaryCheckpoint = ConcurrentCollections.newConcurrentMap();

    private final ThreadPool threadPool;
    private final SetOnce<SegmentReplicationSourceFactory> sourceFactory;

    public SegmentReplicator(ThreadPool threadPool) {
        this.onGoingReplications = new ReplicationCollection<>(logger, threadPool);
        this.threadPool = threadPool;
        this.sourceFactory = new SetOnce<>();
    }

    /**
     * Starts a replication event for the given shard.
     * @param shard - {@link IndexShard} replica shard
     */
    public void startReplication(IndexShard shard) {
        if (sourceFactory.get() == null) return;
        startReplication(
            shard,
            shard.getLatestReplicationCheckpoint(),
            sourceFactory.get().get(shard),
            new SegmentReplicationTargetService.SegmentReplicationListener() {
                @Override
                public void onReplicationDone(SegmentReplicationState state) {
                    logger.trace("Completed replication for {}", shard.shardId());
                }

                @Override
                public void onReplicationFailure(SegmentReplicationState state, ReplicationFailedException e, boolean sendShardFailure) {
                    logger.error(() -> new ParameterizedMessage("Failed segment replication for {}", shard.shardId()), e);
                    if (sendShardFailure) {
                        shard.failShard("unrecoverable replication failure", e);
                    }
                }
            }
        );
    }

    void setSourceFactory(SegmentReplicationSourceFactory sourceFactory) {
        this.sourceFactory.set(sourceFactory);
    }

    /**
     * Start a round of replication and sync to at least the given checkpoint.
     * @param indexShard - {@link IndexShard} replica shard
     * @param checkpoint - {@link ReplicationCheckpoint} checkpoint to sync to
     * @param listener - {@link ReplicationListener}
     * @return {@link SegmentReplicationTarget} target event orchestrating the event.
     */
    SegmentReplicationTarget startReplication(
        final IndexShard indexShard,
        final ReplicationCheckpoint checkpoint,
        final SegmentReplicationSource source,
        final SegmentReplicationTargetService.SegmentReplicationListener listener
    ) {
        final SegmentReplicationTarget target = new SegmentReplicationTarget(indexShard, checkpoint, source, listener);
        startReplication(target, indexShard.getRecoverySettings().activityTimeout());
        return target;
    }

    /**
     * Retrieves segment replication statistics for a specific shard.
     * Its computed based on the last and first entry in the replicationCheckpointStats map.
     * The Last entry gives the Bytes behind, and the difference in the first and last entry provides the lag.
     *
     * @param shardId The shardId to get statistics for
     * @return ReplicationStats containing bytes behind and replication lag information
     */
    public ReplicationStats getSegmentReplicationStats(final ShardId shardId) {
        final ConcurrentNavigableMap<Long, ReplicationCheckpointStats> existingCheckpointStats = replicationCheckpointStats.get(shardId);
        if (existingCheckpointStats == null || existingCheckpointStats.isEmpty()) {
            return ReplicationStats.empty();
        }

        Map.Entry<Long, ReplicationCheckpointStats> lowestEntry = existingCheckpointStats.firstEntry();
        Map.Entry<Long, ReplicationCheckpointStats> highestEntry = existingCheckpointStats.lastEntry();

        long bytesBehind = highestEntry.getValue().getBytesBehind();
        long replicationLag = bytesBehind > 0L
            ? TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lowestEntry.getValue().getTimestamp())
            : 0;

        return new ReplicationStats(bytesBehind, bytesBehind, replicationLag);
    }

    /**
     * Updates the latest checkpoint of the primary for the replica shard and then
     * calculates checkpoint statistics for the replica shard with the latest checkpoint information.
     * This method maintains statistics about how far behind replica shards are from the primary.
     * It calculates the bytes behind by comparing the latest-received and current checkpoint in the indexShard,
     * and it maintains the bytes behind and timestamp for each segmentInfosVersion of latestCheckPoint.
     * <pre>
     * Example:
     * {
     *     [replica][0] : {
     *                       7 : {bytesBehind=0, timestamp=1700220000000}
     *                       8 : {bytesBehind=100, timestamp=1700330000000}
     *                       9 : {bytesBehind=150, timestamp=1700440000000}
     *                    }
     * }
     * </pre>
     * @param latestReceivedCheckPoint The most recent checkpoint from the primary
     * @param indexShard The index shard where its updated
     */
    public void updateReplicationCheckpointStats(final ReplicationCheckpoint latestReceivedCheckPoint, final IndexShard indexShard) {
        ReplicationCheckpoint primaryCheckPoint = this.primaryCheckpoint.get(indexShard.shardId());
        if (primaryCheckPoint == null || latestReceivedCheckPoint.isAheadOf(primaryCheckPoint)) {
            this.primaryCheckpoint.put(indexShard.shardId(), latestReceivedCheckPoint);
            calculateReplicationCheckpointStats(latestReceivedCheckPoint, indexShard);
        }
    }

    /**
     * Removes checkpoint statistics for all checkpoints up to and including the last successful sync
     * and recalculates the bytes behind value for the last replicationCheckpointStats entry.
     * This helps maintain only relevant checkpoint information and clean up old data.
     *
     * @param indexShard The index shard to prune checkpoints for
     */
    protected void pruneCheckpointsUpToLastSync(final IndexShard indexShard) {
        ReplicationCheckpoint latestCheckpoint = this.primaryCheckpoint.get(indexShard.shardId());
        if (latestCheckpoint != null) {
            ReplicationCheckpoint indexReplicationCheckPoint = indexShard.getLatestReplicationCheckpoint();
            long segmentInfoVersion = indexReplicationCheckPoint.getSegmentInfosVersion();
            final ConcurrentNavigableMap<Long, ReplicationCheckpointStats> existingCheckpointStats = replicationCheckpointStats.get(
                indexShard.shardId()
            );

            if (existingCheckpointStats != null && !existingCheckpointStats.isEmpty()) {
                existingCheckpointStats.keySet().removeIf(key -> key < segmentInfoVersion);
                Map.Entry<Long, ReplicationCheckpointStats> lastEntry = existingCheckpointStats.lastEntry();
                if (lastEntry != null) {
                    lastEntry.getValue().setBytesBehind(calculateBytesBehind(latestCheckpoint, indexReplicationCheckPoint));
                }
            }
        }
    }

    private void calculateReplicationCheckpointStats(final ReplicationCheckpoint latestReceivedCheckPoint, final IndexShard indexShard) {
        ReplicationCheckpoint indexShardReplicationCheckpoint = indexShard.getLatestReplicationCheckpoint();
        if (indexShardReplicationCheckpoint != null) {
            long segmentInfosVersion = latestReceivedCheckPoint.getSegmentInfosVersion();
            long bytesBehind = calculateBytesBehind(latestReceivedCheckPoint, indexShardReplicationCheckpoint);
            if (bytesBehind > 0) {
                ConcurrentNavigableMap<Long, ReplicationCheckpointStats> existingCheckpointStats = replicationCheckpointStats.get(
                    indexShard.shardId()
                );
                if (existingCheckpointStats != null) {
                    existingCheckpointStats.computeIfAbsent(
                        segmentInfosVersion,
                        k -> new ReplicationCheckpointStats(bytesBehind, latestReceivedCheckPoint.getCreatedTimeStamp())
                    );
                }
            }
        }
    }

    private long calculateBytesBehind(final ReplicationCheckpoint latestCheckPoint, final ReplicationCheckpoint replicationCheckpoint) {
        Store.RecoveryDiff diff = Store.segmentReplicationDiff(latestCheckPoint.getMetadataMap(), replicationCheckpoint.getMetadataMap());

        return diff.missing.stream().mapToLong(StoreFileMetadata::length).sum();
    }

    public void initializeStats(ShardId shardId) {
        replicationCheckpointStats.computeIfAbsent(shardId, k -> new ConcurrentSkipListMap<>());
    }

    private static class ReplicationCheckpointStats {
        private long bytesBehind;
        private final long timestamp;

        public ReplicationCheckpointStats(long bytesBehind, long timestamp) {
            this.bytesBehind = bytesBehind;
            this.timestamp = timestamp;
        }

        public long getBytesBehind() {
            return bytesBehind;
        }

        public void setBytesBehind(long bytesBehind) {
            this.bytesBehind = bytesBehind;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    /**
     * Runnable implementation to trigger a replication event.
     */
    private class ReplicationRunner extends AbstractRunnable {

        final long replicationId;

        public ReplicationRunner(long replicationId) {
            this.replicationId = replicationId;
        }

        @Override
        public void onFailure(Exception e) {
            onGoingReplications.fail(replicationId, new ReplicationFailedException("Unexpected Error during replication", e), false);
        }

        @Override
        public void doRun() {
            start(replicationId);
        }
    }

    private void start(final long replicationId) {
        final SegmentReplicationTarget target;
        try (ReplicationCollection.ReplicationRef<SegmentReplicationTarget> replicationRef = onGoingReplications.get(replicationId)) {
            // This check is for handling edge cases where the reference is removed before the ReplicationRunner is started by the
            // threadpool.
            if (replicationRef == null) {
                return;
            }
            target = replicationRef.get();
        }
        target.startReplication(new ActionListener<>() {
            @Override
            public void onResponse(Void o) {
                logger.debug(() -> new ParameterizedMessage("Finished replicating {} marking as done.", target.description()));
                pruneCheckpointsUpToLastSync(target.indexShard());
                onGoingReplications.markAsDone(replicationId);
                if (target.state().getIndex().recoveredFileCount() != 0 && target.state().getIndex().recoveredBytes() != 0) {
                    completedReplications.put(target.shardId(), target.state());
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug("Replication failed {}", target.description());
                if (isStoreCorrupt(target) || e instanceof CorruptIndexException || e instanceof OpenSearchCorruptionException) {
                    onGoingReplications.fail(replicationId, new ReplicationFailedException("Store corruption during replication", e), true);
                    return;
                }
                onGoingReplications.fail(replicationId, new ReplicationFailedException("Segment Replication failed", e), false);
            }
        }, this::updateReplicationCheckpointStats);
    }

    // pkg-private for integration tests
    void startReplication(final SegmentReplicationTarget target, TimeValue timeout) {
        final long replicationId;
        try {
            replicationId = onGoingReplications.startSafe(target, timeout);
        } catch (ReplicationFailedException e) {
            // replication already running for shard.
            target.fail(e, false);
            return;
        }
        logger.trace(() -> new ParameterizedMessage("Added new replication to collection {}", target.description()));
        threadPool.generic().execute(new ReplicationRunner(replicationId));
    }

    private boolean isStoreCorrupt(SegmentReplicationTarget target) {
        // ensure target is not already closed. In that case
        // we can assume the store is not corrupt and that the replication
        // event completed successfully.
        if (target.refCount() > 0) {
            final Store store = target.store();
            if (store.tryIncRef()) {
                try {
                    return store.isMarkedCorrupted();
                } catch (IOException ex) {
                    logger.warn("Unable to determine if store is corrupt", ex);
                    return false;
                } finally {
                    store.decRef();
                }
            }
        }
        // store already closed.
        return false;
    }

    int size() {
        return onGoingReplications.size();
    }

    void cancel(ShardId shardId, String reason) {
        onGoingReplications.cancelForShard(shardId, reason);
        replicationCheckpointStats.remove(shardId);
        primaryCheckpoint.remove(shardId);
    }

    SegmentReplicationTarget get(ShardId shardId) {
        return onGoingReplications.getOngoingReplicationTarget(shardId);
    }

    ReplicationCheckpoint getPrimaryCheckpoint(ShardId shardId) {
        return primaryCheckpoint.getOrDefault(shardId, ReplicationCheckpoint.empty(shardId));
    }

    ReplicationCollection.ReplicationRef<SegmentReplicationTarget> get(long id) {
        return onGoingReplications.get(id);
    }

    SegmentReplicationState getCompleted(ShardId shardId) {
        return completedReplications.get(shardId);
    }

    ReplicationCollection.ReplicationRef<SegmentReplicationTarget> get(long id, ShardId shardId) {
        return onGoingReplications.getSafe(id, shardId);
    }
}
