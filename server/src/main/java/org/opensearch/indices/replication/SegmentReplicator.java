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
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationCollection;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationListener;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Map;

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
    private final ThreadPool threadPool;

    public SegmentReplicator(ThreadPool threadPool) {
        this.onGoingReplications = new ReplicationCollection<>(logger, threadPool);
        this.threadPool = threadPool;
    }

    // TODO: Add public entrypoint for replication on an interval to be invoked via IndexService

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
        });
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
    }

    SegmentReplicationTarget get(ShardId shardId) {
        return onGoingReplications.getOngoingReplicationTarget(shardId);
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
