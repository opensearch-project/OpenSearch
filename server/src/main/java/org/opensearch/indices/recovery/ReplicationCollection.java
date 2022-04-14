/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.common.concurrent.AutoCloseableRefCounted;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.index.shard.IndexShardClosedException;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.replication.common.ReplicationTarget;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class ReplicationCollection<T extends ReplicationTarget> {

    /** This is the single source of truth for ongoing recoveries. If it's not here, it was canceled or done */
    private final ConcurrentMap<Long, T> onGoingReplications = ConcurrentCollections.newConcurrentMap();

    protected final Logger logger;
    private final ThreadPool threadPool;

    public ReplicationCollection(Logger logger, ThreadPool threadPool) {
        this.logger = logger;
        this.threadPool = threadPool;
    }

    public long startReplication(T target, TimeValue activityTimeout) {
        startRecoveryInternal(target, activityTimeout);
        return target.getId();
    }

    public Map<Long, T> getOngoingReplications() {
        return onGoingReplications;
    }

    protected void startRecoveryInternal(T replicationTarget, TimeValue activityTimeout) {
        ReplicationTarget existingTarget = onGoingReplications.putIfAbsent(replicationTarget.getId(), replicationTarget);
        assert existingTarget == null : "found two RecoveryStatus instances with the same id";
        logger.trace(
            "{} started recovery from {}, id [{}]",
            replicationTarget.indexShard().shardId(),
            replicationTarget.sourceNode(),
            replicationTarget.getId()
        );
        threadPool.schedule(
            new ReplicationMonitor(replicationTarget.getId(), replicationTarget.lastAccessTime(), activityTimeout),
            activityTimeout,
            ThreadPool.Names.GENERIC
        );
    }

    public T getReplicationTarget(long id) {
        return onGoingReplications.get(id);
    }

    /**
     * gets the {@link RecoveryTarget } for a given id. The RecoveryStatus returned has it's ref count already incremented
     * to make sure it's safe to use. However, you must call {@link RecoveryTarget#decRef()} when you are done with it, typically
     * by using this method in a try-with-resources clause.
     * <p>
     * Returns null if recovery is not found
     */
    public ReplicationRef<T> getReplication(long id) {
        T status = onGoingReplications.get(id);
        if (status != null && status.tryIncRef()) {
            return new ReplicationRef<>(status);
        }
        return null;
    }

    /** Similar to {@link #getReplicationTarget(long)} but throws an exception if no recovery is found */
    public ReplicationRef<T> getReplicationSafe(long id, ShardId shardId) {
        ReplicationRef<T> recoveryRef = getReplication(id);
        if (recoveryRef == null) {
            throw new IndexShardClosedException(shardId);
        }
        T target = recoveryRef.get();
        assert target.indexShard().shardId().equals(shardId);
        return recoveryRef;
    }

    /** cancel the recovery with the given id (if found) and remove it from the recovery collection */
    public boolean cancelRecovery(long id, String reason) {
        T removed = onGoingReplications.remove(id);
        boolean cancelled = false;
        if (removed != null) {
            logger.trace(
                "{} canceled recovery from {}, id [{}] (reason [{}])",
                removed.indexShard().shardId(),
                removed.sourceNode(),
                removed.getId(),
                reason
            );
            removed.cancel(reason);
            cancelled = true;
        }
        return cancelled;
    }

    /**
     * fail the recovery with the given id (if found) and remove it from the recovery collection
     *
     * @param id               id of the recovery to fail
     * @param e                exception with reason for the failure
     * @param sendShardFailure true a shard failed message should be sent to the master
     */
    public void failReplication(long id, OpenSearchException e, boolean sendShardFailure) {
        T removed = onGoingReplications.remove(id);
        if (removed != null) {
            logger.trace(
                "{} failing recovery from {}, id [{}]. Send shard failure: [{}]",
                removed.indexShard().shardId(),
                removed.sourceNode(),
                removed.getId(),
                sendShardFailure
            );
            removed.fail(e, sendShardFailure);
        }
    }

    /** mark the recovery with the given id as done (if found) */
    public void markReplicationAsDone(long id) {
        T removed = onGoingReplications.remove(id);
        if (removed != null) {
            logger.trace(
                "{} marking recovery from {} as done, id [{}]",
                removed.indexShard().shardId(),
                removed.sourceNode(),
                removed.getId()
            );
            removed.markAsDone();
        }
    }

    /** the number of ongoing recoveries */
    public int size() {
        return onGoingReplications.size();
    }

    /**
     * cancel all ongoing recoveries for the given shard
     *
     * @param reason       reason for cancellation
     * @param shardId      shardId for which to cancel recoveries
     * @return true if a recovery was cancelled
     */
    public boolean cancelRecoveriesForShard(ShardId shardId, String reason) {
        boolean cancelled = false;
        List<T> matchedRecoveries = new ArrayList<>();
        synchronized (onGoingReplications) {
            for (Iterator<T> it = onGoingReplications.values().iterator(); it.hasNext();) {
                T status = it.next();
                if (status.indexShard().shardId().equals(shardId)) {
                    matchedRecoveries.add(status);
                    it.remove();
                }
            }
        }
        for (T removed : matchedRecoveries) {
            logger.trace(
                "{} canceled recovery from {}, id [{}] (reason [{}])",
                removed.indexShard().shardId(),
                removed.sourceNode(),
                removed.getId(),
                reason
            );
            removed.cancel(reason);
            cancelled = true;
        }
        return cancelled;
    }

    /**
     * a reference to {@link ReplicationTarget}, which implements {@link AutoCloseable}. closing the reference
     * causes {@link RecoveryTarget#decRef()} to be called. This makes sure that the underlying resources
     * will not be freed until {@link ReplicationRef#close()} is called.
     */
    public static class ReplicationRef<T extends ReplicationTarget> extends AutoCloseableRefCounted<T> {

        public ReplicationRef(T target) {
            super(target);
            target.setLastAccessTime();
        }
    }

    private class ReplicationMonitor extends AbstractRunnable {
        private final long recoveryId;
        private final TimeValue checkInterval;

        private volatile long lastSeenAccessTime;

        private ReplicationMonitor(long id, long lastSeenAccessTime, TimeValue checkInterval) {
            this.recoveryId = id;
            this.checkInterval = checkInterval;
            this.lastSeenAccessTime = lastSeenAccessTime;
        }

        @Override
        public void onFailure(Exception e) {
            logger.error(() -> new ParameterizedMessage("unexpected error while monitoring recovery [{}]", recoveryId), e);
        }

        @Override
        protected void doRun() throws Exception {
            T target = onGoingReplications.get(recoveryId);
            if (target == null) {
                logger.trace("[monitor] no target found for [{}], shutting down", recoveryId);
                return;
            }
            long accessTime = target.lastAccessTime();
            if (accessTime == lastSeenAccessTime) {
                String message = "no activity after [" + checkInterval + "]";
                failReplication(
                    recoveryId,
                    new OpenSearchTimeoutException(message),
                    true // to be safe, we don't know what go stuck
                );
                return;
            }
            lastSeenAccessTime = accessTime;
            logger.trace("[monitor] rescheduling check for [{}]. last access time is [{}]", recoveryId, lastSeenAccessTime);
            threadPool.schedule(this, checkInterval, ThreadPool.Names.GENERIC);
        }
    }
}
