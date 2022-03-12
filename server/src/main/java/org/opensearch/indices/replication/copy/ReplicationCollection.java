/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices.replication.copy;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.common.concurrent.AutoCloseableRefCounted;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardClosedException;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.replication.SegmentReplicationReplicaService;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * This class holds a collection of all on going recoveries on the current node (i.e., the node is the target node
 * of those recoveries). The class is used to guarantee concurrent semantics such that once a recoveries was done/cancelled/failed
 * no other thread will be able to find it. Last, the {@link ReplicationRef} inner class verifies that replication temporary files
 * and store will only be cleared once on going usage is finished.
 */
public class ReplicationCollection {

    /**
     * This is the single source of truth for ongoing recoveries. If it's not here, it was canceled or done
     */
    private final ConcurrentMap<Long, SegmentReplicationTarget> onGoingReplications = ConcurrentCollections.newConcurrentMap();

    private final Logger logger;
    private final ThreadPool threadPool;

    public ReplicationCollection(Logger logger, ThreadPool threadPool) {
        this.logger = logger;
        this.threadPool = threadPool;
    }

    public long startReplication(
        ReplicationCheckpoint checkpoint,
        IndexShard indexShard,
        PrimaryShardReplicationSource source,
        SegmentReplicationReplicaService.ReplicationListener listener,
        TimeValue activityTimeout
    ) {
        SegmentReplicationTarget replicationTarget = new SegmentReplicationTarget(checkpoint, indexShard, source, listener);
        startReplicationInternal(replicationTarget, activityTimeout);
        return replicationTarget.getId();
    }

    private void startReplicationInternal(SegmentReplicationTarget replicationTarget, TimeValue activityTimeout) {
        SegmentReplicationTarget existingTarget = onGoingReplications.putIfAbsent(replicationTarget.getId(), replicationTarget);
        assert existingTarget == null : "found two ReplicationStatus instances with the same id";
        logger.trace("{} started segment replication id [{}]", replicationTarget.indexShard().shardId(), replicationTarget.getId());
        threadPool.schedule(
            new ReplicationMonitor(replicationTarget.getId(), replicationTarget.lastAccessTime(), activityTimeout),
            activityTimeout,
            ThreadPool.Names.GENERIC
        );
    }

    public SegmentReplicationTarget getReplicationTarget(long id) {
        return onGoingReplications.get(id);
    }

    /**
     * gets the {@link SegmentReplicationTarget } for a given id. The ReplicationStatus returned has it's ref count already incremented
     * to make sure it's safe to use. However, you must call {@link SegmentReplicationTarget#decRef()} when you are done with it, typically
     * by using this method in a try-with-resources clause.
     * <p>
     * Returns null if replication is not found
     */
    public ReplicationRef getReplication(long id) {
        SegmentReplicationTarget status = onGoingReplications.get(id);
        if (status != null && status.tryIncRef()) {
            return new ReplicationRef(status);
        }
        return null;
    }

    /**
     * Similar to {@link #getReplication(long)} but throws an exception if no replication is found
     */
    public ReplicationRef getReplicationSafe(long id, ShardId shardId) {
        ReplicationRef replicationRef = getReplication(id);
        if (replicationRef == null) {
            throw new IndexShardClosedException(shardId);
        }
        SegmentReplicationTarget replicationTarget = replicationRef.get(SegmentReplicationTarget.class);
        assert replicationTarget.indexShard().shardId().equals(shardId);
        return replicationRef;
    }

    /** cancel the replication with the given id (if found) and remove it from the replication collection */
    public boolean cancelReplication(long id, String reason) {
        SegmentReplicationTarget removed = onGoingReplications.remove(id);
        boolean cancelled = false;
        if (removed != null) {
            logger.trace("{} canceled replication, id [{}] (reason [{}])", removed.indexShard().shardId(), removed.getId(), reason);
            removed.cancel(reason);
            cancelled = true;
        }
        return cancelled;
    }

    /**
     * fail the replication with the given id (if found) and remove it from the replication collection
     *
     * @param id               id of the replication to fail
     * @param e                exception with reason for the failure
     * @param sendShardFailure true a shard failed message should be sent to the master
     */
    public void failReplication(long id, ReplicationFailedException e, boolean sendShardFailure) {
        SegmentReplicationTarget removed = onGoingReplications.remove(id);
        if (removed != null) {
            logger.trace(
                "{} failing segment replication  id [{}]. Send shard failure: [{}]",
                removed.indexShard().shardId(),
                removed.getId(),
                sendShardFailure
            );
            removed.fail(e, sendShardFailure);
        }
    }

    /**
     * mark the replication with the given id as done (if found)
     */
    public void markReplicationAsDone(long id) {
        SegmentReplicationTarget removed = onGoingReplications.remove(id);
        if (removed != null) {
            logger.trace("{} marking replication as done, id [{}]", removed.indexShard().shardId(), removed.getId());
            removed.markAsDone();
        }
    }

    /**
     * the number of ongoing recoveries
     */
    public int size() {
        return onGoingReplications.size();
    }

    /**
     * cancel all ongoing recoveries for the given shard
     *
     * @param reason  reason for cancellation
     * @param shardId shardId for which to cancel recoveries
     * @return true if a replication was cancelled
     */
    public boolean cancelRecoveriesForShard(ShardId shardId, String reason) {
        boolean cancelled = false;
        List<SegmentReplicationTarget> matchedRecoveries = new ArrayList<>();
        synchronized (onGoingReplications) {
            for (Iterator<SegmentReplicationTarget> it = onGoingReplications.values().iterator(); it.hasNext();) {
                SegmentReplicationTarget status = it.next();
                if (status.indexShard().shardId().equals(shardId)) {
                    matchedRecoveries.add(status);
                    it.remove();
                }
            }
        }
        for (SegmentReplicationTarget removed : matchedRecoveries) {
            logger.trace("{} canceled segment replication id [{}] (reason [{}])", removed.indexShard().shardId(), removed.getId(), reason);
            removed.cancel(reason);
            cancelled = true;
        }
        return cancelled;
    }

    /**
     * a reference to {@link SegmentReplicationTarget}, which implements {@link AutoCloseable}. closing the reference
     * causes {@link SegmentReplicationTarget#decRef()} to be called. This makes sure that the underlying resources
     * will not be freed until {@link ReplicationRef#close()} is called.
     */
    public static class ReplicationRef extends AutoCloseableRefCounted {

        /**
         * Important: {@link SegmentReplicationTarget#tryIncRef()} should
         * be *successfully* called on target before
         */
        public ReplicationRef(SegmentReplicationTarget target) {
            super(target);
            target.setLastAccessTime();
        }
    }

    private class ReplicationMonitor extends AbstractRunnable {
        private final long replicationId;
        private final TimeValue checkInterval;

        private volatile long lastSeenAccessTime;

        private ReplicationMonitor(long replicationId, long lastSeenAccessTime, TimeValue checkInterval) {
            this.replicationId = replicationId;
            this.checkInterval = checkInterval;
            this.lastSeenAccessTime = lastSeenAccessTime;
        }

        @Override
        public void onFailure(Exception e) {
            logger.error(() -> new ParameterizedMessage("unexpected error while monitoring replication [{}]", replicationId), e);
        }

        @Override
        protected void doRun() throws Exception {
            SegmentReplicationTarget replicationTarget = onGoingReplications.get(replicationId);
            if (replicationTarget == null) {
                logger.trace("[monitor] no replicationTarget found for [{}], shutting down", replicationId);
                return;
            }
            long accessTime = replicationTarget.lastAccessTime();
            if (accessTime == lastSeenAccessTime) {
                String message = "no activity after [" + checkInterval + "]";
                failReplication(
                    replicationId,
                    new ReplicationFailedException(replicationTarget.indexShard(), message, new OpenSearchTimeoutException(message)),
                    true // to be safe, we don't know what go stuck
                );
                return;
            }
            lastSeenAccessTime = accessTime;
            logger.trace("[monitor] rescheduling check for [{}]. last access time is [{}]", replicationId, lastSeenAccessTime);
            threadPool.schedule(this, checkInterval, ThreadPool.Names.GENERIC);
        }
    }

}
