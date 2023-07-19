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

package org.opensearch.indices.replication.common;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.concurrent.AutoCloseableRefCounted;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.AbstractRunnable;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardClosedException;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * This class holds a collection of all on going replication events on the current node (i.e., the node is the target node
 * of those events). The class is used to guarantee concurrent semantics such that once an event was done/cancelled/failed
 * no other thread will be able to find it. Last, the {@link ReplicationRef} inner class verifies that temporary files
 * and store will only be cleared once on going usage is finished.
 *
 * @opensearch.internal
 */
public class ReplicationCollection<T extends ReplicationTarget> {

    /** This is the single source of truth for ongoing target events. If it's not here, it was canceled or done */
    private final ConcurrentMap<Long, T> onGoingTargetEvents = ConcurrentCollections.newConcurrentMap();

    private final Logger logger;
    private final ThreadPool threadPool;

    public ReplicationCollection(Logger logger, ThreadPool threadPool) {
        this.logger = logger;
        this.threadPool = threadPool;
    }

    /**
     * Starts a new target event for a given shard, fails the given target if this shard is already replicating.
     * @param target ReplicationTarget to start
     * @param activityTimeout timeout for entire replication event
     * @return The replication id
     */
    public long startSafe(T target, TimeValue activityTimeout) {
        synchronized (onGoingTargetEvents) {
            final boolean isPresent = onGoingTargetEvents.values()
                .stream()
                .map(ReplicationTarget::shardId)
                .anyMatch(t -> t.equals(target.shardId()));
            if (isPresent) {
                throw new ReplicationFailedException("Shard " + target.shardId() + " is already replicating");
            } else {
                return start(target, activityTimeout);
            }
        }
    }

    /**
     * Starts a new target event for the given shard, source node and state
     *
     * @return the id of the new target event.
     */
    public long start(T target, TimeValue activityTimeout) {
        startInternal(target, activityTimeout);
        return target.getId();
    }

    private void startInternal(T target, TimeValue activityTimeout) {
        T existingTarget = onGoingTargetEvents.putIfAbsent(target.getId(), target);
        assert existingTarget == null : "found two Target instances with the same id";
        logger.trace("started {}", target.description());
        threadPool.schedule(
            new ReplicationMonitor(target.getId(), target.lastAccessTime(), activityTimeout),
            activityTimeout,
            ThreadPool.Names.GENERIC
        );
    }

    /**
     * Resets the target event and performs a restart on the current index shard
     *
     * @see IndexShard#performRecoveryRestart()
     * @return newly created Target
     */
    @SuppressWarnings(value = "unchecked")
    public T reset(final long id, final TimeValue activityTimeout) {
        T oldTarget = null;
        final T newTarget;

        try {
            synchronized (onGoingTargetEvents) {
                // swap targets in a synchronized block to ensure that the newly added target is picked up by
                // cancelForShard whenever the old target is picked up
                oldTarget = onGoingTargetEvents.remove(id);
                if (oldTarget == null) {
                    return null;
                }

                newTarget = (T) oldTarget.retryCopy();
                startInternal(newTarget, activityTimeout);
            }

            // Closes the current target
            boolean successfulReset = oldTarget.reset(newTarget.cancellableThreads());
            if (successfulReset) {
                logger.trace("restarted {}, previous id [{}]", newTarget.description(), oldTarget.getId());
                return newTarget;
            } else {
                logger.trace(
                    "{} could not be reset as it is already cancelled, previous id [{}]",
                    newTarget.description(),
                    oldTarget.getId()
                );
                cancel(newTarget.getId(), "cancelled during reset");
                return null;
            }
        } catch (Exception e) {
            // fail shard to be safe
            assert oldTarget != null;
            oldTarget.notifyListener(new ReplicationFailedException("Unable to reset target", e), true);
            return null;
        }
    }

    public T getTarget(long id) {
        return onGoingTargetEvents.get(id);
    }

    /**
     * gets the {@link ReplicationTarget } for a given id. The ShardTarget returned has it's ref count already incremented
     * to make sure it's safe to use. However, you must call {@link ReplicationTarget#decRef()} when you are done with it, typically
     * by using this method in a try-with-resources clause.
     * <p>
     * Returns null if target event is not found
     */
    public ReplicationRef<T> get(long id) {
        T status = onGoingTargetEvents.get(id);
        if (status != null && status.tryIncRef()) {
            return new ReplicationRef<T>(status);
        }
        return null;
    }

    /** Similar to {@link #get(long)} but throws an exception if no target is found */
    public ReplicationRef<T> getSafe(long id, ShardId shardId) {
        ReplicationRef<T> ref = get(id);
        if (ref == null) {
            throw new IndexShardClosedException(shardId);
        }
        assert ref.get().indexShard().shardId().equals(shardId);
        return ref;
    }

    /** cancel the target with the given id (if found) and remove it from the target collection */
    public boolean cancel(long id, String reason) {
        T removed = onGoingTargetEvents.remove(id);
        boolean cancelled = false;
        if (removed != null) {
            logger.trace("canceled {} (reason [{}])", removed.description(), reason);
            removed.cancel(reason);
            cancelled = true;
        }
        return cancelled;
    }

    /**
     * fail the target with the given id (if found) and remove it from the target collection
     *
     * @param id               id of the target to fail
     * @param e                exception with reason for the failure
     * @param sendShardFailure true a shard failed message should be sent to the master
     */
    public void fail(long id, ReplicationFailedException e, boolean sendShardFailure) {
        T removed = onGoingTargetEvents.remove(id);
        if (removed != null) {
            logger.trace("failing {}. Send shard failure: [{}]", removed.description(), sendShardFailure);
            removed.fail(e, sendShardFailure);
        }
    }

    /** mark the target with the given id as done (if found) */
    public void markAsDone(long id) {
        T removed = onGoingTargetEvents.remove(id);
        if (removed != null) {
            logger.trace("Marking {} as done", removed.description());
            removed.markAsDone();
        }
    }

    /** the number of ongoing target events */
    public int size() {
        return onGoingTargetEvents.size();
    }

    /**
     * cancel all ongoing targets for the given shard
     *
     * @param reason       reason for cancellation
     * @param shardId      shardId for which to cancel targets
     * @return true if a target was cancelled
     */
    public boolean cancelForShard(ShardId shardId, String reason) {
        boolean cancelled = false;
        List<T> matchedTargets = new ArrayList<>();
        synchronized (onGoingTargetEvents) {
            for (Iterator<T> it = onGoingTargetEvents.values().iterator(); it.hasNext();) {
                T status = it.next();
                if (status.indexShard().shardId().equals(shardId)) {
                    matchedTargets.add(status);
                    it.remove();
                }
            }
        }
        for (T removed : matchedTargets) {
            logger.trace("canceled {} (reason [{}])", removed.description(), reason);
            removed.cancel(reason);
            cancelled = true;
        }
        return cancelled;
    }

    /**
     * Trigger cancel on the target but do not remove it from the collection.
     * This is intended to be called to ensure replication events are removed from the collection
     * only when the target has closed.
     *
     * @param shardId {@link ShardId} shard events to cancel
     * @param reason {@link String} reason for cancellation
     */
    public void requestCancel(ShardId shardId, String reason) {
        for (T value : onGoingTargetEvents.values()) {
            if (value.shardId().equals(shardId)) {
                value.cancel(reason);
            }
        }
    }

    /**
     * Get target for shard
     *
     * @param shardId      shardId
     * @return ReplicationTarget for input shardId
     */
    public T getOngoingReplicationTarget(ShardId shardId) {
        final List<T> replicationTargetList = onGoingTargetEvents.values()
            .stream()
            .filter(t -> t.indexShard.shardId().equals(shardId))
            .collect(Collectors.toList());
        assert replicationTargetList.size() <= 1 : "More than one on-going replication targets";
        return replicationTargetList.size() > 0 ? replicationTargetList.get(0) : null;
    }

    /**
     * a reference to {@link ReplicationTarget}, which implements {@link AutoCloseable}. closing the reference
     * causes {@link ReplicationTarget#decRef()} to be called. This makes sure that the underlying resources
     * will not be freed until {@link ReplicationRef#close()} is called.
     *
     * @opensearch.internal
     */
    public static class ReplicationRef<T extends ReplicationTarget> extends AutoCloseableRefCounted<T> {

        /**
         * Important: {@link ReplicationTarget#tryIncRef()} should
         * be *successfully* called on status before
         */
        public ReplicationRef(T status) {
            super(status);
            status.setLastAccessTime();
        }
    }

    private class ReplicationMonitor extends AbstractRunnable {
        private final long id;
        private final TimeValue checkInterval;

        private volatile long lastSeenAccessTime;

        private ReplicationMonitor(long id, long lastSeenAccessTime, TimeValue checkInterval) {
            this.id = id;
            this.checkInterval = checkInterval;
            this.lastSeenAccessTime = lastSeenAccessTime;
        }

        @Override
        public void onFailure(Exception e) {
            logger.error(() -> new ParameterizedMessage("unexpected error while monitoring [{}]", id), e);
        }

        @Override
        protected void doRun() throws Exception {
            T status = onGoingTargetEvents.get(id);
            if (status == null) {
                logger.trace("[monitor] no status found for [{}], shutting down", id);
                return;
            }
            long accessTime = status.lastAccessTime();
            if (accessTime == lastSeenAccessTime) {
                String message = "no activity after [" + checkInterval + "]";
                fail(
                    id,
                    new ReplicationFailedException(message),
                    true // to be safe, we don't know what go stuck
                );
                return;
            }
            lastSeenAccessTime = accessTime;
            logger.trace("[monitor] rescheduling check for [{}]. last access time is [{}]", id, lastSeenAccessTime);
            threadPool.schedule(this, checkInterval, ThreadPool.Names.GENERIC);
        }
    }

}
