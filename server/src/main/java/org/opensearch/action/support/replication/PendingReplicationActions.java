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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.support.replication;

import org.opensearch.action.support.RetryableAction;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.lease.Releasable;
import org.opensearch.index.shard.PrimaryShardClosedException;
import org.opensearch.index.shard.IndexShardClosedException;
import org.opensearch.index.shard.ReplicationGroup;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Pending Replication Actions
 *
 * @opensearch.internal
 */
public class PendingReplicationActions implements Consumer<ReplicationGroup>, Releasable {

    private final Map<String, Set<RetryableAction<?>>> onGoingReplicationActions = ConcurrentCollections.newConcurrentMap();
    private final ShardId shardId;
    private final ThreadPool threadPool;
    private volatile long replicationGroupVersion = -1;

    public PendingReplicationActions(ShardId shardId, ThreadPool threadPool) {
        this.shardId = shardId;
        this.threadPool = threadPool;
    }

    public void addPendingAction(String allocationId, RetryableAction<?> replicationAction) {
        Set<RetryableAction<?>> ongoingActionsOnNode = onGoingReplicationActions.get(allocationId);
        if (ongoingActionsOnNode != null) {
            ongoingActionsOnNode.add(replicationAction);
            if (onGoingReplicationActions.containsKey(allocationId) == false) {
                replicationAction.cancel(
                    new IndexShardClosedException(
                        shardId,
                        "Replica unavailable - replica could have left ReplicationGroup or IndexShard might have closed"
                    )
                );
            }
        } else {
            replicationAction.cancel(
                new IndexShardClosedException(
                    shardId,
                    "Replica unavailable - replica could have left ReplicationGroup or IndexShard might have closed"
                )
            );
        }
    }

    public void removeReplicationAction(String allocationId, RetryableAction<?> action) {
        Set<RetryableAction<?>> ongoingActionsOnNode = onGoingReplicationActions.get(allocationId);
        if (ongoingActionsOnNode != null) {
            ongoingActionsOnNode.remove(action);
        }
    }

    @Override
    public void accept(ReplicationGroup replicationGroup) {
        if (isNewerVersion(replicationGroup)) {
            synchronized (this) {
                if (isNewerVersion(replicationGroup)) {
                    acceptNewTrackedAllocationIds(replicationGroup.getTrackedAllocationIds());
                    replicationGroupVersion = replicationGroup.getVersion();
                }
            }
        }
    }

    private boolean isNewerVersion(ReplicationGroup replicationGroup) {
        // Relative comparison to mitigate long overflow
        return replicationGroup.getVersion() - replicationGroupVersion > 0;
    }

    // Visible for testing
    synchronized void acceptNewTrackedAllocationIds(Set<String> trackedAllocationIds) {
        for (String targetAllocationId : trackedAllocationIds) {
            onGoingReplicationActions.putIfAbsent(targetAllocationId, ConcurrentCollections.newConcurrentSet());
        }
        ArrayList<Set<RetryableAction<?>>> toCancel = new ArrayList<>();
        for (String allocationId : onGoingReplicationActions.keySet()) {
            if (trackedAllocationIds.contains(allocationId) == false) {
                toCancel.add(onGoingReplicationActions.remove(allocationId));
            }
        }

        cancelActions(toCancel, () -> new IndexShardClosedException(shardId, "Replica left ReplicationGroup"));
    }

    @Override
    public synchronized void close() {
        ArrayList<Set<RetryableAction<?>>> toCancel = new ArrayList<>(onGoingReplicationActions.values());
        onGoingReplicationActions.clear();

        cancelActions(toCancel, () -> new PrimaryShardClosedException(shardId));
    }

    private void cancelActions(ArrayList<Set<RetryableAction<?>>> toCancel, Supplier<IndexShardClosedException> exceptionSupplier) {
        threadPool.executor(ThreadPool.Names.GENERIC)
            .execute(() -> toCancel.stream().flatMap(Collection::stream).forEach(action -> action.cancel(exceptionSupplier.get())));
    }
}
