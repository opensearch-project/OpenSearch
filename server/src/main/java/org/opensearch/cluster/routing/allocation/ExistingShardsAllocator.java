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

package org.opensearch.cluster.routing.allocation;

import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.RoutingChangesObserver;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.gateway.GatewayAllocator;
import org.opensearch.gateway.ShardsBatchGatewayAllocator;

import java.util.ArrayList;
import java.util.List;

/**
 * Searches for, and allocates, shards for which there is an existing on-disk copy somewhere in the cluster. The default implementation is
 * {@link GatewayAllocator} and {@link ShardsBatchGatewayAllocator}, but plugins can supply their own implementations too.
 *
 * @opensearch.internal
 */
public interface ExistingShardsAllocator {

    /**
     * Allows plugins to override how we allocate shards that may already exist on disk in the cluster.
     */
    Setting<String> EXISTING_SHARDS_ALLOCATOR_SETTING = Setting.simpleString(
        "index.allocation.existing_shards_allocator",
        GatewayAllocator.ALLOCATOR_NAME,
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex
    );

    /**
     * Boolean setting to enable/disable batch allocation of unassigned shards already existing on disk.
     * This will allow sending all Unassigned Shards to the ExistingShard Allocator to  make decision to allocate
     * in one or more go.
     *
     * Enable this setting if your ExistingShardAllocator is implementing the
     * {@link ExistingShardsAllocator#allocateAllUnassignedShards(RoutingAllocation, boolean)} method.
     * The default implementation of this method is not optimized and assigns shards one by one.
     *
     * If no plugin overrides {@link ExistingShardsAllocator} then default implementation will be use for it , i.e,
     * {@link ShardsBatchGatewayAllocator}.
     *
     * This setting is experimental at this point.
     */
    Setting<Boolean> EXISTING_SHARDS_ALLOCATOR_BATCH_MODE = Setting.boolSetting(
        "cluster.allocator.existing_shards_allocator.batch_enabled",
        false,
        Setting.Property.NodeScope
    );

    /**
     * Called before starting a round of allocation, allowing the allocator to invalidate some caches if appropriate.
     */
    void beforeAllocation(RoutingAllocation allocation);

    /**
     * Called during a round of allocation after attempting to allocate all the primaries but before any replicas, allowing the allocator
     * to prepare for replica allocation.
     */
    void afterPrimariesBeforeReplicas(RoutingAllocation allocation);

    /**
     * Allocate any unassigned shards in the given {@link RoutingAllocation} for which this {@link ExistingShardsAllocator} is responsible.
     */
    void allocateUnassigned(
        ShardRouting shardRouting,
        RoutingAllocation allocation,
        UnassignedAllocationHandler unassignedAllocationHandler
    );

    default TimeValue getAllocatorTimeout() {
        return null;
    }

    default void removeAndIgnorePendingUnassignedShards(RoutingAllocation allocation) {
    }

    /**
     * Allocate all unassigned shards in the given {@link RoutingAllocation} for which this {@link ExistingShardsAllocator} is responsible.
     * Default implementation calls {@link #allocateUnassigned(ShardRouting, RoutingAllocation, UnassignedAllocationHandler)} for each Unassigned shard
     * and is kept here for backward compatibility.
     *
     * Allocation service will currently run the default implementation of it implemented by {@link ShardsBatchGatewayAllocator}
     */
    default List<Runnable> allocateAllUnassignedShards(RoutingAllocation allocation, boolean primary) {
        RoutingNodes.UnassignedShards.UnassignedIterator iterator = allocation.routingNodes().unassigned().iterator();
        List<Runnable> runnables = new ArrayList<>();
        while (iterator.hasNext()) {
            ShardRouting shardRouting = iterator.next();
            if (shardRouting.primary() == primary) {
                runnables.add(() -> allocateUnassigned(shardRouting, allocation, iterator));
            }
        }
        return runnables;
    }

    /**
     * Returns an explanation for a single unassigned shard.
     */
    AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting unassignedShard, RoutingAllocation routingAllocation);

    /**
     * Called when this node becomes the elected cluster-manager and when it stops being the elected cluster-manager, so that implementations can clean up any
     * in-flight activity from an earlier mastership.
     */
    void cleanCaches();

    /**
     * Called when the given shards have started, so that implementations can invalidate caches and clean up any in-flight activity for
     * those shards.
     */
    void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation);

    /**
     * Called when the given shards have failed, so that implementations can invalidate caches and clean up any in-flight activity for
     * those shards.
     */
    void applyFailedShards(List<FailedShard> failedShards, RoutingAllocation allocation);

    /**
     * @return the number of in-flight fetches under this allocator's control.
     */
    int getNumberOfInFlightFetches();

    /**
     * Used by {@link ExistingShardsAllocator#allocateUnassigned} to handle its allocation decisions. A restricted interface to
     * {@link RoutingNodes.UnassignedShards.UnassignedIterator} to limit what allocators can do.
     */
    interface UnassignedAllocationHandler {

        /**
         * Initializes the current unassigned shard and moves it from the unassigned list.
         *
         * @param existingAllocationId allocation id to use. If null, a fresh allocation id is generated.
         */
        ShardRouting initialize(
            String nodeId,
            @Nullable String existingAllocationId,
            long expectedShardSize,
            RoutingChangesObserver routingChangesObserver
        );

        /**
         * Removes and ignores the unassigned shard (will be ignored for this run, but
         * will be added back to unassigned once the metadata is constructed again).
         * Typically this is used when an allocation decision prevents a shard from being allocated such
         * that subsequent consumers of this API won't try to allocate this shard again.
         *
         * @param attempt the result of the allocation attempt
         */
        void removeAndIgnore(UnassignedInfo.AllocationStatus attempt, RoutingChangesObserver changes);

        /**
         * updates the unassigned info and recovery source on the current unassigned shard
         *
         * @param  unassignedInfo the new unassigned info to use
         * @param  recoverySource the new recovery source to use
         * @return the shard with unassigned info updated
         */
        ShardRouting updateUnassigned(UnassignedInfo unassignedInfo, RecoverySource recoverySource, RoutingChangesObserver changes);
    }
}
