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

package org.opensearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.action.support.nodes.BaseNodesResponse;
import org.opensearch.cluster.ClusterManagerMetrics;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.RerouteService;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.opensearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.opensearch.cluster.routing.allocation.FailedShard;
import org.opensearch.cluster.routing.allocation.RoutingAllocation;
import org.opensearch.common.Priority;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.store.TransportNodesListShardStoreMetadata;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Allocator for the gateway
 *
 * @opensearch.internal
 */
public class GatewayAllocator implements ExistingShardsAllocator {

    public static final String ALLOCATOR_NAME = "gateway_allocator";

    private static final Logger logger = LogManager.getLogger(GatewayAllocator.class);

    private final RerouteService rerouteService;

    private final PrimaryShardAllocator primaryShardAllocator;
    private final ReplicaShardAllocator replicaShardAllocator;

    private final ConcurrentMap<
        ShardId,
        AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards>> asyncFetchStarted = ConcurrentCollections
            .newConcurrentMap();
    private final ConcurrentMap<ShardId, AsyncShardFetch<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata>> asyncFetchStore =
        ConcurrentCollections.newConcurrentMap();
    private Set<String> lastSeenEphemeralIds = Collections.emptySet();

    @Inject
    public GatewayAllocator(
        RerouteService rerouteService,
        TransportNodesListGatewayStartedShards startedAction,
        TransportNodesListShardStoreMetadata storeAction,
        ClusterManagerMetrics clusterManagerMetrics
    ) {
        this.rerouteService = rerouteService;
        this.primaryShardAllocator = new InternalPrimaryShardAllocator(startedAction, clusterManagerMetrics);
        this.replicaShardAllocator = new InternalReplicaShardAllocator(storeAction, clusterManagerMetrics);
    }

    @Override
    public void cleanCaches() {
        Releasables.close(asyncFetchStarted.values());
        asyncFetchStarted.clear();
        Releasables.close(asyncFetchStore.values());
        asyncFetchStore.clear();
    }

    // for tests
    protected GatewayAllocator() {
        this.rerouteService = null;
        this.primaryShardAllocator = null;
        this.replicaShardAllocator = null;
    }

    @Override
    public int getNumberOfInFlightFetches() {
        int count = 0;
        for (AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetch : asyncFetchStarted.values()) {
            count += fetch.getNumberOfInFlightFetches();
        }
        for (AsyncShardFetch<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetch : asyncFetchStore.values()) {
            count += fetch.getNumberOfInFlightFetches();
        }
        return count;
    }

    @Override
    public void applyStartedShards(final List<ShardRouting> startedShards, final RoutingAllocation allocation) {
        for (ShardRouting startedShard : startedShards) {
            Releasables.close(asyncFetchStarted.remove(startedShard.shardId()));
            Releasables.close(asyncFetchStore.remove(startedShard.shardId()));
        }
    }

    @Override
    public void applyFailedShards(final List<FailedShard> failedShards, final RoutingAllocation allocation) {
        for (FailedShard failedShard : failedShards) {
            Releasables.close(asyncFetchStarted.remove(failedShard.getRoutingEntry().shardId()));
            Releasables.close(asyncFetchStore.remove(failedShard.getRoutingEntry().shardId()));
        }
    }

    @Override
    public void beforeAllocation(final RoutingAllocation allocation) {
        assert primaryShardAllocator != null;
        assert replicaShardAllocator != null;
        ensureAsyncFetchStorePrimaryRecency(allocation);
    }

    @Override
    public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {
        assert replicaShardAllocator != null;
        if (allocation.routingNodes().hasInactiveShards()) {
            // cancel existing recoveries if we have a better match
            replicaShardAllocator.processExistingRecoveries(allocation);
        }
    }

    @Override
    public void allocateUnassigned(
        ShardRouting shardRouting,
        final RoutingAllocation allocation,
        UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        assert primaryShardAllocator != null;
        assert replicaShardAllocator != null;
        innerAllocatedUnassigned(allocation, primaryShardAllocator, replicaShardAllocator, shardRouting, unassignedAllocationHandler);
    }

    // allow for testing infra to change shard allocators implementation
    protected static void innerAllocatedUnassigned(
        RoutingAllocation allocation,
        PrimaryShardAllocator primaryShardAllocator,
        ReplicaShardAllocator replicaShardAllocator,
        ShardRouting shardRouting,
        ExistingShardsAllocator.UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        assert shardRouting.unassigned();
        if (shardRouting.primary()) {
            primaryShardAllocator.allocateUnassigned(shardRouting, allocation, unassignedAllocationHandler);
        } else {
            replicaShardAllocator.allocateUnassigned(shardRouting, allocation, unassignedAllocationHandler);
        }
    }

    @Override
    public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting unassignedShard, RoutingAllocation routingAllocation) {
        assert unassignedShard.unassigned();
        assert routingAllocation.debugDecision();
        if (unassignedShard.primary()) {
            assert primaryShardAllocator != null;
            return primaryShardAllocator.makeAllocationDecision(unassignedShard, routingAllocation, logger);
        } else {
            assert replicaShardAllocator != null;
            return replicaShardAllocator.makeAllocationDecision(unassignedShard, routingAllocation, logger);
        }
    }

    /**
     * Clear the fetched data for the primary to ensure we do not cancel recoveries based on excessively stale data.
     */
    private void ensureAsyncFetchStorePrimaryRecency(RoutingAllocation allocation) {
        DiscoveryNodes nodes = allocation.nodes();
        if (hasNewNodes(nodes)) {
            final Set<String> newEphemeralIds = StreamSupport.stream(Spliterators.spliterator(nodes.getDataNodes().entrySet(), 0), false)
                .map(node -> node.getValue().getEphemeralId())
                .collect(Collectors.toSet());
            // Invalidate the cache if a data node has been added to the cluster. This ensures that we do not cancel a recovery if a node
            // drops out, we fetch the shard data, then some indexing happens and then the node rejoins the cluster again. There are other
            // ways we could decide to cancel a recovery based on stale data (e.g. changing allocation filters or a primary failure) but
            // making the wrong decision here is not catastrophic so we only need to cover the common case.
            logger.trace(
                () -> new ParameterizedMessage(
                    "new nodes {} found, clearing primary async-fetch-store cache",
                    Sets.difference(newEphemeralIds, lastSeenEphemeralIds)
                )
            );
            asyncFetchStore.values().forEach(fetch -> clearCacheForPrimary(fetch, allocation));
            // recalc to also (lazily) clear out old nodes.
            this.lastSeenEphemeralIds = newEphemeralIds;
        }
    }

    private static void clearCacheForPrimary(
        AsyncShardFetch<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetch,
        RoutingAllocation allocation
    ) {
        assert fetch.shardAttributesMap.size() == 1 : "expected only one shard";
        ShardId shardId = fetch.shardAttributesMap.keySet().iterator().next();
        ShardRouting primary = allocation.routingNodes().activePrimary(shardId);
        if (primary != null) {
            fetch.clearCacheForNode(primary.currentNodeId());
        }
    }

    private boolean hasNewNodes(DiscoveryNodes nodes) {
        for (final DiscoveryNode node : nodes.getDataNodes().values()) {
            if (lastSeenEphemeralIds.contains(node.getEphemeralId()) == false) {
                return true;
            }
        }
        return false;
    }

    class InternalAsyncFetch<T extends BaseNodeResponse> extends AsyncShardFetch<T> {

        InternalAsyncFetch(
            Logger logger,
            String type,
            ShardId shardId,
            String customDataPath,
            Lister<? extends BaseNodesResponse<T>, T> action,
            ClusterManagerMetrics clusterManagerMetrics
        ) {
            super(logger, type, shardId, customDataPath, action, clusterManagerMetrics);
        }

        @Override
        protected void reroute(String reroutingKey, String reason) {
            logger.trace("{} scheduling reroute for {}", reroutingKey, reason);
            assert rerouteService != null;
            rerouteService.reroute(
                "async_shard_fetch",
                Priority.HIGH,
                ActionListener.wrap(
                    r -> logger.trace("{} scheduled reroute completed for {}", reroutingKey, reason),
                    e -> logger.debug(new ParameterizedMessage("{} scheduled reroute failed for {}", reroutingKey, reason), e)
                )
            );
        }
    }

    class InternalPrimaryShardAllocator extends PrimaryShardAllocator {

        private final TransportNodesListGatewayStartedShards startedAction;
        private final ClusterManagerMetrics clusterManagerMetrics;

        InternalPrimaryShardAllocator(TransportNodesListGatewayStartedShards startedAction, ClusterManagerMetrics clusterManagerMetrics) {
            this.startedAction = startedAction;
            this.clusterManagerMetrics = clusterManagerMetrics;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetchData(
            ShardRouting shard,
            RoutingAllocation allocation
        ) {
            AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetch = asyncFetchStarted.computeIfAbsent(
                shard.shardId(),
                shardId -> new InternalAsyncFetch<>(
                    logger,
                    "shard_started",
                    shardId,
                    IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(shard.index()).getSettings()),
                    startedAction,
                    clusterManagerMetrics
                )
            );
            AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> shardState = fetch.fetchData(
                allocation.nodes(),
                new HashMap<>() {
                    {
                        put(shard.shardId(), allocation.getIgnoreNodes(shard.shardId()));
                    }
                }
            );

            if (shardState.hasData()) {
                shardState.processAllocation(allocation);
            }
            return shardState;
        }
    }

    class InternalReplicaShardAllocator extends ReplicaShardAllocator {

        private final TransportNodesListShardStoreMetadata storeAction;
        private final ClusterManagerMetrics clusterManagerMetrics;

        InternalReplicaShardAllocator(TransportNodesListShardStoreMetadata storeAction, ClusterManagerMetrics clusterManagerMetrics) {
            this.storeAction = storeAction;
            this.clusterManagerMetrics = clusterManagerMetrics;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetchData(
            ShardRouting shard,
            RoutingAllocation allocation
        ) {
            AsyncShardFetch<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetch = asyncFetchStore.computeIfAbsent(
                shard.shardId(),
                shardId -> new InternalAsyncFetch<>(
                    logger,
                    "shard_store",
                    shard.shardId(),
                    IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(shard.index()).getSettings()),
                    storeAction,
                    clusterManagerMetrics
                )
            );
            AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> shardStores = fetch.fetchData(
                allocation.nodes(),
                new HashMap<>() {
                    {
                        put(shard.shardId(), allocation.getIgnoreNodes(shard.shardId()));
                    }
                }
            );
            if (shardStores.hasData()) {
                shardStores.processAllocation(allocation);
            }
            return shardStores;
        }

        @Override
        protected boolean hasInitiatedFetching(ShardRouting shard) {
            return asyncFetchStore.get(shard.shardId()) != null;
        }
    }
}
