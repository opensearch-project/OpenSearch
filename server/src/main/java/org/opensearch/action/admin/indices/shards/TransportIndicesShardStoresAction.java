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

package org.opensearch.action.admin.indices.shards;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.CollectionUtil;
import org.opensearch.action.ActionListener;
import org.opensearch.action.FailedNodeException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterShardHealth;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.RoutingNodes;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.common.util.concurrent.CountDown;
import org.opensearch.gateway.AsyncShardFetch;
import org.opensearch.gateway.TransportNodesListGatewayStartedShards;
import org.opensearch.gateway.TransportNodesListGatewayStartedShards.NodeGatewayStartedShards;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Transport action that reads the cluster state for shards with the requested criteria (see {@link ClusterHealthStatus}) of specific
 * indices and fetches store information from all the nodes using {@link TransportNodesListGatewayStartedShards}
 *
 * @opensearch.internal
 */
public class TransportIndicesShardStoresAction extends TransportClusterManagerNodeReadAction<
    IndicesShardStoresRequest,
    IndicesShardStoresResponse> {

    private static final Logger logger = LogManager.getLogger(TransportIndicesShardStoresAction.class);

    private final TransportNodesListGatewayStartedShards listShardStoresInfo;

    @Inject
    public TransportIndicesShardStoresAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        TransportNodesListGatewayStartedShards listShardStoresInfo
    ) {
        super(
            IndicesShardStoresAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            IndicesShardStoresRequest::new,
            indexNameExpressionResolver
        );
        this.listShardStoresInfo = listShardStoresInfo;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected IndicesShardStoresResponse read(StreamInput in) throws IOException {
        return new IndicesShardStoresResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        IndicesShardStoresRequest request,
        ClusterState state,
        ActionListener<IndicesShardStoresResponse> listener
    ) {
        final RoutingTable routingTables = state.routingTable();
        final RoutingNodes routingNodes = state.getRoutingNodes();
        final String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request);
        final Set<Tuple<ShardId, String>> shardsToFetch = new HashSet<>();

        logger.trace("using cluster state version [{}] to determine shards", state.version());
        // collect relevant shard ids of the requested indices for fetching store infos
        for (String index : concreteIndices) {
            IndexRoutingTable indexShardRoutingTables = routingTables.index(index);
            if (indexShardRoutingTables == null) {
                continue;
            }
            final String customDataPath = IndexMetadata.INDEX_DATA_PATH_SETTING.get(state.metadata().index(index).getSettings());
            for (IndexShardRoutingTable routing : indexShardRoutingTables) {
                final int shardId = routing.shardId().id();
                ClusterShardHealth shardHealth = new ClusterShardHealth(shardId, routing);
                if (request.shardStatuses().contains(shardHealth.getStatus())) {
                    shardsToFetch.add(Tuple.tuple(routing.shardId(), customDataPath));
                }
            }
        }

        // async fetch store infos from all the nodes
        // NOTE: instead of fetching shard store info one by one from every node (nShards * nNodes requests)
        // we could fetch all shard store info from every node once (nNodes requests)
        // we have to implement a TransportNodesAction instead of using TransportNodesListGatewayStartedShards
        // for fetching shard stores info, that operates on a list of shards instead of a single shard
        new AsyncShardStoresInfoFetches(state.nodes(), routingNodes, shardsToFetch, listener).start();
    }

    @Override
    protected ClusterBlockException checkBlock(IndicesShardStoresRequest request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_READ, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    /**
     * Information for async shard stores
     *
     * @opensearch.internal
     */
    private class AsyncShardStoresInfoFetches {
        private final DiscoveryNodes nodes;
        private final RoutingNodes routingNodes;
        private final Set<Tuple<ShardId, String>> shards;
        private final ActionListener<IndicesShardStoresResponse> listener;
        private CountDown expectedOps;
        private final Queue<InternalAsyncFetch.Response> fetchResponses;

        AsyncShardStoresInfoFetches(
            DiscoveryNodes nodes,
            RoutingNodes routingNodes,
            Set<Tuple<ShardId, String>> shards,
            ActionListener<IndicesShardStoresResponse> listener
        ) {
            this.nodes = nodes;
            this.routingNodes = routingNodes;
            this.shards = shards;
            this.listener = listener;
            this.fetchResponses = new ConcurrentLinkedQueue<>();
            this.expectedOps = new CountDown(shards.size());
        }

        void start() {
            if (shards.isEmpty()) {
                listener.onResponse(new IndicesShardStoresResponse());
            } else {
                for (Tuple<ShardId, String> shard : shards) {
                    InternalAsyncFetch fetch = new InternalAsyncFetch(logger, "shard_stores", shard.v1(), shard.v2(), listShardStoresInfo);
                    fetch.fetchData(nodes, Collections.<String>emptySet());
                }
            }
        }

        /**
         * Internal async fetch
         *
         * @opensearch.internal
         */
        private class InternalAsyncFetch extends AsyncShardFetch<NodeGatewayStartedShards> {

            InternalAsyncFetch(
                Logger logger,
                String type,
                ShardId shardId,
                String customDataPath,
                TransportNodesListGatewayStartedShards action
            ) {
                super(logger, type, shardId, customDataPath, action);
            }

            @Override
            protected synchronized void processAsyncFetch(
                List<NodeGatewayStartedShards> responses,
                List<FailedNodeException> failures,
                long fetchingRound
            ) {
                fetchResponses.add(new Response(shardId, responses, failures));
                if (expectedOps.countDown()) {
                    finish();
                }
            }

            void finish() {
                final Map<String, Map<Integer, List<IndicesShardStoresResponse.StoreStatus>>> indicesStoreStatusesBuilder = new HashMap<>();

                java.util.List<IndicesShardStoresResponse.Failure> failureBuilder = new ArrayList<>();
                for (Response fetchResponse : fetchResponses) {
                    final Map<Integer, java.util.List<IndicesShardStoresResponse.StoreStatus>> indexStoreStatuses =
                        indicesStoreStatusesBuilder.get(fetchResponse.shardId.getIndexName());
                    final Map<Integer, java.util.List<IndicesShardStoresResponse.StoreStatus>> indexShardsBuilder;
                    if (indexStoreStatuses == null) {
                        indexShardsBuilder = new HashMap<>();
                    } else {
                        indexShardsBuilder = new HashMap<>(indexStoreStatuses);
                    }
                    java.util.List<IndicesShardStoresResponse.StoreStatus> storeStatuses = indexShardsBuilder.get(
                        fetchResponse.shardId.id()
                    );
                    if (storeStatuses == null) {
                        storeStatuses = new ArrayList<>();
                    }
                    for (NodeGatewayStartedShards response : fetchResponse.responses) {
                        if (shardExistsInNode(response)) {
                            IndicesShardStoresResponse.StoreStatus.AllocationStatus allocationStatus = getAllocationStatus(
                                fetchResponse.shardId.getIndexName(),
                                fetchResponse.shardId.id(),
                                response.getNode()
                            );
                            storeStatuses.add(
                                new IndicesShardStoresResponse.StoreStatus(
                                    response.getNode(),
                                    response.allocationId(),
                                    allocationStatus,
                                    response.storeException()
                                )
                            );
                        }
                    }
                    CollectionUtil.timSort(storeStatuses);
                    indexShardsBuilder.put(fetchResponse.shardId.id(), storeStatuses);
                    indicesStoreStatusesBuilder.put(fetchResponse.shardId.getIndexName(), Collections.unmodifiableMap(indexShardsBuilder));
                    for (FailedNodeException failure : fetchResponse.failures) {
                        failureBuilder.add(
                            new IndicesShardStoresResponse.Failure(
                                failure.nodeId(),
                                fetchResponse.shardId.getIndexName(),
                                fetchResponse.shardId.id(),
                                failure.getCause()
                            )
                        );
                    }
                }
                listener.onResponse(
                    new IndicesShardStoresResponse(indicesStoreStatusesBuilder, Collections.unmodifiableList(failureBuilder))
                );
            }

            private IndicesShardStoresResponse.StoreStatus.AllocationStatus getAllocationStatus(
                String index,
                int shardID,
                DiscoveryNode node
            ) {
                for (ShardRouting shardRouting : routingNodes.node(node.getId())) {
                    ShardId shardId = shardRouting.shardId();
                    if (shardId.id() == shardID && shardId.getIndexName().equals(index)) {
                        if (shardRouting.primary()) {
                            return IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY;
                        } else if (shardRouting.assignedToNode()) {
                            return IndicesShardStoresResponse.StoreStatus.AllocationStatus.REPLICA;
                        } else {
                            return IndicesShardStoresResponse.StoreStatus.AllocationStatus.UNUSED;
                        }
                    }
                }
                return IndicesShardStoresResponse.StoreStatus.AllocationStatus.UNUSED;
            }

            /**
             * A shard exists/existed in a node only if shard state file exists in the node
             */
            private boolean shardExistsInNode(final NodeGatewayStartedShards response) {
                return response.storeException() != null || response.allocationId() != null;
            }

            @Override
            protected void reroute(ShardId shardId, String reason) {
                // no-op
            }

            /**
             * Response for shard stores action
             *
             * @opensearch.internal
             */
            public class Response {
                private final ShardId shardId;
                private final List<NodeGatewayStartedShards> responses;
                private final List<FailedNodeException> failures;

                Response(ShardId shardId, List<NodeGatewayStartedShards> responses, List<FailedNodeException> failures) {
                    this.shardId = shardId;
                    this.responses = responses;
                    this.failures = failures;
                }
            }
        }
    }
}
