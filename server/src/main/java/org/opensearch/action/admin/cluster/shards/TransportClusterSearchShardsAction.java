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

package org.opensearch.action.admin.cluster.shards;

import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.clustermanager.TransportClusterManagerNodeReadAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Transport action for searching shards
 *
 * @opensearch.internal
 */
public class TransportClusterSearchShardsAction extends TransportClusterManagerNodeReadAction<
    ClusterSearchShardsRequest,
    ClusterSearchShardsResponse> {

    private final IndicesService indicesService;

    @Inject
    public TransportClusterSearchShardsAction(
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClusterSearchShardsAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            ClusterSearchShardsRequest::new,
            indexNameExpressionResolver
        );
        this.indicesService = indicesService;
    }

    @Override
    protected String executor() {
        // all in memory work here...
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(ClusterSearchShardsRequest request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_READ, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected ClusterSearchShardsResponse read(StreamInput in) throws IOException {
        return new ClusterSearchShardsResponse(in);
    }

    @Override
    protected void clusterManagerOperation(
        final ClusterSearchShardsRequest request,
        final ClusterState state,
        final ActionListener<ClusterSearchShardsResponse> listener
    ) {
        ClusterState clusterState = clusterService.state();
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(clusterState, request);
        Map<String, Set<String>> routingMap = indexNameExpressionResolver.resolveSearchRouting(state, request.routing(), request.indices());
        Map<String, AliasFilter> indicesAndFilters = new HashMap<>();
        Set<String> indicesAndAliases = indexNameExpressionResolver.resolveExpressions(clusterState, request.indices());
        for (String index : concreteIndices) {
            final AliasFilter aliasFilter = indicesService.buildAliasFilter(clusterState, index, indicesAndAliases);
            final String[] aliases = indexNameExpressionResolver.indexAliases(
                clusterState,
                index,
                aliasMetadata -> true,
                true,
                indicesAndAliases
            );
            indicesAndFilters.put(index, new AliasFilter(aliasFilter.getQueryBuilder(), aliases));
        }

        Set<String> nodeIds = new HashSet<>();
        GroupShardsIterator<ShardIterator> groupShardsIterator = clusterService.operationRouting()
            .searchShards(clusterState, concreteIndices, routingMap, request.preference());
        ShardRouting shard;
        ClusterSearchShardsGroup[] groupResponses = new ClusterSearchShardsGroup[groupShardsIterator.size()];
        int currentGroup = 0;
        for (ShardIterator shardIt : groupShardsIterator) {
            ShardId shardId = shardIt.shardId();
            ShardRouting[] shardRoutings = new ShardRouting[shardIt.size()];
            int currentShard = 0;
            shardIt.reset();
            while ((shard = shardIt.nextOrNull()) != null) {
                shardRoutings[currentShard++] = shard;
                nodeIds.add(shard.currentNodeId());
            }
            groupResponses[currentGroup++] = new ClusterSearchShardsGroup(shardId, shardRoutings);
        }
        DiscoveryNode[] nodes = new DiscoveryNode[nodeIds.size()];
        int currentNode = 0;
        for (String nodeId : nodeIds) {
            nodes[currentNode++] = clusterState.getNodes().get(nodeId);
        }
        listener.onResponse(new ClusterSearchShardsResponse(groupResponses, nodes, indicesAndFilters));
    }
}
