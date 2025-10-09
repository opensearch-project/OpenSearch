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

package org.opensearch.action.admin.indices.cache.clear;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.indices.IndicesService;
import org.opensearch.node.Node;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Predicate;

/**
 * Indices clear cache action.
 *
 * @opensearch.internal
 */
public class TransportClearIndicesCacheAction extends TransportBroadcastByNodeAction<
    ClearIndicesCacheRequest,
    ClearIndicesCacheResponse,
    TransportBroadcastByNodeAction.EmptyResult> {

    private final IndicesService indicesService;

    private final Node node;
    private final Logger clearActionLogger = LogManager.getLogger(getClass());

    @Inject
    public TransportClearIndicesCacheAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        Node node,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ClearIndicesCacheAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            ClearIndicesCacheRequest::new,
            ThreadPool.Names.MANAGEMENT,
            false
        );
        this.indicesService = indicesService;
        this.node = node;
    }

    @Override
    protected EmptyResult readShardResult(StreamInput in) throws IOException {
        return EmptyResult.readEmptyResultFrom(in);
    }

    @Override
    protected ClearIndicesCacheResponse newResponse(
        ClearIndicesCacheRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<EmptyResult> responses,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new ClearIndicesCacheResponse(totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ClearIndicesCacheRequest readRequestFrom(StreamInput in) throws IOException {
        return new ClearIndicesCacheRequest(in);
    }

    @Override
    protected EmptyResult shardOperation(ClearIndicesCacheRequest request, ShardRouting shardRouting) {
        if (request.fileCache()) {
            if (node.fileCache() != null) {
                ShardPath shardPath = ShardPath.loadFileCachePath(node.getNodeEnvironment(), shardRouting.shardId());
                Predicate<Path> pathStartsWithShardPathPredicate = path -> path.startsWith(shardPath.getDataPath());
                node.fileCache().prune(pathStartsWithShardPathPredicate);
            }
        }

        indicesService.clearIndexShardCache(
            shardRouting.shardId(),
            request.queryCache(),
            request.fieldDataCache(),
            request.requestCache(),
            request.fields()
        );
        return EmptyResult.INSTANCE;
    }

    @Override
    protected void nodeOperation(List<EmptyResult> results, List<BroadcastShardOperationFailedException> accumulatedExceptions) {
        try {
            indicesService.forceClearNodewideCaches();
        } catch (Exception e) {
            clearActionLogger.warn("Node-wide force cache clear failed; marked keys will be cleaned at next scheduled cache cleanup", e);
        }
    }

    /**
     * The refresh request works against *all* shards.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, ClearIndicesCacheRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, ClearIndicesCacheRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, ClearIndicesCacheRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }
}
