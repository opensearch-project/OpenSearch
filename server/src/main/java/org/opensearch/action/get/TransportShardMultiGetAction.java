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

package org.opensearch.action.get;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.action.ActionListener;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.TransportActions;
import org.opensearch.action.support.single.shard.TransportSingleShardAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.index.IndexService;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Perform the shard multi get action
 *
 * @opensearch.internal
 */
public class TransportShardMultiGetAction extends TransportSingleShardAction<MultiGetShardRequest, MultiGetShardResponse> {

    private static final String ACTION_NAME = MultiGetAction.NAME + "[shard]";

    private final IndicesService indicesService;

    @Inject
    public TransportShardMultiGetAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            ACTION_NAME,
            threadPool,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            MultiGetShardRequest::new,
            ThreadPool.Names.GET
        );
        this.indicesService = indicesService;
    }

    @Override
    protected boolean isSubAction() {
        return true;
    }

    @Override
    protected Writeable.Reader<MultiGetShardResponse> getResponseReader() {
        return MultiGetShardResponse::new;
    }

    @Override
    protected boolean resolveIndex(MultiGetShardRequest request) {
        return true;
    }

    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        return clusterService.operationRouting()
            .getShards(state, request.request().index(), request.request().shardId(), request.request().preference());
    }

    @Override
    protected void asyncShardOperation(MultiGetShardRequest request, ShardId shardId, ActionListener<MultiGetShardResponse> listener)
        throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        if (request.realtime()) { // we are not tied to a refresh cycle here anyway
            super.asyncShardOperation(request, shardId, listener);
        } else {
            indexShard.awaitShardSearchActive(b -> {
                try {
                    super.asyncShardOperation(request, shardId, listener);
                } catch (Exception ex) {
                    listener.onFailure(ex);
                }
            });
        }
    }

    @Override
    protected MultiGetShardResponse shardOperation(MultiGetShardRequest request, ShardId shardId) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());

        if (request.refresh() && !request.realtime()) {
            indexShard.refresh("refresh_flag_mget");
        }

        MultiGetShardResponse response = new MultiGetShardResponse();
        for (int i = 0; i < request.locations.size(); i++) {
            MultiGetRequest.Item item = request.items.get(i);
            try {
                GetResult getResult = indexShard.getService()
                    .get(item.id(), item.storedFields(), request.realtime(), item.version(), item.versionType(), item.fetchSourceContext());
                response.add(request.locations.get(i), new GetResponse(getResult));
            } catch (RuntimeException e) {
                if (TransportActions.isShardNotAvailableException(e)) {
                    throw e;
                } else {
                    logger.debug(() -> new ParameterizedMessage("{} failed to execute multi_get for [{}]", shardId, item.id()), e);
                    response.add(request.locations.get(i), new MultiGetResponse.Failure(request.index(), item.id(), e));
                }
            }
        }

        return response;
    }

    @Override
    protected String getExecutor(MultiGetShardRequest request, ShardId shardId) {
        final ClusterState clusterState = clusterService.state();
        if (clusterState.metadata().index(shardId.getIndex()).isSystem()) {
            return ThreadPool.Names.SYSTEM_READ;
        } else if (indicesService.indexServiceSafe(shardId.getIndex()).getIndexSettings().isSearchThrottled()) {
            return ThreadPool.Names.SEARCH_THROTTLED;
        } else {
            return super.getExecutor(request, shardId);
        }
    }
}
