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

package org.opensearch.action.admin.indices.upgrade.get;

import org.opensearch.Version;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Transport Action for Upgrading an Index
 *
 * @opensearch.internal
 */
public class TransportUpgradeStatusAction extends TransportBroadcastByNodeAction<
    UpgradeStatusRequest,
    UpgradeStatusResponse,
    ShardUpgradeStatus> {

    private final IndicesService indicesService;

    @Inject
    public TransportUpgradeStatusAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            UpgradeStatusAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            UpgradeStatusRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.indicesService = indicesService;
    }

    /**
     * Getting upgrade stats from *all* active shards.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, UpgradeStatusRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, UpgradeStatusRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, UpgradeStatusRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected ShardUpgradeStatus readShardResult(StreamInput in) throws IOException {
        return new ShardUpgradeStatus(in);
    }

    @Override
    protected UpgradeStatusResponse newResponse(
        UpgradeStatusRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<ShardUpgradeStatus> responses,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new UpgradeStatusResponse(
            responses.toArray(new ShardUpgradeStatus[0]),
            totalShards,
            successfulShards,
            failedShards,
            shardFailures
        );
    }

    @Override
    protected UpgradeStatusRequest readRequestFrom(StreamInput in) throws IOException {
        return new UpgradeStatusRequest(in);
    }

    @Override
    protected ShardUpgradeStatus shardOperation(UpgradeStatusRequest request, ShardRouting shardRouting) {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
        List<Segment> segments = indexShard.segments(false);
        long total_bytes = 0;
        long to_upgrade_bytes = 0;
        long to_upgrade_bytes_ancient = 0;
        for (Segment seg : segments) {
            total_bytes += seg.sizeInBytes;
            if (seg.version.major != Version.CURRENT.luceneVersion.major) {
                to_upgrade_bytes_ancient += seg.sizeInBytes;
                to_upgrade_bytes += seg.sizeInBytes;
            } else if (seg.version.minor != Version.CURRENT.luceneVersion.minor) {
                // TODO: this comparison is bogus! it would cause us to upgrade even with the same format
                // instead, we should check if the codec has changed
                to_upgrade_bytes += seg.sizeInBytes;
            }
        }

        return new ShardUpgradeStatus(indexShard.routingEntry(), total_bytes, to_upgrade_bytes, to_upgrade_bytes_ancient);
    }
}
