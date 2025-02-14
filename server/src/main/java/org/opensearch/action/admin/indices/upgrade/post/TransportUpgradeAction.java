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

package org.opensearch.action.admin.indices.upgrade.post;

import org.opensearch.Version;
import org.opensearch.action.PrimaryMissingActionException;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.tasks.Task;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Upgrade index/indices action.
 *
 * @opensearch.internal
 */
public class TransportUpgradeAction extends TransportBroadcastByNodeAction<UpgradeRequest, UpgradeResponse, ShardUpgradeResult> {

    private final IndicesService indicesService;
    private final NodeClient client;

    @Inject
    public TransportUpgradeAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NodeClient client
    ) {
        super(
            UpgradeAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            UpgradeRequest::new,
            ThreadPool.Names.FORCE_MERGE
        );
        this.indicesService = indicesService;
        this.client = client;
    }

    @Override
    protected UpgradeResponse newResponse(
        UpgradeRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<ShardUpgradeResult> shardUpgradeResults,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        Map<String, Integer> successfulPrimaryShards = new HashMap<>();
        Map<String, Tuple<Version, org.apache.lucene.util.Version>> versions = new HashMap<>();
        for (ShardUpgradeResult result : shardUpgradeResults) {
            successfulShards++;
            String index = result.getShardId().getIndex().getName();
            if (result.primary()) {
                Integer count = successfulPrimaryShards.get(index);
                successfulPrimaryShards.put(index, count == null ? 1 : count + 1);
            }
            Tuple<Version, org.apache.lucene.util.Version> versionTuple = versions.get(index);
            if (versionTuple == null) {
                versions.put(index, new Tuple<>(result.upgradeVersion(), result.oldestLuceneSegment()));
            } else {
                // We already have versions for this index - let's see if we need to update them based on the current shard
                Version version = versionTuple.v1();
                org.apache.lucene.util.Version luceneVersion = versionTuple.v2();
                // For the metadata we are interested in the _latest_ Elasticsearch version that was processing the metadata
                // Since we rewrite the mapping during upgrade the metadata is always rewritten by the latest version
                if (result.upgradeVersion().after(versionTuple.v1())) {
                    version = result.upgradeVersion();
                }
                // For the lucene version we are interested in the _oldest_ lucene version since it determines the
                // oldest version that we need to support
                if (result.oldestLuceneSegment().onOrAfter(versionTuple.v2()) == false) {
                    luceneVersion = result.oldestLuceneSegment();
                }
                versions.put(index, new Tuple<>(version, luceneVersion));
            }
        }
        Map<String, Tuple<Version, String>> updatedVersions = new HashMap<>();
        Metadata metadata = clusterState.metadata();
        for (Map.Entry<String, Tuple<Version, org.apache.lucene.util.Version>> versionEntry : versions.entrySet()) {
            String index = versionEntry.getKey();
            Integer primaryCount = successfulPrimaryShards.get(index);
            int expectedPrimaryCount = metadata.index(index).getNumberOfShards();
            if (primaryCount == metadata.index(index).getNumberOfShards()) {
                updatedVersions.put(index, new Tuple<>(versionEntry.getValue().v1(), versionEntry.getValue().v2().toString()));
            } else {
                logger.warn(
                    "Not updating settings for the index [{}] because upgraded of some primary shards failed - "
                        + "expected[{}], received[{}]",
                    index,
                    expectedPrimaryCount,
                    primaryCount == null ? 0 : primaryCount
                );
            }
        }

        return new UpgradeResponse(updatedVersions, totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ShardUpgradeResult shardOperation(UpgradeRequest request, ShardRouting shardRouting) throws IOException {
        IndexShard indexShard = indicesService.indexServiceSafe(shardRouting.shardId().getIndex()).getShard(shardRouting.shardId().id());
        org.apache.lucene.util.Version oldestLuceneSegment = indexShard.upgrade(request);
        // We are using the current version of Elasticsearch as upgrade version since we update mapping to match the current version
        return new ShardUpgradeResult(shardRouting.shardId(), indexShard.routingEntry().primary(), Version.CURRENT, oldestLuceneSegment);
    }

    @Override
    protected ShardUpgradeResult readShardResult(StreamInput in) throws IOException {
        return new ShardUpgradeResult(in);
    }

    @Override
    protected UpgradeRequest readRequestFrom(StreamInput in) throws IOException {
        return new UpgradeRequest(in);
    }

    /**
     * The upgrade request works against *all* shards.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, UpgradeRequest request, String[] concreteIndices) {
        ShardsIterator iterator = clusterState.routingTable().allShards(concreteIndices);
        Set<String> indicesWithMissingPrimaries = indicesWithMissingPrimaries(clusterState, concreteIndices);
        if (indicesWithMissingPrimaries.isEmpty()) {
            return iterator;
        }
        // If some primary shards are not available the request should fail.
        throw new PrimaryMissingActionException(
            "Cannot upgrade indices because the following indices are missing primary shards " + indicesWithMissingPrimaries
        );
    }

    /**
     * Finds all indices that have not all primaries available
     */
    private Set<String> indicesWithMissingPrimaries(ClusterState clusterState, String[] concreteIndices) {
        Set<String> indices = new HashSet<>();
        RoutingTable routingTable = clusterState.routingTable();
        for (String index : concreteIndices) {
            IndexRoutingTable indexRoutingTable = routingTable.index(index);
            if (indexRoutingTable.allPrimaryShardsActive() == false) {
                indices.add(index);
            }
        }
        return indices;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, UpgradeRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, UpgradeRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
    }

    @Override
    protected void doExecute(Task task, UpgradeRequest request, final ActionListener<UpgradeResponse> listener) {
        super.doExecute(task, request, ActionListener.wrap(upgradeResponse -> {
            if (upgradeResponse.versions().isEmpty()) {
                listener.onResponse(upgradeResponse);
            } else {
                updateSettings(upgradeResponse, listener);
            }
        }, listener::onFailure));
    }

    private void updateSettings(final UpgradeResponse upgradeResponse, final ActionListener<UpgradeResponse> listener) {
        UpgradeSettingsRequest upgradeSettingsRequest = new UpgradeSettingsRequest(upgradeResponse.versions());
        client.executeLocally(
            UpgradeSettingsAction.INSTANCE,
            upgradeSettingsRequest,
            ActionListener.delegateFailure(
                listener,
                (delegatedListener, updateSettingsResponse) -> delegatedListener.onResponse(upgradeResponse)
            )
        );
    }
}
