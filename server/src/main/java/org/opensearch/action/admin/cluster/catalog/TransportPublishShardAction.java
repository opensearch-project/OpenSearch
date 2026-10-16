/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.catalog;

import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.opensearch.catalog.MetadataClient;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardsIterator;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectoryFactory;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * Broadcast transport action that fans out catalog publish to data nodes holding primary
 * shards. Each shard-level handler builds a {@link RemoteSegmentStoreDirectory} and calls
 * {@link MetadataClient#publish}. Same pattern as {@code TransportForceMergeAction}.
 *
 * @opensearch.experimental
 */
public class TransportPublishShardAction extends TransportBroadcastByNodeAction<
    PublishShardRequest,
    PublishShardResponse,
    TransportBroadcastByNodeAction.EmptyResult> {

    private final MetadataClient metadataClient;
    private final RemoteSegmentStoreDirectoryFactory remoteDirectoryFactory;
    private final ClusterService clusterService;

    @Inject
    public TransportPublishShardAction(
        ClusterService clusterService,
        TransportService transportService,
        MetadataClient metadataClient,
        RemoteSegmentStoreDirectoryFactory remoteDirectoryFactory,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            PublishShardAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            PublishShardRequest::new,
            ThreadPool.Names.SNAPSHOT
        );
        this.metadataClient = metadataClient;
        this.remoteDirectoryFactory = remoteDirectoryFactory;
        this.clusterService = clusterService;
    }

    @Override
    protected EmptyResult readShardResult(StreamInput in) throws IOException {
        return EmptyResult.readEmptyResultFrom(in);
    }

    @Override
    protected PublishShardResponse newResponse(
        PublishShardRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<EmptyResult> responses,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new PublishShardResponse(totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected PublishShardRequest readRequestFrom(StreamInput in) throws IOException {
        return new PublishShardRequest(in);
    }

    @Override
    protected EmptyResult shardOperation(PublishShardRequest request, ShardRouting shardRouting) throws IOException {
        String indexName = shardRouting.getIndexName();
        int shardId = shardRouting.shardId().id();

        IndexMetadata indexMetadata = clusterService.state().metadata().index(indexName);
        IndexSettings indexSettings = new IndexSettings(indexMetadata, clusterService.getSettings());

        RemoteSegmentStoreDirectory remoteDirectory = (RemoteSegmentStoreDirectory) remoteDirectoryFactory.newDirectory(
            IndexMetadata.INDEX_REMOTE_SEGMENT_STORE_REPOSITORY_SETTING.get(indexMetadata.getSettings()),
            indexMetadata.getIndexUUID(),
            shardRouting.shardId(),
            indexSettings.getRemoteStorePathStrategy()
        );

        metadataClient.publish(indexName, remoteDirectory, shardId);
        return EmptyResult.INSTANCE;
    }

    /** Only primary shards — each shard is published once. */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, PublishShardRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allShardsSatisfyingPredicate(concreteIndices, ShardRouting::primary);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, PublishShardRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, PublishShardRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }
}
