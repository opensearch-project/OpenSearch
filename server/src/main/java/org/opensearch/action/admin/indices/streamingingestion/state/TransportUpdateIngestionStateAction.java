/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.state;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
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
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardNotFoundException;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.pollingingest.IngestionSettings;
import org.opensearch.indices.pollingingest.StreamPoller;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.opensearch.indices.pollingingest.StreamPoller.ResetState.RESET_BY_OFFSET;
import static org.opensearch.indices.pollingingest.StreamPoller.ResetState.RESET_BY_TIMESTAMP;

/**
 * Transport action for updating ingestion state on provided shards. Shard level failures are provided if there are
 * errors during updating shard state.
 *
 * <p>This is for internal use and will not be exposed to the user directly. </p>
 *
 * @opensearch.experimental
 */
public class TransportUpdateIngestionStateAction extends TransportBroadcastByNodeAction<
    UpdateIngestionStateRequest,
    UpdateIngestionStateResponse,
    ShardIngestionState> {

    private final IndicesService indicesService;

    @Inject
    public TransportUpdateIngestionStateAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        super(
            UpdateIngestionStateAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            UpdateIngestionStateRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.indicesService = indicesService;
    }

    /**
     * Indicates the shards to consider.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, UpdateIngestionStateRequest request, String[] concreteIndices) {
        Set<String> allActiveIndexSet = new HashSet<>();
        for (String index : concreteIndices) {
            IndexMetadata indexMetadata = clusterState.metadata().index(index);
            if (indexMetadata != null && isAllActiveIngestionEnabled(indexMetadata)) {
                allActiveIndexSet.add(index);
            }
        }

        Set<Integer> shardSet = Arrays.stream(request.getShards()).boxed().collect(Collectors.toSet());
        Predicate<ShardRouting> shardFilter = shardRouting -> shardRouting.primary()
            || allActiveIndexSet.contains(shardRouting.getIndexName());
        if (shardSet.isEmpty() == false) {
            shardFilter = shardFilter.and(shardRouting -> shardSet.contains(shardRouting.shardId().getId()));
        }

        return clusterState.routingTable().allShardsSatisfyingPredicate(request.getIndex(), shardFilter);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, UpdateIngestionStateRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, UpdateIngestionStateRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, request.indices());
    }

    @Override
    protected ShardIngestionState readShardResult(StreamInput in) throws IOException {
        return new ShardIngestionState(in);
    }

    @Override
    protected UpdateIngestionStateResponse newResponse(
        UpdateIngestionStateRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<ShardIngestionState> responses,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        return new UpdateIngestionStateResponse(true, totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected UpdateIngestionStateRequest readRequestFrom(StreamInput in) throws IOException {
        return new UpdateIngestionStateRequest(in);
    }

    /**
     * Updates shard ingestion states depending on the requested changes.
     */
    @Override
    protected ShardIngestionState shardOperation(UpdateIngestionStateRequest request, ShardRouting shardRouting) {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
        if (indexShard.routingEntry() == null) {
            throw new ShardNotFoundException(indexShard.shardId());
        }

        try {
            // update shard pointer
            if (request.getResetSettings() != null && request.getResetSettings().length > 0) {
                ResumeIngestionRequest.ResetSettings resetSettings = getResetSettingsForShard(request, indexShard);
                StreamPoller.ResetState resetState = getStreamPollerResetState(resetSettings);
                String resetValue = resetSettings != null ? resetSettings.getValue() : null;
                if (resetState != null && resetValue != null) {
                    IngestionSettings ingestionSettings = IngestionSettings.builder()
                        .setResetState(resetState)
                        .setResetValue(resetValue)
                        .build();
                    indexShard.updateShardIngestionState(ingestionSettings);
                }
            }

            // update ingestion state
            if (request.getIngestionPaused() != null) {
                IngestionSettings ingestionSettings = IngestionSettings.builder().setIsPaused(request.getIngestionPaused()).build();
                indexShard.updateShardIngestionState(ingestionSettings);
            }

            return indexShard.getIngestionState();
        } catch (final AlreadyClosedException e) {
            throw new ShardNotFoundException(indexShard.shardId());
        }
    }

    private StreamPoller.ResetState getStreamPollerResetState(ResumeIngestionRequest.ResetSettings resetSettings) {
        if (resetSettings == null || resetSettings.getMode() == null) {
            return null;
        }

        return switch (resetSettings.getMode()) {
            case OFFSET -> RESET_BY_OFFSET;
            case TIMESTAMP -> RESET_BY_TIMESTAMP;
        };
    }

    private ResumeIngestionRequest.ResetSettings getResetSettingsForShard(UpdateIngestionStateRequest request, IndexShard indexShard) {
        ResumeIngestionRequest.ResetSettings[] resetSettings = request.getResetSettings();
        int targetShardId = indexShard.shardId().id();
        return Arrays.stream(resetSettings).filter(setting -> setting.getShard() == targetShardId).findFirst().orElse(null);
    }

    private boolean isAllActiveIngestionEnabled(IndexMetadata indexMetadata) {
        return indexMetadata.useIngestionSource()
            && indexMetadata.getIngestionSource() != null
            && indexMetadata.getIngestionSource().isAllActiveIngestionEnabled();
    }
}
