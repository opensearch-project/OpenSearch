/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.replication;

import org.opensearch.action.support.ActionFilters;
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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.SegmentReplicationPerGroupStats;
import org.opensearch.index.SegmentReplicationPressureService;
import org.opensearch.index.SegmentReplicationShardStats;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.replication.SegmentReplicationState;
import org.opensearch.indices.replication.SegmentReplicationTargetService;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Transport action for shard segment replication operation. This transport action does not actually
 * perform segment replication, it only reports on metrics/stats of segment replication event (both active and complete).
 *
 * @opensearch.internal
 */
public class TransportSegmentReplicationStatsAction extends TransportBroadcastByNodeAction<
    SegmentReplicationStatsRequest,
    SegmentReplicationStatsResponse,
    SegmentReplicationShardStatsResponse> {

    private final SegmentReplicationTargetService targetService;
    private final IndicesService indicesService;
    private final SegmentReplicationPressureService pressureService;

    @Inject
    public TransportSegmentReplicationStatsAction(
        ClusterService clusterService,
        TransportService transportService,
        IndicesService indicesService,
        SegmentReplicationTargetService targetService,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        SegmentReplicationPressureService pressureService
    ) {
        super(
            SegmentReplicationStatsAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            indexNameExpressionResolver,
            SegmentReplicationStatsRequest::new,
            ThreadPool.Names.MANAGEMENT
        );
        this.indicesService = indicesService;
        this.targetService = targetService;
        this.pressureService = pressureService;
    }

    @Override
    protected SegmentReplicationShardStatsResponse readShardResult(StreamInput in) throws IOException {
        return new SegmentReplicationShardStatsResponse(in);
    }

    @Override
    protected SegmentReplicationStatsResponse newResponse(
        SegmentReplicationStatsRequest request,
        int totalShards,
        int successfulShards,
        int failedShards,
        List<SegmentReplicationShardStatsResponse> responses,
        List<DefaultShardOperationFailedException> shardFailures,
        ClusterState clusterState
    ) {
        String[] shards = request.shards();
        final List<Integer> shardsToFetch = Arrays.stream(shards).map(Integer::valueOf).collect(Collectors.toList());

        // organize replica responses by allocationId.
        final Map<String, SegmentReplicationState> replicaStats = new HashMap<>();
        // map of index name to list of replication group stats.
        final Map<String, List<SegmentReplicationPerGroupStats>> primaryStats = new HashMap<>();
        // search replica responses
        final Set<SegmentReplicationShardStats> searchReplicaSegRepShardStats = new HashSet<>();

        for (SegmentReplicationShardStatsResponse response : responses) {
            if (response != null) {
                if (response.getReplicaStats() != null) {
                    final ShardRouting shardRouting = response.getReplicaStats().getShardRouting();
                    if (shardsToFetch.isEmpty() || shardsToFetch.contains(shardRouting.shardId().getId())) {
                        replicaStats.putIfAbsent(shardRouting.allocationId().getId(), response.getReplicaStats());
                    }
                }

                if (response.getSegmentReplicationShardStats() != null) {
                    searchReplicaSegRepShardStats.add(response.getSegmentReplicationShardStats());
                }

                if (response.getPrimaryStats() != null) {
                    final ShardId shardId = response.getPrimaryStats().getShardId();
                    if (shardsToFetch.isEmpty() || shardsToFetch.contains(shardId.getId())) {
                        primaryStats.compute(shardId.getIndexName(), (k, v) -> {
                            if (v == null) {
                                final ArrayList<SegmentReplicationPerGroupStats> list = new ArrayList<>();
                                list.add(response.getPrimaryStats());
                                return list;
                            } else {
                                v.add(response.getPrimaryStats());
                                return v;
                            }
                        });
                    }
                }
            }
        }
        // combine the replica stats to the shard stat entry in each group.
        for (Map.Entry<String, List<SegmentReplicationPerGroupStats>> entry : primaryStats.entrySet()) {
            for (SegmentReplicationPerGroupStats group : entry.getValue()) {
                for (SegmentReplicationShardStats replicaStat : group.getReplicaStats()) {
                    replicaStat.setCurrentReplicationState(replicaStats.getOrDefault(replicaStat.getAllocationId(), null));
                }
            }
        }
        // combine the search replica stats with the stats of other replicas
        for (Map.Entry<String, List<SegmentReplicationPerGroupStats>> entry : primaryStats.entrySet()) {
            for (SegmentReplicationPerGroupStats group : entry.getValue()) {
                Set<SegmentReplicationShardStats> updatedSet = new HashSet<>(group.getReplicaStats());
                updatedSet.addAll(searchReplicaSegRepShardStats);
                group.setReplicaStats(updatedSet);
            }
        }

        return new SegmentReplicationStatsResponse(totalShards, successfulShards, failedShards, primaryStats, shardFailures);
    }

    @Override
    protected SegmentReplicationStatsRequest readRequestFrom(StreamInput in) throws IOException {
        return new SegmentReplicationStatsRequest(in);
    }

    @Override
    protected SegmentReplicationShardStatsResponse shardOperation(SegmentReplicationStatsRequest request, ShardRouting shardRouting) {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
        ShardId shardId = shardRouting.shardId();

        if (indexShard.indexSettings().isSegRepEnabledOrRemoteNode() == false) {
            return null;
        }

        if (shardRouting.primary()) {
            return new SegmentReplicationShardStatsResponse(pressureService.getStatsForShard(indexShard));
        } else if (shardRouting.isSearchOnly()) {
            SegmentReplicationShardStats segmentReplicationShardStats = calcualteSegmentReplicationShardStats(
                shardRouting,
                indexShard,
                shardId,
                request.activeOnly()
            );
            return new SegmentReplicationShardStatsResponse(segmentReplicationShardStats);
        } else {
            return new SegmentReplicationShardStatsResponse(getSegmentReplicationState(shardId, request.activeOnly()));
        }
    }

    @Override
    protected ShardsIterator shards(ClusterState state, SegmentReplicationStatsRequest request, String[] concreteIndices) {
        return state.routingTable().allShardsIncludingRelocationTargets(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, SegmentReplicationStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(
        ClusterState state,
        SegmentReplicationStatsRequest request,
        String[] concreteIndices
    ) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    private SegmentReplicationShardStats calcualteSegmentReplicationShardStats(
        ShardRouting shardRouting,
        IndexShard indexShard,
        ShardId shardId,
        boolean isActiveOnly
    ) {
        ReplicationCheckpoint indexReplicationCheckpoint = indexShard.getLatestReplicationCheckpoint();
        SegmentReplicationState segmentReplicationState = getSegmentReplicationState(shardId, isActiveOnly);
        if (segmentReplicationState != null) {
            ReplicationCheckpoint latestReplicationCheckpointReceived = segmentReplicationState.getLatestReplicationCheckpoint();

            SegmentReplicationShardStats segmentReplicationShardStats = new SegmentReplicationShardStats(
                shardRouting.allocationId().getId(),
                calculateCheckpointsBehind(indexReplicationCheckpoint, latestReplicationCheckpointReceived),
                calculateBytesBehind(indexReplicationCheckpoint, latestReplicationCheckpointReceived),
                0,
                calculateCurrentReplicationLag(shardId),
                getLastCompletedReplicationLag(shardId)
            );

            segmentReplicationShardStats.setCurrentReplicationState(segmentReplicationState);
            return segmentReplicationShardStats;
        } else {
            return new SegmentReplicationShardStats(shardRouting.allocationId().getId(), 0, 0, 0, 0, 0);
        }
    }

    private SegmentReplicationState getSegmentReplicationState(ShardId shardId, boolean isActiveOnly) {
        if (isActiveOnly) {
            return targetService.getOngoingEventSegmentReplicationState(shardId);
        } else {
            return targetService.getSegmentReplicationState(shardId);
        }
    }

    private long calculateCheckpointsBehind(
        ReplicationCheckpoint indexReplicationCheckpoint,
        ReplicationCheckpoint latestReplicationCheckpointReceived
    ) {
        if (latestReplicationCheckpointReceived != null) {
            return latestReplicationCheckpointReceived.getSegmentInfosVersion() - indexReplicationCheckpoint.getSegmentInfosVersion();
        }
        return 0;
    }

    private long calculateBytesBehind(
        ReplicationCheckpoint indexReplicationCheckpoint,
        ReplicationCheckpoint latestReplicationCheckpointReceived
    ) {
        if (latestReplicationCheckpointReceived != null) {
            Store.RecoveryDiff diff = Store.segmentReplicationDiff(
                latestReplicationCheckpointReceived.getMetadataMap(),
                indexReplicationCheckpoint.getMetadataMap()
            );
            return diff.missing.stream().mapToLong(StoreFileMetadata::length).sum();
        }
        return 0;
    }

    private long calculateCurrentReplicationLag(ShardId shardId) {
        SegmentReplicationState ongoingEventSegmentReplicationState = targetService.getOngoingEventSegmentReplicationState(shardId);
        return ongoingEventSegmentReplicationState != null ? ongoingEventSegmentReplicationState.getTimer().time() : 0;
    }

    private long getLastCompletedReplicationLag(ShardId shardId) {
        SegmentReplicationState lastCompletedSegmentReplicationState = targetService.getlatestCompletedEventSegmentReplicationState(
            shardId
        );
        return lastCompletedSegmentReplicationState != null ? lastCompletedSegmentReplicationState.getTimer().time() : 0;
    }
}
