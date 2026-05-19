/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.split.InPlaceSplitShardClusterStateUpdateRequest;
import org.opensearch.cluster.AckedClusterStateUpdateTask;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ack.ClusterStateUpdateResponse;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterManagerTask;
import org.opensearch.cluster.service.ClusterManagerTaskThrottler;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Priority;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.action.ActionListener;

import java.util.function.BiFunction;

/**
 * Service responsible for applying in-place shard split requests to cluster state.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class MetadataInPlaceSplitShardService {
    private static final Logger logger = LogManager.getLogger(MetadataInPlaceSplitShardService.class);

    private final ClusterService clusterService;
    private final AllocationService allocationService;
    private final ClusterManagerTaskThrottler.ThrottlingKey splitShardTaskKey;

    public MetadataInPlaceSplitShardService(final ClusterService clusterService, final AllocationService allocationService) {
        this.clusterService = clusterService;
        this.allocationService = allocationService;
        this.splitShardTaskKey = clusterService.registerClusterManagerTask(ClusterManagerTask.IN_PLACE_SPLIT_SHARD, true);
    }

    /**
     * Submits a cluster state update task to split a shard in-place.
     */
    public void split(final InPlaceSplitShardClusterStateUpdateRequest request, final ActionListener<ClusterStateUpdateResponse> listener) {
        clusterService.submitStateUpdateTask(
            "in-place-split-shard [" + request.getShardId() + "] of index [" + request.getIndex() + "], cause [" + request.cause() + "]",
            new AckedClusterStateUpdateTask<>(Priority.URGENT, request, listener) {
                @Override
                protected ClusterStateUpdateResponse newResponse(boolean acknowledged) {
                    return new ClusterStateUpdateResponse(acknowledged);
                }

                @Override
                public ClusterManagerTaskThrottler.ThrottlingKey getClusterManagerThrottlingKey() {
                    return splitShardTaskKey;
                }

                @Override
                public ClusterState execute(ClusterState currentState) {
                    return applySplitShardRequest(currentState, request, allocationService::reroute);
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.trace(
                        () -> new ParameterizedMessage(
                            "[{}] of index [{}] failed to split online",
                            request.getShardId(),
                            request.getIndex()
                        ),
                        e
                    );
                    super.onFailure(source, e);
                }
            }
        );
    }

    /**
     * Applies a shard split request to the given cluster state. Updates the split metadata
     * in the index metadata and triggers a reroute so that child shards get allocated.
     */
    static ClusterState applySplitShardRequest(
        ClusterState currentState,
        InPlaceSplitShardClusterStateUpdateRequest request,
        BiFunction<ClusterState, String, ClusterState> rerouteRoutingTable
    ) {
        IndexMetadata curIndexMetadata = currentState.metadata().index(request.getIndex());
        if (curIndexMetadata == null) {
            throw new IllegalArgumentException("Index [" + request.getIndex() + "] not found");
        }

        if (curIndexMetadata.getNumberOfVirtualShards() != -1) {
            throw new IllegalArgumentException(
                "In-place shard split is not supported on index [" + request.getIndex() + "] with virtual shards enabled"
            );
        }

        if (currentState.nodes().getMinNodeVersion().equals(currentState.nodes().getMaxNodeVersion()) == false
            || currentState.nodes().getMinNodeVersion().before(Version.V_3_7_0)) {
            throw new IllegalArgumentException(
                "In-place shard split requires all nodes to be on the same version, at or above " + Version.V_3_7_0
            );
        }

        int shardId = request.getShardId();
        SplitShardsMetadata splitShardsMetadata = curIndexMetadata.getSplitShardsMetadata();

        if (splitShardsMetadata.getInProgressSplitShardIds().contains(shardId)) {
            throw new IllegalArgumentException("Splitting of shard [" + shardId + "] is already in progress");
        }

        if (splitShardsMetadata.isSplitParent(shardId)) {
            throw new IllegalArgumentException("Shard [" + shardId + "] has already been split.");
        }

        ShardRouting primaryShard = currentState.routingTable()
            .shardRoutingTable(curIndexMetadata.getIndex().getName(), shardId)
            .primaryShard();
        if (primaryShard.relocating()) {
            throw new IllegalArgumentException(
                "Cannot split shard [" + shardId + "] on index [" + request.getIndex() + "] because it is currently relocating"
            );
        }
        if (primaryShard.started() == false) {
            throw new IllegalArgumentException(
                "Cannot split shard ["
                    + shardId
                    + "] on index ["
                    + request.getIndex()
                    + "] because the primary shard is not started, current state: "
                    + primaryShard.state()
            );
        }

        RoutingTable.Builder routingTableBuilder = RoutingTable.builder(currentState.routingTable());
        Metadata.Builder metadataBuilder = Metadata.builder(currentState.metadata());
        IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(curIndexMetadata);

        SplitShardsMetadata.Builder splitMetadataBuilder = new SplitShardsMetadata.Builder(splitShardsMetadata);
        splitMetadataBuilder.splitShard(shardId, request.getSplitInto());
        indexMetadataBuilder.splitShardsMetadata(splitMetadataBuilder.build());

        RoutingTable routingTable = routingTableBuilder.build();
        metadataBuilder.put(indexMetadataBuilder);

        ClusterState updatedState = ClusterState.builder(currentState).metadata(metadataBuilder).routingTable(routingTable).build();
        return rerouteRoutingTable.apply(updatedState, "shard [" + shardId + "] of index [" + request.getIndex() + "] split");
    }
}
