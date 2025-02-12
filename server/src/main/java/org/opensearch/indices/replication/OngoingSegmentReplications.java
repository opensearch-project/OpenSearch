/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.OpenSearchException;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.FileChunkWriter;
import org.opensearch.indices.recovery.RecoverySettings;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Manages references to ongoing segrep events on a node.
 * Each replica will have a new {@link SegmentReplicationSourceHandler} created when starting replication.
 *
 * @opensearch.internal
 */
class OngoingSegmentReplications {

    private static final Logger logger = LogManager.getLogger(OngoingSegmentReplications.class);
    private final RecoverySettings recoverySettings;
    private final IndicesService indicesService;
    private final Map<String, SegmentReplicationSourceHandler> allocationIdToHandlers;

    /**
     * Constructor.
     *
     * @param indicesService   {@link IndicesService}
     * @param recoverySettings {@link RecoverySettings}
     */
    OngoingSegmentReplications(IndicesService indicesService, RecoverySettings recoverySettings) {
        this.indicesService = indicesService;
        this.recoverySettings = recoverySettings;
        this.allocationIdToHandlers = ConcurrentCollections.newConcurrentMap();
    }

    /**
     * Start sending files to the replica.
     *
     * @param request  {@link GetSegmentFilesRequest}
     * @param listener {@link ActionListener} that resolves when sending files is complete.
     */
    void startSegmentCopy(GetSegmentFilesRequest request, ActionListener<GetSegmentFilesResponse> listener) {
        final SegmentReplicationSourceHandler handler = allocationIdToHandlers.get(request.getTargetAllocationId());
        if (handler != null) {
            if (handler.isReplicating()) {
                throw new OpenSearchException(
                    "Replication to shard {}, on node {} has already started",
                    request.getCheckpoint().getShardId(),
                    request.getTargetNode()
                );
            }
            // update the given listener to release the CopyState before it resolves.
            final ActionListener<GetSegmentFilesResponse> wrappedListener = ActionListener.runBefore(
                listener,
                () -> allocationIdToHandlers.remove(request.getTargetAllocationId())
            );
            handler.sendFiles(request, wrappedListener);
        } else {
            listener.onResponse(new GetSegmentFilesResponse(Collections.emptyList()));
        }
    }

    /**
     * Prepare for a Replication event. This method constructs a {@link SegmentReplicationSourceHandler} that orchestrates segment copy and
     * will internally incref files for copy.
     *
     * @param request         {@link CheckpointInfoRequest}
     * @param fileChunkWriter {@link FileChunkWriter} writer to handle sending files over the transport layer.
     * @return {@link SegmentReplicationSourceHandler} the built CopyState for this replication event.
     */
    SegmentReplicationSourceHandler prepareForReplication(CheckpointInfoRequest request, FileChunkWriter fileChunkWriter) {
        return allocationIdToHandlers.computeIfAbsent(request.getTargetAllocationId(), aId -> {
            try {
                // From the checkpoint's shard ID, fetch the IndexShard
                final ShardId shardId = request.getCheckpoint().getShardId();
                final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
                final IndexShard indexShard = indexService.getShard(shardId.id());
                return new SegmentReplicationSourceHandler(
                    request.getTargetNode(),
                    fileChunkWriter,
                    indexShard,
                    request.getTargetAllocationId(),
                    Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
                    recoverySettings.getMaxConcurrentFileChunks()
                );
            } catch (IOException e) {
                throw new UncheckedIOException("Error creating replication handler", e);
            }
        });
    }

    /**
     * Cancel all Replication events for the given shard, intended to be called when a primary is shutting down.
     *
     * @param shard  {@link IndexShard}
     * @param reason {@link String} - Reason for the cancel
     */
    void cancel(IndexShard shard, String reason) {
        cancelHandlers(handler -> handler.shardId().equals(shard.shardId()), reason);
    }

    /**
     * Cancel all Replication events for the given allocation ID, intended to be called when a primary is shutting down.
     *
     * @param allocationId {@link String} - Allocation ID.
     * @param reason       {@link String} - Reason for the cancel
     */
    void cancel(String allocationId, String reason) {
        final SegmentReplicationSourceHandler handler = allocationIdToHandlers.remove(allocationId);
        if (handler != null) {
            handler.cancel(reason);
        }
    }

    /**
     * Cancel any ongoing replications for a given {@link DiscoveryNode}
     *
     * @param node {@link DiscoveryNode} node for which to cancel replication events.
     */
    void cancelReplication(DiscoveryNode node) {
        cancelHandlers(handler -> handler.getTargetNode().equals(node), "Node left");
    }

    int size() {
        return allocationIdToHandlers.size();
    }

    // Visible for tests.
    Map<String, SegmentReplicationSourceHandler> getHandlers() {
        return allocationIdToHandlers;
    }

    /**
     * Clear handlers for any allocationIds not in sync.
     * @param shardId {@link ShardId}
     * @param inSyncAllocationIds {@link List} of in-sync allocation Ids.
     */
    void clearOutOfSyncIds(ShardId shardId, Set<String> inSyncAllocationIds) {
        cancelHandlers(
            (handler) -> handler.shardId().equals(shardId) && inSyncAllocationIds.contains(handler.getAllocationId()) == false,
            "Shard is no longer in-sync with the primary"
        );
    }

    /**
     * Remove handlers from allocationIdToHandlers map based on a filter predicate.
     */
    private void cancelHandlers(Predicate<? super SegmentReplicationSourceHandler> predicate, String reason) {
        final List<String> allocationIds = allocationIdToHandlers.values()
            .stream()
            .filter(predicate)
            .map(SegmentReplicationSourceHandler::getAllocationId)
            .collect(Collectors.toList());
        if (allocationIds.size() == 0) {
            return;
        }
        logger.warn(() -> new ParameterizedMessage("Cancelling replications for allocationIds {}", allocationIds));
        for (String allocationId : allocationIds) {
            cancel(allocationId, reason);
        }
    }
}
