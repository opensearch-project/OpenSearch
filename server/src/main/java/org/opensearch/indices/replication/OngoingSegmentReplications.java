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
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.index.Index;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.FileChunkWriter;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.common.CopyState;
import org.opensearch.indices.replication.common.ReplicationFailedException;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Manages references to ongoing segrep events on a node.
 * Each replica will have a new {@link SegmentReplicationSourceHandler} created when starting replication.
 * CopyStates will be cached for reuse between replicas and only released when all replicas have finished copying segments.
 *
 * @opensearch.internal
 */
class OngoingSegmentReplications implements Closeable {
    private final RecoverySettings recoverySettings;
    private final IndicesService indicesService;
    private final Map<String, SegmentReplicationSourceHandler> allocationIdToHandlers;
    private static final Logger logger = LogManager.getLogger(OngoingSegmentReplications.class);
    private final Map<ShardId, CopyState> copyStateMap;

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
        this.copyStateMap = Collections.synchronizedMap(new HashMap<>());
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
            final ActionListener<GetSegmentFilesResponse> wrappedListener = ActionListener.runBefore(listener, () -> {
                final SegmentReplicationSourceHandler sourceHandler = allocationIdToHandlers.remove(request.getTargetAllocationId());
                if (sourceHandler != null) {
                    sourceHandler.close();
                }
            });
            if (request.getFilesToFetch().isEmpty()) {
                wrappedListener.onResponse(new GetSegmentFilesResponse(Collections.emptyList()));
            } else {
                handler.sendFiles(request, wrappedListener);
            }
        } else {
            listener.onResponse(new GetSegmentFilesResponse(Collections.emptyList()));
        }
    }

    /**
     * Prepare for a Replication event. This method constructs a {@link CopyState} holding files to be sent off of the current
     * node's store.  This state is intended to be sent back to Replicas before copy is initiated so the replica can perform a diff against its
     * local store.  It will then build a handler to orchestrate the segment copy that will be stored locally and started on a subsequent request from replicas
     * with the list of required files.
     *
     * @param request         {@link CheckpointInfoRequest}
     * @param fileChunkWriter {@link FileChunkWriter} writer to handle sending files over the transport layer.
     * @return {@link CopyState} the built CopyState for this replication event.
     * @throws IOException - When there is an IO error building CopyState.
     */
    void prepareForReplication(CheckpointInfoRequest request, FileChunkWriter fileChunkWriter, GatedCloseable<CopyState> copyState) {
        final SegmentReplicationSourceHandler segmentReplicationSourceHandler = allocationIdToHandlers.putIfAbsent(
            request.getTargetAllocationId(),
            createTargetHandler(request.getTargetNode(), copyState, request.getTargetAllocationId(), fileChunkWriter)
        );
        if (segmentReplicationSourceHandler != null) {
            logger.error("Shard is already replicating, id {}", request.getReplicationId());
            throw new OpenSearchException(
                "Shard copy {} on node {} already replicating to {} rejecting id {}",
                request.getCheckpoint().getShardId(),
                request.getTargetNode(),
                segmentReplicationSourceHandler.getCopyState().getCheckpoint(),
                request.getReplicationId()
            );
        }
    }

    /**
     * Cancel all Replication events for the given shard, intended to be called when a primary is shutting down.
     *
     * @param shard  {@link IndexShard}
     * @param reason {@link String} - Reason for the cancel
     */
    synchronized void cancel(IndexShard shard, String reason) {
        cancelHandlers(handler -> handler.getCopyState().getShard().shardId().equals(shard.shardId()), reason);
        if (shard.routingEntry().primary()) {
            clearCopyStateForShard(shard.shardId());
        }
    }

    /**
     * Cancel all Replication events for the given allocation ID, intended to be called when a primary is shutting down.
     *
     * @param allocationId {@link String} - Allocation ID.
     * @param reason       {@link String} - Reason for the cancel
     */
    synchronized void cancel(String allocationId, String reason) {
        final SegmentReplicationSourceHandler handler = allocationIdToHandlers.remove(allocationId);
        if (handler != null) {
            handler.cancel(reason);
            try {
                handler.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
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

    private SegmentReplicationSourceHandler createTargetHandler(
        DiscoveryNode node,
        GatedCloseable<CopyState> copyState,
        String allocationId,
        FileChunkWriter fileChunkWriter
    ) {
        return new SegmentReplicationSourceHandler(
            node,
            fileChunkWriter,
            copyState,
            allocationId,
            Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
            recoverySettings.getMaxConcurrentFileChunks()
        );
    }

    /**
     * Remove handlers from allocationIdToHandlers map based on a filter predicate.
     * This will also decref the handler's CopyState reference.
     */
    private void cancelHandlers(Predicate<? super SegmentReplicationSourceHandler> predicate, String reason) {
        final List<String> allocationIds = allocationIdToHandlers.values()
            .stream()
            .filter(predicate)
            .map(SegmentReplicationSourceHandler::getAllocationId)
            .collect(Collectors.toList());
        for (String allocationId : allocationIds) {
            cancel(allocationId, reason);
        }
    }

    public synchronized void updateCopyState(IndexShard indexShard) throws IOException {
        // We can only compute CopyState for shards that have started.
        if (indexShard.state() == IndexShardState.STARTED) {
            final CopyState state = new CopyState(indexShard);
            final CopyState oldCopyState = copyStateMap.remove(indexShard.shardId());
            if (oldCopyState != null) {
                oldCopyState.decRef();
            }
            // build the CopyState object and cache it before returning
            copyStateMap.put(indexShard.shardId(), state);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        for (CopyState value : copyStateMap.values()) {
            value.decRef();
        }
    }

    public synchronized void clearCopyStateForShard(ShardId shardId) {
        final CopyState remove = copyStateMap.remove(shardId);
        if (remove != null) {
            remove.decRef();
        }
    }

    public synchronized GatedCloseable<CopyState> getLatestCopyState(ShardId shardId) {
        final CopyState copyState = copyStateMap.get(shardId);
        if (copyState != null) {
            copyState.incRef();
            return new GatedCloseable<>(copyState, copyState::decRef);
        }
        throw new ReplicationFailedException("Primary has no computed copyState");
    }

    protected Map<ShardId, CopyState> getCopyStateMap() {
        return copyStateMap;
    }
}
