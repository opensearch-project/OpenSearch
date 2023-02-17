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
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardNotStartedException;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.FileChunkWriter;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.common.CopyState;

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
    private final Map<ShardId, CopyState> copyStateMap;
    private static final Logger logger = LogManager.getLogger(OngoingSegmentReplications.class);

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
     * Prepare for a Replication event. This method  builds a handler to orchestrate segment copy that will be stored locally and started on a subsequent request from replicas
     * with the list of required files.
     *
     * @param request         {@link CheckpointInfoRequest}
     * @param fileChunkWriter {@link FileChunkWriter} writer to handle sending files over the transport layer.
     * @param copyState       {@link GatedCloseable} containing {@link CopyState} that will be released upon copy completion.
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
     * Cancel all Replication events for the given shard, intended to be called when a primary is shutting down.
     *
     * @param shard  {@link IndexShard}
     * @param reason {@link String} - Reason for the cancel
     */
    void cancel(IndexShard shard, String reason) {
        if (shard.routingEntry().primary()) {
            cancelHandlers(handler -> handler.getCopyState().getShard().shardId().equals(shard.shardId()), reason);
            final CopyState remove = copyStateMap.remove(shard.shardId());
            if (remove != null) {
                remove.decRef();
            }
        }
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

    /**
     * Build and store a new {@link CopyState} for the given {@link IndexShard}.
     *
     * @param indexShard - Primary shard.
     */
    public void setCopyState(IndexShard indexShard) {
        if (indexShard.state() == IndexShardState.STARTED && indexShard.verifyPrimaryMode()) {
            try {
                final CopyState state = new CopyState(indexShard);
                final CopyState oldCopyState = copyStateMap.remove(indexShard.shardId());
                if (oldCopyState != null) {
                    oldCopyState.decRef();
                }
                copyStateMap.put(indexShard.shardId(), state);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    /**
     * Get the latest {@link CopyState} for the given shardId. This method returns an incref'd CopyState wrapped
     * in a {@link GatedCloseable}, when released the copyState is decRef'd.
     *
     * @param shardId {@link ShardId}
     * @return {@link GatedCloseable} Closeable containing the CopyState.
     */
    public GatedCloseable<CopyState> getCopyState(ShardId shardId) {
        final CopyState copyState = copyStateMap.get(shardId);
        if (copyState != null) {
            copyState.incRef();
            return new GatedCloseable<>(copyState, copyState::decRef);
        }
        final IndexService indexService = indicesService.indexService(shardId.getIndex());
        final IndexShard indexShard = indexService.getShard(shardId.id());
        throw new IndexShardNotStartedException(shardId, indexShard.state());
    }

    // for tests.
    Map<ShardId, CopyState> getCopyStateMap() {
        return copyStateMap;
    }

    @Override
    public void close() throws IOException {
        // Extra check to ensure all copyState has been cleaned up.
        for (CopyState value : copyStateMap.values()) {
             if (value.refCount() > 0) {
                 value.decRef();
             }
        }
    }
}
