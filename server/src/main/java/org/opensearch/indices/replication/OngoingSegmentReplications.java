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
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.FileChunkWriter;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.CopyState;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Manages references to ongoing segrep events on a node.
 * Each replica will have a new {@link SegmentReplicationSourceHandler} created when starting replication.
 * CopyStates will be cached for reuse between replicas and only released when all replicas have finished copying segments.
 *
 * @opensearch.internal
 */
class OngoingSegmentReplications {

    private static final Logger logger = LogManager.getLogger(OngoingSegmentReplications.class);
    private final RecoverySettings recoverySettings;
    private final IndicesService indicesService;
    private final Map<ReplicationCheckpoint, CopyState> copyStateMap;
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
        this.copyStateMap = Collections.synchronizedMap(new HashMap<>());
        this.allocationIdToHandlers = ConcurrentCollections.newConcurrentMap();
    }

    /**
     * Operations on the {@link #copyStateMap} member.
     */

    /**
     * A synchronized method that checks {@link #copyStateMap} for the given {@link ReplicationCheckpoint} key
     * and returns the cached value if one is present. If the key is not present, a {@link CopyState}
     * object is constructed and stored in the map before being returned.
     */
    synchronized CopyState getCachedCopyState(ReplicationCheckpoint checkpoint) throws IOException {
        if (isInCopyStateMap(checkpoint)) {
            final CopyState copyState = fetchFromCopyStateMap(checkpoint);
            // we incref the copyState for every replica that is using this checkpoint.
            // decref will happen when copy completes.
            copyState.incRef();
            return copyState;
        } else {
            // From the checkpoint's shard ID, fetch the IndexShard
            ShardId shardId = checkpoint.getShardId();
            final IndexService indexService = indicesService.indexService(shardId.getIndex());
            final IndexShard indexShard = indexService.getShard(shardId.id());
            // build the CopyState object and cache it before returning
            final CopyState copyState = new CopyState(checkpoint, indexShard);

            /**
             * Use the checkpoint from the request as the key in the map, rather than
             * the checkpoint from the created CopyState. This maximizes cache hits
             * if replication targets make a request with an older checkpoint.
             * Replication targets are expected to fetch the checkpoint in the response
             * CopyState to bring themselves up to date.
             */
            addToCopyStateMap(checkpoint, copyState);
            return copyState;
        }
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
                    removeCopyState(sourceHandler.getCopyState());
                }
            });
            if (request.getFilesToFetch().isEmpty()) {
                // before completion, alert the primary of the replica's state.
                handler.getCopyState()
                    .getShard()
                    .updateVisibleCheckpointForShard(request.getTargetAllocationId(), handler.getCopyState().getCheckpoint());
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
    CopyState prepareForReplication(CheckpointInfoRequest request, FileChunkWriter fileChunkWriter) throws IOException {
        final CopyState copyState = getCachedCopyState(request.getCheckpoint());
        allocationIdToHandlers.compute(request.getTargetAllocationId(), (allocationId, segrepHandler) -> {
            if (segrepHandler != null) {
                logger.warn("Override handler for allocation id {}", request.getTargetAllocationId());
                cancelHandlers(handler -> handler.getAllocationId().equals(request.getTargetAllocationId()), "cancel due to retry");
            }
            return createTargetHandler(request.getTargetNode(), copyState, request.getTargetAllocationId(), fileChunkWriter);
        });
        return copyState;
    }

    /**
     * Cancel all Replication events for the given shard, intended to be called when a primary is shutting down.
     *
     * @param shard  {@link IndexShard}
     * @param reason {@link String} - Reason for the cancel
     */
    synchronized void cancel(IndexShard shard, String reason) {
        cancelHandlers(handler -> handler.getCopyState().getShard().shardId().equals(shard.shardId()), reason);
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
            removeCopyState(handler.getCopyState());
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

    /**
     * Checks if the {@link #copyStateMap} has the input {@link ReplicationCheckpoint}
     * as a key by invoking {@link Map#containsKey(Object)}.
     */
    boolean isInCopyStateMap(ReplicationCheckpoint replicationCheckpoint) {
        return copyStateMap.containsKey(replicationCheckpoint);
    }

    int size() {
        return allocationIdToHandlers.size();
    }

    // Visible for tests.
    Map<String, SegmentReplicationSourceHandler> getHandlers() {
        return allocationIdToHandlers;
    }

    int cachedCopyStateSize() {
        return copyStateMap.size();
    }

    private SegmentReplicationSourceHandler createTargetHandler(
        DiscoveryNode node,
        CopyState copyState,
        String allocationId,
        FileChunkWriter fileChunkWriter
    ) {
        return new SegmentReplicationSourceHandler(
            node,
            fileChunkWriter,
            copyState.getShard().getThreadPool(),
            copyState,
            allocationId,
            Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
            recoverySettings.getMaxConcurrentFileChunks()
        );
    }

    /**
     * Adds the input {@link CopyState} object to {@link #copyStateMap}.
     * The key is the CopyState's {@link ReplicationCheckpoint} object.
     */
    private void addToCopyStateMap(ReplicationCheckpoint checkpoint, CopyState copyState) {
        copyStateMap.putIfAbsent(checkpoint, copyState);
    }

    /**
     * Given a {@link ReplicationCheckpoint}, return the corresponding
     * {@link CopyState} object, if any, from {@link #copyStateMap}.
     */
    private CopyState fetchFromCopyStateMap(ReplicationCheckpoint replicationCheckpoint) {
        return copyStateMap.get(replicationCheckpoint);
    }

    /**
     * Remove a CopyState. Intended to be called after a replication event completes.
     * This method will remove a copyState from the copyStateMap only if its refCount hits 0.
     *
     * @param copyState {@link CopyState}
     */
    private synchronized void removeCopyState(CopyState copyState) {
        if (copyState.decRef() == true) {
            copyStateMap.remove(copyState.getRequestedReplicationCheckpoint());
        }
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
        if (allocationIds.size() == 0) {
            return;
        }
        logger.warn(() -> new ParameterizedMessage("Cancelling replications for allocationIds {}", allocationIds));
        for (String allocationId : allocationIds) {
            cancel(allocationId, reason);
        }
    }

    /**
     * Clear copystate and target handlers for any non insync allocationIds.
     * @param shardId {@link ShardId}
     * @param inSyncAllocationIds {@link List} of in-sync allocation Ids.
     */
    public void clearOutOfSyncIds(ShardId shardId, Set<String> inSyncAllocationIds) {
        cancelHandlers(
            (handler) -> handler.getCopyState().getShard().shardId().equals(shardId)
                && inSyncAllocationIds.contains(handler.getAllocationId()) == false,
            "Shard is no longer in-sync with the primary"
        );
    }
}
