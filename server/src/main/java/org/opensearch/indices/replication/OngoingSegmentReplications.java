/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.ActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.index.IndexService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.CopyState;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Manages references to ongoing segrep events on a node.
 *
 * @opensearch.internal
 */
class OngoingSegmentReplications {

    private final RecoverySettings recoverySettings;
    private final IndicesService indicesService;
    private final Map<ReplicationCheckpoint, CopyState> copyStateMap;
    private final Map<DiscoveryNode, SegmentReplicationSourceHandler> nodesToHandlers;

    /**
     * Constructor.
     * @param indicesService {@link IndicesService}
     * @param recoverySettings {@link RecoverySettings}
     */
    OngoingSegmentReplications(IndicesService indicesService, RecoverySettings recoverySettings) {
        this.indicesService = indicesService;
        this.recoverySettings = recoverySettings;
        this.copyStateMap = Collections.synchronizedMap(new HashMap<>());
        this.nodesToHandlers = Collections.synchronizedMap(new HashMap<>());
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

    void cancelReplication(DiscoveryNode node) {
        if (nodesToHandlers.containsKey(node)) {
            final SegmentReplicationSourceHandler handler = nodesToHandlers.remove(node);
            handler.cancel("Cancel on node left");
            removeCopyState(handler.getCopyState());
        }
    }

    SegmentReplicationSourceHandler createTargetHandler(
        DiscoveryNode node,
        CopyState copyState,
        RemoteSegmentFileChunkWriter segmentFileChunkWriter
    ) {
        return new SegmentReplicationSourceHandler(
            node,
            segmentFileChunkWriter,
            copyState.getShard().getThreadPool(),
            copyState,
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
     * Checks if the {@link #copyStateMap} has the input {@link ReplicationCheckpoint}
     * as a key by invoking {@link Map#containsKey(Object)}.
     */
    boolean isInCopyStateMap(ReplicationCheckpoint replicationCheckpoint) {
        return copyStateMap.containsKey(replicationCheckpoint);
    }

    void startSegmentCopy(GetSegmentFilesRequest request, ActionListener<GetSegmentFilesResponse> listener) {
        final DiscoveryNode node = request.getTargetNode();
        if (nodesToHandlers.containsKey(node)) {
            final SegmentReplicationSourceHandler handler = nodesToHandlers.get(node);
            // update the given listener to release the CopyState before it resolves.
            final ActionListener<GetSegmentFilesResponse> wrappedListener = ActionListener.runBefore(listener, () -> {
                final SegmentReplicationSourceHandler sourceHandler = nodesToHandlers.remove(node);
                removeCopyState(sourceHandler.getCopyState());
            });
            handler.sendFiles(request, wrappedListener);
        } else {
            listener.onResponse(new GetSegmentFilesResponse(Collections.emptyList()));
        }
    }

    /**
     * Remove a CopyState. Intended to be called after a replication event completes.
     * This method will remove a copyState from the copyStateMap only if its refCount hits 0.
     * @param copyState {@link CopyState}
     */
    private synchronized void removeCopyState(CopyState copyState) {
        copyState.decRef();
        if (copyState.refCount() <= 0) {
            copyStateMap.remove(copyState.getRequestedReplicationCheckpoint());
        }
    }

    /**
     * Prepare for a Replication event. This method constructs a {@link CopyState} holding files to be sent off of the current
     * nodes's store.  This state is intended to be sent back to Replicas before copy is initiated so the replica can perform a diff against its
     * local store.  It will then build a handler to orchestrate the segment copy that will be stored locally and started on a subsequent request from replicas
     * with the list of required files.
     * @param request {@link CheckpointInfoRequest}
     * @param segmentSegmentFileChunkWriter {@link RemoteSegmentFileChunkWriter} writer to handle sending files over the transport layer.
     * @return {@link CopyState} the built CopyState for this replication event.
     * @throws IOException - When there is an IO error building CopyState.
     */
    CopyState prepareForReplication(CheckpointInfoRequest request, RemoteSegmentFileChunkWriter segmentSegmentFileChunkWriter)
        throws IOException {
        final CopyState copyState = getCachedCopyState(request.getCheckpoint());
        final SegmentReplicationSourceHandler handler = createTargetHandler(
            request.getTargetNode(),
            copyState,
            segmentSegmentFileChunkWriter
        );
        nodesToHandlers.putIfAbsent(request.getTargetNode(), handler);
        return copyState;
    }

    int size() {
        return nodesToHandlers.size();
    }

    int cachedCopyStateSize() {
        return copyStateMap.size();
    }

    /**
     * Cancel all Replication events for the given shard, intended to be called when the current primary is shutting down.
     * @param shard {@link IndexShard}
     * @param reason  {@link String} - Reason for the cancel
     */
    public void cancel(IndexShard shard, String reason) {
        for (SegmentReplicationSourceHandler entry : nodesToHandlers.values()) {
            if (entry.getCopyState().getShard().equals(shard)) {
                entry.cancel(reason);
            }
        }
        copyStateMap.clear();
    }
}
