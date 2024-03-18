/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.ActionListenerResponseHandler;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import java.util.List;
import java.util.function.BiConsumer;

import static org.opensearch.indices.replication.SegmentReplicationSourceService.Actions.GET_CHECKPOINT_INFO;
import static org.opensearch.indices.replication.SegmentReplicationSourceService.Actions.GET_SEGMENT_FILES;

/**
 * Implementation of a {@link SegmentReplicationSource} where the source is a primary node.
 * This code executes on the target node.
 *
 * @opensearch.internal
 */
public class PrimaryShardReplicationSource implements SegmentReplicationSource {

    private final TransportService transportService;

    private final DiscoveryNode sourceNode;
    private final DiscoveryNode targetNode;
    private final String targetAllocationId;
    private final RecoverySettings recoverySettings;

    public PrimaryShardReplicationSource(
        DiscoveryNode targetNode,
        String targetAllocationId,
        TransportService transportService,
        RecoverySettings recoverySettings,
        DiscoveryNode sourceNode
    ) {
        this.targetAllocationId = targetAllocationId;
        this.transportService = transportService;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.recoverySettings = recoverySettings;
    }

    @Override
    public void getCheckpointMetadata(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        ActionListener<CheckpointInfoResponse> listener
    ) {
        final CheckpointInfoRequest request = new CheckpointInfoRequest(replicationId, targetAllocationId, targetNode, checkpoint);
        transportService.sendRequest(
            sourceNode,
            GET_CHECKPOINT_INFO,
            request,
            TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionRetryTimeout()).build(),
            new ActionListenerResponseHandler<>(listener, CheckpointInfoResponse::new, ThreadPool.Names.GENERIC)
        );
    }

    @Override
    public void getSegmentFiles(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        List<StoreFileMetadata> filesToFetch,
        IndexShard indexShard,
        BiConsumer<String, Long> fileProgressTracker,
        ActionListener<GetSegmentFilesResponse> listener
    ) {
        // fileProgressTracker is a no-op for node to node recovery
        // MultiFileWriter takes care of progress tracking for downloads in this scenario
        // TODO: Move state management and tracking into replication methods and use chunking and data
        // copy mechanisms only from MultiFileWriter
        final GetSegmentFilesRequest request = new GetSegmentFilesRequest(
            replicationId,
            targetAllocationId,
            targetNode,
            filesToFetch,
            checkpoint
        );
        transportService.sendRequest(
            sourceNode,
            GET_SEGMENT_FILES,
            request,
            TransportRequestOptions.builder().withTimeout(recoverySettings.internalActionLongTimeout()).build(),
            new ActionListenerResponseHandler<>(listener, GetSegmentFilesResponse::new, ThreadPool.Names.GENERIC)
        );
    }

    @Override
    public String getDescription() {
        return sourceNode.getName();
    }
}
