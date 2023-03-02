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
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RetryableTransportClient;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.transport.TransportRequestOptions;
import org.opensearch.transport.TransportService;

import java.util.List;

import static org.opensearch.indices.replication.SegmentReplicationSourceService.Actions.GET_CHECKPOINT_INFO;
import static org.opensearch.indices.replication.SegmentReplicationSourceService.Actions.GET_SEGMENT_FILES;

/**
 * Implementation of a {@link SegmentReplicationSource} where the source is a primary node.
 * This code executes on the target node.
 *
 * @opensearch.internal
 */
public class PrimaryShardReplicationSource implements SegmentReplicationSource {

    private static final Logger logger = LogManager.getLogger(PrimaryShardReplicationSource.class);

    private final RetryableTransportClient transportClient;

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
        this.transportClient = new RetryableTransportClient(
            transportService,
            sourceNode,
            recoverySettings.internalActionRetryTimeout(),
            logger
        );
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
        final Writeable.Reader<CheckpointInfoResponse> reader = CheckpointInfoResponse::new;
        final ActionListener<CheckpointInfoResponse> responseListener = ActionListener.map(listener, r -> r);
        final CheckpointInfoRequest request = new CheckpointInfoRequest(replicationId, targetAllocationId, targetNode, checkpoint);
        transportClient.executeRetryableAction(GET_CHECKPOINT_INFO, request, responseListener, reader);
    }

    @Override
    public void getSegmentFiles(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        List<StoreFileMetadata> filesToFetch,
        Store store,
        ActionListener<GetSegmentFilesResponse> listener
    ) {
        final Writeable.Reader<GetSegmentFilesResponse> reader = GetSegmentFilesResponse::new;
        final ActionListener<GetSegmentFilesResponse> responseListener = ActionListener.map(listener, r -> r);
        final GetSegmentFilesRequest request = new GetSegmentFilesRequest(
            replicationId,
            targetAllocationId,
            targetNode,
            filesToFetch,
            checkpoint
        );
        final TransportRequestOptions options = TransportRequestOptions.builder()
            .withTimeout(recoverySettings.internalActionLongTimeout())
            .build();
        transportClient.executeRetryableAction(GET_SEGMENT_FILES, request, options, responseListener, reader);
    }

    @Override
    public String getDescription() {
        return sourceNode.getName();
    }

    @Override
    public void cancel() {
        transportClient.cancel();
    }

}
