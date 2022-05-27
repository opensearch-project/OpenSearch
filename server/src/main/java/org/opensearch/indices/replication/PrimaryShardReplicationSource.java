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
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RetryableTransportClient;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.transport.TransportService;

import java.util.List;

import static org.opensearch.indices.replication.SegmentReplicationSourceService.Actions.GET_CHECKPOINT_INFO;
import static org.opensearch.indices.replication.SegmentReplicationSourceService.Actions.GET_SEGMENT_FILES;

/**
 * Implementation of a {@link SegmentReplicationSource} where the source is a primary node
 *
 * @opensearch.internal
 */
public class PrimaryShardReplicationSource implements SegmentReplicationSource {

    private final RetryableTransportClient transportClient;
    private final DiscoveryNode localNode;
    private final String allocationId;

    public PrimaryShardReplicationSource(
        TransportService transportService,
        RecoverySettings recoverySettings,
        DiscoveryNode targetNode,
        DiscoveryNode localNode,
        String allocationId
    ) {
        this.transportClient = new RetryableTransportClient(transportService, targetNode, recoverySettings.internalActionRetryTimeout());
        this.localNode = localNode;
        this.allocationId = allocationId;
    }

    @Override
    public void getCheckpointMetadata(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        ActionListener<CheckpointInfoResponse> listener
    ) {
        final Writeable.Reader<CheckpointInfoResponse> reader = CheckpointInfoResponse::new;
        final ActionListener<CheckpointInfoResponse> responseListener = ActionListener.map(listener, r -> r);
        final CheckpointInfoRequest request = new CheckpointInfoRequest(replicationId, allocationId, localNode, checkpoint);
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
        final GetSegmentFilesRequest request = new GetSegmentFilesRequest(replicationId, allocationId, localNode, filesToFetch, checkpoint);
        transportClient.executeRetryableAction(GET_SEGMENT_FILES, request, responseListener, reader);
    }
}
