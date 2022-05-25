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
import static org.opensearch.indices.replication.SegmentReplicationSourceService.Actions.GET_FILES;

/**
 * Implementation of {@link SegmentReplicationSource} where the source is another Node.
 *
 * @opensearch.internal
 */
public class PeerReplicationSource implements SegmentReplicationSource {

    private DiscoveryNode targetNode;
    private String allocationId;
    private RetryableTransportClient transportClient;

    public PeerReplicationSource(
        TransportService transportService,
        RecoverySettings recoverySettings,
        DiscoveryNode sourceNode,
        DiscoveryNode targetNode,
        String allocationId
    ) {
        transportClient = new RetryableTransportClient(transportService, sourceNode, recoverySettings.internalActionLongTimeout());
        this.targetNode = targetNode;
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
        CheckpointInfoRequest request = new CheckpointInfoRequest(replicationId, allocationId, targetNode, checkpoint);
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

        GetSegmentFilesRequest request = new GetSegmentFilesRequest(replicationId, allocationId, targetNode, filesToFetch, checkpoint);
        transportClient.executeRetryableAction(GET_FILES, request, responseListener, reader);
    }
}
