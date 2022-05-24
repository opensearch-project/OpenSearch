/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.recovery.RetryableTransportClient;
import org.opensearch.transport.TransportService;

import java.util.List;

import static org.opensearch.indices.replication.SegmentReplicationSourceService.Actions.GET_CHECKPOINT_INFO;
import static org.opensearch.indices.replication.SegmentReplicationSourceService.Actions.GET_FILES;

/**
 * Implementation of {@link SegmentReplicationSource} where the source is another Node.
 *
 * @opensearch.internal
 */
public class PeerReplicationSource extends RetryableTransportClient implements SegmentReplicationSource {

    private DiscoveryNode localNode;
    private String allocationId;

    public PeerReplicationSource(
        TransportService transportService,
        RecoverySettings recoverySettings,
        DiscoveryNode targetNode,
        DiscoveryNode localNode,
        String allocationId
    ) {
        super(transportService, recoverySettings, targetNode);
        this.localNode = localNode;
        this.allocationId = allocationId;
    }

    @Override
    public void getCheckpointMetadata(long replicationId, ReplicationCheckpoint checkpoint, StepListener<CheckpointInfoResponse> listener) {
        final Writeable.Reader<CheckpointInfoResponse> reader = CheckpointInfoResponse::new;
        final ActionListener<CheckpointInfoResponse> responseListener = ActionListener.map(listener, r -> r);
        GetCheckpointInfoRequest request = new GetCheckpointInfoRequest(replicationId, allocationId, localNode, checkpoint);
        executeRetryableAction(GET_CHECKPOINT_INFO, request, responseListener, reader);
    }

    @Override
    public void getFiles(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        Store store,
        List<StoreFileMetadata> filesToFetch,
        StepListener<GetFilesResponse> listener
    ) {
        final Writeable.Reader<GetFilesResponse> reader = GetFilesResponse::new;
        final ActionListener<GetFilesResponse> responseListener = ActionListener.map(listener, r -> r);

        GetFilesRequest request = new GetFilesRequest(replicationId, allocationId, localNode, filesToFetch, checkpoint);
        executeRetryableAction(GET_FILES, request, responseListener, reader);
    }
}
