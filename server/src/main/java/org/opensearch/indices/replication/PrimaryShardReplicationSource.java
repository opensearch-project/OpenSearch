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

/**
 * Implementation of a {@link SegmentReplicationSource} where the source is a primary node
 *
 * @opensearch.internal
 */
public class PrimaryShardReplicationSource implements SegmentReplicationSource {

    /**
     * Internal actions used by the segment replication source service on the primary shard
     *
     * @opensearch.internal
     */
    public static class Actions {
        public static final String GET_CHECKPOINT_INFO = "internal:index/shard/replication/get_checkpoint_info";
        public static final String GET_SEGMENT_FILES = "internal:index/shard/replication/get_segment_files";
    }

    private final RetryableTransportClient transportClient;
    private final RecoverySettings recoverySettings;
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
        this.recoverySettings = recoverySettings;
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
        // TODO CheckpointInfoRequest and execute action
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
        // TODO GetSegmentFilesRequest and execute action
    }
}
