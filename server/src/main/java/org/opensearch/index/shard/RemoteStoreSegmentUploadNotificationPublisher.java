/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.common.inject.Inject;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.checkpoint.SegmentReplicationCheckpointPublisher;


/**
 * Hook to publish notification after primary uploads segments to the remote store.
 *
 * @opensearch.internal
 */
public class RemoteStoreSegmentUploadNotificationPublisher {
    private final SegmentReplicationCheckpointPublisher segRepPublisher;

    @Inject
    public RemoteStoreSegmentUploadNotificationPublisher(SegmentReplicationCheckpointPublisher segRepPublisher) {
        this.segRepPublisher = segRepPublisher;
    }

    public void notifySegmentUpload(IndexShard indexShard, ReplicationCheckpoint checkpoint) {
        // TODO: Add separate publisher for CCR.
        // we don't call indexShard.getLatestReplicationCheckpoint() as it might have a newer refreshed checkpoint.
        // Instead we send the one which has been uploaded to remote store.
        if (segRepPublisher != null) segRepPublisher.publish(indexShard, checkpoint);
    }

    public static final RemoteStoreSegmentUploadNotificationPublisher EMPTY = new RemoteStoreSegmentUploadNotificationPublisher(null);
}
