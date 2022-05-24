/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.StepListener;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;

import java.util.List;

/**
 * Represents the source of a replication event.
 *
 * @opensearch.internal
 */
public interface SegmentReplicationSource {

    void getCheckpointMetadata(long replicationId, ReplicationCheckpoint checkpoint, StepListener<CheckpointInfoResponse> listener);

    void getFiles(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        Store store,
        List<StoreFileMetadata> filesToFetch,
        StepListener<GetSegmentFilesResponse> listener
    );
}
