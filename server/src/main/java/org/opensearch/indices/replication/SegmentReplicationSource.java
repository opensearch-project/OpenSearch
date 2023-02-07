/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.ActionListener;
import org.opensearch.common.util.CancellableThreads.ExecutionCancelledException;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.util.List;

/**
 * Represents the source of a replication event.
 *
 * @opensearch.internal
 */
public interface SegmentReplicationSource {

    /**
     * Get Metadata for a ReplicationCheckpoint.
     *
     * @param replicationId {@link long} - ID of the replication event.
     * @param checkpoint    {@link ReplicationCheckpoint} Checkpoint to fetch metadata for.
     * @param listener      {@link ActionListener} listener that completes with a {@link CheckpointInfoResponse}.
     */
    void getCheckpointMetadata(long replicationId, ReplicationCheckpoint checkpoint, ActionListener<CheckpointInfoResponse> listener);

    /**
     * Fetch the requested segment files.  Passes a listener that completes when files are stored locally.
     *
     * @param replicationId {@link long} - ID of the replication event.
     * @param checkpoint    {@link ReplicationCheckpoint} Checkpoint to fetch metadata for.
     * @param filesToFetch  {@link List} List of files to fetch.
     * @param store         {@link Store} Reference to the local store.
     * @param listener      {@link ActionListener} Listener that completes with the list of files copied.
     */
    void getSegmentFiles(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        List<StoreFileMetadata> filesToFetch,
        Store store,
        ActionListener<GetSegmentFilesResponse> listener
    );

    /**
     * Get the source description
     */
    String getDescription();

    /**
     * Cancel any ongoing requests, should resolve any ongoing listeners with onFailure with a {@link ExecutionCancelledException}.
     */
    default void cancel() {}
}
