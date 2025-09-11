/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.opensearch.action.StepListener;
import org.opensearch.common.UUIDs;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.MergedSegmentCheckpoint;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationListener;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * Represents the target of a merged segment replication event.
 *
 * @opensearch.internal
 */
public class MergedSegmentReplicationTarget extends AbstractSegmentReplicationTarget {
    public final static String MERGE_REPLICATION_PREFIX = "merge.";

    public MergedSegmentReplicationTarget(
        IndexShard indexShard,
        ReplicationCheckpoint checkpoint,
        SegmentReplicationSource source,
        ReplicationListener listener
    ) {
        super("merged_segment_replication_target", indexShard, checkpoint, source, listener);
    }

    @Override
    protected String getPrefix() {
        return MERGE_REPLICATION_PREFIX + UUIDs.randomBase64UUID() + ".";
    }

    @Override
    protected void getCheckpointMetadata(StepListener<CheckpointInfoResponse> checkpointInfoListener) {
        checkpointInfoListener.onResponse(new CheckpointInfoResponse(checkpoint, checkpoint.getMetadataMap(), null));
    }

    @Override
    protected void updateCheckpoint(ReplicationCheckpoint checkpoint, BiConsumer<ReplicationCheckpoint, IndexShard> checkpointUpdater) {}

    @Override
    protected void getFilesFromSource(
        CheckpointInfoResponse checkpointInfo,
        List<StoreFileMetadata> filesToFetch,
        StepListener<GetSegmentFilesResponse> getFilesListener
    ) {
        source.getMergedSegmentFiles(
            getId(),
            checkpoint,
            filesToFetch,
            indexShard,
            this::updateMergedSegmentFileRecoveryBytes,
            getFilesListener
        );
    }

    @Override
    protected void finalizeReplication(CheckpointInfoResponse checkpointInfoResponse) throws Exception {
        assert checkpoint instanceof MergedSegmentCheckpoint;
        multiFileWriter.renameAllTempFiles();
        indexShard.addPendingMergeSegmentCheckpoint((MergedSegmentCheckpoint) checkpoint);
    }

    @Override
    public MergedSegmentReplicationTarget retryCopy() {
        return new MergedSegmentReplicationTarget(indexShard, checkpoint, source, listener);
    }

    protected void updateMergedSegmentFileRecoveryBytes(String fileName, long bytesRecovered) {
        indexShard.mergedSegmentTransferTracker().addTotalBytesReceived(bytesRecovered);
        updateFileRecoveryBytes(fileName, bytesRecovered);
    }
}
