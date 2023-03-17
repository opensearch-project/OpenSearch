/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.common.UUIDs;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.MultiFileWriter;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationListener;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.replication.common.ReplicationTarget;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Represents the target of a replication event.
 *
 * @opensearch.internal
 */
public class SegmentReplicationTarget extends ReplicationTarget {

    private final ReplicationCheckpoint checkpoint;
    private final SegmentReplicationSource source;
    private final SegmentReplicationState state;
    protected final MultiFileWriter multiFileWriter;

    public ReplicationCheckpoint getCheckpoint() {
        return this.checkpoint;
    }

    public SegmentReplicationTarget(
        ReplicationCheckpoint checkpoint,
        IndexShard indexShard,
        SegmentReplicationSource source,
        ReplicationListener listener
    ) {
        super("replication_target", indexShard, new ReplicationLuceneIndex(), listener);
        this.checkpoint = checkpoint;
        this.source = source;
        this.state = new SegmentReplicationState(
            indexShard.routingEntry(),
            stateIndex,
            getId(),
            source.getDescription(),
            indexShard.recoveryState().getTargetNode()
        );
        this.multiFileWriter = new MultiFileWriter(indexShard.store(), stateIndex, getPrefix(), logger, this::ensureRefCount);
    }

    @Override
    protected void closeInternal() {
        try {
            multiFileWriter.close();
        } finally {
            super.closeInternal();
        }
    }

    @Override
    protected String getPrefix() {
        return "replication." + UUIDs.randomBase64UUID() + ".";
    }

    @Override
    protected void onDone() {
        state.setStage(SegmentReplicationState.Stage.DONE);
    }

    @Override
    public SegmentReplicationState state() {
        return state;
    }

    public SegmentReplicationTarget retryCopy() {
        return new SegmentReplicationTarget(checkpoint, indexShard, source, listener);
    }

    @Override
    public String description() {
        return "Segment replication from " + source.toString();
    }

    @Override
    public void notifyListener(ReplicationFailedException e, boolean sendShardFailure) {
        // Cancellations still are passed to our SegmentReplicationListener as failures, if we have failed because of cancellation
        // update the stage.
        final Throwable cancelledException = ExceptionsHelper.unwrap(e, CancellableThreads.ExecutionCancelledException.class);
        if (cancelledException != null) {
            state.setStage(SegmentReplicationState.Stage.CANCELLED);
        }
        listener.onFailure(state(), e, sendShardFailure);
    }

    @Override
    public boolean reset(CancellableThreads newTargetCancellableThreads) throws IOException {
        // TODO
        return false;
    }

    @Override
    public void writeFileChunk(
        StoreFileMetadata metadata,
        long position,
        BytesReference content,
        boolean lastChunk,
        int totalTranslogOps,
        ActionListener<Void> listener
    ) {
        try {
            multiFileWriter.writeFileChunk(metadata, position, content, lastChunk);
            listener.onResponse(null);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * Start the Replication event.
     * @param listener {@link ActionListener} listener.
     */
    public void startReplication(ActionListener<Void> listener) {
        cancellableThreads.setOnCancel((reason, beforeCancelEx) -> {
            // This method only executes when cancellation is triggered by this node and caught by a call to checkForCancel,
            // SegmentReplicationSource does not share CancellableThreads.
            final CancellableThreads.ExecutionCancelledException executionCancelledException =
                new CancellableThreads.ExecutionCancelledException("replication was canceled reason [" + reason + "]");
            notifyListener(new ReplicationFailedException("Segment replication failed", executionCancelledException), false);
            throw executionCancelledException;
        });
        state.setStage(SegmentReplicationState.Stage.REPLICATING);
        final StepListener<CheckpointInfoResponse> checkpointInfoListener = new StepListener<>();
        final StepListener<GetSegmentFilesResponse> getFilesListener = new StepListener<>();
        final StepListener<Void> finalizeListener = new StepListener<>();

        cancellableThreads.checkForCancel();
        logger.trace("[shardId {}] Replica starting replication [id {}]", shardId().getId(), getId());
        // Get list of files to copy from this checkpoint.
        state.setStage(SegmentReplicationState.Stage.GET_CHECKPOINT_INFO);
        source.getCheckpointMetadata(getId(), checkpoint, checkpointInfoListener);

        checkpointInfoListener.whenComplete(checkpointInfo -> getFiles(checkpointInfo, getFilesListener), listener::onFailure);
        getFilesListener.whenComplete(
            response -> finalizeReplication(checkpointInfoListener.result(), finalizeListener),
            listener::onFailure
        );
        finalizeListener.whenComplete(r -> listener.onResponse(null), listener::onFailure);
    }

    private void getFiles(CheckpointInfoResponse checkpointInfo, StepListener<GetSegmentFilesResponse> getFilesListener)
        throws IOException {
        cancellableThreads.checkForCancel();
        state.setStage(SegmentReplicationState.Stage.FILE_DIFF);
        final Store.RecoveryDiff diff = Store.segmentReplicationDiff(checkpointInfo.getMetadataMap(), indexShard.getSegmentMetadataMap());
        logger.trace("Replication diff for checkpoint {} {}", checkpointInfo.getCheckpoint(), diff);
        /*
         * Segments are immutable. So if the replica has any segments with the same name that differ from the one in the incoming
         * snapshot from source that means the local copy of the segment has been corrupted/changed in some way and we throw an
         * IllegalStateException to fail the shard
         */
        if (diff.different.isEmpty() == false) {
            IllegalStateException illegalStateException = new IllegalStateException(
                new ParameterizedMessage(
                    "Shard {} has local copies of segments that differ from the primary {}",
                    indexShard.shardId(),
                    diff.different
                ).getFormattedMessage()
            );
            ReplicationFailedException rfe = new ReplicationFailedException(
                indexShard.shardId(),
                "different segment files",
                illegalStateException
            );
            fail(rfe, true);
            throw rfe;
        }

        for (StoreFileMetadata file : diff.missing) {
            state.getIndex().addFileDetail(file.name(), file.length(), false);
        }
        // always send a req even if not fetching files so the primary can clear the copyState for this shard.
        state.setStage(SegmentReplicationState.Stage.GET_FILES);
        cancellableThreads.checkForCancel();
        source.getSegmentFiles(getId(), checkpointInfo.getCheckpoint(), diff.missing, store, getFilesListener);
    }

    private void finalizeReplication(CheckpointInfoResponse checkpointInfoResponse, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            cancellableThreads.checkForCancel();
            state.setStage(SegmentReplicationState.Stage.FINALIZE_REPLICATION);
            try {
                multiFileWriter.renameAllTempFiles();
                final Store store = store();
                store.incRef();
                // Deserialize the new SegmentInfos object sent from the primary.
                final ReplicationCheckpoint responseCheckpoint = checkpointInfoResponse.getCheckpoint();
                SegmentInfos infos = SegmentInfos.readCommit(
                    store.directory(),
                    toIndexInput(checkpointInfoResponse.getInfosBytes()),
                    responseCheckpoint.getSegmentsGen()
                );
                cancellableThreads.checkForCancel();
                indexShard.finalizeReplication(infos);
                store.cleanupAndPreserveLatestCommitPoint("finalize - clean with in memory infos", infos);
            } catch (CorruptIndexException | IndexFormatTooNewException | IndexFormatTooOldException ex) {
                // this is a fatal exception at this stage.
                // this means we transferred files from the remote that have not be checksummed and they are
                // broken. We have to clean up this shard entirely, remove all files and bubble it up to the
                // source shard since this index might be broken there as well? The Source can handle this and checks
                // its content on disk if possible.
                try {
                    try {
                        store.removeCorruptionMarker();
                    } finally {
                        Lucene.cleanLuceneIndex(store.directory()); // clean up and delete all files
                    }
                } catch (Exception e) {
                    logger.debug("Failed to clean lucene index", e);
                    ex.addSuppressed(e);
                }
                ReplicationFailedException rfe = new ReplicationFailedException(
                    indexShard.shardId(),
                    "failed to clean after replication",
                    ex
                );
                fail(rfe, true);
                throw rfe;
            } catch (Exception ex) {
                ReplicationFailedException rfe = new ReplicationFailedException(
                    indexShard.shardId(),
                    "failed to clean after replication",
                    ex
                );
                fail(rfe, true);
                throw rfe;
            } finally {
                store.decRef();
            }
            return null;
        });
    }

    /**
     * This method formats our byte[] containing the primary's SegmentInfos into lucene's {@link ChecksumIndexInput} that can be
     * passed to SegmentInfos.readCommit
     */
    private ChecksumIndexInput toIndexInput(byte[] input) {
        return new BufferedChecksumIndexInput(
            new ByteBuffersIndexInput(new ByteBuffersDataInput(Arrays.asList(ByteBuffer.wrap(input))), "SegmentInfos")
        );
    }

    @Override
    protected void onCancel(String reason) {
        cancellableThreads.cancel(reason);
        source.cancel();
    }
}
