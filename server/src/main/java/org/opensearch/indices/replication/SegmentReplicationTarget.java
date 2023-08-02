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
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.OpenSearchCorruptionException;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.common.UUIDs;
import org.opensearch.core.common.bytes.BytesReference;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

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

    public final static String REPLICATION_PREFIX = "replication.";

    public SegmentReplicationTarget(IndexShard indexShard, SegmentReplicationSource source, ReplicationListener listener) {
        super("replication_target", indexShard, new ReplicationLuceneIndex(), listener);
        this.checkpoint = indexShard.getLatestReplicationCheckpoint();
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
        return REPLICATION_PREFIX + UUIDs.randomBase64UUID() + ".";
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
        return new SegmentReplicationTarget(indexShard, source, listener);
    }

    @Override
    public String description() {
        return String.format(Locale.ROOT, "Id:[%d] Shard:[%s] Source:[%s]", getId(), shardId(), source.getDescription());
    }

    @Override
    public void notifyListener(ReplicationFailedException e, boolean sendShardFailure) {
        listener.onFailure(state(), e, sendShardFailure);
    }

    @Override
    public boolean reset(CancellableThreads newTargetCancellableThreads) throws IOException {
        // TODO
        return false;
    }

    public ReplicationCheckpoint getCheckpoint() {
        return this.checkpoint;
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
     *
     * @param listener {@link ActionListener} listener.
     */
    public void startReplication(ActionListener<Void> listener) {
        cancellableThreads.setOnCancel((reason, beforeCancelEx) -> {
            throw new CancellableThreads.ExecutionCancelledException("replication was canceled reason [" + reason + "]");
        });
        // TODO: Remove this useless state.
        state.setStage(SegmentReplicationState.Stage.REPLICATING);
        final StepListener<CheckpointInfoResponse> checkpointInfoListener = new StepListener<>();
        final StepListener<GetSegmentFilesResponse> getFilesListener = new StepListener<>();

        logger.trace(new ParameterizedMessage("Starting Replication Target: {}", description()));
        // Get list of files to copy from this checkpoint.
        state.setStage(SegmentReplicationState.Stage.GET_CHECKPOINT_INFO);
        cancellableThreads.checkForCancel();
        source.getCheckpointMetadata(getId(), checkpoint, checkpointInfoListener);

        checkpointInfoListener.whenComplete(checkpointInfo -> {
            final List<StoreFileMetadata> filesToFetch = getFiles(checkpointInfo);
            state.setStage(SegmentReplicationState.Stage.GET_FILES);
            cancellableThreads.checkForCancel();
            source.getSegmentFiles(getId(), checkpointInfo.getCheckpoint(), filesToFetch, indexShard, getFilesListener);
        }, listener::onFailure);

        getFilesListener.whenComplete(response -> {
            finalizeReplication(checkpointInfoListener.result(), getFilesListener.result());
            listener.onResponse(null);
        }, listener::onFailure);
    }

    private List<StoreFileMetadata> getFiles(CheckpointInfoResponse checkpointInfo) throws IOException {
        cancellableThreads.checkForCancel();
        state.setStage(SegmentReplicationState.Stage.FILE_DIFF);
        final Store.RecoveryDiff diff = Store.segmentReplicationDiff(checkpointInfo.getMetadataMap(), indexShard.getSegmentMetadataMap());
        logger.trace(() -> new ParameterizedMessage("Replication diff for checkpoint {} {}", checkpointInfo.getCheckpoint(), diff));
        /*
         * Segments are immutable. So if the replica has any segments with the same name that differ from the one in the incoming
         * snapshot from source that means the local copy of the segment has been corrupted/changed in some way and we throw an
         * IllegalStateException to fail the shard
         */
        if (diff.different.isEmpty() == false) {
            throw new OpenSearchCorruptionException(
                new ParameterizedMessage(
                    "Shard {} has local copies of segments that differ from the primary {}",
                    indexShard.shardId(),
                    diff.different
                ).getFormattedMessage()
            );
        }

        for (StoreFileMetadata file : diff.missing) {
            state.getIndex().addFileDetail(file.name(), file.length(), false);
        }
        return diff.missing;
    }

    private void finalizeReplication(CheckpointInfoResponse checkpointInfoResponse, GetSegmentFilesResponse getSegmentFilesResponse)
        throws OpenSearchCorruptionException {
        cancellableThreads.checkForCancel();
        state.setStage(SegmentReplicationState.Stage.FINALIZE_REPLICATION);
        // Handle empty SegmentInfos bytes for recovering replicas
        if (checkpointInfoResponse.getInfosBytes() == null) {
            return;
        }
        Store store = null;
        try {
            store = store();
            store.incRef();
            Map<String, String> tempFileNames;
            if (this.indexShard.indexSettings().isRemoteStoreEnabled() == true) {
                tempFileNames = getSegmentFilesResponse.getFiles()
                    .stream()
                    .collect(Collectors.toMap(StoreFileMetadata::name, StoreFileMetadata::name));
            } else {
                tempFileNames = multiFileWriter.getTempFileNames();
            }
            store.buildInfosFromBytes(
                tempFileNames,
                checkpointInfoResponse.getInfosBytes(),
                checkpointInfoResponse.getCheckpoint().getSegmentsGen(),
                indexShard::finalizeReplication,
                this.indexShard.indexSettings().isRemoteStoreEnabled() == true
                    ? (files) -> {}
                    : (files) -> indexShard.store().renameTempFilesSafe(files)
            );
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
            throw new OpenSearchCorruptionException(ex);
        } catch (AlreadyClosedException ex) {
            // In this case the shard is closed at some point while updating the reader.
            // This can happen when the engine is closed in a separate thread.
            logger.warn("Shard is already closed, closing replication");
        } catch (OpenSearchException ex) {
            /*
             Ignore closed replication target as it can happen due to index shard closed event in a separate thread.
             In such scenario, ignore the exception
             */
            assert cancellableThreads.isCancelled() : "Replication target closed but segment replication not cancelled";
        } catch (Exception ex) {
            throw new OpenSearchCorruptionException(ex);
        } finally {
            if (store != null) {
                store.decRef();
            }
        }
    }

    /**
     * Trigger a cancellation, this method will not close the target a subsequent call to #fail is required from target service.
     */
    @Override
    public void cancel(String reason) {
        if (finished.get() == false) {
            logger.trace(new ParameterizedMessage("Cancelling replication for target {}", description()));
            cancellableThreads.cancel(reason);
            source.cancel();
        }
    }
}
