/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.OpenSearchCorruptionException;
import org.opensearch.action.StepListener;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesReference;
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
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Abstract base class for segment replication.
 *
 * @opensearch.internal
 */
public abstract class AbstractSegmentReplicationTarget extends ReplicationTarget {

    protected final ReplicationCheckpoint checkpoint;
    protected final SegmentReplicationSource source;
    protected final SegmentReplicationState state;
    protected final MultiFileWriter multiFileWriter;
    protected final boolean isRetry;

    public AbstractSegmentReplicationTarget(
        String name,
        IndexShard indexShard,
        ReplicationCheckpoint checkpoint,
        SegmentReplicationSource source,
        boolean isRetry,
        ReplicationListener listener
    ) {
        super(name, indexShard, new ReplicationLuceneIndex(), listener);
        this.checkpoint = checkpoint;
        this.source = source;
        this.isRetry = isRetry;
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
    protected void onCancel(String reason) {
        try {
            notifyListener(new ReplicationFailedException(reason), false);
        } finally {
            source.cancel();
            cancellableThreads.cancel(reason);
        }
    }

    @Override
    protected void onDone() {
        state.setStage(SegmentReplicationState.Stage.DONE);
    }

    @Override
    public SegmentReplicationState state() {
        return state;
    }

    @Override
    public String description() {
        return String.format(
            Locale.ROOT,
            "Id:[%d] Checkpoint [%s] Shard:[%s] Source:[%s]",
            getId(),
            getCheckpoint(),
            shardId(),
            source.getDescription()
        );
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
    public void startReplication(ActionListener<Void> listener, BiConsumer<ReplicationCheckpoint, IndexShard> checkpointUpdater) {
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
        getCheckpointMetadata(checkpointInfoListener);

        checkpointInfoListener.whenComplete(checkpointInfo -> {
            ReplicationCheckpoint getMetadataCheckpoint = checkpointInfo.getCheckpoint();
            // Only enforce strict checkpoint validation during normal replication, not during recovery.
            // During recovery (shard is INITIALIZING or RELOCATING), the replica may have a stale checkpoint
            // from before a restart, and should accept the primary's current state even if it appears older.
            // See: https://github.com/opensearch-project/OpenSearch/issues/19234
            boolean isRecovering = indexShard.routingEntry().initializing() || indexShard.routingEntry().relocating();
            if (indexShard.indexSettings().isSegRepLocalEnabled()
                && checkpoint.isAheadOf(getMetadataCheckpoint)
                && false == isRecovering
                && false == isRetry) {
                // Fixes https://github.com/opensearch-project/OpenSearch/issues/18490
                listener.onFailure(
                    new ReplicationFailedException(
                        "Rejecting stale metadata checkpoint ["
                            + getMetadataCheckpoint
                            + "] since initial checkpoint ["
                            + checkpoint
                            + "] is ahead of it"
                    )
                );
                return;
            }
            updateCheckpoint(checkpointInfo.getCheckpoint(), checkpointUpdater);
            final List<StoreFileMetadata> filesToFetch = getFiles(checkpointInfo);
            state.setStage(SegmentReplicationState.Stage.GET_FILES);
            cancellableThreads.checkForCancel();
            getFilesFromSource(checkpointInfo, filesToFetch, getFilesListener);
        }, listener::onFailure);

        getFilesListener.whenComplete(response -> {
            cancellableThreads.checkForCancel();
            state.setStage(SegmentReplicationState.Stage.FINALIZE_REPLICATION);
            finalizeReplication(checkpointInfoListener.result());
            listener.onResponse(null);
        }, listener::onFailure);
    }

    protected abstract void getCheckpointMetadata(StepListener<CheckpointInfoResponse> checkpointInfoListener);

    protected abstract void updateCheckpoint(
        ReplicationCheckpoint checkpoint,
        BiConsumer<ReplicationCheckpoint, IndexShard> checkpointUpdater
    );

    protected abstract void getFilesFromSource(
        CheckpointInfoResponse checkpointInfo,
        List<StoreFileMetadata> filesToFetch,
        StepListener<GetSegmentFilesResponse> getFilesListener
    );

    protected abstract void finalizeReplication(CheckpointInfoResponse checkpointInfoResponse) throws Exception;

    protected List<StoreFileMetadata> getFiles(CheckpointInfoResponse checkpointInfo) throws IOException {
        cancellableThreads.checkForCancel();
        state.setStage(SegmentReplicationState.Stage.FILE_DIFF);

        // Return an empty list for warm indices, In this case, replica shards don't require downloading files from remote storage
        // as replicas will sync all files from remote in case of failure.
        if (indexShard.indexSettings().isWarmIndex()) {
            return Collections.emptyList();
        }
        final Store.RecoveryDiff diff = Store.segmentReplicationDiff(checkpointInfo.getMetadataMap(), indexShard.getSegmentMetadataMap());
        // local files
        final Set<String> localFiles = Set.of(indexShard.store().directory().listAll());
        // set of local files that can be reused
        final Set<String> reuseFiles = diff.missing.stream()
            .filter(storeFileMetadata -> localFiles.contains(storeFileMetadata.name()))
            .filter(this::validateLocalChecksum)
            .map(StoreFileMetadata::name)
            .collect(Collectors.toSet());

        final List<StoreFileMetadata> missingFiles = diff.missing.stream()
            .filter(md -> reuseFiles.contains(md.name()) == false)
            .collect(Collectors.toList());

        logger.trace(
            () -> new ParameterizedMessage(
                "Replication diff for checkpoint {} {} {}",
                checkpointInfo.getCheckpoint(),
                missingFiles,
                diff.different
            )
        );
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

        for (StoreFileMetadata file : missingFiles) {
            state.getIndex().addFileDetail(file.name(), file.length(), false);
        }
        return missingFiles;
    }

    // pkg private for tests
    private boolean validateLocalChecksum(StoreFileMetadata file) {
        try (IndexInput indexInput = indexShard.store().directory().openInput(file.name(), IOContext.READONCE)) {
            String checksum = Store.digestToString(CodecUtil.retrieveChecksum(indexInput));
            if (file.checksum().equals(checksum)) {
                return true;
            } else {
                // clear local copy with mismatch. Safe because file is not referenced by active reader.
                store.deleteQuiet(file.name());
                return false;
            }
        } catch (IOException e) {
            logger.warn("Error reading " + file, e);
            // Delete file on exceptions so that it can be re-downloaded. This is safe to do as this file is local only
            // and not referenced by reader.
            try {
                indexShard.store().directory().deleteFile(file.name());
            } catch (IOException ex) {
                throw new UncheckedIOException("Error reading " + file, e);
            }
            return false;
        }
    }

    /**
     * Updates the state to reflect recovery progress for the given file and
     * updates the last access time for the target.
     * @param fileName Name of the file being downloaded
     * @param bytesRecovered Number of bytes recovered
     */
    protected void updateFileRecoveryBytes(String fileName, long bytesRecovered) {
        ReplicationLuceneIndex index = state.getIndex();
        if (index != null) {
            index.addRecoveredBytesToFile(fileName, bytesRecovered);
        }
        setLastAccessTime();
    }
}
