/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.OpenSearchCorruptionException;
import org.opensearch.action.StepListener;
import org.opensearch.common.UUIDs;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;
import org.opensearch.indices.replication.common.ReplicationFailedException;
import org.opensearch.indices.replication.common.ReplicationListener;

import java.util.List;
import java.util.function.BiConsumer;

/**
 * Represents the target of a replication event.
 *
 * @opensearch.internal
 */
public class SegmentReplicationTarget extends AbstractSegmentReplicationTarget {
    public final static String REPLICATION_PREFIX = "replication.";

    public SegmentReplicationTarget(
        IndexShard indexShard,
        ReplicationCheckpoint checkpoint,
        SegmentReplicationSource source,
        ReplicationListener listener
    ) {
        super("replication_target", indexShard, checkpoint, source, listener);
    }

    @Override
    protected String getPrefix() {
        return REPLICATION_PREFIX + UUIDs.randomBase64UUID() + ".";
    }

    @Override
    protected void getCheckpointMetadata(StepListener<CheckpointInfoResponse> checkpointInfoListener) {
        source.getCheckpointMetadata(getId(), checkpoint, checkpointInfoListener);
    }

    @Override
    protected void updateCheckpoint(ReplicationCheckpoint checkpoint, BiConsumer<ReplicationCheckpoint, IndexShard> checkpointUpdater) {
        checkpointUpdater.accept(checkpoint, this.indexShard);
    }

    @Override
    protected void getFilesFromSource(
        CheckpointInfoResponse checkpointInfo,
        List<StoreFileMetadata> filesToFetch,
        StepListener<GetSegmentFilesResponse> getFilesListener
    ) {
        source.getSegmentFiles(
            getId(),
            checkpointInfo.getCheckpoint(),
            filesToFetch,
            indexShard,
            this::updateFileRecoveryBytes,
            getFilesListener
        );
    }

    @Override
    protected void finalizeReplication(CheckpointInfoResponse checkpointInfoResponse) throws Exception {
        // Handle empty SegmentInfos bytes for recovering replicas
        if (checkpointInfoResponse.getInfosBytes() == null) {
            return;
        }
        Store store = null;
        try {
            store = store();
            store.incRef();
            multiFileWriter.renameAllTempFiles();
            final SegmentInfos infos = store.buildSegmentInfos(
                checkpointInfoResponse.getInfosBytes(),
                checkpointInfoResponse.getCheckpoint().getSegmentsGen()
            );
            indexShard.finalizeReplication(infos);
        } catch (CorruptIndexException | IndexFormatTooNewException | IndexFormatTooOldException ex) {
            // this is a fatal exception at this stage.
            // this means we transferred files from the remote that have not be checksummed and they are
            // broken. We have to clean up this shard entirely, remove all files and bubble it up.
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
        } catch (CancellableThreads.ExecutionCancelledException ex) {
            /*
             Ignore closed replication target as it can happen due to index shard closed event in a separate thread.
             In such scenario, ignore the exception
             */
            assert cancellableThreads.isCancelled() : "Replication target cancelled but cancellable threads not cancelled";
        } catch (Exception ex) {
            throw new ReplicationFailedException(ex);
        } finally {
            if (store != null) {
                store.decRef();
            }
        }
    }

    @Override
    public SegmentReplicationTarget retryCopy() {
        return new SegmentReplicationTarget(indexShard, checkpoint, source, listener);
    }
}
