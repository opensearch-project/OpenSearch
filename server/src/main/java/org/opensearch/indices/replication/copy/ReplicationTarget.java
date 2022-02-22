/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.indices.replication.copy;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.Directory;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.common.UUIDs;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.util.concurrent.AbstractRefCounted;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.MultiFileWriter;
import org.opensearch.indices.recovery.RecoveryRequestTracker;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.indices.replication.SegmentReplicationReplicaService;
import org.opensearch.indices.replication.checkpoint.TransportCheckpointInfoResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Orchestrates a replication event for a replica shard.
 */
public class ReplicationTarget extends AbstractRefCounted {

    public ReplicationCheckpoint getCheckpoint() {
        return checkpoint;
    }

    private final ReplicationCheckpoint checkpoint;
    private static final AtomicLong idGenerator = new AtomicLong();
    private final AtomicBoolean finished = new AtomicBoolean();
    private final long replicationId;
    private final IndexShard indexShard;
    private final Logger logger;
    private final PrimaryShardReplicationSource source;
    private final SegmentReplicationReplicaService.ReplicationListener listener;
    private final Store store;
    private final MultiFileWriter multiFileWriter;
    private final RecoveryRequestTracker requestTracker = new RecoveryRequestTracker();
    private final ReplicationState state;
    private volatile long lastAccessTime = System.nanoTime();

    private static final String REPLICATION_PREFIX = "replication.";

    /**
     * Creates a new replication target object that represents a replication to the provided source.
     *
     * @param indexShard local shard where we want to recover to
     * @param source     source of the recovery where we recover from
     * @param listener   called when recovery is completed/failed
     */
    public ReplicationTarget(ReplicationCheckpoint checkpoint, IndexShard indexShard, PrimaryShardReplicationSource source, SegmentReplicationReplicaService.ReplicationListener listener) {
        super("replication_status");
        this.checkpoint = checkpoint;
        this.indexShard = indexShard;
        this.logger = Loggers.getLogger(getClass(), indexShard.shardId());
        this.replicationId = idGenerator.incrementAndGet();
        this.source = source;
        this.listener = listener;
        this.store = indexShard.store();
        final String tempFilePrefix = REPLICATION_PREFIX + UUIDs.randomBase64UUID() + ".";
        state = new ReplicationState(new RecoveryState.Index());
        this.multiFileWriter = new MultiFileWriter(
            indexShard.store(),
            state.getIndex(),
            tempFilePrefix,
            logger,
            this::ensureRefCount
        );
        ;
        // make sure the store is not released until we are done.
        store.incRef();
    }

    public void startReplication(ActionListener<ReplicationResponse> listener) {
        final StepListener<TransportCheckpointInfoResponse> checkpointInfoListener = new StepListener<>();
        final StepListener<GetFilesResponse> getFilesListener = new StepListener<>();
        final StepListener<Void> finalizeListener = new StepListener<>();

        // Get list of files to copy from this checkpoint.
        source.getCheckpointInfo(replicationId, checkpoint, checkpointInfoListener);

        checkpointInfoListener.whenComplete(checkpointInfo -> getFiles(checkpointInfo, getFilesListener), listener::onFailure);
        getFilesListener.whenComplete(response -> finalizeReplication(checkpointInfoListener.result(), finalizeListener), listener::onFailure);
        finalizeListener.whenComplete(r -> listener.onResponse(new ReplicationResponse()), listener::onFailure);
    }


    public Store store() {
        ensureRefCount();
        return store;
    }

    public void finalizeReplication(TransportCheckpointInfoResponse checkpointInfo, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            // first, we go and move files that were created with the recovery id suffix to
            // the actual names, its ok if we have a corrupted index here, since we have replicas
            // to recover from in case of a full cluster shutdown just when this code executes...
            multiFileWriter.renameAllTempFiles();
            final Store store = store();
            store.incRef();
            try {
                store.cleanupAndVerify("recovery CleanFilesRequestHandler", checkpointInfo.getSnapshot());
                if (indexShard.getRetentionLeases().leases().isEmpty()) {
                    // if empty, may be a fresh IndexShard, so write an empty leases file to disk
                    indexShard.persistRetentionLeases();
                    assert indexShard.loadRetentionLeases().leases().isEmpty();
                } else {
                    assert indexShard.assertRetentionLeasesPersisted();
                }
                final long segmentsGen = checkpointInfo.getCheckpoint().getSegmentsGen();
                // force an fsync if we are receiving a new gen.
                if (segmentsGen > indexShard.getLatestSegmentInfos().getGeneration()) {
                    final Directory directory = store().directory();
                    directory.sync(Arrays.asList(directory.listAll()));
                }
                indexShard.updateCurrentInfos(segmentsGen, checkpointInfo.getInfosBytes(), checkpointInfo.getCheckpoint().getSeqNo());
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
                ReplicationFailedException rfe = new ReplicationFailedException(indexShard.shardId(), "failed to clean after recovery", ex);
                fail(rfe, true);
                throw rfe;
            } catch (Exception ex) {
                ReplicationFailedException rfe = new ReplicationFailedException(indexShard.shardId(), "failed to clean after recovery", ex);
                fail(rfe, true);
                throw rfe;
            } finally {
                store.decRef();
            }
            return null;
        });
    }

    public long getReplicationId() {
        return replicationId;
    }

    public IndexShard getIndexShard() {
        return indexShard;
    }

    @Override
    protected void closeInternal() {
        store.decRef();
    }

    public ReplicationState state() {
        return state;
    }

    private void getFiles(TransportCheckpointInfoResponse checkpointInfo, StepListener<GetFilesResponse> getFilesListener) throws IOException {
        final Store.MetadataSnapshot snapshot = checkpointInfo.getSnapshot();
        Store.MetadataSnapshot localMetadata = getMetadataSnapshot();
        final Store.RecoveryDiff diff = snapshot.recoveryDiff(localMetadata);
        logger.debug("Recovery diff {}", diff);
        final List<StoreFileMetadata> filesToFetch = Stream.concat(diff.missing.stream(), diff.different.stream())
            .collect(Collectors.toList());
        for (StoreFileMetadata file : filesToFetch) {
            state.getIndex().addFileDetail(file.name(), file.length(), false);
        }
        if (filesToFetch.isEmpty()) {
            getFilesListener.onResponse(new GetFilesResponse());
        }
        source.getFiles(replicationId, checkpointInfo.getCheckpoint(), filesToFetch, getFilesListener);
    }

    private Store.MetadataSnapshot getMetadataSnapshot() throws IOException {
        if (indexShard.recoveryState().getStage() == RecoveryState.Stage.INIT) {
            return Store.MetadataSnapshot.EMPTY;
        }
        return store.getMetadata(indexShard.getLatestSegmentInfos());
    }

    public ActionListener<Void> markRequestReceivedAndCreateListener(long requestSeqNo, ActionListener<Void> listener) {
        return requestTracker.markReceivedAndCreateListener(requestSeqNo, listener);
    }

    public void writeFileChunk(
        StoreFileMetadata fileMetadata,
        long position,
        BytesReference content,
        boolean lastChunk,
        ActionListener<Void> listener
    ) {
        try {
            multiFileWriter.writeFileChunk(fileMetadata, position, content, lastChunk);
            listener.onResponse(null);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    /**
     * return the last time this RecoveryStatus was used (based on System.nanoTime()
     */
    public long lastAccessTime() {
        return lastAccessTime;
    }

    /**
     * sets the lasAccessTime flag to now
     */
    public void setLastAccessTime() {
        lastAccessTime = System.nanoTime();
    }

    private void ensureRefCount() {
        if (refCount() <= 0) {
            throw new OpenSearchException(
                "RecoveryStatus is used but it's refcount is 0. Probably a mismatch between incRef/decRef " + "calls"
            );
        }
    }

    /**
     * mark the current recovery as done
     */
    public void markAsDone() {
        if (finished.compareAndSet(false, true)) {
            try {
                // might need to do something on index shard here.
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
            listener.onReplicationDone(state());
        }
    }

    /**
     * fail the recovery and call listener
     *
     * @param e                exception that encapsulating the failure
     * @param sendShardFailure indicates whether to notify the master of the shard failure
     */
    public void fail(ReplicationFailedException e, boolean sendShardFailure) {
        if (finished.compareAndSet(false, true)) {
            try {
                listener.onReplicationFailure(state(), e, sendShardFailure);
            } finally {
                try {
//                    cancellableThreads.cancel("failed recovery [" + ExceptionsHelper.stackTrace(e) + "]");
                } finally {
                    // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                    decRef();
                }
            }
        }
    }

    /**
     * cancel the recovery. calling this method will clean temporary files and release the store
     * unless this object is in use (in which case it will be cleaned once all ongoing users call
     * {@link #decRef()}
     */
    public void cancel(String reason) {
        if (finished.compareAndSet(false, true)) {
            try {
                logger.debug("recovery canceled (reason: [{}])", reason);
//                cancellableThreads.cancel(reason);
            } finally {
                // release the initial reference. recovery files will be cleaned as soon as ref count goes to zero, potentially now
                decRef();
            }
        }
    }
}
