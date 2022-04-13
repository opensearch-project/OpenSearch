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

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ByteBuffersDataInput;
import org.apache.lucene.store.ByteBuffersIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.StepListener;
import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.RecoveryIndex;
import org.opensearch.indices.replication.SegmentReplicationReplicaService;
import org.opensearch.indices.replication.checkpoint.TransportCheckpointInfoResponse;
import org.opensearch.indices.replication.common.ReplicationTarget;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Orchestrates a replication event for a replica shard.
 */
public class SegmentReplicationTarget extends ReplicationTarget {

    private static final String REPLICATION_PREFIX = "replication.";

    private final ReplicationCheckpoint checkpoint;
    private final PrimaryShardReplicationSource source;
    private final SegmentReplicationState state;

    /**
     * Creates a new replication target object that represents a replication to the provided source.
     *
     * @param indexShard local shard where we want to recover to
     * @param source     source of the recovery where we recover from
     * @param listener   called when recovery is completed/failed
     */
    public SegmentReplicationTarget(
        ReplicationCheckpoint checkpoint,
        IndexShard indexShard,
        PrimaryShardReplicationSource source,
        SegmentReplicationReplicaService.SegmentReplicationListener listener
    ) {
        super("replication_status", indexShard, new RecoveryIndex(), listener);
        this.checkpoint = checkpoint;
        this.source = source;
        state = new SegmentReplicationState(recoveryStateIndex);
    }

    @Override
    protected String getPrefix() {
        return REPLICATION_PREFIX;
    }

    @Override
    protected void onDone() {
        indexShard.markReplicationComplete();
    }

    @Override
    protected void onCancel(String reason) {
        indexShard.markReplicationComplete();
    }

    @Override
    protected void onFail(OpenSearchException e, boolean sendShardFailure) {
        indexShard.markReplicationComplete();
    }

    /**
     * Wrapper method around {@link #fail(OpenSearchException, boolean)}
     * to enforce stronger typing of the input exception instance.
     */
    public void fail(ReplicationFailedException e, boolean sendShardFailure) {
        super.fail(e, sendShardFailure);
    }

    @Override
    public SegmentReplicationState state() {
        return state;
    }

    @Override
    public DiscoveryNode sourceNode() {
        return null;
    }

    public void startReplication(ActionListener<ReplicationResponse> listener) {
        final StepListener<TransportCheckpointInfoResponse> checkpointInfoListener = new StepListener<>();
        final StepListener<GetFilesResponse> getFilesListener = new StepListener<>();
        final StepListener<Void> finalizeListener = new StepListener<>();

        // Get list of files to copy from this checkpoint.
        source.getCheckpointInfo(getId(), checkpoint, checkpointInfoListener);

        checkpointInfoListener.whenComplete(checkpointInfo -> getFiles(checkpointInfo, getFilesListener), listener::onFailure);
        getFilesListener.whenComplete(
            response -> finalizeReplication(checkpointInfoListener.result(), finalizeListener),
            listener::onFailure
        );
        finalizeListener.whenComplete(r -> listener.onResponse(new ReplicationResponse()), listener::onFailure);
    }

    private void getFiles(TransportCheckpointInfoResponse checkpointInfo, StepListener<GetFilesResponse> getFilesListener)
        throws IOException {
        final Store.MetadataSnapshot snapshot = checkpointInfo.getSnapshot();
        Store.MetadataSnapshot localMetadata = getMetadataSnapshot();
        final Store.RecoveryDiff diff = snapshot.recoveryDiff(localMetadata);
        logger.debug("Recovery diff {}", diff);
        final List<StoreFileMetadata> filesToFetch = Stream.concat(diff.missing.stream(), diff.different.stream())
            .collect(Collectors.toList());

        Set<String> storeFiles = new HashSet<>(Arrays.asList(store.directory().listAll()));
        final Set<StoreFileMetadata> pendingDeleteFiles = checkpointInfo.getPendingDeleteFiles()
            .stream()
            .filter(f -> storeFiles.contains(f.name()) == false)
            .collect(Collectors.toSet());

        filesToFetch.addAll(pendingDeleteFiles);

        for (StoreFileMetadata file : filesToFetch) {
            state.getIndex().addFileDetail(file.name(), file.length(), false);
        }
        if (filesToFetch.isEmpty()) {
            getFilesListener.onResponse(new GetFilesResponse());
        }
        source.getFiles(getId(), checkpointInfo.getCheckpoint(), filesToFetch, getFilesListener);
    }

    private void finalizeReplication(TransportCheckpointInfoResponse checkpointInfoResponse, ActionListener<Void> listener) {
        ActionListener.completeWith(listener, () -> {
            // first, we go and move files that were created with the recovery id suffix to
            // the actual names, its ok if we have a corrupted index here, since we have replicas
            // to recover from in case of a full cluster shutdown just when this code executes...
            multiFileWriter.renameAllTempFiles();
            final Store store = store();
            store.incRef();
            try {
                // Deserialize the new SegmentInfos object sent from the primary.
                final ReplicationCheckpoint responseCheckpoint = checkpointInfoResponse.getCheckpoint();
                SegmentInfos infos = SegmentInfos.readCommit(
                    store.directory(),
                    toIndexInput(checkpointInfoResponse.getInfosBytes()),
                    responseCheckpoint.getSegmentsGen()
                );

                indexShard.finalizeReplication(infos, checkpointInfoResponse.getSnapshot(), responseCheckpoint.getSeqNo());
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

    /**
     * This method formats our byte[] containing the primary's SegmentInfos into lucene's {@link ChecksumIndexInput} that can be
     * passed to SegmentInfos.readCommit
     */
    private ChecksumIndexInput toIndexInput(byte[] input) {
        return new BufferedChecksumIndexInput(
            new ByteBuffersIndexInput(new ByteBuffersDataInput(Arrays.asList(ByteBuffer.wrap(input))), "SegmentInfos")
        );
    }

    private Store.MetadataSnapshot getMetadataSnapshot() throws IOException {
        if (indexShard.state().equals(IndexShardState.STARTED) == false) {
            return Store.MetadataSnapshot.EMPTY;
        }
        return store.getMetadata(indexShard.getLatestSegmentInfos());
    }
}
