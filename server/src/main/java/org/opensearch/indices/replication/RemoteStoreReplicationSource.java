/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.Version;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.store.CompositeStoreDirectory;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.replication.checkpoint.RemoteStoreMergedSegmentCheckpoint;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Implementation of a {@link SegmentReplicationSource} where the source is remote store.
 *
 * @opensearch.internal
 */
public class RemoteStoreReplicationSource implements SegmentReplicationSource {

    private static final Logger logger = LogManager.getLogger(RemoteStoreReplicationSource.class);

    private final IndexShard indexShard;
    private final RemoteSegmentStoreDirectory remoteDirectory; // Fallback for legacy cases
    private final CancellableThreads cancellableThreads = new CancellableThreads();

    public RemoteStoreReplicationSource(IndexShard indexShard) {
        this.indexShard = indexShard;

        // Try to get CompositeRemoteDirectory first, fallback to RemoteSegmentStoreDirectory
        FilterDirectory remoteStoreDirectory = (FilterDirectory) indexShard.remoteStore().directory();
        FilterDirectory byteSizeCachingStoreDirectory = (FilterDirectory) remoteStoreDirectory.getDelegate();
        this.remoteDirectory = (RemoteSegmentStoreDirectory) byteSizeCachingStoreDirectory.getDelegate();
    }

    @Override
    public void getCheckpointMetadata(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        ActionListener<CheckpointInfoResponse> listener
    ) {
        Map<String, StoreFileMetadata> metadataMap;
        // TODO: Need to figure out a way to pass this information for segment metadata via remote store.
        try (final GatedCloseable<SegmentInfos> segmentInfosSnapshot = indexShard.getSegmentInfosSnapshot()) {
            final Version version = segmentInfosSnapshot.get().getCommitLuceneVersion();
            final RemoteSegmentMetadata mdFile = getRemoteSegmentMetadata();

            // Handle null metadata file case
            if (mdFile == null) {
                // During initial recovery flow, the remote store might not
                // have metadata as primary hasn't uploaded anything yet.
                if (indexShard.state().equals(IndexShardState.STARTED) == false) {
                    // Non-started shard during recovery
                    listener.onResponse(new CheckpointInfoResponse(checkpoint, Collections.emptyMap(), null));
                    return;
                } else if (indexShard.routingEntry().isSearchOnly()) {
                    // Allow search-only replicas to become active without metadata
                    logger.debug("Search-only replica proceeding without remote metadata: {}", indexShard.shardId());
                    listener.onResponse(
                        new CheckpointInfoResponse(indexShard.getLatestReplicationCheckpoint(), Collections.emptyMap(), null)
                    );
                    return;
                } else {
                    // Regular replicas should not be active without metadata
                    listener.onFailure(
                        new IllegalStateException("Remote metadata file can't be null if shard is active: " + indexShard.shardId())
                    );
                    return;
                }
            }

            // Process metadata when it exists
            metadataMap = mdFile.getMetadata()
                .entrySet()
                .stream()
                .collect(
                    Collectors.toMap(
                        e -> new FileMetadata(e.getKey()).file(),
                        e -> new StoreFileMetadata(
                            e.getValue().getOriginalFilename(),
                            e.getValue().getLength(),
                            Store.digestToString(Long.valueOf(e.getValue().getChecksum())),
                            version,
                            null,
                            new FileMetadata(e.getKey()).dataFormat()
                        )
                    )
                );
            listener.onResponse(new CheckpointInfoResponse(mdFile.getReplicationCheckpoint(), metadataMap, mdFile.getSegmentInfosBytes()));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public void getSegmentFiles(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        List<StoreFileMetadata> filesToFetch,
        IndexShard indexShard,
        BiConsumer<String, Long> fileProgressTracker,
        ActionListener<GetSegmentFilesResponse> listener
    ) {
        try {
            if (filesToFetch.isEmpty()) {
                listener.onResponse(new GetSegmentFilesResponse(Collections.emptyList()));
                return;
            }
            logger.debug("Downloading format-aware segment files from remote store {}", filesToFetch);
            if (remoteMetadataExists()) {
                final CompositeStoreDirectory storeDirectory = indexShard.store().compositeStoreDirectory();
                final List<FileMetadata> directoryFiles = List.of(storeDirectory.listFileMetadata());

                final List<FileMetadata> toDownloadFileMetadata = new ArrayList<>();

                for (StoreFileMetadata storeFileMetadata : filesToFetch) {
                    String fileName = storeFileMetadata.name();
                    String dataFormat = storeFileMetadata.dataFormat() != null ? storeFileMetadata.dataFormat() : "lucene";

                    // Create FileMetadata for format-aware operations
                    FileMetadata fileMetadata = new FileMetadata(dataFormat, fileName);

                    // Verify file doesn't already exist in local directory
                    if (directoryFiles.contains(fileMetadata)) {
                        logger.info("ReplicationCheckpoint: {}, filesToFetch: {}", checkpoint.getSegmentInfosVersion(), filesToFetch);
                        logger.info(directoryFiles);
                        continue;
                    }
                    assert directoryFiles.contains(fileMetadata) == false : "Local store already contains the file " + fileMetadata;

                    toDownloadFileMetadata.add(fileMetadata);

                    logger.trace("Queuing format-aware file for download: {} with format: {}", fileName, dataFormat);
                }

                // Use CompositeStoreDirectory with format-aware progress tracking
                final CompositeStoreDirectoryStatsWrapper statsWrapper = new CompositeStoreDirectoryStatsWrapper(storeDirectory, fileProgressTracker);

                // After the for loop that builds toDownloadFileMetadata
                if (toDownloadFileMetadata.isEmpty()) {
                    logger.debug("All files already exist locally, skipping download");
                    listener.onResponse(new GetSegmentFilesResponse(filesToFetch));
                    return;
                }

                indexShard.getFileDownloader()
                    .downloadAsync(
                        cancellableThreads,
                        remoteDirectory,
                        statsWrapper,
                        toDownloadFileMetadata,
                        ActionListener.map(listener, r -> new GetSegmentFilesResponse(filesToFetch))
                    );
            } else {
                listener.onResponse(new GetSegmentFilesResponse(filesToFetch));
            }
        } catch (IOException | RuntimeException e) {
            listener.onFailure(e);
        }
    }

    /**
     * {@inheritDoc}
     **/
    @Override
    public void getMergedSegmentFiles(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        List<StoreFileMetadata> filesToFetch,
        IndexShard indexShard,
        BiConsumer<String, Long> fileProgressTracker,
        ActionListener<GetSegmentFilesResponse> listener
    ) {
        assert checkpoint instanceof RemoteStoreMergedSegmentCheckpoint;

        final Directory storeDirectory = indexShard.store().directory();
        ActionListener<GetSegmentFilesResponse> notifyOnceListener = ActionListener.notifyOnce(listener);

        List<String> toDownloadSegmentNames = filesToFetch.stream().map(StoreFileMetadata::name).toList();

        CountDownLatch latch = new CountDownLatch(1);
        indexShard.getFileDownloader()
            .downloadAsync(cancellableThreads, remoteDirectory, storeDirectory, toDownloadSegmentNames, ActionListener.wrap(r -> {
                latch.countDown();
                notifyOnceListener.onResponse(new GetSegmentFilesResponse(filesToFetch));
            }, e -> {
                latch.countDown();
                notifyOnceListener.onFailure(e);
            }));
        try {
            if (latch.await(
                indexShard.getRecoverySettings().getMergedSegmentReplicationTimeout().millis(),
                TimeUnit.MILLISECONDS
            ) == false) {
                notifyOnceListener.onFailure(new TimeoutException("Timed out waiting for merged segments download from remote store"));
                logger.warn(
                    () -> new ParameterizedMessage(
                        "Merged segments download from remote store timed out. Segments: {}",
                        toDownloadSegmentNames
                    )
                );
            }
        } catch (InterruptedException e) {
            notifyOnceListener.onFailure(e);
            logger.warn(() -> new ParameterizedMessage("Exception thrown while trying to get merged segment files. Continuing. {}", e));
        }
    }

    @Override
    public void cancel() {
        this.cancellableThreads.cancel("Canceled by target");
    }

    @Override
    public String getDescription() {
        return "RemoteStoreReplicationSource";
    }

    private RemoteSegmentMetadata getRemoteSegmentMetadata() throws IOException {
        if (remoteDirectory == null) {
            throw new IllegalStateException("No remote directory available for shard: " + indexShard.shardId());
        }

        AtomicReference<RemoteSegmentMetadata> mdFile = new AtomicReference<>();
        cancellableThreads.executeIO(() -> mdFile.set(remoteDirectory.init()));
        return mdFile.get();
    }

    private boolean remoteMetadataExists() throws IOException {
        if (remoteDirectory == null) {
            return false;
        }

        final AtomicBoolean metadataExists = new AtomicBoolean(false);
        cancellableThreads.executeIO(() -> metadataExists.set(remoteDirectory.readLatestMetadataFile() != null));
        return metadataExists.get();
    }
}
