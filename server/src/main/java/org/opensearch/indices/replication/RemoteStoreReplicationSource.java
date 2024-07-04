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
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.Version;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.util.CancellableThreads;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
    private final RemoteSegmentStoreDirectory remoteDirectory;
    private final CancellableThreads cancellableThreads = new CancellableThreads();

    public RemoteStoreReplicationSource(IndexShard indexShard) {
        this.indexShard = indexShard;
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
            // During initial recovery flow, the remote store might not
            // have metadata as primary hasn't uploaded anything yet.
            if (mdFile == null && indexShard.state().equals(IndexShardState.STARTED) == false) {
                listener.onResponse(new CheckpointInfoResponse(checkpoint, Collections.emptyMap(), null));
                return;
            }
            assert mdFile != null : "Remote metadata file can't be null if shard is active " + indexShard.state();
            metadataMap = mdFile.getMetadata()
                .entrySet()
                .stream()
                .collect(
                    Collectors.toMap(
                        e -> e.getKey(),
                        e -> new StoreFileMetadata(
                            e.getValue().getOriginalFilename(),
                            e.getValue().getLength(),
                            Store.digestToString(Long.valueOf(e.getValue().getChecksum())),
                            version,
                            null
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
            logger.debug("Downloading segment files from remote store {}", filesToFetch);

            if (remoteMetadataExists()) {
                final Directory storeDirectory = indexShard.store().directory();
                final Collection<String> directoryFiles = List.of(storeDirectory.listAll());
                final List<String> toDownloadSegmentNames = new ArrayList<>();
                for (StoreFileMetadata fileMetadata : filesToFetch) {
                    String file = fileMetadata.name();
                    assert directoryFiles.contains(file) == false || indexShard.indexSettings().isStoreLocalityPartial() : "Local store already contains the file " + file;
                    toDownloadSegmentNames.add(file);
                }
                if(indexShard.indexSettings().isStoreLocalityPartial()) {
                    listener.onResponse(new GetSegmentFilesResponse(filesToFetch));
                    return;
                }
                indexShard.getFileDownloader()
                    .downloadAsync(
                        cancellableThreads,
                        remoteDirectory,
                        new ReplicationStatsDirectoryWrapper(storeDirectory, fileProgressTracker),
                        toDownloadSegmentNames,
                        ActionListener.map(listener, r -> new GetSegmentFilesResponse(filesToFetch))
                    );
            } else {
                listener.onResponse(new GetSegmentFilesResponse(filesToFetch));
            }
        } catch (IOException | RuntimeException e) {
            listener.onFailure(e);
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

    private boolean remoteMetadataExists() throws IOException {
        final AtomicBoolean metadataExists = new AtomicBoolean(false);
        cancellableThreads.executeIO(() -> metadataExists.set(remoteDirectory.readLatestMetadataFile() != null));
        return metadataExists.get();
    }

    private RemoteSegmentMetadata getRemoteSegmentMetadata() throws IOException {
        AtomicReference<RemoteSegmentMetadata> mdFile = new AtomicReference<>();
        cancellableThreads.executeIO(() -> mdFile.set(remoteDirectory.init()));
        return mdFile.get();
    }
}
