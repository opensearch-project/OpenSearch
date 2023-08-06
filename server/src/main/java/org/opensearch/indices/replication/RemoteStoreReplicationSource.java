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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
        final Version version = indexShard.getSegmentInfosSnapshot().get().getCommitLuceneVersion();
        try {
            RemoteSegmentMetadata mdFile = remoteDirectory.init();
            // During initial recovery flow, the remote store might not have metadata as primary hasn't uploaded anything yet.
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
        ActionListener<GetSegmentFilesResponse> listener
    ) {
        try {
            if (filesToFetch.isEmpty()) {
                listener.onResponse(new GetSegmentFilesResponse(Collections.emptyList()));
                return;
            }
            logger.trace("Downloading segments files from remote store {}", filesToFetch);

            RemoteSegmentMetadata remoteSegmentMetadata = remoteDirectory.readLatestMetadataFile();
            List<StoreFileMetadata> downloadedSegments = new ArrayList<>();
            Collection<String> directoryFiles = List.of(indexShard.store().directory().listAll());
            if (remoteSegmentMetadata != null) {
                try {
                    indexShard.store().incRef();
                    indexShard.remoteStore().incRef();
                    final Directory storeDirectory = indexShard.store().directory();
                    for (StoreFileMetadata fileMetadata : filesToFetch) {
                        String file = fileMetadata.name();
                        assert directoryFiles.contains(file) == false : "Local store already contains the file " + file;
                        storeDirectory.copyFrom(remoteDirectory, file, file, IOContext.DEFAULT);
                        downloadedSegments.add(fileMetadata);
                    }
                    logger.trace("Downloaded segments from remote store {}", downloadedSegments);
                } finally {
                    indexShard.store().decRef();
                    indexShard.remoteStore().decRef();
                }
            }
            listener.onResponse(new GetSegmentFilesResponse(downloadedSegments));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public String getDescription() {
        return "RemoteStoreReplicationSource";
    }
}
