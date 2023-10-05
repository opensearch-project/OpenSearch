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
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

import java.nio.file.Path;
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
        try (final GatedCloseable<SegmentInfos> segmentInfosSnapshot = indexShard.getSegmentInfosSnapshot()) {
            final Version version = segmentInfosSnapshot.get().getCommitLuceneVersion();
            RemoteSegmentMetadata mdFile = remoteDirectory.init();
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
        ActionListener<GetSegmentFilesResponse> listener
    ) {
        try {
            if (filesToFetch.isEmpty()) {
                listener.onResponse(new GetSegmentFilesResponse(Collections.emptyList()));
                return;
            }
            logger.debug("Downloading segment files from remote store {}", filesToFetch);

            RemoteSegmentMetadata remoteSegmentMetadata = remoteDirectory.readLatestMetadataFile();
            List<StoreFileMetadata> toDownloadSegments = new ArrayList<>();
            Collection<String> directoryFiles = List.of(indexShard.store().directory().listAll());
            if (remoteSegmentMetadata != null) {
                try {
                    indexShard.store().incRef();
                    indexShard.remoteStore().incRef();
                    final Directory storeDirectory = indexShard.store().directory();
                    final ShardPath shardPath = indexShard.shardPath();
                    for (StoreFileMetadata fileMetadata : filesToFetch) {
                        String file = fileMetadata.name();
                        assert directoryFiles.contains(file) == false : "Local store already contains the file " + file;
                        toDownloadSegments.add(fileMetadata);
                    }
                    downloadSegments(storeDirectory, remoteDirectory, toDownloadSegments, shardPath, listener);
                    logger.debug("Downloaded segment files from remote store {}", toDownloadSegments);
                } finally {
                    indexShard.store().decRef();
                    indexShard.remoteStore().decRef();
                }
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    private void downloadSegments(
        Directory storeDirectory,
        RemoteSegmentStoreDirectory remoteStoreDirectory,
        List<StoreFileMetadata> toDownloadSegments,
        ShardPath shardPath,
        ActionListener<GetSegmentFilesResponse> completionListener
    ) {
        final Path indexPath = shardPath == null ? null : shardPath.resolveIndex();
        for (StoreFileMetadata storeFileMetadata : toDownloadSegments) {
            final PlainActionFuture<String> segmentListener = PlainActionFuture.newFuture();
            remoteStoreDirectory.copyTo(storeFileMetadata.name(), storeDirectory, indexPath, segmentListener);
            segmentListener.actionGet();
        }
        completionListener.onResponse(new GetSegmentFilesResponse(toDownloadSegments));
    }

    @Override
    public String getDescription() {
        return "RemoteStoreReplicationSource";
    }
}
