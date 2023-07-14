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
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.util.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.store.Store;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.remote.metadata.RemoteSegmentMetadata;
import org.opensearch.indices.replication.checkpoint.ReplicationCheckpoint;

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

    private static final Logger logger = LogManager.getLogger(PrimaryShardReplicationSource.class);

    private final IndexShard indexShard;

    public RemoteStoreReplicationSource(IndexShard indexShard) {
        this.indexShard = indexShard;
    }

    @Override
    public void getCheckpointMetadata(
        long replicationId,
        ReplicationCheckpoint checkpoint,
        ActionListener<CheckpointInfoResponse> listener
    ) {
        FilterDirectory remoteStoreDirectory = (FilterDirectory) indexShard.remoteStore().directory();
        FilterDirectory byteSizeCachingStoreDirectory = (FilterDirectory) remoteStoreDirectory.getDelegate();
        RemoteSegmentStoreDirectory remoteDirectory = (RemoteSegmentStoreDirectory) byteSizeCachingStoreDirectory.getDelegate();

        Map<String, StoreFileMetadata> metadataMap;
        // TODO: Need to figure out a way to pass this information for segment metadata via remote store.
        final Version version = indexShard.getSegmentInfosSnapshot().get().getCommitLuceneVersion();
        try {
            RemoteSegmentMetadata mdFile = remoteDirectory.readLatestMetadataFile();
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
            // TODO: GET current checkpoint from remote store.
            listener.onResponse(new CheckpointInfoResponse(checkpoint, metadataMap, null));
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
            indexShard.syncSegmentsFromRemoteSegmentStore(false, true, false);
            listener.onResponse(new GetSegmentFilesResponse(Collections.emptyList()));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    public String getDescription() {
        return "remote store";
    }
}
