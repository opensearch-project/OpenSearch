/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.engine.NRTReplicationEngineFactory;
import org.opensearch.index.replication.OpenSearchIndexLevelReplicationTestCase;
import org.opensearch.indices.replication.common.ReplicationType;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;

public class SegmentReplicationWithRemoteIndexShardTests extends OpenSearchIndexLevelReplicationTestCase {
    private static final Settings settings = Settings.builder()
        .put(IndexMetadata.SETTING_REPLICATION_TYPE, ReplicationType.SEGMENT)
        .put(IndexMetadata.SETTING_REMOTE_STORE_ENABLED, true)
        .put(IndexMetadata.SETTING_REMOTE_STORE_REPOSITORY, "temp-fs")
        .put(IndexMetadata.SETTING_REMOTE_TRANSLOG_STORE_ENABLED, false)
        .build();

    public void testReplicaSyncingFromRemoteStore() throws IOException {
        ReplicationGroup shards = createGroup(1, settings, indexMapping, new NRTReplicationEngineFactory(), createTempDir());
        final IndexShard primaryShard = shards.getPrimary();
        final IndexShard replicaShard = shards.getReplicas().get(0);
        shards.startPrimary();
        shards.startAll();
        indexDoc(primaryShard, "_doc", "1");
        indexDoc(primaryShard, "_doc", "2");
        primaryShard.refresh("test");
        assertDocs(primaryShard, "1", "2");
        flushShard(primaryShard);

        replicaShard.syncSegmentsFromRemoteSegmentStore(true, true, false);
        assertDocs(replicaShard, "1", "2");
        closeShards(primaryShard, replicaShard);
    }

    public void testReplicaSyncingFromRemoteStoreWithMissingSegmentFile() throws IOException {
        Path remotePath = createTempDir();
        ReplicationGroup shards = createGroup(1, settings, indexMapping, new NRTReplicationEngineFactory(), remotePath);
        final IndexShard primaryShard = shards.getPrimary();
        final IndexShard replicaShard = shards.getReplicas().get(0);
        shards.startPrimary();
        shards.startAll();
        // Ingest docs into primary
        indexDoc(primaryShard, "_doc", "1");
        indexDoc(primaryShard, "_doc", "2");
        primaryShard.refresh("test");
        assertDocs(primaryShard, "1", "2");

        // Copy the metadata file
        Directory remoteFSDirectory = new NIOFSDirectory(remotePath.resolve("indices/_na_/0/index/"));
        String metadataFile = Arrays.stream(remoteFSDirectory.listAll())
            .filter(f -> f.startsWith("metadata"))
            .max(Comparator.comparing(String::valueOf))
            .get();
        logger.info("Copying metadata from {}", metadataFile);
        byte[] metadataBytes;
        try (IndexInput indexInput = remoteFSDirectory.openInput(metadataFile, IOContext.DEFAULT)) {
            metadataBytes = new byte[(int) indexInput.length()];
            indexInput.readBytes(metadataBytes, 0, (int) indexInput.length());
        }

        // Add more docs to primary and refresh
        indexDoc(primaryShard, "_doc", "3");
        indexDoc(primaryShard, "_doc", "4");
        primaryShard.refresh("test");
        assertDocs(primaryShard, "1", "2", "3", "4");

        // Override the metadata file with older content to simulate the race condition
        metadataFile = Arrays.stream(remoteFSDirectory.listAll())
            .filter(f -> f.startsWith("metadata"))
            .max(Comparator.comparing(String::valueOf))
            .get();
        logger.info("Overriding metadata content for {}", metadataFile);
        remoteFSDirectory.deleteFile(metadataFile);
        IndexOutput output = remoteFSDirectory.createOutput(metadataFile, IOContext.DEFAULT);
        output.writeBytes(metadataBytes, metadataBytes.length);
        output.close();
        assertThrows(IndexShardRecoveryException.class, () -> replicaShard.syncSegmentsFromRemoteSegmentStore(true, true, false));

        indexDoc(primaryShard, "_doc", "5");
        indexDoc(primaryShard, "_doc", "6");
        primaryShard.refresh("test");
        assertDocs(primaryShard, "1", "2", "3", "4", "5", "6");
        replicaShard.syncSegmentsFromRemoteSegmentStore(true, true, false);
        assertDocs(replicaShard, "1", "2", "3", "4", "5", "6");
        closeShards(primaryShard, replicaShard);
    }
}
