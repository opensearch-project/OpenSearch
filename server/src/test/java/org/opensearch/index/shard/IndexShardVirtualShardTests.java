/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.VirtualShardRoutingHelper;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.MediaTypeRegistry;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class IndexShardVirtualShardTests extends IndexShardTestCase {
    private static final int TOTAL_VIRTUAL_SHARDS = 20;

    private Settings virtualShardSettings() {
        return Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_VIRTUAL_SHARDS, TOTAL_VIRTUAL_SHARDS).build();
    }

    public void testExtractVirtualShard() throws Exception {
        IndexShard shard = newStartedShard(true, virtualShardSettings());
        int docCount = 50;
        for (int i = 0; i < docCount; i++) {
            indexDoc(shard, "_doc", String.valueOf(i), "{\"value\":" + i + "}");
        }
        shard.refresh("test");
        flushShard(shard, true);

        int targetVShard = 0;
        int expectedCount = 0;
        IndexMetadata indexMetadata = shard.indexSettings.getIndexMetadata();
        for (int i = 0; i < docCount; i++) {
            String docId = String.valueOf(i);
            int vShardId = VirtualShardRoutingHelper.computeVirtualShardId(indexMetadata, docId, null);
            if (vShardId == targetVShard) {
                expectedCount++;
            }
        }
        assertTrue("Expected at least one doc in target vShard", expectedCount > 0);

        Path tempPath = createTempDir("vshard_extract");
        shard.extractVirtualShard(targetVShard, tempPath);

        try (Directory dir = FSDirectory.open(tempPath); DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals(expectedCount, reader.numDocs());
        }

        closeShards(shard);
    }

    public void testExtractWithRoutingField() throws Exception {
        IndexShard shard = newStartedShard(true, virtualShardSettings());
        int docCount = 40;
        for (int i = 0; i < docCount; i++) {
            String routing = "route_" + (i % 3);
            indexDoc(shard, String.valueOf(i), "{\"value\":" + i + "}", MediaTypeRegistry.JSON, routing);
        }
        shard.refresh("test");
        flushShard(shard, true);

        int targetVShard = 2;
        int expectedCount = 0;
        IndexMetadata indexMetadata = shard.indexSettings.getIndexMetadata();
        for (int i = 0; i < docCount; i++) {
            String routing = "route_" + (i % 3);
            int vShardId = VirtualShardRoutingHelper.computeVirtualShardId(indexMetadata, String.valueOf(i), routing);
            if (vShardId == targetVShard) {
                expectedCount++;
            }
        }

        Path tempPath = createTempDir("vshard_routing");
        shard.extractVirtualShard(targetVShard, tempPath);

        try (Directory dir = FSDirectory.open(tempPath); DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals(expectedCount, reader.numDocs());
        }

        closeShards(shard);
    }

    public void testExtractOnClosedShard() throws Exception {
        IndexShard shard = newStartedShard(true, virtualShardSettings());
        indexDoc(shard, "_doc", "1", "{\"value\":1}");
        shard.refresh("test");
        flushShard(shard, true);
        closeShards(shard);

        Path tempPath = createTempDir("vshard_closed");
        expectThrows(IndexShardClosedException.class, () -> shard.extractVirtualShard(0, tempPath));
    }

    public void testExtractEmptyVirtualShard() throws Exception {
        IndexShard shard = newStartedShard(true, virtualShardSettings());
        indexDoc(shard, "_doc", "1", "{\"value\":1}");
        shard.refresh("test");
        flushShard(shard, true);

        IndexMetadata indexMetadata = shard.indexSettings.getIndexMetadata();
        int occupiedVShard = VirtualShardRoutingHelper.computeVirtualShardId(indexMetadata, "1", null);
        int emptyVShard = (occupiedVShard + 1) % TOTAL_VIRTUAL_SHARDS;

        Path tempPath = createTempDir("vshard_empty");
        shard.extractVirtualShard(emptyVShard, tempPath);

        try (Directory dir = FSDirectory.open(tempPath); DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals(0, reader.numDocs());
        }

        closeShards(shard);
    }

    public void testExtractWithOverrideMap() throws Exception {
        IndexShard shard = newStartedShard(true, virtualShardSettings());
        int docCount = 50;
        for (int i = 0; i < docCount; i++) {
            indexDoc(shard, "_doc", String.valueOf(i), "{\"value\":" + i + "}");
        }
        shard.refresh("test");
        flushShard(shard, true);

        int targetVShard = 0;

        int expectedCount = 0;
        IndexMetadata indexMetadata = shard.indexSettings.getIndexMetadata();
        for (int i = 0; i < docCount; i++) {
            int vShardId = VirtualShardRoutingHelper.computeVirtualShardId(indexMetadata, String.valueOf(i), null);
            if (vShardId == targetVShard) {
                expectedCount++;
            }
        }

        Path tempPath = createTempDir("vshard_override");
        shard.extractVirtualShard(targetVShard, tempPath);

        try (Directory dir = FSDirectory.open(tempPath); DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals("Override map should not change vShard membership", expectedCount, reader.numDocs());
        }

        int defaultPhysical = VirtualShardRoutingHelper.resolvePhysicalShardId(shard.indexSettings.getIndexMetadata(), targetVShard);
        assertEquals(0, defaultPhysical);

        Map<String, String> overrides = new HashMap<>();
        overrides.put(String.valueOf(targetVShard), "3");
        IndexMetadata.Builder overrideBuilder = IndexMetadata.builder("test_override")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_VIRTUAL_SHARDS, TOTAL_VIRTUAL_SHARDS)
            )
            .numberOfShards(5)
            .numberOfReplicas(1);
        overrideBuilder.putCustom(VirtualShardRoutingHelper.VIRTUAL_SHARDS_CUSTOM_METADATA_KEY, overrides);
        IndexMetadata overriddenMetadata = overrideBuilder.build();
        int overriddenPhysical = VirtualShardRoutingHelper.resolvePhysicalShardId(overriddenMetadata, targetVShard);
        assertEquals(3, overriddenPhysical);

        closeShards(shard);
    }

    public void testExtractVShardIdBoundsCheck() throws Exception {
        IndexShard shard = newStartedShard(true, virtualShardSettings());
        Path tempPath = createTempDir("vshard_bounds");

        expectThrows(IllegalArgumentException.class, () -> shard.extractVirtualShard(-1, tempPath));
        expectThrows(IllegalArgumentException.class, () -> shard.extractVirtualShard(TOTAL_VIRTUAL_SHARDS, tempPath));
        expectThrows(IllegalArgumentException.class, () -> shard.extractVirtualShard(TOTAL_VIRTUAL_SHARDS + 5, tempPath));

        closeShards(shard);
    }

    public void testExtractPartitionedIndex() throws Exception {
        IndexMetadata.Builder builder = IndexMetadata.builder("test")
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                    .put(IndexMetadata.SETTING_NUMBER_OF_VIRTUAL_SHARDS, TOTAL_VIRTUAL_SHARDS)
                    .put(IndexMetadata.SETTING_ROUTING_PARTITION_SIZE, 3)
            )
            .numberOfShards(5)
            .numberOfReplicas(1)
            .primaryTerm(0, primaryTerm)
            .putMapping("{ \"_routing\": { \"required\": true } }");
        IndexMetadata metadata = builder.build();
        IndexShard shard = newStartedShard(
            p -> newShard(new org.opensearch.core.index.shard.ShardId(metadata.getIndex(), 0), p, "node1", metadata, null),
            true
        );

        int docCount = 60;
        for (int i = 0; i < docCount; i++) {
            String routing = "custom_route_" + (i % 4);
            // In a partitioned index, both _id and _routing determine the placement
            indexDoc(shard, String.valueOf(i), "{\"value\":" + i + "}", MediaTypeRegistry.JSON, routing);
        }
        shard.refresh("test");
        flushShard(shard, true);

        int targetVShard = 1;
        int expectedCount = 0;
        IndexMetadata indexMetadata = shard.indexSettings.getIndexMetadata();

        // Emulate the routing computation for the expected count
        for (int i = 0; i < docCount; i++) {
            String routing = "custom_route_" + (i % 4);
            int vShardId = VirtualShardRoutingHelper.computeVirtualShardId(indexMetadata, String.valueOf(i), routing);
            if (vShardId == targetVShard) {
                expectedCount++;
            }
        }

        Path tempPath = createTempDir("vshard_partitioned");
        shard.extractVirtualShard(targetVShard, tempPath);

        try (Directory dir = FSDirectory.open(tempPath); DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals("Partitioned extraction mismatched", expectedCount, reader.numDocs());
        }

        closeShards(shard);
    }

    public void testExtractOverwritesExistingOutputIndex() throws Exception {
        IndexShard shard = newStartedShard(true, virtualShardSettings());
        int docCount = 40;
        for (int i = 0; i < docCount; i++) {
            indexDoc(shard, "_doc", String.valueOf(i), "{\"value\":" + i + "}");
        }
        shard.refresh("test");
        flushShard(shard, true);

        int targetVShard = 0;
        int expectedCount = 0;
        IndexMetadata indexMetadata = shard.indexSettings.getIndexMetadata();
        for (int i = 0; i < docCount; i++) {
            int vShardId = VirtualShardRoutingHelper.computeVirtualShardId(indexMetadata, String.valueOf(i), null);
            if (vShardId == targetVShard) {
                expectedCount++;
            }
        }

        Path tempPath = createTempDir("vshard_existing_output");
        try (Directory dir = FSDirectory.open(tempPath); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(null))) {
            writer.addDocument(new Document());
            writer.commit();
        }

        shard.extractVirtualShard(targetVShard, tempPath);

        try (Directory dir = FSDirectory.open(tempPath); DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals(expectedCount, reader.numDocs());
        }

        closeShards(shard);
    }
}
