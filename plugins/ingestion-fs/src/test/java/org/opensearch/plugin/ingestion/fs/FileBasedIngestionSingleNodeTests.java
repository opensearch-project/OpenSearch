/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.fs;

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.admin.indices.streamingingestion.pause.PauseIngestionResponse;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionRequest;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionResponse;
import org.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.indices.pollingingest.PollingIngestStats;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.opensearch.transport.client.Requests;
import org.junit.Before;

import java.io.BufferedWriter;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class FileBasedIngestionSingleNodeTests extends OpenSearchSingleNodeTestCase {
    private Path ingestionDir;
    private final String stream = "test_stream";
    private final String index = "test_index";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(FilePlugin.class);
    }

    @Before
    public void setupIngestionFile() throws Exception {
        ingestionDir = createTempDir().resolve("fs_ingestion");
        Path streamDir = ingestionDir.resolve(stream);
        Files.createDirectories(streamDir);

        Path shardFile = streamDir.resolve("0.ndjson");
        try (
            BufferedWriter writer = Files.newBufferedWriter(
                shardFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
            )
        ) {
            writer.write("{\"_id\":\"1\",\"_version\":\"1\",\"_op_type\":\"index\",\"_source\":{\"name\":\"alice\", \"age\": 30}}\n");
            writer.write("{\"_id\":\"2\",\"_version\":\"1\",\"_op_type\":\"index\",\"_source\":{\"name\":\"bob\", \"age\": 35}}\n");
            writer.flush();
        }

        try (FileChannel channel = FileChannel.open(shardFile, StandardOpenOption.READ)) {
            channel.force(true);
        }

        // Wait for file to be fully visible by reading it back
        // This prevents race conditions where tests start before file is ready
        assertBusy(() -> {
            java.util.List<String> lines = Files.readAllLines(shardFile, StandardCharsets.UTF_8);
            assertEquals("File should have exactly 2 lines", 2, lines.size());
            assertTrue("First line should contain alice", lines.get(0).contains("alice"));
            assertTrue("Second line should contain bob", lines.get(1).contains("bob"));
        });
    }

    public void testFileIngestion() throws Exception {
        String mappings = """
            {
              "properties": {
                "name": { "type": "text" },
                "age": { "type": "integer" }
              }
            }
            """;

        createIndexWithMappingSource(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "FILE")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.param.stream", stream)
                .put("ingestion_source.param.base_directory", ingestionDir.toString())
                .put("index.replication.type", "SEGMENT")
                .build(),
            mappings
        );
        ensureGreen(index);

        assertBusy(() -> {
            RangeQueryBuilder query = new RangeQueryBuilder("age").gte(0);
            SearchResponse response = client().prepareSearch(index).setQuery(query).get();
            assertEquals(2, response.getHits().getTotalHits().value());
        });

        PauseIngestionResponse pauseResponse = client().admin().indices().pauseIngestion(Requests.pauseIngestionRequest(index)).get();
        assertTrue(pauseResponse.isAcknowledged());
        assertTrue(pauseResponse.isShardsAcknowledged());
        assertBusy(() -> {
            GetIngestionStateResponse ingestionState = client().admin()
                .indices()
                .getIngestionState(Requests.getIngestionStateRequest(index))
                .get();
            assertEquals(0, ingestionState.getFailedShards());
            assertTrue(
                Arrays.stream(ingestionState.getShardStates())
                    .allMatch(state -> state.isPollerPaused() && state.getPollerState().equalsIgnoreCase("paused"))
            );
        });

        ResumeIngestionResponse resumeResponse = client().admin()
            .indices()
            .resumeIngestion(Requests.resumeIngestionRequest(index, 0, ResumeIngestionRequest.ResetSettings.ResetMode.OFFSET, "0"))
            .get();
        assertTrue(resumeResponse.isAcknowledged());
        assertTrue(resumeResponse.isShardsAcknowledged());
        assertBusy(() -> {
            GetIngestionStateResponse ingestionState = client().admin()
                .indices()
                .getIngestionState(Requests.getIngestionStateRequest(index))
                .get();
            assertTrue(
                Arrays.stream(ingestionState.getShardStates())
                    .allMatch(
                        state -> state.isPollerPaused() == false
                            && (state.getPollerState().equalsIgnoreCase("polling") || state.getPollerState().equalsIgnoreCase("processing"))
                    )
            );
        });

        // cleanup the test index
        client().admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
    }

    public void testFileIngestionOnMissingFiles() throws Exception {
        String mappings = """
            {
              "properties": {
                "name": { "type": "text" },
                "age": { "type": "integer" }
              }
            }
            """;

        createIndexWithMappingSource(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "FILE")
                .put("ingestion_source.pointer.init.reset", "latest")
                .put("ingestion_source.param.stream", stream)
                .put("ingestion_source.param.base_directory", "unknown_directory")
                .put("index.replication.type", "SEGMENT")
                .build(),
            mappings
        );
        ensureGreen(index);

        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(0);
        SearchResponse response = client().prepareSearch(index).setQuery(query).get();
        assertEquals(0, response.getHits().getTotalHits().value());

        // cleanup the test index
        client().admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
    }

    public void testFileIngestionFromLatestPointer() throws Exception {
        String mappings = """
            {
              "properties": {
                "name": { "type": "text" },
                "age": { "type": "integer" }
              }
            }
            """;

        createIndexWithMappingSource(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "FILE")
                .put("ingestion_source.pointer.init.reset", "latest")
                .put("ingestion_source.param.stream", stream)
                .put("ingestion_source.param.base_directory", ingestionDir.toString())
                .put("index.replication.type", "SEGMENT")
                .build(),
            mappings
        );
        ensureGreen(index);
        RangeQueryBuilder query = new RangeQueryBuilder("age").gte(0);
        SearchResponse response = client().prepareSearch(index).setQuery(query).get();
        assertEquals(0, response.getHits().getTotalHits().value());

        // cleanup the test index
        client().admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
    }

    public void testFileIngestionFromProvidedPointer() throws Exception {
        String mappings = """
            {
              "properties": {
                "name": { "type": "text" },
                "age": { "type": "integer" }
              }
            }
            """;

        createIndexWithMappingSource(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "FILE")
                .put("ingestion_source.pointer.init.reset", "reset_by_offset")
                .put("ingestion_source.pointer.init.reset.value", "1")
                .put("ingestion_source.param.stream", stream)
                .put("ingestion_source.param.base_directory", ingestionDir.toString())
                .put("index.replication.type", "SEGMENT")
                .build(),
            mappings
        );
        ensureGreen(index);
        assertBusy(() -> {
            RangeQueryBuilder query = new RangeQueryBuilder("age").gte(0);
            SearchResponse response = client().prepareSearch(index).setQuery(query).get();
            assertEquals(1, response.getHits().getTotalHits().value());
        });

        // cleanup the test index
        client().admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
    }

    public void testPointerBasedLag() throws Exception {
        String mappings = """
            {
              "properties": {
                "name": { "type": "text" },
                "age": { "type": "integer" }
              }
            }
            """;

        // Create index with empty file (no messages)
        Path streamDir = ingestionDir.resolve(stream);
        Path shardFile = streamDir.resolve("0.ndjson");
        Files.write(shardFile, new byte[0]); // Empty file

        createIndexWithMappingSource(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "FILE")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.pointer_based_lag_update_interval", "3s")
                .put("ingestion_source.param.stream", stream)
                .put("ingestion_source.param.base_directory", ingestionDir.toString())
                .put("index.replication.type", "SEGMENT")
                .build(),
            mappings
        );
        ensureGreen(index);

        // Lag should be 0 since there are no messages
        waitForState(() -> {
            PollingIngestStats stats = getPollingIngestStats(index);
            return stats != null && stats.getConsumerStats().pointerBasedLag() == 0L;
        });

        // Add messages to the file
        try (
            BufferedWriter writer = Files.newBufferedWriter(
                shardFile,
                StandardCharsets.UTF_8,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
            )
        ) {
            writer.write("{\"_id\":\"1\",\"_version\":\"1\",\"_op_type\":\"index\",\"_source\":{\"name\":\"alice\", \"age\": 30}}\n");
            writer.write("{\"_id\":\"2\",\"_version\":\"1\",\"_op_type\":\"index\",\"_source\":{\"name\":\"bob\", \"age\": 35}}\n");
            writer.flush();
        }

        try (FileChannel channel = FileChannel.open(shardFile, StandardOpenOption.READ)) {
            channel.force(true);
        }

        // Wait for messages to be processed
        waitForState(() -> {
            SearchResponse response = client().prepareSearch(index).setQuery(new RangeQueryBuilder("age").gte(0)).get();
            return response.getHits().getTotalHits().value() == 2;
        });

        // Lag should be 0 after all messages are consumed
        waitForState(() -> {
            PollingIngestStats stats = getPollingIngestStats(index);
            return stats != null && stats.getConsumerStats().pointerBasedLag() == 0L;
        });

        // cleanup
        client().admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
    }

    public void testPointerBasedLagAfterPause() throws Exception {
        String mappings = """
            {
              "properties": {
                "name": { "type": "text" },
                "age": { "type": "integer" }
              }
            }
            """;

        createIndexWithMappingSource(
            index,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("ingestion_source.type", "FILE")
                .put("ingestion_source.pointer.init.reset", "earliest")
                .put("ingestion_source.pointer_based_lag_update_interval", "3s")
                .put("ingestion_source.param.stream", stream)
                .put("ingestion_source.param.base_directory", ingestionDir.toString())
                .put("index.replication.type", "SEGMENT")
                .build(),
            mappings
        );
        ensureGreen(index);

        // Wait for initial messages to be processed
        waitForState(() -> {
            SearchResponse response = client().prepareSearch(index).setQuery(new RangeQueryBuilder("age").gte(0)).get();
            return response.getHits().getTotalHits().value() == 2;
        });

        // Pause ingestion
        PauseIngestionResponse pauseResponse = client().admin().indices().pauseIngestion(Requests.pauseIngestionRequest(index)).get();
        assertTrue(pauseResponse.isAcknowledged());
        assertTrue(pauseResponse.isShardsAcknowledged());

        // Wait for pause to take effect
        waitForState(() -> {
            GetIngestionStateResponse ingestionState = client().admin()
                .indices()
                .getIngestionState(Requests.getIngestionStateRequest(index))
                .get();
            return ingestionState.getFailedShards() == 0
                && Arrays.stream(ingestionState.getShardStates())
                    .allMatch(state -> state.isPollerPaused() && state.getPollerState().equalsIgnoreCase("paused"));
        });

        // Add more messages to the file while paused
        Path streamDir = ingestionDir.resolve(stream);
        Path shardFile = streamDir.resolve("0.ndjson");
        try (BufferedWriter writer = Files.newBufferedWriter(shardFile, StandardCharsets.UTF_8, StandardOpenOption.APPEND)) {
            writer.write("{\"_id\":\"3\",\"_version\":\"1\",\"_op_type\":\"index\",\"_source\":{\"name\":\"charlie\", \"age\": 40}}\n");
            writer.write("{\"_id\":\"4\",\"_version\":\"1\",\"_op_type\":\"index\",\"_source\":{\"name\":\"diana\", \"age\": 45}}\n");
            writer.write("{\"_id\":\"5\",\"_version\":\"1\",\"_op_type\":\"index\",\"_source\":{\"name\":\"eve\", \"age\": 50}}\n");
            writer.flush();
        }

        try (FileChannel channel = FileChannel.open(shardFile, StandardOpenOption.READ)) {
            channel.force(true);
        }

        // Wait for lag to be calculated (lag is updated every 3 seconds in this test)
        waitForState(() -> {
            PollingIngestStats stats = getPollingIngestStats(index);
            return stats != null && stats.getConsumerStats().pointerBasedLag() == 3L;
        });

        // cleanup
        client().admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
    }

    /**
     * Helper method to get polling ingest stats for the index
     */
    private PollingIngestStats getPollingIngestStats(String indexName) {
        IndexStats indexStats = client().admin().indices().prepareStats(indexName).get().getIndex(indexName);
        ShardStats[] shards = indexStats.getShards();
        if (shards.length > 0) {
            return shards[0].getPollingIngestStats();
        }
        return null;
    }

    private void waitForState(Callable<Boolean> checkState) throws Exception {
        assertBusy(() -> {
            if (checkState.call() == false) {
                fail("Provided state requirements not met");
            }
        }, 1, TimeUnit.MINUTES);
    }
}
