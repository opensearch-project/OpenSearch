/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.fs;

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.streamingingestion.pause.PauseIngestionResponse;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionRequest;
import org.opensearch.action.admin.indices.streamingingestion.resume.ResumeIngestionResponse;
import org.opensearch.action.admin.indices.streamingingestion.state.GetIngestionStateResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.RangeQueryBuilder;
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
                    .allMatch(state -> state.isPollerPaused() && state.pollerState().equalsIgnoreCase("paused"))
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
                            && (state.pollerState().equalsIgnoreCase("polling") || state.pollerState().equalsIgnoreCase("processing"))
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
}
