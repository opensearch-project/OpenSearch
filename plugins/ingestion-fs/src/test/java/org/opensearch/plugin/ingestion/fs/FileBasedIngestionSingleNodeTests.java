/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.fs;

import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Before;

import java.io.BufferedWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Collections;

public class FileBasedIngestionSingleNodeTests extends OpenSearchSingleNodeTestCase {
    private Path ingestionDir;
    private final String topic = "test_topic";
    private final String index = "test_index";

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(FilePlugin.class);
    }

    @Before
    public void setupIngestionFile() throws Exception {
        ingestionDir = createTempDir().resolve("fs_ingestion");
        Path topicDir = ingestionDir.resolve(topic);
        Files.createDirectories(topicDir);

        Path shardFile = topicDir.resolve("0.ndjson");
        try (BufferedWriter writer = Files.newBufferedWriter(shardFile, StandardCharsets.UTF_8, StandardOpenOption.CREATE)) {
            writer.write("{\"_id\":\"1\",\"_version\":\"1\",\"_op_type\":\"index\",\"_source\":{\"name\":\"alice\", \"age\": 30}}\n");
            writer.write("{\"_id\":\"2\",\"_version\":\"1\",\"_op_type\":\"index\",\"_source\":{\"name\":\"bob\", \"age\": 35}}\n");
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
                .put("ingestion_source.param.topic", topic)
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

        // cleanup the test index
        client().admin().indices().delete(new DeleteIndexRequest(index)).actionGet();
    }
}
