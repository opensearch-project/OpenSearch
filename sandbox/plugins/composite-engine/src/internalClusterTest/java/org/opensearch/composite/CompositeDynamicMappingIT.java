/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Integration test validating dynamic mapping support with composite parquet index.
 * Documents with new fields (not in the original mapping) should be indexed successfully,
 * and the resulting Parquet files should contain all fields including dynamically added ones.
 *
 * Requires JDK 25 and sandbox enabled. Run with:
 * ./gradlew :sandbox:plugins:composite-engine:internalClusterTest \
 *   --tests "*.CompositeDynamicMappingIT" \
 *   -Dsandbox.enabled=true
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class CompositeDynamicMappingIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-dynamic-mapping";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ParquetDataFormatPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class, DataFusionPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    /**
     * Tests that documents with dynamically added fields are indexed successfully
     * into a composite parquet index. The flow is:
     * 1. Create index with initial mapping (field_keyword, field_number)
     * 2. Index documents matching the initial schema
     * 3. Index documents with NEW fields not in the original mapping (dynamic mapping)
     * 4. Refresh + flush
     * 5. Verify all documents indexed, parquet files generated with correct segments
     */
    public void testDynamicMappingWithParquet() throws IOException {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        // Create index with initial mapping
        CreateIndexResponse createResponse = client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(indexSettings)
            .setMapping("field_keyword", "type=keyword", "field_number", "type=integer")
            .get();
        assertTrue("Index creation should be acknowledged", createResponse.isAcknowledged());
        ensureGreen(INDEX_NAME);

        // Index documents with initial schema
        for (int i = 0; i < 5; i++) {
            IndexResponse response = client().prepareIndex()
                .setIndex(INDEX_NAME)
                .setSource("field_keyword", "value_" + i, "field_number", i)
                .get();
            assertEquals(RestStatus.CREATED, response.status());
        }

        // Verify dynamic fields are NOT yet in the mapping
        GetMappingsResponse mappingsResponse = client().admin().indices().prepareGetMappings(INDEX_NAME).get();
        Map<String, Object> mappingSource = mappingsResponse.mappings().get(INDEX_NAME).sourceAsMap();
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) mappingSource.get("properties");
        assertTrue("Mapping should contain initial field 'field_keyword'", properties.containsKey("field_keyword"));
        assertTrue("Mapping should contain initial field 'field_number'", properties.containsKey("field_number"));
        assertFalse("Mapping should NOT contain 'dynamic_text' yet", properties.containsKey("dynamic_text"));
        assertFalse("Mapping should NOT contain 'dynamic_long' yet", properties.containsKey("dynamic_long"));

        // Index documents with NEW dynamic fields
        for (int i = 5; i < 10; i++) {
            IndexResponse response = client().prepareIndex()
                .setIndex(INDEX_NAME)
                .setSource(
                    "field_keyword",
                    "value_" + i,
                    "field_number",
                    i,
                    "dynamic_text",
                    "dynamic_value_" + i,
                    "dynamic_long",
                    (long) i * 1000
                )
                .get();
            assertEquals(RestStatus.CREATED, response.status());
        }

        // Verify dynamic fields are now present in the mapping
        mappingsResponse = client().admin().indices().prepareGetMappings(INDEX_NAME).get();
        mappingSource = mappingsResponse.mappings().get(INDEX_NAME).sourceAsMap();
        @SuppressWarnings("unchecked")
        Map<String, Object> updatedProperties = (Map<String, Object>) mappingSource.get("properties");
        assertTrue("Mapping should contain dynamic field 'dynamic_text'", updatedProperties.containsKey("dynamic_text"));
        assertTrue("Mapping should contain dynamic field 'dynamic_long'", updatedProperties.containsKey("dynamic_long"));

        // Refresh and flush to produce parquet files
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(INDEX_NAME).get();
        assertEquals(RestStatus.OK, refreshResponse.getStatus());
        FlushResponse flushResponse = client().admin().indices().prepareFlush(INDEX_NAME).get();
        assertEquals(RestStatus.OK, flushResponse.getStatus());

        // Verify parquet files on disk
        IndexShard shard = getIndexShard(
            internalCluster().getDataNodeNames().iterator().next(),
            new ShardId(resolveIndex(INDEX_NAME), 0),
            INDEX_NAME
        );
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        assertTrue("Parquet directory should exist", Files.isDirectory(parquetDir));

        List<Path> parquetFiles;
        try (Stream<Path> paths = Files.list(parquetDir)) {
            parquetFiles = paths.filter(p -> p.toString().endsWith(".parquet")).collect(Collectors.toList());
        }
        assertFalse("Should have at least one parquet file", parquetFiles.isEmpty());

        // Verify row count from parquet file metadata
        long totalRows = 0;
        for (Path pf : parquetFiles) {
            ParquetFileMetadata meta = RustBridge.getFileMetadata(pf.toString());
            assertNotNull("Parquet file metadata should not be null: " + pf, meta);
            totalRows += meta.numRows();
        }
        assertEquals("Total rows across parquet files should equal 10", 10, totalRows);

        // Verify content via readAsJson
        List<Map<String, Object>> allRows = new ArrayList<>();
        for (Path pf : parquetFiles) {
            allRows.addAll(parseJsonRows(RustBridge.readAsJson(pf.toString())));
        }
        assertEquals(10, allRows.size());
        // Verify dynamic fields are present in the rows that should have them
        long rowsWithDynamicFields = allRows.stream()
            .filter(row -> row.containsKey("dynamic_text") && row.get("dynamic_text") != null)
            .count();
        assertEquals("5 rows should have dynamic_text field populated", 5, rowsWithDynamicFields);

        // After flush, the writer is now immutable. Index more docs with another new dynamic field
        // to verify schema evolution across writer generations (new writer created with fresh schema).
        for (int i = 10; i < 15; i++) {
            IndexResponse response = client().prepareIndex()
                .setIndex(INDEX_NAME)
                .setSource("field_keyword", "value_" + i, "field_number", i, "dynamic_extra", "extra_" + i)
                .get();
            assertEquals(RestStatus.CREATED, response.status());
        }

        // Verify new dynamic field in mapping
        mappingsResponse = client().admin().indices().prepareGetMappings(INDEX_NAME).get();
        mappingSource = mappingsResponse.mappings().get(INDEX_NAME).sourceAsMap();
        @SuppressWarnings("unchecked")
        Map<String, Object> finalProperties = (Map<String, Object>) mappingSource.get("properties");
        assertTrue("Mapping should contain dynamic field 'dynamic_extra'", finalProperties.containsKey("dynamic_extra"));

        // Refresh + flush again
        client().admin().indices().prepareRefresh(INDEX_NAME).get();
        client().admin().indices().prepareFlush(INDEX_NAME).get();

        // Verify all 15 rows on disk
        try (Stream<Path> paths = Files.list(parquetDir)) {
            parquetFiles = paths.filter(p -> p.toString().endsWith(".parquet")).collect(Collectors.toList());
        }
        totalRows = 0;
        for (Path pf : parquetFiles) {
            totalRows += RustBridge.getFileMetadata(pf.toString()).numRows();
        }
        assertEquals("Total rows across parquet files should equal 15", 15, totalRows);

        // Verify content
        allRows.clear();
        for (Path pf : parquetFiles) {
            allRows.addAll(parseJsonRows(RustBridge.readAsJson(pf.toString())));
        }
        assertEquals(15, allRows.size());
        long rowsWithExtra = allRows.stream().filter(row -> row.containsKey("dynamic_extra") && row.get("dynamic_extra") != null).count();
        assertEquals("5 rows should have dynamic_extra field populated", 5, rowsWithExtra);

        ensureGreen(INDEX_NAME);
    }

    public void testConflictingDynamicMappings() {
        String indexName = "test-conflict";

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse createResponse = client().admin().indices().prepareCreate(indexName).setSettings(indexSettings).get();
        assertTrue(createResponse.isAcknowledged());
        ensureGreen(indexName);

        // First doc: foo inferred as long
        client().prepareIndex(indexName).setId("1").setSource("foo", 3).get();

        // Second doc: foo as text — should fail
        try {
            client().prepareIndex(indexName).setId("2").setSource("foo", "bar").get();
            fail("Indexing request should have failed!");
        } catch (Exception e) {
            assertTrue(
                "Expected type conflict error but got: " + e.getMessage(),
                e.getMessage().contains("failed to parse field [foo] of type [long]")
                    || e.getMessage().contains("mapper [foo] cannot be changed from type [long] to [text]")
            );
        }
    }

    public void testConcurrentDynamicUpdates() throws Throwable {
        String indexName = "test-concurrent";

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse createResponse = client().admin().indices().prepareCreate(indexName).setSettings(indexSettings).get();
        assertTrue(createResponse.isAcknowledged());
        ensureGreen(indexName);

        final int numThreads = 32;
        final Thread[] indexThreads = new Thread[numThreads];
        final CountDownLatch startLatch = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            indexThreads[i] = new Thread(() -> {
                try {
                    startLatch.await();
                    client().prepareIndex(indexName).setId("a_" + threadId).setSource("fieldA_" + threadId, "valueA_" + threadId).get();
                    Thread.sleep(1000);
                    client().prepareIndex(indexName).setId("b_" + threadId).setSource("fieldB_" + threadId, "valueB_" + threadId).get();
                    Thread.sleep(1000);
                    client().admin().indices().prepareRefresh(indexName).get();
                } catch (Exception e) {
                    error.compareAndSet(null, e);
                }
            });
            indexThreads[i].start();
        }
        startLatch.countDown();
        for (Thread thread : indexThreads) {
            thread.join();
        }
        if (error.get() != null) {
            throw error.get();
        }

        // Final refresh + flush to ensure everything is on disk
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();

        // Verify all 64 fields (32 fieldA + 32 fieldB) in mapping
        GetMappingsResponse mappings = client().admin().indices().prepareGetMappings(indexName).get();
        Map<String, Object> mappingSource = mappings.getMappings().get(indexName).sourceAsMap();
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) mappingSource.get("properties");
        for (int i = 0; i < numThreads; i++) {
            assertTrue("Mapping should contain fieldA_" + i, properties.containsKey("fieldA_" + i));
            assertTrue("Mapping should contain fieldB_" + i, properties.containsKey("fieldB_" + i));
        }

        // Read parquet files and verify content
        IndexShard shard = getIndexShard(
            internalCluster().getDataNodeNames().iterator().next(),
            new ShardId(resolveIndex(indexName), 0),
            indexName
        );
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        assertTrue("Parquet directory should exist", Files.isDirectory(parquetDir));

        List<Path> parquetFiles;
        try (Stream<Path> paths = Files.list(parquetDir)) {
            parquetFiles = paths.filter(p -> p.toString().endsWith(".parquet")).collect(Collectors.toList());
        }

        // Verify row count
        long totalRows = 0;
        for (Path pf : parquetFiles) {
            totalRows += RustBridge.getFileMetadata(pf.toString()).numRows();
        }
        assertEquals("Total rows should equal 64 (32 threads * 2 docs each)", 64, totalRows);

        // Verify content
        List<Map<String, Object>> allRows = new ArrayList<>();
        for (Path pf : parquetFiles) {
            allRows.addAll(parseJsonRows(RustBridge.readAsJson(pf.toString())));
        }
        assertEquals(64, allRows.size());

        // Verify each fieldA_{i} and fieldB_{i} has its value in exactly one row
        Set<String> foundA = new HashSet<>();
        Set<String> foundB = new HashSet<>();
        for (Map<String, Object> row : allRows) {
            for (int i = 0; i < numThreads; i++) {
                if (("valueA_" + i).equals(row.get("fieldA_" + i))) {
                    foundA.add("fieldA_" + i);
                }
                if (("valueB_" + i).equals(row.get("fieldB_" + i))) {
                    foundB.add("fieldB_" + i);
                }
            }
        }
        assertEquals("All 32 fieldA values should be in parquet", numThreads, foundA.size());
        assertEquals("All 32 fieldB values should be in parquet", numThreads, foundB.size());
    }

    @SuppressWarnings("unchecked")
    @SuppressForbidden(reason = "JSON parsing for test verification of parquet output")
    private List<Map<String, Object>> parseJsonRows(String json) throws IOException {
        try (
            XContentParser parser = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                json
            )
        ) {
            return parser.list().stream().map(o -> (Map<String, Object>) o).collect(Collectors.toList());
        }
    }
}
