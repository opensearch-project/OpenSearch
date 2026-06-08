/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Integration test validating dynamic mapping support with composite parquet index.
 * Documents with new fields (not in the original mapping) should be indexed successfully,
 * and the resulting Parquet files should contain all fields including dynamically added ones.
 * <p>
 * Requires JDK 25 and sandbox enabled. Run with:
 * ./gradlew :sandbox:plugins:composite-engine:internalClusterTest \
 * --tests "*.CompositeDynamicMappingIT" \
 * -Dsandbox.enabled=true
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1)
public class CompositeDynamicMappingIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-dynamic-mapping";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            ArrowBasePlugin.class,
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    /**
     * Tests dynamic mapping with parquet primary + lucene secondary.
     * Verifies that dynamically added fields appear in both formats.
     * <p>
     * Note: The Lucene secondary writer stores inverted indexes for text/keyword fields
     * (for search) and __row_id__ as doc values (for cross-format correlation).
     * Numeric fields and field values are only in Parquet.
     */
    public void testDynamicMappingWithParquetPrimaryLuceneSecondary() throws Exception {
        String indexName = "test-dynamic-composite";

        CreateIndexResponse createResponse = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(parquetPrimaryLuceneSecondarySettings())
            .setMapping("field_keyword", "type=keyword", "field_number", "type=integer")
            .get();
        assertTrue(createResponse.isAcknowledged());
        ensureGreen(indexName);

        // Index docs with initial schema
        for (int i = 0; i < 5; i++) {
            IndexResponse response = client().prepareIndex()
                .setIndex(indexName)
                .setSource("field_keyword", "value_" + i, "field_number", i)
                .get();
            assertEquals(RestStatus.CREATED, response.status());
        }

        // Index docs with dynamic fields
        indexDocsWithDynamicFields(indexName, 5, 10);

        // Refresh + flush
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();

        // Verify parquet
        IndexShard shard = getIndexShard(indexName);
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        try (GatedCloseable<List<Path>> parquetFilesRef = listParquetFiles(parquetDir, shard)) {
            List<Path> parquetFiles = parquetFilesRef.get();
            List<Map<String, Object>> parquetRows = readAllParquetRows(parquetFiles);
            assertEquals("Parquet should have 10 rows", 10, parquetRows.size());
            assertDynamicFieldCount(parquetRows, "dynamic_text", 5);
            assertDynamicFieldCount(parquetRows, "dynamic_long", 5);
        }

        // Verify lucene secondary: doc count + indexed fields present
        Path luceneDir = shard.shardPath().resolveIndex();
        List<Map<String, Object>> luceneRows = readAllLuceneDocs(luceneDir);
        assertEquals("Lucene should have 10 docs", 10, luceneRows.size());

        // __row_id__ should be present in all docs (only doc values field in lucene secondary)
        long rowsWithRowId = luceneRows.stream().filter(r -> r.containsKey("__row_id__")).count();
        assertEquals("All 10 Lucene docs should have __row_id__", 10, rowsWithRowId);

        // Verify that the lucene index has the expected indexed fields (inverted index)
        assertLuceneIndexedFieldsPresent(luceneDir, Set.of("field_keyword", "dynamic_text"));
        ensureNoActiveMerges(indexName);
    }

    public void testConflictingDynamicMappings() {
        String indexName = "test-conflict";

        CreateIndexResponse createResponse = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(parquetPrimaryLuceneSecondarySettings())
            .get();
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

    /**
     * Tests concurrent dynamic mapping updates with parquet primary + lucene secondary.
     * Verifies both formats contain all dynamically created fields.
     */
    public void testConcurrentDynamicUpdatesWithLuceneSecondary() throws Throwable {
        String indexName = "test-concurrent-composite";

        CreateIndexResponse createResponse = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(parquetPrimaryLuceneSecondarySettings())
            .get();
        assertTrue(createResponse.isAcknowledged());
        ensureGreen(indexName);

        final int numThreads = 32;
        runConcurrentIndexing(indexName, numThreads);

        // Verify mappings
        assertConcurrentMappings(indexName, numThreads);

        // Verify parquet
        IndexShard shard = getIndexShard(indexName);
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        try (GatedCloseable<List<Path>> parquetFilesRef = listParquetFiles(parquetDir, shard)) {
            List<Path> parquetFiles = parquetFilesRef.get();
            assertEquals("Parquet total rows should be 64", 64, getParquetRowCount(parquetFiles));
            List<Map<String, Object>> parquetRows = readAllParquetRows(parquetFiles);
            assertEquals(64, parquetRows.size());
            assertConcurrentFieldValues(parquetRows, numThreads);
        }

        // Verify lucene secondary: doc count + all dynamic fields indexed
        Path luceneDir = shard.shardPath().resolveIndex();
        List<Map<String, Object>> luceneRows = readAllLuceneDocs(luceneDir);
        assertEquals("Lucene doc count should be 64", 64, luceneRows.size());

        // __row_id__ present in all docs
        long rowsWithRowId = luceneRows.stream().filter(r -> r.containsKey("__row_id__")).count();
        assertEquals("All 64 Lucene docs should have __row_id__", 64, rowsWithRowId);

        // Verify all dynamic fieldA_*/fieldB_* are indexed in Lucene
        Set<String> expectedFields = new HashSet<>();
        for (int i = 0; i < numThreads; i++) {
            expectedFields.add("fieldA_" + i);
            expectedFields.add("fieldB_" + i);
        }
        assertLuceneIndexedFieldsPresent(luceneDir, expectedFields);
        ensureNoActiveMerges(indexName);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Private helpers: index settings
    // ══════════════════════════════════════════════════════════════════════

    private Settings parquetOnlySettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();
    }

    private Settings parquetPrimaryLuceneSecondarySettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();
    }

    // ══════════════════════════════════════════════════════════════════════
    // Private helpers: indexing
    // ══════════════════════════════════════════════════════════════════════

    private void indexDocsWithDynamicFields(String indexName, int from, int to) {
        for (int i = from; i < to; i++) {
            IndexResponse response = client().prepareIndex()
                .setIndex(indexName)
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
    }

    private void runConcurrentIndexing(String indexName, int numThreads) throws Throwable {
        final Thread[] indexThreads = new Thread[numThreads];
        final CountDownLatch startLatch = new CountDownLatch(1);
        final AtomicReference<Throwable> error = new AtomicReference<>();

        for (int i = 0; i < numThreads; i++) {
            final int threadId = i;
            indexThreads[i] = new Thread(() -> {
                try {
                    startLatch.await();
                    IndexResponse respA = client().prepareIndex(indexName)
                        .setId("a_" + threadId)
                        .setSource("fieldA_" + threadId, "valueA_" + threadId)
                        .get();
                    assert respA.status() == RestStatus.CREATED : "index a_" + threadId + " failed: " + respA.status();
                    Thread.sleep(1000);
                    IndexResponse respB = client().prepareIndex(indexName)
                        .setId("b_" + threadId)
                        .setSource("fieldB_" + threadId, "valueB_" + threadId)
                        .get();
                    assert respB.status() == RestStatus.CREATED : "index b_" + threadId + " failed: " + respB.status();
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

        // Final refresh + flush
        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();
    }

    // Wait for any in-flight merges to complete to avoid file handle leaks when the cluster shuts down while merge threads still hold open
    // readers.
    // The best way to do so is to trigger a force merge to single segment and ensure it completes so that no merges will happen again until
    // non new documents are ingested
    private void ensureNoActiveMerges(String indexName) throws IOException {
        try {
            assertBusy(() -> {
                try {
                    client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).get();
                } catch (Exception e) {
                    // forceMerge throws if background merges are active — retry
                }
                try (GatedCloseable<CatalogSnapshot> cs = getIndexShard(indexName).getCatalogSnapshot()) {
                    assertEquals("Segment count after force merge should be 1", 1, cs.get().getSegments().size());
                }
            });
        } catch (Exception e) {
            throw new IOException("Timed out waiting for merges to complete", e);
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Private helpers: shard access
    // ══════════════════════════════════════════════════════════════════════

    private IndexShard getIndexShard(String indexName) {
        return getIndexShard(internalCluster().getDataNodeNames().iterator().next(), new ShardId(resolveIndex(indexName), 0), indexName);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Private helpers: parquet verification
    // ══════════════════════════════════════════════════════════════════════

    private GatedCloseable<List<Path>> listParquetFiles(Path parquetDir, IndexShard shard) throws IOException {
        assertTrue("Parquet directory should exist", Files.isDirectory(parquetDir));
        GatedCloseable<CatalogSnapshot> snapshot = shard.getCatalogSnapshot();
        List<Path> paths = new ArrayList<>();
        for (Segment segment : snapshot.get().getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            if (wfs != null) {
                for (String file : wfs.files()) {
                    paths.add(parquetDir.resolve(file));
                }
            }
        }
        return new GatedCloseable<>(paths, snapshot::close);
    }

    private long getParquetRowCount(List<Path> parquetFiles) throws IOException {
        long totalRows = 0;
        for (Path pf : parquetFiles) {
            ParquetFileMetadata meta = RustBridge.getFileMetadata(pf.toString());
            assertNotNull("Parquet file metadata should not be null: " + pf, meta);
            totalRows += meta.numRows();
        }
        return totalRows;
    }

    @SuppressForbidden(reason = "JSON parsing for test verification of parquet output")
    private List<Map<String, Object>> readAllParquetRows(List<Path> parquetFiles) throws IOException {
        List<Map<String, Object>> allRows = new ArrayList<>();
        for (Path pf : parquetFiles) {
            allRows.addAll(parseJsonRows(RustBridge.readAsJson(pf.toString())));
        }
        return allRows;
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

    // ══════════════════════════════════════════════════════════════════════
    // Private helpers: lucene verification
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Reads all documents from a Lucene index directory, extracting doc values fields
     * into a list of maps (one map per document).
     */
    private List<Map<String, Object>> readAllLuceneDocs(Path luceneDir) throws IOException {
        List<Map<String, Object>> rows = new ArrayList<>();
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                LeafReader leaf = ctx.reader();
                for (int doc = 0; doc < leaf.maxDoc(); doc++) {
                    Map<String, Object> row = new HashMap<>();
                    for (FieldInfo fi : leaf.getFieldInfos()) {
                        if (fi.getDocValuesType() == DocValuesType.SORTED_NUMERIC) {
                            SortedNumericDocValues dv = leaf.getSortedNumericDocValues(fi.name);
                            if (dv != null && dv.advanceExact(doc)) {
                                row.put(fi.name, dv.nextValue());
                            }
                        } else if (fi.getDocValuesType() == DocValuesType.SORTED_SET) {
                            SortedSetDocValues dv = leaf.getSortedSetDocValues(fi.name);
                            if (dv != null && dv.advanceExact(doc)) {
                                long ord = dv.nextOrd();
                                if (ord >= 0) {
                                    row.put(fi.name, dv.lookupOrd(ord).utf8ToString());
                                }
                            }
                        }
                    }
                    rows.add(row);
                }
            }
        }
        return rows;
    }

    // ══════════════════════════════════════════════════════════════════════
    // Private helpers: assertions
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Asserts that the given fields are present in the index mapping, polling with assertBusy
     * to account for the cluster-manager applying its own cluster state after publication completes.
     */
    private void assertMappingsContain(String indexName, String... expectedFields) throws Exception {
        assertBusy(() -> {
            GetMappingsResponse mappings = client().admin().indices().prepareGetMappings(indexName).get();
            Map<String, Object> mappingSource = mappings.getMappings().get(indexName).sourceAsMap();
            @SuppressWarnings("unchecked")
            Map<String, Object> properties = (Map<String, Object>) mappingSource.get("properties");
            for (String field : expectedFields) {
                assertTrue("Mapping should contain field '" + field + "'", properties.containsKey(field));
            }
        });
    }

    private void assertDynamicFieldCount(List<Map<String, Object>> rows, String fieldName, long expectedCount) {
        long count = rows.stream().filter(row -> row.containsKey(fieldName) && row.get(fieldName) != null).count();
        assertEquals(expectedCount + " rows should have " + fieldName + " field populated", expectedCount, count);
    }

    private void assertConcurrentMappings(String indexName, int numThreads) throws Exception {
        String[] expectedFields = new String[numThreads * 2];
        for (int i = 0; i < numThreads; i++) {
            expectedFields[i * 2] = "fieldA_" + i;
            expectedFields[i * 2 + 1] = "fieldB_" + i;
        }
        assertMappingsContain(indexName, expectedFields);
    }

    private void assertConcurrentFieldValues(List<Map<String, Object>> rows, int numThreads) {
        Set<String> foundA = new HashSet<>();
        Set<String> foundB = new HashSet<>();
        for (Map<String, Object> row : rows) {
            for (int i = 0; i < numThreads; i++) {
                if (("valueA_" + i).equals(row.get("fieldA_" + i))) foundA.add("fieldA_" + i);
                if (("valueB_" + i).equals(row.get("fieldB_" + i))) foundB.add("fieldB_" + i);
            }
        }
        assertEquals("All 32 fieldA values should be present", numThreads, foundA.size());
        assertEquals("All 32 fieldB values should be present", numThreads, foundB.size());
    }

    /**
     * Asserts that the given fields exist in the Lucene index as indexed fields (inverted index).
     * The Lucene secondary stores inverted indexes for search but not doc values for field values.
     */
    private void assertLuceneIndexedFieldsPresent(Path luceneDir, Set<String> expectedFields) throws IOException {
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            Set<String> allFields = new HashSet<>();
            for (LeafReaderContext ctx : reader.leaves()) {
                for (FieldInfo fi : ctx.reader().getFieldInfos()) {
                    allFields.add(fi.name);
                }
            }
            for (String expected : expectedFields) {
                assertTrue(
                    "Lucene index should contain field '" + expected + "', found fields: " + allFields,
                    allFields.contains(expected)
                );
            }
        }
    }
}
