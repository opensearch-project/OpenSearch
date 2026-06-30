/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Randomized integration tests for composite field configurations.
 * Verifies that fields with various index/doc_values combinations are correctly
 * stored in parquet (columnar) and lucene (inverted index) based on their capabilities.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CompositeFieldConfigRandomizedIT extends AbstractCompositeEngineIT {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).build();
    }

    private Settings dfaSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();
    }

    private void startCluster() {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
    }

    /**
     * Tests randomized field configurations where keyword and numeric fields randomly
     * have index=true or index=false. Verifies that:
     * - All fields appear in parquet regardless of index setting
     * - Only indexed fields appear in lucene FieldInfos
     * - __row_id__ is always present in lucene
     */
    public void testRandomizedFieldConfigIndexingAndVerification() throws Exception {
        startCluster();
        String indexName = "test-randomized-field-config";

        // Randomly decide index=true/false for keyword and numeric fields
        boolean kw1Indexed = randomBoolean();
        boolean kw2Indexed = randomBoolean();
        boolean kw3Indexed = randomBoolean();
        boolean longIndexed = randomBoolean();
        boolean doubleIndexed = randomBoolean();

        // Text fields always need index=true
        logger.info(
            "Random config: f_keyword_1.index={}, f_keyword_2.index={}, f_keyword_3.index={}, "
                + "f_long.index={}, f_double.index={}, f_text_1.index=true, f_text_2.index=true, f_match_only_text.index=true",
            kw1Indexed,
            kw2Indexed,
            kw3Indexed,
            longIndexed,
            doubleIndexed
        );

        String mapping = "{\n"
            + "  \"properties\": {\n"
            + "    \"f_keyword_1\": { \"type\": \"keyword\", \"index\": "
            + kw1Indexed
            + ", \"doc_values\": true },\n"
            + "    \"f_keyword_2\": { \"type\": \"keyword\", \"index\": "
            + kw2Indexed
            + ", \"doc_values\": true },\n"
            + "    \"f_keyword_3\": { \"type\": \"keyword\", \"index\": "
            + kw3Indexed
            + ", \"doc_values\": true },\n"
            + "    \"f_text_1\": { \"type\": \"text\" },\n"
            + "    \"f_text_2\": { \"type\": \"text\" },\n"
            + "    \"f_match_only_text\": { \"type\": \"match_only_text\" },\n"
            + "    \"f_long\": { \"type\": \"long\", \"index\": "
            + longIndexed
            + ", \"doc_values\": true },\n"
            + "    \"f_double\": { \"type\": \"double\", \"index\": "
            + doubleIndexed
            + ", \"doc_values\": true }\n"
            + "  }\n"
            + "}";

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(dfaSettings())
            .setMapping(mapping)
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(indexName);

        // Index 5 documents with values for all fields
        for (int i = 1; i <= 5; i++) {
            assertEquals(
                RestStatus.CREATED,
                client().prepareIndex(indexName)
                    .setSource(
                        "f_keyword_1",
                        "kw1_val_" + i,
                        "f_keyword_2",
                        "kw2_val_" + i,
                        "f_keyword_3",
                        "kw3_val_" + i,
                        "f_text_1",
                        "text one content " + i,
                        "f_text_2",
                        "text two content " + i,
                        "f_match_only_text",
                        "match only " + i,
                        "f_long",
                        (long) (i * 100),
                        "f_double",
                        i * 1.5
                    )
                    .get()
                    .status()
            );
        }

        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();

        // Verify doc count = 5
        assertDocCount(indexName, 5);

        // Verify parquet has columnar fields (keyword + numeric always have doc_values)
        IndexShard shard = getPrimaryShard(indexName);
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        List<Map<String, Object>> parquetRows = readAllParquetRows(parquetDir, shard);
        assertEquals(5, parquetRows.size());

        // keyword and numeric fields always stored in parquet (COLUMNAR_STORAGE via doc_values)
        for (String field : List.of("f_keyword_1", "f_keyword_2", "f_keyword_3", "f_long", "f_double")) {
            String finalField = field;
            assertTrue(
                field + " should be present in parquet with non-null values",
                parquetRows.stream().allMatch(r -> r.containsKey(finalField) && r.get(finalField) != null)
            );
        }
        // text/match_only_text are stored in parquet via STORED_FIELDS but may appear differently
        for (String field : List.of("f_text_1", "f_text_2")) {
            String finalField = field;
            assertTrue(field + " should be present in parquet", parquetRows.stream().anyMatch(r -> r.containsKey(finalField)));
        }

        // Verify lucene fields
        Path luceneDir = shard.shardPath().resolveIndex();
        Set<String> luceneFields = getLuceneFields(luceneDir);

        // __row_id__ is always present
        assertTrue("__row_id__ should always be in lucene", luceneFields.contains("__row_id__"));

        // Text fields with index=true should be in lucene
        assertTrue("f_text_1 should be in lucene", luceneFields.contains("f_text_1"));
        assertTrue("f_text_2 should be in lucene", luceneFields.contains("f_text_2"));
        assertTrue("f_match_only_text should be in lucene", luceneFields.contains("f_match_only_text"));

        // Keyword fields: present in lucene only if indexed
        assertFieldInLucene(luceneFields, "f_keyword_1", kw1Indexed);
        assertFieldInLucene(luceneFields, "f_keyword_2", kw2Indexed);
        assertFieldInLucene(luceneFields, "f_keyword_3", kw3Indexed);

        // Numeric fields with index=false should NOT appear in lucene FieldInfos
        if (!longIndexed) {
            assertFalse("f_long with index=false should NOT be in lucene", luceneFields.contains("f_long"));
        }
        if (!doubleIndexed) {
            assertFalse("f_double with index=false should NOT be in lucene", luceneFields.contains("f_double"));
        }
    }

    /**
     * Tests keyword and numeric fields with index=false, doc_values=true (columnar-only).
     * Verifies that parquet stores the data (COLUMNAR_STORAGE) while lucene does not
     * have these fields (no FULL_TEXT_SEARCH requested).
     */
    public void testColumnarOnlyFieldsIndexAndVerify() throws Exception {
        startCluster();
        String indexName = "test-columnar-only";

        String mapping = "{\n"
            + "  \"properties\": {\n"
            + "    \"col_keyword\": { \"type\": \"keyword\", \"index\": false, \"doc_values\": true },\n"
            + "    \"col_long\": { \"type\": \"long\", \"index\": false, \"doc_values\": true },\n"
            + "    \"col_double\": { \"type\": \"double\", \"index\": false, \"doc_values\": true }\n"
            + "  }\n"
            + "}";

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(dfaSettings())
            .setMapping(mapping)
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(indexName);

        // Index 5 documents
        for (int i = 1; i <= 5; i++) {
            assertEquals(
                RestStatus.CREATED,
                client().prepareIndex(indexName)
                    .setSource("col_keyword", "value_" + i, "col_long", (long) (i * 10), "col_double", i * 2.5)
                    .get()
                    .status()
            );
        }

        client().admin().indices().prepareRefresh(indexName).get();
        client().admin().indices().prepareFlush(indexName).get();

        // Verify doc count
        assertDocCount(indexName, 5);

        // Verify parquet has all columnar fields with data
        IndexShard shard = getPrimaryShard(indexName);
        Path parquetDir = shard.shardPath().getDataPath().resolve("parquet");
        List<Map<String, Object>> parquetRows = readAllParquetRows(parquetDir, shard);
        assertEquals(5, parquetRows.size());

        for (String field : List.of("col_keyword", "col_long", "col_double")) {
            String finalField = field;
            assertTrue(
                field + " should be in parquet (COLUMNAR_STORAGE satisfied)",
                parquetRows.stream().allMatch(r -> r.containsKey(finalField) && r.get(finalField) != null)
            );
        }

        // Verify lucene does NOT have these fields (no FULL_TEXT_SEARCH requested)
        Path luceneDir = shard.shardPath().resolveIndex();
        Set<String> luceneFields = getLuceneFields(luceneDir);

        assertFalse("col_keyword should NOT be in lucene (index=false)", luceneFields.contains("col_keyword"));
        assertFalse("col_long should NOT be in lucene (index=false)", luceneFields.contains("col_long"));
        assertFalse("col_double should NOT be in lucene (index=false)", luceneFields.contains("col_double"));

        // __row_id__ should still be present
        assertTrue("__row_id__ should always be in lucene", luceneFields.contains("__row_id__"));
    }

    // ══════════════════════════════════════════════════════════════════════
    // Private helpers
    // ══════════════════════════════════════════════════════════════════════

    private void assertFieldInLucene(Set<String> luceneFields, String fieldName, boolean shouldBePresent) {
        if (shouldBePresent) {
            assertTrue(fieldName + " with index=true should be in lucene", luceneFields.contains(fieldName));
        } else {
            assertFalse(fieldName + " with index=false should NOT be in lucene", luceneFields.contains(fieldName));
        }
    }

    private void assertDocCount(String indexName, long expectedCount) {
        IndicesStatsResponse stats = client().admin().indices().prepareStats(indexName).clear().setDocs(true).get();
        assertEquals(expectedCount, stats.getIndex(indexName).getPrimaries().getDocs().getCount());
    }

    @SuppressForbidden(reason = "JSON parsing for test verification")
    private List<Map<String, Object>> readAllParquetRows(Path parquetDir, IndexShard shard) throws IOException {
        assertTrue("Parquet directory should exist", Files.isDirectory(parquetDir));
        List<Map<String, Object>> allRows = new ArrayList<>();
        try (GatedCloseable<CatalogSnapshot> snapshot = shard.getCatalogSnapshot()) {
            for (Segment segment : snapshot.get().getSegments()) {
                WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
                if (wfs != null) {
                    for (String file : wfs.files()) {
                        Path filePath = parquetDir.resolve(file);
                        allRows.addAll(parseJsonRows(RustBridge.readAsJson(filePath.toString())));
                    }
                }
            }
        }
        return allRows;
    }

    @SuppressWarnings("unchecked")
    @SuppressForbidden(reason = "JSON parsing for test verification")
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

    private Set<String> getLuceneFields(Path luceneDir) throws IOException {
        Set<String> allFields = new HashSet<>();
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                for (FieldInfo fi : ctx.reader().getFieldInfos()) {
                    allFields.add(fi.name);
                }
            }
        }
        return allFields;
    }
}
