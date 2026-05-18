/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.parquet.bridge.ParquetFileMetadata;
import org.opensearch.parquet.bridge.RustBridge;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.util.FeatureFlags;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Integration test for composite refresh (flush) with sort columns configured.
 * Verifies that sort-on-close in Parquet and reorder in Lucene produce correct results
 * at the individual segment level (pre-merge).
 *
 * @opensearch.experimental
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeRefreshSortedIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-composite-refresh-sorted";

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

    @Override
    public void tearDown() throws Exception {
        try {
            client().admin().indices().prepareDelete(INDEX_NAME).get();
        } catch (Exception e) {
            // index may not exist
        }
        super.tearDown();
    }

    // ══════════════════════════════════════════════════════════════════════
    // Tests
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Verifies that a single flush with sort columns produces a Parquet file
     * sorted by age DESC (nulls first), name ASC (nulls last).
     */
    public void testSortedRefreshProducesSortedParquet() throws Exception {
        createIndex(sortedParquetOnlySettings());

        // Index documents in deliberately unsorted order
        indexDoc("charlie", 30);
        indexDoc("alice", 50);
        indexDoc("bob", 10);
        indexDoc("dave", 50);
        indexDoc("eve", 30);

        flushAndRefresh();

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        assertEquals(Set.of("parquet"), snapshot.getDataFormats());
        verifyParquetRowCount(snapshot, 5);
        verifyParquetSortOrder(snapshot);
        verifyParquetRowIdSequential(snapshot);
    }

    /**
     * Verifies sorted refresh with Lucene secondary:
     * - Parquet is sorted
     * - Lucene __row_id__ is sequential (RowIdMapping applied)
     * - Cross-format consistency: reading Parquet and Lucene in physical order produces
     *   the same {@code name} and {@code tag} values at every position. {@code tag} is
     *   a keyword field NOT in the sort key — it confirms non-sort fields are also
     *   correctly co-located across formats. Numeric fields like {@code age} live only
     *   in Parquet, so cannot be compared across formats.
     */
    public void testSortedRefreshWithLuceneSecondary() throws Exception {
        createIndex(sortedParquetWithLuceneSettings());

        indexDoc("charlie", 30, "blue");
        indexDoc("alice", 50, "red");
        indexDoc("bob", 10, "green");
        indexDoc("dave", 50, "yellow");
        indexDoc("eve", 30, "purple");

        flushAndRefresh();

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        Set<String> formats = snapshot.getDataFormats();
        assertTrue("Should have parquet format", formats.contains("parquet"));
        assertTrue("Should have lucene format", formats.contains("lucene"));

        verifyParquetRowCount(snapshot, 5);
        verifyParquetSortOrder(snapshot);
        verifyLuceneDocCount(5);
        verifyLuceneRowIdSequential();
        verifyParquetAndLuceneRowsAlignedSequentially(snapshot);
    }

    /**
     * Verifies that multiple flush cycles produce independently sorted segments.
     */
    public void testMultipleSortedRefreshesProduceIndependentlySortedSegments() throws Exception {
        createIndex(sortedParquetOnlySettings());

        // First batch
        indexDoc("zara", 5);
        indexDoc("alice", 100);
        indexDoc("bob", 50);
        flushAndRefresh();

        // Second batch
        indexDoc("xavier", 200);
        indexDoc("yolanda", 1);
        indexDoc("wendy", 75);
        flushAndRefresh();

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        assertEquals("Should have 2 segments", 2, snapshot.getSegments().size());
        verifyParquetRowCount(snapshot, 6);
        // Each segment should be independently sorted
        verifyParquetSortOrder(snapshot);
    }

    /**
     * Verifies null handling in sorted output: age DESC with nulls first,
     * name ASC with nulls last. Runs against Parquet primary + Lucene secondary
     * to also confirm that the row ID rewrite (driven by Parquet's sort
     * permutation) yields a sequential {@code __row_id__} in Lucene even when
     * the sort key contains nulls.
     */
    public void testSortedRefreshWithNulls() throws Exception {
        createIndex(sortedParquetWithLuceneSettings());

        // Mix of null and non-null values
        indexDoc("alice", 50);
        indexDocNullAge("bob");       // null age → should sort first (nulls first for age)
        indexDoc("charlie", 30);
        indexDocNullAge("dave");      // null age → should sort first
        indexDoc("eve", 50);

        flushAndRefresh();

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        Set<String> formats = snapshot.getDataFormats();
        assertTrue("Should have parquet format", formats.contains("parquet"));
        assertTrue("Should have lucene format", formats.contains("lucene"));

        verifyParquetRowCount(snapshot, 5);
        verifyParquetSortOrder(snapshot);
        verifyLuceneDocCount(5);
        verifyLuceneRowIdSequential();
    }

    /**
     * Verifies correctness with enough rows to trigger the chunked sort path
     * (rows > sort_batch_size). Uses default sort_batch_size of 65536.
     */
    public void testSortedRefreshWithLargeBatch() throws Exception {
        createIndex(sortedParquetWithLuceneSettings());

        int totalDocs = 200;
        for (int i = 0; i < totalDocs; i++) {
            indexDoc("name_" + String.format("%05d", i), randomIntBetween(0, 1000));
        }

        flushAndRefresh();

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        verifyParquetRowCount(snapshot, totalDocs);
        verifyParquetSortOrder(snapshot);
        verifyLuceneDocCount(totalDocs);
        verifyLuceneRowIdSequential();
    }

    // ══════════════════════════════════════════════════════════════════════
    // Helpers: settings
    // ══════════════════════════════════════════════════════════════════════

    private Settings sortedParquetOnlySettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .putList("index.sort.field", "age", "name")
            .putList("index.sort.order", "desc", "asc")
            .putList("index.sort.missing", "_first", "_last")
            .build();
    }

    private Settings sortedParquetWithLuceneSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .putList("index.sort.field", "age", "name")
            .putList("index.sort.order", "desc", "asc")
            .putList("index.sort.missing", "_first", "_last")
            .build();
    }

    // ══════════════════════════════════════════════════════════════════════
    // Helpers: indexing
    // ══════════════════════════════════════════════════════════════════════

    private void createIndex(Settings settings) {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(settings)
            .setMapping("name", "type=keyword", "age", "type=integer", "tag", "type=keyword")
            .get();
        ensureGreen(INDEX_NAME);
    }

    private void indexDoc(String name, int age) {
        IndexResponse response = client().prepareIndex()
            .setIndex(INDEX_NAME)
            .setSource("name", name, "age", age)
            .get();
        assertEquals(RestStatus.CREATED, response.status());
    }

    private void indexDoc(String name, int age, String tag) {
        IndexResponse response = client().prepareIndex()
            .setIndex(INDEX_NAME)
            .setSource("name", name, "age", age, "tag", tag)
            .get();
        assertEquals(RestStatus.CREATED, response.status());
    }

    private void indexDocNullAge(String name) {
        IndexResponse response = client().prepareIndex()
            .setIndex(INDEX_NAME)
            .setSource("name", name)
            .get();
        assertEquals(RestStatus.CREATED, response.status());
    }

    private void flushAndRefresh() {
        RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(INDEX_NAME).get();
        assertEquals(RestStatus.OK, refreshResponse.getStatus());
        FlushResponse flushResponse = client().admin().indices().prepareFlush(INDEX_NAME).setForce(true).get();
        assertEquals(RestStatus.OK, flushResponse.getStatus());
    }

    // ══════════════════════════════════════════════════════════════════════
    // Helpers: verification
    // ══════════════════════════════════════════════════════════════════════

    private void verifyParquetRowCount(DataformatAwareCatalogSnapshot snapshot, int expectedTotalDocs) throws IOException {
        Path parquetDir = getParquetDir();
        long totalRows = 0;
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            assertNotNull("Segment should have parquet files", wfs);
            for (String file : wfs.files()) {
                Path filePath = parquetDir.resolve(file);
                assertTrue("Parquet file should exist: " + filePath, Files.exists(filePath));
                ParquetFileMetadata metadata = RustBridge.getFileMetadata(filePath.toString());
                totalRows += metadata.numRows();
            }
        }
        assertEquals("Total rows should match ingested docs", expectedTotalDocs, totalRows);
    }

    @SuppressForbidden(reason = "JSON parsing for sort order verification")
    private void verifyParquetSortOrder(DataformatAwareCatalogSnapshot snapshot) throws Exception {
        Path parquetDir = getParquetDir();
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            for (String file : wfs.files()) {
                Path filePath = parquetDir.resolve(file);
                String json = RustBridge.readAsJson(filePath.toString());
                List<Map<String, Object>> rows;
                try (
                    XContentParser parser = JsonXContent.jsonXContent.createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        json
                    )
                ) {
                    rows = parser.list().stream().map(o -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> m = (Map<String, Object>) o;
                        return m;
                    }).toList();
                }
                if (rows.size() <= 1) continue;

                for (int i = 1; i < rows.size(); i++) {
                    Object prevAge = rows.get(i - 1).get("age");
                    Object currAge = rows.get(i).get("age");

                    // nulls first for age
                    if (prevAge == null && currAge == null) continue;
                    if (prevAge == null) continue;
                    if (currAge == null) {
                        fail("age null should come before non-null at row " + i);
                    }

                    int prevAgeVal = ((Number) prevAge).intValue();
                    int currAgeVal = ((Number) currAge).intValue();

                    assertTrue(
                        "age should be DESC but found " + prevAgeVal + " before " + currAgeVal + " at row " + i + " in " + file,
                        prevAgeVal >= currAgeVal
                    );

                    // When age is equal, verify name ASC (nulls last)
                    if (prevAgeVal == currAgeVal) {
                        Object prevName = rows.get(i - 1).get("name");
                        Object currName = rows.get(i).get("name");

                        if (prevName != null && currName == null) continue;
                        if (prevName == null && currName != null) {
                            fail("name nulls should be last at row " + i + " in " + file);
                        }
                        if (prevName != null && currName != null) {
                            assertTrue(
                                "name should be ASC but found '" + prevName + "' before '" + currName + "' at row " + i + " in " + file,
                                ((String) prevName).compareTo((String) currName) <= 0
                            );
                        }
                    }
                }
            }
        }
    }

    private void verifyLuceneDocCount(int expectedTotalDocs) throws IOException {
        Path luceneDir = getLuceneDir();
        assertTrue("Lucene directory should exist", Files.exists(luceneDir));
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals("Lucene doc count should match", expectedTotalDocs, reader.numDocs());
        }
    }

    private void verifyLuceneRowIdSequential() throws IOException {
        Path luceneDir = getLuceneDir();
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                SortedNumericDocValues rowIdDV = ctx.reader().getSortedNumericDocValues(DocumentInput.ROW_ID_FIELD);
                if (rowIdDV == null) continue;

                long expectedRowId = 0;
                for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
                    if (rowIdDV.advanceExact(doc)) {
                        long rowId = rowIdDV.nextValue();
                        assertEquals(
                            DocumentInput.ROW_ID_FIELD + " should be sequential, expected " + expectedRowId + " but got " + rowId
                                + " at doc " + doc,
                            expectedRowId,
                            rowId
                        );
                        expectedRowId++;
                    }
                }
            }
        }
    }

    @SuppressForbidden(reason = "JSON parsing for row ID verification")
    private void verifyParquetRowIdSequential(DataformatAwareCatalogSnapshot snapshot) throws Exception {
        Path parquetDir = getParquetDir();
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            for (String file : wfs.files()) {
                Path filePath = parquetDir.resolve(file);
                String json = RustBridge.readAsJson(filePath.toString());
                List<Map<String, Object>> rows;
                try (
                    XContentParser parser = JsonXContent.jsonXContent.createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        json
                    )
                ) {
                    rows = parser.list().stream().map(o -> {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> m = (Map<String, Object>) o;
                        return m;
                    }).toList();
                }

                long expectedRowId = 0;
                for (Map<String, Object> row : rows) {
                    Object rowIdObj = row.get(DocumentInput.ROW_ID_FIELD);
                    assertNotNull(
                        DocumentInput.ROW_ID_FIELD + " should be present in parquet row " + expectedRowId + " in " + file,
                        rowIdObj
                    );
                    long rowId = ((Number) rowIdObj).longValue();
                    assertEquals(
                        DocumentInput.ROW_ID_FIELD + " should be sequential, expected " + expectedRowId + " but got " + rowId + " in "
                            + file,
                        expectedRowId,
                        rowId
                    );
                    expectedRowId++;
                }
            }
        }
    }

    /**
     * Reads Parquet rows and Lucene documents in physical (storage) order from the
     * single-shard, single-segment index and asserts that at every position i,
     * Parquet row i and Lucene doc i agree on keyword fields ({@code name} and
     * {@code tag}).
     * <p>
     * The Lucene secondary writer only persists text/keyword fields (in the inverted
     * index) and {@code __row_id__} (as doc values). Numeric fields like {@code age}
     * are intentionally NOT stored in the Lucene secondary — they live only in
     * Parquet. Keyword fields are also stored only in the inverted index (no doc
     * values), so we read them via TermsEnum/postings and reconstruct the per-doc
     * value.
     * <p>
     * The {@code tag} field is NOT part of the sort key — verifying it confirms the
     * row ID rewrite correctly co-locates non-sort fields across formats too.
     */
    @SuppressForbidden(reason = "JSON parsing for cross-format row alignment")
    private void verifyParquetAndLuceneRowsAlignedSequentially(DataformatAwareCatalogSnapshot snapshot) throws Exception {
        // Read Parquet rows in physical order
        Path parquetDir = getParquetDir();
        List<Map<String, Object>> parquetRows = new java.util.ArrayList<>();
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            for (String file : wfs.files()) {
                String json = RustBridge.readAsJson(parquetDir.resolve(file).toString());
                try (
                    XContentParser parser = JsonXContent.jsonXContent.createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        json
                    )
                ) {
                    for (Object obj : parser.list()) {
                        @SuppressWarnings("unchecked")
                        Map<String, Object> row = (Map<String, Object>) obj;
                        parquetRows.add(row);
                    }
                }
            }
        }

        // Reconstruct per-doc keyword field values from Lucene's inverted index
        Path luceneDir = getLuceneDir();
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals("Test assumes a single Lucene leaf for sequential alignment", 1, reader.leaves().size());
            org.apache.lucene.index.LeafReader leaf = reader.leaves().get(0).reader();

            String[] luceneNames = readKeywordValuesPerDoc(leaf, "name");
            String[] luceneTags = readKeywordValuesPerDoc(leaf, "tag");

            assertEquals("Parquet and Lucene must have same row count", parquetRows.size(), luceneNames.length);

            for (int i = 0; i < parquetRows.size(); i++) {
                Map<String, Object> pq = parquetRows.get(i);
                String pqName = (String) pq.get("name");
                String pqTag = (String) pq.get("tag");
                assertEquals("name mismatch at position " + i, pqName, luceneNames[i]);
                assertEquals("tag mismatch at position " + i, pqTag, luceneTags[i]);
            }
        }
    }

    /**
     * Reconstructs the per-doc keyword value for the given field by iterating
     * the inverted index. Each doc is expected to have at most one term for the
     * field. Returns an array indexed by Lucene doc ID.
     */
    private String[] readKeywordValuesPerDoc(org.apache.lucene.index.LeafReader leaf, String fieldName) throws IOException {
        String[] values = new String[leaf.maxDoc()];
        org.apache.lucene.index.Terms terms = leaf.terms(fieldName);
        assertNotNull("Lucene index should have field '" + fieldName + "' in inverted index", terms);
        org.apache.lucene.index.TermsEnum it = terms.iterator();
        org.apache.lucene.util.BytesRef term;
        while ((term = it.next()) != null) {
            String value = term.utf8ToString();
            org.apache.lucene.index.PostingsEnum postings = it.postings(null);
            int doc;
            while ((doc = postings.nextDoc()) != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS) {
                values[doc] = value;
            }
        }
        return values;
    }

    // ══════════════════════════════════════════════════════════════════════
    // Helpers: accessors
    // ══════════════════════════════════════════════════════════════════════

    private DataformatAwareCatalogSnapshot getCatalogSnapshot() throws IOException {
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, getNodeName());
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(INDEX_NAME));
        IndexShard shard = indexService.getShard(0);
        CommitStats commitStats = shard.commitStats();
        assertNotNull(commitStats);
        assertNotNull(commitStats.getUserData());
        assertTrue(commitStats.getUserData().containsKey(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY));
        return DataformatAwareCatalogSnapshot.deserializeFromString(
            commitStats.getUserData().get(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY),
            Function.identity()
        );
    }

    private Path getParquetDir() {
        IndexShard shard = getPrimaryShard();
        return shard.shardPath().getDataPath().resolve("parquet");
    }

    private Path getLuceneDir() {
        IndexShard shard = getPrimaryShard();
        return shard.shardPath().resolveIndex();
    }

    private IndexShard getPrimaryShard() {
        String nodeName = getNodeName();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(INDEX_NAME));
        return indexService.getShard(0);
    }

    private String getNodeName() {
        String nodeId = getClusterState().routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
        return getClusterState().nodes().get(nodeId).getName();
    }
}
