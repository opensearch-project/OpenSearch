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
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
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
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
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
        // ParquetDataFormatPlugin sources its allocator from the unified native-allocator
        // framework's ingest pool, so the framework plugin must be installed.
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
     * Verifies correctness with a randomized number of documents and fields.
     * Uses random(100, 10000) docs and random(3, 10) integer sort fields to
     * exercise the sort path with varying data shapes.
     */
    public void testSortedRefreshWithRandomizedData() throws Exception {
        int numFields = randomIntBetween(3, 10);
        String[] fieldNames = new String[numFields];
        String[] sortFields = new String[numFields];
        String[] sortOrders = new String[numFields];
        String[] sortMissing = new String[numFields];
        for (int f = 0; f < numFields; f++) {
            fieldNames[f] = "field_" + f;
            sortFields[f] = fieldNames[f];
            sortOrders[f] = randomBoolean() ? "asc" : "desc";
            sortMissing[f] = sortOrders[f].equals("asc") ? "_last" : "_first";
        }

        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .putList("index.sort.field", sortFields)
            .putList("index.sort.order", sortOrders)
            .putList("index.sort.missing", sortMissing)
            .build();

        // Build mapping: all fields are integers for sort compatibility
        String[] mappingArgs = new String[numFields * 2];
        for (int f = 0; f < numFields; f++) {
            mappingArgs[f * 2] = fieldNames[f];
            mappingArgs[f * 2 + 1] = "type=integer";
        }
        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(settings).setMapping(mappingArgs).get();
        ensureGreen(INDEX_NAME);

        int totalDocs = randomIntBetween(100, 10000);
        logger.info(
            "testSortedRefreshWithRandomizedData: {} docs, {} fields, sort orders: {}",
            totalDocs,
            numFields,
            Arrays.toString(sortOrders)
        );

        for (int i = 0; i < totalDocs; i++) {
            Map<String, Object> source = new HashMap<>();
            for (int f = 0; f < numFields; f++) {
                // Occasionally emit null values to exercise null handling in sort
                if (randomIntBetween(0, 9) == 0) {
                    continue; // skip field → null
                }
                source.put(fieldNames[f], randomIntBetween(0, 1000));
            }
            IndexResponse response = client().prepareIndex().setIndex(INDEX_NAME).setSource(source).get();
            assertEquals(RestStatus.CREATED, response.status());
        }

        flushAndRefresh();

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        verifyParquetRowCount(snapshot, totalDocs);
        verifyParquetSortOrderMultiField(snapshot, fieldNames, sortOrders, sortMissing);
        verifyLuceneDocCount(totalDocs);
        verifyLuceneRowIdSequential();
    }

    /**
     * Parquet-only refresh without any sort configuration. Confirms the
     * non-sort flush path still emits sequential {@code __row_id__} values
     * and the expected row count. There is no permutation produced by Parquet,
     * so this exercises the {@code FlushInput.EMPTY} flow end-to-end.
     */
    public void testUnsortedRefreshParquetOnly() throws Exception {
        createIndex(unsortedParquetOnlySettings());

        // Insertion order is preserved end-to-end since no sort is configured.
        indexDoc("charlie", 30);
        indexDoc("alice", 50);
        indexDoc("bob", 10);
        indexDoc("dave", 50);
        indexDoc("eve", 30);

        flushAndRefresh();

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        assertEquals(Set.of("parquet"), snapshot.getDataFormats());
        verifyParquetRowCount(snapshot, 5);
        verifyParquetRowIdSequential(snapshot);
    }

    /**
     * Parquet primary + Lucene secondary without sort. Without a sort
     * permutation, Parquet does not produce a {@code RowIdMapping} and the
     * Lucene secondary writer takes the unsorted path. Both formats should:
     *   - hold the same row count,
     *   - expose sequential {@code __row_id__} (Parquet rows and Lucene docs),
     *   - align position-for-position on shared keyword fields, since both
     *     follow insertion order with no reorder applied.
     */
    public void testUnsortedRefreshWithLuceneSecondary() throws Exception {
        createIndex(unsortedParquetWithLuceneSettings());

        // Deliberately unsorted insertion: confirms no sort is applied (otherwise
        // verifyParquetAndLuceneRowsAlignedSequentially would still pass but only
        // because both sides reordered the same way; a position-wise match against
        // insertion order is the stronger check below).
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
        verifyParquetRowIdSequential(snapshot);
        verifyLuceneDocCount(5);
        verifyLuceneRowIdSequential();
        // Position-wise alignment of Parquet rows and Lucene docs on shared
        // keyword fields (name, tag). Without sort, both must be in insertion order.
        verifyParquetAndLuceneRowsAlignedSequentially(snapshot);

        // Stronger check: insertion order is preserved end-to-end. We assert the
        // exact name sequence to rule out any silent reordering.
        verifyLuceneNamesInOrder(new String[] { "charlie", "alice", "bob", "dave", "eve" });
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

    private Settings unsortedParquetOnlySettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();
    }

    private Settings unsortedParquetWithLuceneSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();
    }

    private Settings unsortedLuceneOnlySettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "lucene")
            .putList("index.composite.secondary_data_formats")
            .build();
    }

    private Settings sortedLuceneOnlySettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "lucene")
            .putList("index.composite.secondary_data_formats")
            .putList("index.sort.field", "age")
            .putList("index.sort.order", "desc")
            .putList("index.sort.missing", "_first")
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
        IndexResponse response = client().prepareIndex().setIndex(INDEX_NAME).setSource("name", name, "age", age).get();
        assertEquals(RestStatus.CREATED, response.status());
    }

    private void indexDoc(String name, int age, String tag) {
        IndexResponse response = client().prepareIndex().setIndex(INDEX_NAME).setSource("name", name, "age", age, "tag", tag).get();
        assertEquals(RestStatus.CREATED, response.status());
    }

    private void indexDocNullAge(String name) {
        IndexResponse response = client().prepareIndex().setIndex(INDEX_NAME).setSource("name", name).get();
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

    /**
     * Verifies Parquet sort order for a dynamic set of integer sort fields.
     * Supports any combination of ASC/DESC with configurable null placement.
     */
    @SuppressForbidden(reason = "JSON parsing for sort order verification")
    private void verifyParquetSortOrderMultiField(
        DataformatAwareCatalogSnapshot snapshot,
        String[] fieldNames,
        String[] sortOrders,
        String[] sortMissing
    ) throws Exception {
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
                    int cmp = compareRowsByMultiField(rows.get(i - 1), rows.get(i), fieldNames, sortOrders, sortMissing);
                    assertTrue(
                        "Sort order violated at row " + i + " in " + file + ": prev=" + rows.get(i - 1) + " curr=" + rows.get(i),
                        cmp <= 0
                    );
                }
            }
        }
    }

    /**
     * Compares two rows by the multi-field sort key. Returns negative if prev comes before curr
     * (correct order), zero if equal, positive if prev comes after curr (violation).
     */
    private int compareRowsByMultiField(
        Map<String, Object> prev,
        Map<String, Object> curr,
        String[] fieldNames,
        String[] sortOrders,
        String[] sortMissing
    ) {
        for (int f = 0; f < fieldNames.length; f++) {
            Object prevVal = prev.get(fieldNames[f]);
            Object currVal = curr.get(fieldNames[f]);
            boolean isAsc = "asc".equals(sortOrders[f]);
            boolean nullsFirst = "_first".equals(sortMissing[f]);

            int cmp = compareValues(prevVal, currVal, isAsc, nullsFirst);
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    /**
     * Compares two nullable integer values according to sort direction and null placement.
     * Returns negative if in correct order, zero if equal, positive if violated.
     */
    private int compareValues(Object prev, Object curr, boolean isAsc, boolean nullsFirst) {
        if (prev == null && curr == null) return 0;
        if (prev == null) {
            // prev is null: correct if nullsFirst, violation if nullsLast
            return nullsFirst ? -1 : 1;
        }
        if (curr == null) {
            // curr is null: violation if nullsFirst, correct if nullsLast
            return nullsFirst ? 1 : -1;
        }
        int prevInt = ((Number) prev).intValue();
        int currInt = ((Number) curr).intValue();
        int natural = Integer.compare(prevInt, currInt);
        return isAsc ? natural : -natural;
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
                            DocumentInput.ROW_ID_FIELD
                                + " should be sequential, expected "
                                + expectedRowId
                                + " but got "
                                + rowId
                                + " at doc "
                                + doc,
                            expectedRowId,
                            rowId
                        );
                        expectedRowId++;
                    }
                }
            }
        }
    }

    /**
     * Verifies that the Lucene segment is physically sorted by the configured IndexSort
     * (age DESC nulls first). Reads age from sorted numeric doc values.
     */
    private void verifyLuceneSortOrder() throws IOException {
        Path luceneDir = getLuceneDir();
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                org.apache.lucene.index.LeafReader leaf = ctx.reader();
                int maxDoc = leaf.maxDoc();
                if (maxDoc <= 1) continue;

                SortedNumericDocValues ageDV = leaf.getSortedNumericDocValues("age");

                Long prevAge = null;
                boolean prevAgeNull = true;
                for (int doc = 0; doc < maxDoc; doc++) {
                    Long age = null;
                    if (ageDV != null && ageDV.advanceExact(doc)) {
                        age = ageDV.nextValue();
                    }

                    if (doc > 0) {
                        // age DESC nulls first
                        if (prevAgeNull && age == null) {
                            // both null — ok
                        } else if (prevAgeNull) {
                            // prev was null, current is non-null — ok (nulls first)
                        } else if (age == null) {
                            fail("null age at doc " + doc + " came after non-null (expected nulls first)");
                        } else {
                            assertTrue("age should be DESC at doc " + doc + ", got " + prevAge + " before " + age, prevAge >= age);
                        }
                    }
                    prevAge = age;
                    prevAgeNull = (age == null);
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
                        DocumentInput.ROW_ID_FIELD
                            + " should be sequential, expected "
                            + expectedRowId
                            + " but got "
                            + rowId
                            + " in "
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
        List<Map<String, Object>> parquetRows = new ArrayList<>();
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
     * Asserts that the {@code name} keyword field, read in Lucene doc order from
     * the single-segment index, matches the given expected sequence. Used by
     * unsorted-path tests to confirm insertion order is preserved end-to-end.
     */
    private void verifyLuceneNamesInOrder(String[] expectedNames) throws IOException {
        Path luceneDir = getLuceneDir();
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals("Test assumes a single Lucene leaf for sequential alignment", 1, reader.leaves().size());
            org.apache.lucene.index.LeafReader leaf = reader.leaves().get(0).reader();
            String[] actual = readKeywordValuesPerDoc(leaf, "name");
            assertEquals("Lucene doc count should match expected sequence length", expectedNames.length, actual.length);
            for (int i = 0; i < expectedNames.length; i++) {
                assertEquals("name at lucene docId " + i, expectedNames[i], actual[i]);
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
