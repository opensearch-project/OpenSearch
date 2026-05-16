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
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.DataFormatAwareEngine;
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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

// The Tokio IO runtime worker thread (used by the Rust merge k-way merge sort) is a process-lifetime
// singleton that persists after tests complete. It polls for new async IO tasks between merges.
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeMergeIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-composite-merge";
    private static final String MERGE_ENABLED_PROPERTY = "opensearch.pluggable.dataformat.merge.enabled";

    // ══════════════════════════════════════════════════════════════════════
    // Framework lifecycle & configuration
    // ══════════════════════════════════════════════════════════════════════

    @Override
    public void setUp() throws Exception {
        enableMerge();
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        try {
            client().admin().indices().prepareDelete(INDEX_NAME).get();
        } catch (Exception e) {
            // index may not exist if test failed before creation
        }
        super.tearDown();
        disableMerge();
    }

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

    // ══════════════════════════════════════════════════════════════════════
    // Tests
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Verifies background merge produces a valid merged parquet file
     * with correct row count and source files cleaned up.
     */
    public void testBackgroundMerge() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(unsortedSettings())
            .setMapping("name", "type=keyword", "age", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        int docsPerCycle = 5;
        int refreshCycles = 15;
        indexDocsAcrossMultipleRefreshes(refreshCycles, docsPerCycle);
        int totalDocs = refreshCycles * docsPerCycle;

        DataformatAwareCatalogSnapshot snapshot = waitForMerge(refreshCycles);
        assertEquals(Set.of("parquet"), snapshot.getDataFormats());

        verifyRowCount(snapshot, totalDocs);
        verifySegmentGenerationUniqueness(snapshot);
        verifyNoOrphanFiles(snapshot);
    }

    /**
     * Verifies sorted merge with age DESC (nulls first), name ASC (nulls last).
     */
    public void testSortedMerge() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(sortedSettings())
            .setMapping("name", "type=keyword", "age", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        int docsPerCycle = 10;
        int refreshCycles = 15;
        indexDocsWithNullsAcrossRefreshes(refreshCycles, docsPerCycle);
        int totalDocs = refreshCycles * docsPerCycle;

        DataformatAwareCatalogSnapshot snapshot = waitForMerge(refreshCycles);
        assertEquals(Set.of("parquet"), snapshot.getDataFormats());

        verifyRowCount(snapshot, totalDocs);
        verifySortOrder(snapshot);
        verifySegmentGenerationUniqueness(snapshot);
        verifyNoOrphanFiles(snapshot);
    }

    /**
     * Verifies composite merge with Parquet as primary and Lucene as secondary:
     * <ol>
     *   <li>Merge reduces segment count (merge actually happened)</li>
     *   <li>Both "parquet" and "lucene" entries exist in the catalog snapshot</li>
     *   <li>Merged parquet files have correct total row count</li>
     *   <li>Merged lucene directory has correct total document count</li>
     *   <li>Lucene documents have monotonically increasing __row_id__ doc values
     *       (confirms RowIdMapping was applied during secondary merge)</li>
     *   <li>Cross-format validation: parquet row count == lucene doc count for each merged segment</li>
     * </ol>
     */
    public void testParquetPrimaryLuceneSecondaryMerge() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(parquetPrimaryLuceneSecondarySettings())
            .setMapping("name", "type=keyword", "age", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        // Index documents to create multiple segments. Using 15 cycles keeps the workload
        // in line with the other stable composite-merge tests and avoids triggering a second
        // cascaded merge before the first one commits.
        int docsPerCycle = 5;
        int refreshCycles = 15;
        indexDocsAcrossMultipleRefreshes(refreshCycles, docsPerCycle);
        int totalDocs = refreshCycles * docsPerCycle;

        DataformatAwareCatalogSnapshot snapshot = waitForMerge(refreshCycles);

        // Both formats must be present in the catalog
        Set<String> formats = snapshot.getDataFormats();
        assertTrue("Catalog should contain 'parquet' format, got: " + formats, formats.contains("parquet"));
        assertTrue("Catalog should contain 'lucene' format, got: " + formats, formats.contains("lucene"));

        // Verify parquet merged files have correct row count
        verifyRowCount(snapshot, totalDocs);

        // Verify lucene merged directory has correct doc count
        verifyLuceneDocCount(totalDocs);

        // Verify lucene __row_id__ values are monotonically increasing (RowIdMapping applied)
        verifyLuceneRowIdSequential();

        // Cross-format validation: for each segment, parquet rows == lucene segment docs
        verifyCrossFormatConsistency(snapshot);
    }

    /**
     * Verifies sorted composite merge with Parquet primary (sorted) + Lucene secondary:
     * <ol>
     *   <li>Merge reduces segment count</li>
     *   <li>Merged parquet files are sorted by age DESC (nulls first), name ASC (nulls last)</li>
     *   <li>Lucene __row_id__ values are sequential (RowIdMapping applied)</li>
     *   <li>Cross-format consistency: parquet rows match lucene docs by row_id</li>
     * </ol>
     *
     * This is the critical test for RowIdMapping correctness in sorted merges —
     * the primary format reorders rows during merge, and the secondary must apply
     * the same reordering via the mapping.
     */
    public void testSortedParquetPrimaryLuceneSecondaryMerge() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(sortedParquetPrimaryLuceneSecondarySettings())
            .setMapping("name", "type=keyword", "age", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        int docsPerCycle = 10;
        int refreshCycles = 15;
        indexDocsWithNullsAcrossRefreshes(refreshCycles, docsPerCycle);
        int totalDocs = refreshCycles * docsPerCycle;

        DataformatAwareCatalogSnapshot snapshot = waitForMerge(refreshCycles);

        Set<String> formats = snapshot.getDataFormats();
        assertTrue("Catalog should contain 'parquet'", formats.contains("parquet"));
        assertTrue("Catalog should contain 'lucene'", formats.contains("lucene"));

        verifyRowCount(snapshot, totalDocs);
        verifySortOrder(snapshot);
        verifyLuceneDocCount(totalDocs);
        verifyLuceneRowIdSequential();
        verifyCrossFormatConsistency(snapshot);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Private helpers: merge feature flag
    // ══════════════════════════════════════════════════════════════════════

    @SuppressForbidden(reason = "enable pluggable dataformat merge for integration testing")
    private static void enableMerge() {
        System.setProperty(MERGE_ENABLED_PROPERTY, "true");
    }

    @SuppressForbidden(reason = "restore pluggable dataformat merge property after test")
    private static void disableMerge() {
        System.clearProperty(MERGE_ENABLED_PROPERTY);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Private helpers: index settings
    // ══════════════════════════════════════════════════════════════════════

    private Settings unsortedSettings() {
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

    private Settings sortedSettings() {
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

    private Settings parquetPrimaryLuceneSecondarySettings() {
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

    private Settings sortedParquetPrimaryLuceneSecondarySettings() {
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
    // Private helpers: indexing
    // ══════════════════════════════════════════════════════════════════════

    private void indexDocsAcrossMultipleRefreshes(int refreshCycles, int docsPerCycle) {
        for (int cycle = 0; cycle < refreshCycles; cycle++) {
            for (int i = 0; i < docsPerCycle; i++) {
                IndexResponse response = client().prepareIndex()
                    .setIndex(INDEX_NAME)
                    .setSource("name", randomAlphaOfLength(10), "age", randomIntBetween(1, 1000))
                    .get();
                assertEquals(RestStatus.CREATED, response.status());
            }
            RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(INDEX_NAME).get();
            assertEquals(RestStatus.OK, refreshResponse.getStatus());
        }
    }

    private void indexDocsWithNullsAcrossRefreshes(int refreshCycles, int docsPerCycle) {
        for (int cycle = 0; cycle < refreshCycles; cycle++) {
            for (int i = 0; i < docsPerCycle; i++) {
                IndexResponse response;
                if (i % 5 == 0) {
                    response = client().prepareIndex().setIndex(INDEX_NAME).setSource("name", randomAlphaOfLength(10)).get();
                } else {
                    response = client().prepareIndex()
                        .setIndex(INDEX_NAME)
                        .setSource("name", randomAlphaOfLength(10), "age", randomIntBetween(0, 100))
                        .get();
                }
                assertEquals(RestStatus.CREATED, response.status());
            }
            RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(INDEX_NAME).get();
            assertEquals(RestStatus.OK, refreshResponse.getStatus());
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Private helpers: verification
    // ══════════════════════════════════════════════════════════════════════

    private void verifyRowCount(DataformatAwareCatalogSnapshot snapshot, int expectedTotalDocs) throws IOException {
        Path parquetDir = getParquetDir();
        long totalRows = 0;
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            assertNotNull("Segment should have parquet files", wfs);
            for (String file : wfs.files()) {
                Path filePath = parquetDir.resolve(file);
                assertTrue("Parquet file should exist: " + filePath, Files.exists(filePath));
                ParquetFileMetadata metadata = RustBridge.getFileMetadata(filePath.toString());
                assertEquals("WriterFileSet numRows should match actual file metadata for " + file, wfs.numRows(), metadata.numRows());
                totalRows += metadata.numRows();
            }
        }
        assertEquals("Total rows across all segments should match ingested docs", expectedTotalDocs, totalRows);
    }

    private void verifySegmentGenerationUniqueness(DataformatAwareCatalogSnapshot snapshot) {
        List<Long> generations = snapshot.getSegments().stream().map(Segment::generation).toList();
        assertEquals("All segment generations must be unique", generations.size(), generations.stream().distinct().count());
    }

    private void verifyNoOrphanFiles(DataformatAwareCatalogSnapshot snapshot) throws IOException {
        Path parquetDir = getParquetDir();
        Set<String> referencedFiles = new HashSet<>();
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            if (wfs != null) {
                referencedFiles.addAll(wfs.files());
            }
        }
        try (var stream = Files.list(parquetDir)) {
            List<String> diskFiles = stream.filter(Files::isRegularFile)
                .map(p -> p.getFileName().toString())
                .filter(f -> f.endsWith(".parquet"))
                .toList();
            for (String diskFile : diskFiles) {
                assertTrue("Orphan parquet file on disk not referenced by catalog: " + diskFile, referencedFiles.contains(diskFile));
            }
        }
    }

    /**
     * Verifies that merged parquet files have age in DESC order with nulls first,
     * and within same age, name in ASC order with nulls last.
     */
    @SuppressForbidden(reason = "JSON parsing for test verification of parquet output")
    private void verifySortOrder(DataformatAwareCatalogSnapshot snapshot) throws Exception {
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
                    if (prevAge == null) continue; // null before non-null is correct
                    if (currAge == null) {
                        fail("age null should come before non-null, but found non-null at " + (i - 1) + " and null at " + i);
                    }

                    int prevAgeVal = ((Number) prevAge).intValue();
                    int currAgeVal = ((Number) currAge).intValue();

                    assertTrue(
                        "age should be DESC but found " + prevAgeVal + " before " + currAgeVal + " at row " + i,
                        prevAgeVal >= currAgeVal
                    );

                    // When age is equal, verify name ASC (nulls last)
                    if (prevAgeVal == currAgeVal) {
                        Object prevName = rows.get(i - 1).get("name");
                        Object currName = rows.get(i).get("name");

                        if (prevName != null && currName == null) continue; // non-null before null is correct for nulls last
                        if (prevName == null && currName != null) {
                            fail("name nulls should be last, but found null at " + (i - 1) + " and non-null at " + i);
                        }
                        if (prevName != null && currName != null) {
                            assertTrue(
                                "name should be ASC but found '" + prevName + "' before '" + currName + "' at row " + i,
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
        assertTrue("Lucene directory should exist: " + luceneDir, Files.exists(luceneDir));

        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals("Total lucene docs should match ingested docs", expectedTotalDocs, reader.numDocs());
        }
    }

    /**
     * Verifies that __row_id__ doc values in merged lucene segments are sequential
     * (0, 1, 2, ...) within each leaf. This confirms the RowIdMapping from the primary
     * (Parquet) merge was correctly applied to reorder Lucene documents.
     *
     * Sequential (not just monotonic) is required because the RowIdMapping produces
     * a dense mapping — every position from 0..N-1 must be covered.
     */
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
                                + " should be sequential within segment, expected "
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
     * Cross-format data comparison: reads merged parquet file content and merged lucene
     * segments, then verifies that for each row in parquet (identified by __row_id__),
     * the corresponding Lucene document (sorted by __row_id__) has matching field values.
     *
     * Compares both numeric (age) and keyword (name) fields to ensure the RowIdMapping
     * correctly synchronized the two formats during merge.
     *
     * <p>Note: {@code __row_id__} is only unique <em>within</em> a catalog segment
     * (each segment starts row_ids at 0), so rows must be grouped per segment — a global
     * map would silently overwrite rows from segments that happen to share row_ids.
     * Each Lucene leaf is matched to its parquet segment by row count.
     */
    @SuppressForbidden(reason = "JSON parsing for cross-format data comparison")
    private void verifyCrossFormatConsistency(DataformatAwareCatalogSnapshot snapshot) throws Exception {
        Path parquetDir = getParquetDir();
        Path luceneDir = getLuceneDir();

        // Collect parquet rows grouped per catalog segment, indexed by __row_id__
        // (only unique within a segment, so a per-segment map is required).
        List<Map<Long, Map<String, Object>>> parquetSegments = new java.util.ArrayList<>();
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet parquetWfs = segment.dfGroupedSearchableFiles().get("parquet");
            if (parquetWfs == null) continue;
            Map<Long, Map<String, Object>> rowsInSegment = new java.util.HashMap<>();
            for (String file : parquetWfs.files()) {
                Path filePath = parquetDir.resolve(file);
                if (Files.exists(filePath) == false) continue;
                String json = RustBridge.readAsJson(filePath.toString());
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
                        long rowId = ((Number) row.get(DocumentInput.ROW_ID_FIELD)).longValue();
                        rowsInSegment.put(rowId, row);
                    }
                }
            }
            if (rowsInSegment.isEmpty() == false) {
                parquetSegments.add(rowsInSegment);
            }
        }

        assertTrue("Should have parquet rows to compare", parquetSegments.isEmpty() == false);

        // For each Lucene leaf, find the parquet segment whose row count matches and
        // verify every row_id in the leaf resolves to a row in that segment with matching
        // age/name values.
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            int matchedDocs = 0;
            int totalLuceneDocs = 0;
            for (LeafReaderContext ctx : reader.leaves()) {
                int leafDocs = ctx.reader().maxDoc();
                totalLuceneDocs += leafDocs;

                Map<Long, Map<String, Object>> matchingSegment = null;
                for (Map<Long, Map<String, Object>> candidate : parquetSegments) {
                    if (candidate.size() == leafDocs) {
                        matchingSegment = candidate;
                        break;
                    }
                }
                assertNotNull("No parquet segment found with matching row count " + leafDocs, matchingSegment);
                parquetSegments.remove(matchingSegment);

                SortedNumericDocValues rowIdDV = ctx.reader().getSortedNumericDocValues(DocumentInput.ROW_ID_FIELD);
                SortedNumericDocValues ageDV = ctx.reader().getSortedNumericDocValues("age");
                SortedSetDocValues nameDV = ctx.reader().getSortedSetDocValues("name");

                if (rowIdDV == null) continue;

                for (int doc = 0; doc < leafDocs; doc++) {
                    if (rowIdDV.advanceExact(doc) == false) continue;
                    long luceneRowId = rowIdDV.nextValue();

                    Map<String, Object> parquetRow = matchingSegment.get(luceneRowId);
                    assertNotNull(
                        "Lucene doc with " + DocumentInput.ROW_ID_FIELD + "=" + luceneRowId + " should have a matching parquet row",
                        parquetRow
                    );

                    // Compare age field
                    if (ageDV != null && ageDV.advanceExact(doc)) {
                        long luceneAge = ageDV.nextValue();
                        Object parquetAge = parquetRow.get("age");
                        assertNotNull(
                            "Parquet row at " + DocumentInput.ROW_ID_FIELD + "=" + luceneRowId + " should have 'age' field",
                            parquetAge
                        );
                        assertEquals("Age mismatch at row_id=" + luceneRowId, ((Number) parquetAge).longValue(), luceneAge);
                    }

                    // Compare name field (keyword stored as sorted set doc values)
                    if (nameDV != null && nameDV.advanceExact(doc)) {
                        long ord = nameDV.nextOrd();
                        if (ord >= 0) {
                            String luceneName = nameDV.lookupOrd(ord).utf8ToString();
                            Object parquetName = parquetRow.get("name");
                            assertNotNull(
                                "Parquet row at " + DocumentInput.ROW_ID_FIELD + "=" + luceneRowId + " should have 'name' field",
                                parquetName
                            );
                            assertEquals("Name mismatch at row_id=" + luceneRowId, parquetName.toString(), luceneName);
                        }
                    }

                    matchedDocs++;
                }
            }

            assertTrue("Should have matched at least some docs across formats", matchedDocs > 0);
            assertEquals("All lucene docs should have matching parquet rows", totalLuceneDocs, matchedDocs);
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Private helpers: shard/cluster accessors
    // ══════════════════════════════════════════════════════════════════════

    private Path getParquetDir() {
        IndexShard shard = getPrimaryShard();
        return shard.shardPath().getDataPath().resolve("parquet");
    }

    private Path getLuceneDir() {
        // Merged lucene segments live in the shard's standard index folder (ShardPath.resolveIndex()),
        // which resolves to "<shardDataPath>/index". The "<shardDataPath>/lucene" folder is only used
        // for per-writer temporary staging directories (lucene_gen_*), not for the committed merged index.
        IndexShard shard = getPrimaryShard();
        return shard.shardPath().resolveIndex();
    }

    private IndexShard getPrimaryShard() {
        String nodeName = getClusterState().routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
        String nodeNameResolved = getClusterState().nodes().get(nodeName).getName();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeNameResolved);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(INDEX_NAME));
        return indexService.getShard(0);
    }

    private DataformatAwareCatalogSnapshot getCatalogSnapshot() throws IOException {
        IndexShard shard = getPrimaryShard();
        try (GatedCloseable<CatalogSnapshot> snapshot = shard.getCatalogSnapshot()) {
            return DataformatAwareCatalogSnapshot.deserializeFromString(snapshot.get().serializeToString(), Function.identity());
        }
    }

    // ── Randomized sort tests across data types ──

    public void testSortedMergeBySingleField() throws Exception {
        long now = System.currentTimeMillis();
        List<Object[]> fieldSpecs = List.of(
            new Object[] {
                "value_long",
                "type=long",
                (Supplier<Object>) () -> randomBoolean() ? null : randomLongBetween(-1_000_000L, 1_000_000L) },
            new Object[] { "value_float", "type=float", (Supplier<Object>) () -> randomBoolean() ? null : randomFloat() * 1000 },
            new Object[] {
                "value_double",
                "type=double",
                (Supplier<Object>) () -> randomBoolean() ? null : randomDoubleBetween(-1e6, 1e6, true) },
            new Object[] {
                "timestamp",
                "type=date",
                (Supplier<Object>) () -> randomBoolean() ? null : now - randomLongBetween(0, 86400000L * 365) },
            new Object[] { "tag", "type=keyword", (Supplier<Object>) () -> randomBoolean() ? null : randomAlphaOfLengthBetween(3, 8) }
        );

        for (Object[] spec : fieldSpecs) {
            String fieldName = (String) spec[0];
            String fieldType = (String) spec[1];
            @SuppressWarnings("unchecked")
            Supplier<Object> valueSupplier = (Supplier<Object>) spec[2];

            for (String order : List.of("asc", "desc")) {
                for (String missing : List.of("_first", "_last")) {
                    try {
                        Settings settings = Settings.builder()
                            .put(unsortedSettings())
                            .putList("index.sort.field", fieldName)
                            .putList("index.sort.order", order)
                            .putList("index.sort.missing", missing)
                            .build();

                        client().admin().indices().prepareCreate(INDEX_NAME).setSettings(settings).setMapping(fieldName, fieldType).get();
                        ensureGreen(INDEX_NAME);

                        int docsPerCycle = 10;
                        int refreshCycles = 15;
                        indexRandomDocs(refreshCycles, docsPerCycle, fieldName, valueSupplier);
                        int totalDocs = refreshCycles * docsPerCycle;

                        DataformatAwareCatalogSnapshot snapshot = waitForMerge(refreshCycles);
                        verifyRowCount(snapshot, totalDocs);
                        verifySortOrderGeneric(snapshot, List.of(fieldName), List.of(order), List.of(missing));
                    } finally {
                        client().admin().indices().prepareDelete(INDEX_NAME).get();
                    }
                }
            }
        }
    }

    public void testSortedMergeMultiColumnRandomized() throws Exception {
        String[] fields = { "age", "score", "tag" };
        String[] types = { "type=integer", "type=long", "type=keyword" };
        String[] orders = { randomFrom("asc", "desc"), randomFrom("asc", "desc"), randomFrom("asc", "desc") };
        String[] missings = { randomFrom("_first", "_last"), randomFrom("_first", "_last"), randomFrom("_first", "_last") };

        Settings settings = Settings.builder()
            .put(unsortedSettings())
            .putList("index.sort.field", fields)
            .putList("index.sort.order", orders)
            .putList("index.sort.missing", missings)
            .build();

        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(settings)
            .setMapping("age", types[0], "score", types[1], "tag", types[2])
            .get();
        ensureGreen(INDEX_NAME);

        int docsPerCycle = 10;
        int refreshCycles = 15;
        for (int cycle = 0; cycle < refreshCycles; cycle++) {
            for (int i = 0; i < docsPerCycle; i++) {
                var source = new java.util.HashMap<String, Object>();
                source.put("age", randomBoolean() ? null : randomIntBetween(0, 100));
                source.put("score", randomBoolean() ? null : randomLongBetween(0, 10000));
                source.put("tag", randomBoolean() ? null : randomAlphaOfLengthBetween(3, 6));
                // Remove null entries so they appear as missing in the doc
                source.values().removeIf(java.util.Objects::isNull);
                IndexResponse response = client().prepareIndex().setIndex(INDEX_NAME).setSource(source).get();
                assertEquals(RestStatus.CREATED, response.status());
            }
            client().admin().indices().prepareRefresh(INDEX_NAME).get();
        }
        int totalDocs = refreshCycles * docsPerCycle;

        waitForMerge(refreshCycles);
        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        verifyRowCount(snapshot, totalDocs);
        verifySortOrderGeneric(snapshot, List.of(fields), List.of(orders), List.of(missings));
    }

    public void testUnsortedMergeWithAllTypes() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(unsortedSettings())
            .setMapping(
                "val_int",
                "type=integer",
                "val_long",
                "type=long",
                "val_float",
                "type=float",
                "val_double",
                "type=double",
                "val_date",
                "type=date",
                "val_keyword",
                "type=keyword"
            )
            .get();
        ensureGreen(INDEX_NAME);

        int docsPerCycle = 8;
        int refreshCycles = 15;
        long now = System.currentTimeMillis();
        for (int cycle = 0; cycle < refreshCycles; cycle++) {
            for (int i = 0; i < docsPerCycle; i++) {
                IndexResponse response = client().prepareIndex()
                    .setIndex(INDEX_NAME)
                    .setSource(
                        "val_int",
                        randomIntBetween(-100, 100),
                        "val_long",
                        randomLongBetween(-100000, 100000),
                        "val_float",
                        randomFloat() * 200 - 100,
                        "val_double",
                        randomDoubleBetween(-1000, 1000, true),
                        "val_date",
                        now - randomLongBetween(0, 86400000L * 30),
                        "val_keyword",
                        randomAlphaOfLength(6)
                    )
                    .get();
                assertEquals(RestStatus.CREATED, response.status());
            }
            client().admin().indices().prepareRefresh(INDEX_NAME).get();
        }
        int totalDocs = refreshCycles * docsPerCycle;

        waitForMerge(refreshCycles);
        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        verifyRowCount(snapshot, totalDocs);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Merge stats tests
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Verifies that after a merge, getMergeStats() reports at least one completed merge
     * and unreferencedFileCleanUpsPerformed > 0 (old segment files were cleaned up).
     */
    public void testMergeStatsAfterMerge() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(unsortedSettings())
            .setMapping("name", "type=keyword", "age", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        int docsPerCycle = 5;
        int refreshCycles = 15;
        indexDocsAcrossMultipleRefreshes(refreshCycles, docsPerCycle);

        waitForMerge(refreshCycles);

        IndexShard shard = getPrimaryShard();
        DataFormatAwareEngine engine = (DataFormatAwareEngine) org.opensearch.index.shard.IndexShardTestCase.getIndexer(shard);

        org.opensearch.index.merge.MergeStats mergeStats = engine.getMergeStats();
        assertNotNull("MergeStats should not be null", mergeStats);
        assertTrue("Total merges should be > 0 after merge", mergeStats.getTotal() > 0);

        long cleanups = engine.unreferencedFileCleanUpsPerformed();
        assertTrue("unreferencedFileCleanUpsPerformed should be > 0 after merge, got: " + cleanups, cleanups > 0);
    }

    /**
     * Verifies that merge stats via the indices stats API (_stats/merge) reflects
     * unreferenced_file_cleanups_performed after a merge.
     */
    public void testMergeStatsViaApi() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(unsortedSettings())
            .setMapping("name", "type=keyword", "age", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        int docsPerCycle = 5;
        int refreshCycles = 15;
        indexDocsAcrossMultipleRefreshes(refreshCycles, docsPerCycle);

        waitForMerge(refreshCycles);

        org.opensearch.action.admin.indices.stats.IndicesStatsResponse statsResponse = client().admin()
            .indices()
            .prepareStats(INDEX_NAME)
            .clear()
            .setMerge(true)
            .get();

        org.opensearch.index.merge.MergeStats mergeStats = statsResponse.getIndex(INDEX_NAME).getShards()[0].getStats().getMerge();
        assertNotNull("MergeStats from API should not be null", mergeStats);
        assertTrue("Total merges via API should be > 0", mergeStats.getTotal() > 0);
        assertTrue(
            "unreferencedFileCleanUpsPerformed via API should be > 0, got: " + mergeStats.getUnreferencedFileCleanUpsPerformed(),
            mergeStats.getUnreferencedFileCleanUpsPerformed() > 0
        );
    }

    // ── Helpers for randomized tests ──

    private DataformatAwareCatalogSnapshot waitForMerge(int refreshCycles) throws Exception {
        flush(INDEX_NAME);
        assertBusy(() -> {
            DataformatAwareCatalogSnapshot snap = getCatalogSnapshot();
            assertTrue(
                "Expected merges to reduce segment count below " + refreshCycles + ", but got: " + snap.getSegments().size(),
                snap.getSegments().size() < refreshCycles
            );
        });
        return getCatalogSnapshot();
    }

    private void indexRandomDocs(int refreshCycles, int docsPerCycle, String fieldName, Supplier<Object> valueSupplier) {
        for (int cycle = 0; cycle < refreshCycles; cycle++) {
            for (int i = 0; i < docsPerCycle; i++) {
                Object value = valueSupplier.get();
                IndexResponse response;
                if (value == null) {
                    response = client().prepareIndex().setIndex(INDEX_NAME).setSource(java.util.Collections.emptyMap()).get();
                } else {
                    response = client().prepareIndex().setIndex(INDEX_NAME).setSource(fieldName, value).get();
                }
                assertEquals(RestStatus.CREATED, response.status());
            }
            client().admin().indices().prepareRefresh(INDEX_NAME).get();
        }
    }

    @SuppressForbidden(reason = "JSON parsing for generic sort verification")
    private void verifySortOrderGeneric(
        DataformatAwareCatalogSnapshot snapshot,
        List<String> sortFields,
        List<String> sortOrders,
        List<String> sortMissing
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
                    int cmp = compareRows(rows.get(i - 1), rows.get(i), sortFields, sortOrders, sortMissing);
                    assertTrue(
                        "Sort order violated at row "
                            + i
                            + " in file "
                            + file
                            + ": prev="
                            + extractSortValues(rows.get(i - 1), sortFields)
                            + " curr="
                            + extractSortValues(rows.get(i), sortFields)
                            + " fields="
                            + sortFields
                            + " orders="
                            + sortOrders
                            + " missing="
                            + sortMissing,
                        cmp <= 0
                    );
                }
            }
        }
    }

    private int compareRows(
        Map<String, Object> prev,
        Map<String, Object> curr,
        List<String> sortFields,
        List<String> sortOrders,
        List<String> sortMissing
    ) {
        for (int f = 0; f < sortFields.size(); f++) {
            String field = sortFields.get(f);
            boolean desc = "desc".equals(sortOrders.get(f));
            boolean nullsFirst = "_first".equals(sortMissing.get(f));

            Object prevVal = prev.get(field);
            Object currVal = curr.get(field);

            int cmp = compareValues(prevVal, currVal, desc, nullsFirst);
            if (cmp != 0) return cmp;
        }
        return 0;
    }

    @SuppressWarnings("unchecked")
    private int compareValues(Object prev, Object curr, boolean desc, boolean nullsFirst) {
        if (prev == null && curr == null) return 0;
        if (prev == null) return nullsFirst ? -1 : 1;
        if (curr == null) return nullsFirst ? 1 : -1;

        int cmp;
        if (prev instanceof Number && curr instanceof Number) {
            cmp = Double.compare(((Number) prev).doubleValue(), ((Number) curr).doubleValue());
        } else if (prev instanceof String && curr instanceof String) {
            cmp = ((String) prev).compareTo((String) curr);
        } else {
            cmp = prev.toString().compareTo(curr.toString());
        }

        return desc ? -cmp : cmp;
    }

    private List<Object> extractSortValues(Map<String, Object> row, List<String> sortFields) {
        return sortFields.stream().map(row::get).toList();
    }

}
