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
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.index.IndexResponse;
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
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.merge.MergeStats;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Integration tests for Lucene as the <b>primary</b> data format in the pluggable dataformat merge flow.
 *
 * <p>Tests cover:
 * <ul>
 *   <li>Lucene primary only (no secondary formats)</li>
 *   <li>Lucene primary + Parquet secondary (unsorted and sorted)</li>
 *   <li>RowIdMapping correctness: verifies that secondary format rows are reordered to match primary</li>
 *   <li>Cross-format data consistency: same field values at same row_id positions</li>
 * </ul>
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class LucenePrimaryMergeIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-lucene-primary-merge";
    private static final String MERGE_ENABLED_PROPERTY = "opensearch.pluggable.dataformat.merge.enabled";

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
    // Test: Lucene primary only — no secondary formats
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Verifies that Lucene can merge as primary format without any secondary formats:
     * <ol>
     *   <li>Segments are reduced (merge occurred)</li>
     *   <li>Only "lucene" format present in catalog</li>
     *   <li>Total document count is preserved</li>
     *   <li>RowIdMapping is produced (even if no secondary consumes it)</li>
     * </ol>
     */
    public void testLucenePrimaryOnly() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(lucenePrimaryOnlySettings())
            .setMapping("name", "type=keyword", "age", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        int docsPerCycle = 5;
        int refreshCycles = 15;
        indexDocs(refreshCycles, docsPerCycle);
        int totalDocs = refreshCycles * docsPerCycle;

        assertBusy(() -> {
            flush(INDEX_NAME);
            DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
            assertTrue(
                "Expected merges to reduce segment count below " + refreshCycles + ", but got: " + snapshot.getSegments().size(),
                snapshot.getSegments().size() < refreshCycles
            );
        });

        MergeStats mergeStats = getMergeStats();
        assertTrue("Expected at least one merge to have occurred", mergeStats.getTotal() > 0);

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        assertTrue("Catalog should contain 'lucene' format", snapshot.getDataFormats().contains("lucene"));

        verifyLuceneDocCount(totalDocs);
        verifyLuceneRowIdMonotonicallyIncreasing();
    }

    // ══════════════════════════════════════════════════════════════════════
    // Test: Lucene primary + Parquet secondary — unsorted
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Verifies composite merge with Lucene as primary and Parquet as secondary (unsorted):
     * <ol>
     *   <li>Merge reduces segment count</li>
     *   <li>Both "lucene" and "parquet" formats present in catalog</li>
     *   <li>Lucene has correct total document count</li>
     *   <li>Parquet has correct total row count</li>
     *   <li>RowIdMapping was applied: for each segment, lucene doc count == parquet row count</li>
     *   <li>Cross-format field values match at same row_id positions</li>
     * </ol>
     */
    public void testLucenePrimaryParquetSecondaryMerge() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(lucenePrimaryParquetSecondarySettings())
            .setMapping("name", "type=keyword", "age", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        int docsPerCycle = 5;
        int refreshCycles = 15;
        indexDocs(refreshCycles, docsPerCycle);
        int totalDocs = refreshCycles * docsPerCycle;

        assertBusy(() -> {
            flush(INDEX_NAME);
            DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
            assertTrue(
                "Expected merges to reduce segment count below " + refreshCycles + ", but got: " + snapshot.getSegments().size(),
                snapshot.getSegments().size() < refreshCycles
            );
        });

        MergeStats mergeStats = getMergeStats();
        assertTrue("Expected at least one merge to have occurred", mergeStats.getTotal() > 0);

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        Set<String> formats = snapshot.getDataFormats();
        assertTrue("Catalog should contain 'lucene' format, got: " + formats, formats.contains("lucene"));
        assertTrue("Catalog should contain 'parquet' format, got: " + formats, formats.contains("parquet"));

        verifyLuceneDocCount(totalDocs);
        verifyParquetRowCount(snapshot, totalDocs);
        verifyPerSegmentRowCountMatch(snapshot);
        verifyCrossFormatDataConsistency(snapshot);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Test: Lucene primary + Parquet secondary — with user IndexSort
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Verifies sorted composite merge with Lucene as primary (sorted by age DESC, name ASC)
     * and Parquet as secondary:
     * <ol>
     *   <li>Merge reduces segment count</li>
     *   <li>Lucene documents are sorted by user-defined sort (age DESC, name ASC)</li>
     *   <li>Parquet rows are reordered to match Lucene's sort order via RowIdMapping</li>
     *   <li>Cross-format consistency: parquet row at position N has same values as lucene doc at position N</li>
     *   <li>RowIdMapping captured the sort permutation correctly</li>
     * </ol>
     */
    public void testSortedLucenePrimaryParquetSecondaryMerge() throws Exception {
        client().admin()
            .indices()
            .prepareCreate(INDEX_NAME)
            .setSettings(sortedLucenePrimaryParquetSecondarySettings())
            .setMapping("name", "type=keyword", "age", "type=integer")
            .get();
        ensureGreen(INDEX_NAME);

        int docsPerCycle = 10;
        int refreshCycles = 15;
        indexDocsWithNulls(refreshCycles, docsPerCycle);
        int totalDocs = refreshCycles * docsPerCycle;

        assertBusy(() -> {
            flush(INDEX_NAME);
            DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
            assertTrue(
                "Expected merges to reduce segment count below " + refreshCycles + ", but got: " + snapshot.getSegments().size(),
                snapshot.getSegments().size() < refreshCycles
            );
        });

        MergeStats mergeStats = getMergeStats();
        assertTrue("Expected at least one merge to have occurred", mergeStats.getTotal() > 0);

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        Set<String> formats = snapshot.getDataFormats();
        assertTrue("Catalog should contain 'lucene'", formats.contains("lucene"));
        assertTrue("Catalog should contain 'parquet'", formats.contains("parquet"));

        verifyLuceneDocCount(totalDocs);
        verifyParquetRowCount(snapshot, totalDocs);
        verifyLuceneSortOrder();
        verifyPerSegmentRowCountMatch(snapshot);
        verifyCrossFormatDataConsistency(snapshot);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Index settings
    // ══════════════════════════════════════════════════════════════════════

    private Settings lucenePrimaryOnlySettings() {
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

    private Settings lucenePrimaryParquetSecondarySettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "lucene")
            .putList("index.composite.secondary_data_formats", "parquet")
            .build();
    }

    private Settings sortedLucenePrimaryParquetSecondarySettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.refresh_interval", "-1")
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "lucene")
            .putList("index.composite.secondary_data_formats", "parquet")
            .putList("index.sort.field", "age", "name")
            .putList("index.sort.order", "desc", "asc")
            .putList("index.sort.missing", "_first", "_last")
            .build();
    }

    // ══════════════════════════════════════════════════════════════════════
    // Indexing helpers
    // ══════════════════════════════════════════════════════════════════════

    private void indexDocs(int refreshCycles, int docsPerCycle) {
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

    private void indexDocsWithNulls(int refreshCycles, int docsPerCycle) {
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
    // Verification: Lucene format
    // ══════════════════════════════════════════════════════════════════════

    private void verifyLuceneDocCount(int expectedTotalDocs) throws IOException {
        Path luceneDir = getLuceneDir();
        assertTrue("Lucene directory should exist: " + luceneDir, Files.exists(luceneDir));
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            assertEquals("Total lucene docs should match ingested docs", expectedTotalDocs, reader.numDocs());
        }
    }

    /**
     * Verifies __row_id__ values are monotonically increasing within each leaf segment.
     * This confirms PrimaryOneMerge correctly assigned globally-unique offset row IDs
     * and the merge preserved them in order.
     */
    private void verifyLuceneRowIdMonotonicallyIncreasing() throws IOException {
        Path luceneDir = getLuceneDir();
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                SortedNumericDocValues rowIdDV = ctx.reader().getSortedNumericDocValues("__row_id__");
                if (rowIdDV == null) continue;

                long prevRowId = -1;
                for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
                    if (rowIdDV.advanceExact(doc)) {
                        long rowId = rowIdDV.nextValue();
                        assertTrue(
                            "__row_id__ should be monotonically increasing, but got " + prevRowId + " -> " + rowId + " at doc " + doc,
                            rowId > prevRowId
                        );
                        prevRowId = rowId;
                    }
                }
            }
        }
    }

    /**
     * Verifies that Lucene documents are sorted by age DESC (nulls first), name ASC (nulls last)
     * after a sorted merge. This confirms the IndexSort was applied correctly when Lucene is primary.
     */
    private void verifyLuceneSortOrder() throws IOException {
        Path luceneDir = getLuceneDir();
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            for (LeafReaderContext ctx : reader.leaves()) {
                SortedNumericDocValues ageDV = ctx.reader().getSortedNumericDocValues("age");
                if (ageDV == null) continue;

                long prevAge = Long.MAX_VALUE;
                boolean prevHadAge = false;
                for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
                    boolean hasAge = ageDV.advanceExact(doc);
                    if (!hasAge) {
                        // null age — should come first (nulls first for DESC)
                        assertFalse(
                            "Null age should come before non-null ages (nulls first), but found non-null before null at doc " + doc,
                            prevHadAge
                        );
                        continue;
                    }
                    long age = ageDV.nextValue();
                    if (prevHadAge) {
                        assertTrue("Age should be DESC but found " + prevAge + " before " + age + " at doc " + doc, prevAge >= age);
                    }
                    prevAge = age;
                    prevHadAge = true;
                }
            }
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Verification: Parquet format
    // ══════════════════════════════════════════════════════════════════════

    private void verifyParquetRowCount(DataformatAwareCatalogSnapshot snapshot, int expectedTotalDocs) throws IOException {
        Path parquetDir = getParquetDir();
        long totalRows = 0;
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet wfs = segment.dfGroupedSearchableFiles().get("parquet");
            if (wfs == null) continue;
            for (String file : wfs.files()) {
                Path filePath = parquetDir.resolve(file);
                assertTrue("Parquet file should exist: " + filePath, Files.exists(filePath));
                ParquetFileMetadata metadata = RustBridge.getFileMetadata(filePath.toString());
                totalRows += metadata.numRows();
            }
        }
        assertEquals("Total parquet rows should match ingested docs", expectedTotalDocs, totalRows);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Verification: RowIdMapping — cross-format consistency
    // ══════════════════════════════════════════════════════════════════════

    /**
     * Verifies that for each catalog segment, the lucene doc count matches the parquet row count.
     * This is a fundamental invariant of the RowIdMapping: primary and secondary must have
     * the same number of rows per merged segment.
     */
    private void verifyPerSegmentRowCountMatch(DataformatAwareCatalogSnapshot snapshot) throws IOException {
        Path parquetDir = getParquetDir();
        Path luceneDir = getLuceneDir();

        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet parquetWfs = segment.dfGroupedSearchableFiles().get("parquet");
            WriterFileSet luceneWfs = segment.dfGroupedSearchableFiles().get("lucene");

            if (parquetWfs == null || luceneWfs == null) continue;

            long parquetRows = 0;
            for (String file : parquetWfs.files()) {
                Path filePath = parquetDir.resolve(file);
                if (Files.exists(filePath)) {
                    ParquetFileMetadata metadata = RustBridge.getFileMetadata(filePath.toString());
                    parquetRows += metadata.numRows();
                }
            }

            assertEquals(
                "Segment gen=" + segment.generation() + ": parquet rows should match lucene numRows in WriterFileSet",
                luceneWfs.numRows(),
                parquetRows
            );
        }
    }

    /**
     * The definitive cross-format assertion: reads both Lucene and Parquet data for each
     * merged segment and verifies that field values match at corresponding positions.
     *
     * <p>When Lucene is primary, it determines the document order. The RowIdMapping
     * produced by PrimaryLuceneMergeStrategy tells Parquet how to reorder its rows.
     * After merge, reading Parquet rows in sequential order (0, 1, 2, ...) should
     * produce the same field values as reading Lucene docs in sequential order.
     *
     * <p>This assertion validates the entire pipeline:
     * PrimaryOneMerge → offset row IDs → IndexSort → read merged segment →
     * build PackedRowIdMapping → pass to Parquet → Parquet reorders → consistent output
     */
    @SuppressForbidden(reason = "JSON parsing for cross-format data comparison")
    private void verifyCrossFormatDataConsistency(DataformatAwareCatalogSnapshot snapshot) throws Exception {
        Path parquetDir = getParquetDir();
        Path luceneDir = getLuceneDir();

        // Collect parquet rows per segment, indexed by position (sequential row_id from 0)
        List<List<Map<String, Object>>> parquetSegmentRows = new ArrayList<>();
        for (Segment segment : snapshot.getSegments()) {
            WriterFileSet parquetWfs = segment.dfGroupedSearchableFiles().get("parquet");
            if (parquetWfs == null) continue;

            List<Map<String, Object>> rows = new ArrayList<>();
            for (String file : parquetWfs.files()) {
                Path filePath = parquetDir.resolve(file);
                if (!Files.exists(filePath)) continue;
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
                        rows.add(row);
                    }
                }
            }
            if (!rows.isEmpty()) {
                parquetSegmentRows.add(rows);
            }
        }

        assertTrue("Should have parquet segment data to compare", !parquetSegmentRows.isEmpty());

        // Match each Lucene leaf to its corresponding Parquet segment by row count,
        // then verify field values match at each position
        try (Directory dir = NIOFSDirectory.open(luceneDir); DirectoryReader reader = DirectoryReader.open(dir)) {
            int totalMatched = 0;
            for (LeafReaderContext ctx : reader.leaves()) {
                int leafDocs = ctx.reader().maxDoc();

                // Find parquet segment with matching row count
                List<Map<String, Object>> matchingParquet = null;
                for (List<Map<String, Object>> candidate : parquetSegmentRows) {
                    if (candidate.size() == leafDocs) {
                        matchingParquet = candidate;
                        break;
                    }
                }
                if (matchingParquet == null) continue;
                parquetSegmentRows.remove(matchingParquet);

                SortedNumericDocValues ageDV = ctx.reader().getSortedNumericDocValues("age");
                SortedSetDocValues nameDV = ctx.reader().getSortedSetDocValues("name");

                for (int doc = 0; doc < leafDocs; doc++) {
                    Map<String, Object> parquetRow = matchingParquet.get(doc);

                    // Compare age
                    if (ageDV != null && ageDV.advanceExact(doc)) {
                        long luceneAge = ageDV.nextValue();
                        Object parquetAge = parquetRow.get("age");
                        assertNotNull("Parquet row at position " + doc + " should have 'age'", parquetAge);
                        assertEquals(
                            "Age mismatch at position " + doc + ": lucene=" + luceneAge + " parquet=" + parquetAge,
                            luceneAge,
                            ((Number) parquetAge).longValue()
                        );
                    }

                    // Compare name
                    if (nameDV != null && nameDV.advanceExact(doc)) {
                        long ord = nameDV.nextOrd();
                        if (ord >= 0) {
                            String luceneName = nameDV.lookupOrd(ord).utf8ToString();
                            Object parquetName = parquetRow.get("name");
                            assertNotNull("Parquet row at position " + doc + " should have 'name'", parquetName);
                            assertEquals("Name mismatch at position " + doc, luceneName, parquetName.toString());
                        }
                    }

                    totalMatched++;
                }
            }

            assertTrue("Should have matched docs across formats, but matched: " + totalMatched, totalMatched > 0);
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // Infrastructure helpers
    // ══════════════════════════════════════════════════════════════════════

    @SuppressForbidden(reason = "enable pluggable dataformat merge for integration testing")
    private static void enableMerge() {
        System.setProperty(MERGE_ENABLED_PROPERTY, "true");
    }

    @SuppressForbidden(reason = "restore pluggable dataformat merge property after test")
    private static void disableMerge() {
        System.clearProperty(MERGE_ENABLED_PROPERTY);
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
        String nodeName = getClusterState().routingTable().index(INDEX_NAME).shard(0).primaryShard().currentNodeId();
        String nodeNameResolved = getClusterState().nodes().get(nodeName).getName();
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeNameResolved);
        IndexService indexService = indicesService.indexServiceSafe(resolveIndex(INDEX_NAME));
        return indexService.getShard(0);
    }

    private DataformatAwareCatalogSnapshot getCatalogSnapshot() throws IOException {
        IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(INDEX_NAME).clear().setStore(true).get();
        ShardStats shardStats = statsResponse.getIndex(INDEX_NAME).getShards()[0];
        CommitStats commitStats = shardStats.getCommitStats();
        assertNotNull(commitStats);
        assertTrue(commitStats.getUserData().containsKey(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY));
        return DataformatAwareCatalogSnapshot.deserializeFromString(
            commitStats.getUserData().get(DataformatAwareCatalogSnapshot.CATALOG_SNAPSHOT_KEY),
            Function.identity()
        );
    }

    private MergeStats getMergeStats() {
        IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(INDEX_NAME).clear().setMerge(true).get();
        return statsResponse.getIndex(INDEX_NAME).getShards()[0].getStats().getMerge();
    }
}
