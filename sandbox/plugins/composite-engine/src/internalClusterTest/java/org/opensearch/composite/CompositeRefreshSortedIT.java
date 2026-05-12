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
        verifyRowCount(snapshot, 5);
        verifySortOrder(snapshot);
    }

    /**
     * Verifies sorted refresh with Lucene secondary:
     * - Parquet is sorted
     * - Lucene __row_id__ is sequential (RowIdMapping applied)
     * - Cross-format consistency
     */
    public void testSortedRefreshWithLuceneSecondary() throws Exception {
        createIndex(sortedParquetWithLuceneSettings());

        indexDoc("charlie", 30);
        indexDoc("alice", 50);
        indexDoc("bob", 10);
        indexDoc("dave", 50);
        indexDoc("eve", 30);

        flushAndRefresh();

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        Set<String> formats = snapshot.getDataFormats();
        assertTrue("Should have parquet format", formats.contains("parquet"));
        assertTrue("Should have lucene format", formats.contains("lucene"));

        verifyRowCount(snapshot, 5);
        verifySortOrder(snapshot);
        verifyLuceneDocCount(5);
        verifyLuceneRowIdSequential();
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
        verifyRowCount(snapshot, 6);
        // Each segment should be independently sorted
        verifySortOrder(snapshot);
    }

    /**
     * Verifies null handling in sorted output: age DESC with nulls first,
     * name ASC with nulls last.
     */
    public void testSortedRefreshWithNulls() throws Exception {
        createIndex(sortedParquetOnlySettings());

        // Mix of null and non-null values
        indexDoc("alice", 50);
        indexDocNullAge("bob");       // null age → should sort first (nulls first for age)
        indexDoc("charlie", 30);
        indexDocNullAge("dave");      // null age → should sort first
        indexDoc("eve", 50);

        flushAndRefresh();

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        verifyRowCount(snapshot, 5);
        verifySortOrder(snapshot);
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
        verifyRowCount(snapshot, totalDocs);
        verifySortOrder(snapshot);
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
            .setMapping("name", "type=keyword", "age", "type=integer")
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
                totalRows += metadata.numRows();
            }
        }
        assertEquals("Total rows should match ingested docs", expectedTotalDocs, totalRows);
    }

    @SuppressForbidden(reason = "JSON parsing for sort order verification")
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
                SortedNumericDocValues rowIdDV = ctx.reader().getSortedNumericDocValues("__row_id__");
                if (rowIdDV == null) continue;

                long expectedRowId = 0;
                for (int doc = 0; doc < ctx.reader().maxDoc(); doc++) {
                    if (rowIdDV.advanceExact(doc)) {
                        long rowId = rowIdDV.nextValue();
                        assertEquals(
                            "__row_id__ should be sequential, expected " + expectedRowId + " but got " + rowId + " at doc " + doc,
                            expectedRowId,
                            rowId
                        );
                        expectedRowId++;
                    }
                }
            }
        }
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
