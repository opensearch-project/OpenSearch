/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Integration tests for composite merge with real Parquet backend.
 *
 * Run with:
 * ./gradlew :sandbox:plugins:composite-engine:internalClusterTest \
 *   --tests "*.CompositeMergeIT" -Dsandbox.enabled=true
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeMergeIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-composite-merge";
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

    @SuppressForbidden(reason = "enable pluggable dataformat merge for integration testing")
    private static void enableMerge() {
        System.setProperty(MERGE_ENABLED_PROPERTY, "true");
    }

    @SuppressForbidden(reason = "restore pluggable dataformat merge property after test")
    private static void disableMerge() {
        System.clearProperty(MERGE_ENABLED_PROPERTY);
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
        assertEquals(Set.of("parquet"), snapshot.getDataFormats());

        verifyRowCount(snapshot, totalDocs);
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
        assertEquals(Set.of("parquet"), snapshot.getDataFormats());

        verifyRowCount(snapshot, totalDocs);
        verifySortOrder(snapshot);
    }

    // ── Settings ──

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

    // ── Indexing ──

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

    // ── Verification ──

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
        assertEquals("Total rows across all segments should match ingested docs", expectedTotalDocs, totalRows);
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

    private Path getParquetDir() {
        IndexShard shard = getPrimaryShard();
        return shard.shardPath().getDataPath().resolve("parquet");
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
