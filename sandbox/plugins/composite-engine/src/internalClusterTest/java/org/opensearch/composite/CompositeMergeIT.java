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
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.dataformat.DataFormatDescriptor;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockReaderManager;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.coord.DataformatAwareCatalogSnapshot;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.store.PrecomputedChecksumStrategy;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchBackEndPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Integration tests for composite merge operations across single and multiple data format engines.
 *
 * Requires JDK 25 and sandbox enabled. Run with:
 * ./gradlew :sandbox:plugins:composite-engine:internalClusterTest \
 *   --tests "*.CompositeMergeIT" \
 *   -Dsandbox.enabled=true
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class CompositeMergeIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-composite-merge";
    private static final String MERGE_ENABLED_PROPERTY = "opensearch.pluggable.dataformat.merge.enabled";

    // ── Mock DataFormatPlugin using test framework stubs ──

    public static class MockParquetDataFormatPlugin extends MockDataFormatPlugin implements SearchBackEndPlugin<Object> {
        private static final MockDataFormat PARQUET_FORMAT = new MockDataFormat("parquet", 0L, Set.of());

        public MockParquetDataFormatPlugin() {
            super(PARQUET_FORMAT);
        }

        @Override
        public Map<String, Supplier<DataFormatDescriptor>> getFormatDescriptors(IndexSettings indexSettings, DataFormatRegistry registry) {
            return Map.of("parquet", () -> new DataFormatDescriptor("parquet", new PrecomputedChecksumStrategy()));
        }

        @Override
        public String name() {
            return "mock-parquet-backend";
        }

        @Override
        public List<String> getSupportedFormats() {
            return List.of("parquet");
        }

        @Override
        public EngineReaderManager<?> createReaderManager(ReaderManagerConfig settings) {
            return new MockReaderManager("parquet");
        }
    }

    // ── Test setup ──

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
        return Arrays.asList(MockParquetDataFormatPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .build();
    }

    // ── Tests ──

    /**
     * Verifies that background merges are triggered automatically after refresh
     * when enough segments accumulate to exceed the TieredMergePolicy threshold.
     * <p>
     * Flow: index docs across many refresh cycles → each refresh calls
     * triggerPossibleMerges() → MergeScheduler picks up merge candidates
     * asynchronously → segment count decreases.
     */
    public void testBackgroundMergeSingleEngine() throws Exception {
        createIndex(INDEX_NAME, singleEngineSettings());
        ensureGreen(INDEX_NAME);

        // Create enough segments to exceed TieredMergePolicy's default threshold (~10)
        int totalSegmentsCreated = indexDocsAcrossMultipleRefreshes(15, 5);

        // Wait for async background merges to complete
        assertBusy(() -> {
            flush(INDEX_NAME);
            DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
            assertTrue(
                "Expected merges to reduce segment count below " + totalSegmentsCreated + ", but got: " + snapshot.getSegments().size(),
                snapshot.getSegments().size() < totalSegmentsCreated
            );
        });

        MergeStats mergeStats = getMergeStats();
        assertTrue("Expected at least one merge to have occurred", mergeStats.getTotal() > 0);

        DataformatAwareCatalogSnapshot snapshot = getCatalogSnapshot();
        assertEquals(Set.of("parquet"), snapshot.getDataFormats());
    }

    // ── Helpers ──

    private Settings singleEngineSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();
    }

    private int indexDocsAcrossMultipleRefreshes(int refreshCycles, int docsPerCycle) {
        for (int cycle = 0; cycle < refreshCycles; cycle++) {
            for (int i = 0; i < docsPerCycle; i++) {
                IndexResponse response = client().prepareIndex()
                    .setIndex(INDEX_NAME)
                    .setSource("field_text", randomAlphaOfLength(10), "field_number", randomIntBetween(1, 1000))
                    .get();
                assertEquals(RestStatus.CREATED, response.status());
            }
            RefreshResponse refreshResponse = client().admin().indices().prepareRefresh(INDEX_NAME).get();
            assertEquals(RestStatus.OK, refreshResponse.getStatus());
        }
        return refreshCycles;
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
