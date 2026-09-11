/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.analytics.sql.SqlPlanRunner;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
import org.opensearch.parquet.ParquetOnlyDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * Correctness IT for the {@code index.sort.field} → DataFusion {@code with_file_sort_order(...)}
 * propagation in the analytics-backend-datafusion vanilla query path.
 *
 * <p>Background. The DatafusionPlugin reads {@code index.sort.field} / {@code index.sort.order} off
 * {@link org.opensearch.index.IndexSettings}, ships them across FFM in {@code df_create_reader},
 * and threads them into {@link org.opensearch.index.engine.exec.EngineReaderManager} →
 * {@code ShardView.sort_fields}/{@code sort_orders}. At session-context build time
 * (Rust-side {@code session_context.rs}) the lists become a
 * {@code ListingOptions::with_file_sort_order(...)} call, which makes DataFusion advertise
 * {@code output_ordering} from {@code DataSourceExec} and unlock the
 * {@code repartition_preserving_order} / {@code sort_prefix} optimizations.
 *
 * <p>{@code with_file_sort_order} is a <b>per-file claim</b> — DataFusion does not verify it. The
 * claim is safe here only because the OpenSearch parquet writer enforces the same sort within
 * each segment via {@code index.sort.field}. The risk we're guarding against in this IT is a
 * regression where:
 *
 * <ul>
 *   <li>the claim doesn't match the writer's actual physical layout, or</li>
 *   <li>the optimizer mis-uses the claim and silently drops or re-orders rows.</li>
 * </ul>
 *
 * <p>Strategy: ingest the same set of rows into two indices — one with {@code index.sort.field}
 * configured, one without — and assert that the query results are byte-identical for several
 * query shapes (filter-only, sort+limit, agg+sort). If our patch lies, results would differ.
 *
 * @opensearch.internal
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 0)
public class IndexSortPropagationIT extends OpenSearchIntegTestCase {

    private static final String IDX_UNSORTED = "sort_prop_unsorted";
    private static final String IDX_SORTED = "sort_prop_sorted";
    private static final int DOC_COUNT = 30;
    private static final int SHARDS = 2;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ArrowBasePlugin.class, CompositeDataFormatPlugin.class, MockCommitterEnginePlugin.class, MockDataFormatPlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(FlightStreamPlugin.class, List.of(ArrowBasePlugin.class.getName())),
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetOnlyDataFormatPlugin.class, Collections.emptyList()),
            classpathPlugin(DataFusionPlugin.class, List.of(AnalyticsPlugin.class.getName()))
        );
    }

    private static PluginInfo classpathPlugin(Class<? extends Plugin> pluginClass, List<String> extendedPlugins) {
        return new PluginInfo(
            pluginClass.getName(),
            "classpath plugin",
            "NA",
            Version.CURRENT,
            "1.8",
            pluginClass.getName(),
            null,
            extendedPlugins,
            false
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(FeatureFlags.STREAM_TRANSPORT, true)
            .build();
    }

    /**
     * Filter-only query — no sort, no limit. Must produce the same rows in both indices.
     */
    public void testFilterOnly_resultsMatchAcrossSortedAndUnsorted() throws Exception {
        createAndSeedBothIndices();
        SqlPlanRunner runner = sqlPlanRunner();

        List<Object[]> unsorted = runner.executeSql(
            "SELECT URL, EventDate, CounterID FROM " + IDX_UNSORTED + " WHERE CounterID > 5"
        );
        List<Object[]> sorted = runner.executeSql(
            "SELECT URL, EventDate, CounterID FROM " + IDX_SORTED + " WHERE CounterID > 5"
        );

        assertResultsEquivalent("filter-only", unsorted, sorted);
    }

    /**
     * Sort+LIMIT — the query that actually exercises {@code sort_prefix} when the ORDER BY
     * matches a prefix of {@code index.sort.field}. Both indices must produce the same top-N
     * rows in the same order.
     */
    public void testSortLimit_resultsMatchAcrossSortedAndUnsorted() throws Exception {
        createAndSeedBothIndices();
        SqlPlanRunner runner = sqlPlanRunner();

        // ORDER BY (CounterID DESC, EventDate DESC) is a prefix of the sort declared on IDX_SORTED.
        String sql = " WHERE CounterID > 0 ORDER BY CounterID DESC, EventDate DESC LIMIT 10";
        List<Object[]> unsorted = runner.executeSql("SELECT URL, EventDate, CounterID FROM " + IDX_UNSORTED + sql);
        List<Object[]> sorted = runner.executeSql("SELECT URL, EventDate, CounterID FROM " + IDX_SORTED + sql);

        assertEquals("LIMIT 10 must produce 10 rows for unsorted", 10, unsorted.size());
        assertEquals("LIMIT 10 must produce 10 rows for sorted", 10, sorted.size());
        // For sort+LIMIT, row ORDER must match — not just multiset equality.
        assertOrderedRowsEqual("sort+limit", unsorted, sorted);
    }

    /**
     * Agg + sort on aggregate output — the sort declaration must NOT bleed into queries that
     * don't reference the sort columns in their ORDER BY.
     */
    public void testAggSortOnAggregate_resultsMatchAcrossSortedAndUnsorted() throws Exception {
        createAndSeedBothIndices();
        SqlPlanRunner runner = sqlPlanRunner();

        String sql = " WHERE CounterID > 0 GROUP BY EventDate ORDER BY COUNT(*) DESC, EventDate ASC LIMIT 5";
        List<Object[]> unsorted = runner.executeSql("SELECT EventDate, COUNT(*) AS c FROM " + IDX_UNSORTED + sql);
        List<Object[]> sorted = runner.executeSql("SELECT EventDate, COUNT(*) AS c FROM " + IDX_SORTED + sql);

        // tiebreaker-stable ORDER BY (count, then date) → ordered comparison is meaningful.
        assertOrderedRowsEqual("agg+sort", unsorted, sorted);
    }

    /**
     * ORDER BY a non-sort column. The sort declaration must not re-route results based on
     * unrelated columns.
     */
    public void testSortOnNonSortColumn_resultsMatchAcrossSortedAndUnsorted() throws Exception {
        createAndSeedBothIndices();
        SqlPlanRunner runner = sqlPlanRunner();

        String sql = " WHERE CounterID > 0 ORDER BY URL ASC LIMIT 8";
        List<Object[]> unsorted = runner.executeSql("SELECT URL, CounterID FROM " + IDX_UNSORTED + sql);
        List<Object[]> sorted = runner.executeSql("SELECT URL, CounterID FROM " + IDX_SORTED + sql);

        assertOrderedRowsEqual("sort-on-non-sort-column", unsorted, sorted);
    }

    // ── Infrastructure ──────────────────────────────────────────────────────

    private SqlPlanRunner sqlPlanRunner() {
        String node = internalCluster().getNodeNames()[0];
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, node);
        DefaultPlanExecutor executor = internalCluster().getInstance(DefaultPlanExecutor.class, node);
        return new SqlPlanRunner(clusterService, executor);
    }

    /**
     * Creates two parallel indices ingested with the same docs:
     *   - {@code IDX_UNSORTED}: no {@code index.sort.field}.
     *   - {@code IDX_SORTED}: {@code index.sort.field=[CounterID, EventDate]} both descending.
     */
    private void createAndSeedBothIndices() {
        // Local helper renamed to avoid ambiguity with OpenSearchIntegTestCase#createIndex
        // overloads (the (String, Settings, String) form would collide on null arguments).
        createCompositeIndex(IDX_UNSORTED, /*sortFields*/ null, /*sortOrders*/ null);
        createCompositeIndex(IDX_SORTED, List.of("CounterID", "EventDate"), List.of("desc", "desc"));
        seed(IDX_UNSORTED);
        seed(IDX_SORTED);
    }

    private void createCompositeIndex(String name, List<String> sortFields, List<String> sortOrders) {
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, SHARDS)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats");
        if (sortFields != null && !sortFields.isEmpty()) {
            settings.putList("index.sort.field", sortFields);
            settings.putList("index.sort.order", sortOrders);
        }

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(name)
            .setSettings(settings.build())
            .setMapping("URL", "type=keyword,index=false", "EventDate", "type=date", "CounterID", "type=integer")
            .get();
        assertTrue("index creation must be acknowledged for " + name, response.isAcknowledged());
        ensureGreen(name);
    }

    /**
     * Seeds the same {@code DOC_COUNT} docs into the given index. Doc IDs are stable so both
     * indices end up with identical content.
     */
    private void seed(String index) {
        for (int i = 0; i < DOC_COUNT; i++) {
            // CounterID: 1..DOC_COUNT, EventDate spans 2026-05-01..2026-05-(1 + i mod 10).
            // Same content for both indices — only physical layout differs because of index.sort.field.
            client().prepareIndex(index)
                .setSource(
                    "URL",
                    "https://example.com/page" + i,
                    "EventDate",
                    "2026-05-" + String.format(Locale.ROOT, "%02d", (i % 10) + 1),
                    "CounterID",
                    i + 1
                )
                .get();
        }
        client().admin().indices().prepareRefresh(index).get();
        client().admin().indices().prepareFlush(index).get();
    }

    /**
     * Multiset comparison — for queries where row order isn't guaranteed (e.g. filter-only).
     * Sorts both lists by a stable key derived from each row before comparing.
     */
    private static void assertResultsEquivalent(String label, List<Object[]> a, List<Object[]> b) {
        assertEquals(label + ": row count mismatch", a.size(), b.size());
        List<String> aKeys = a.stream().map(IndexSortPropagationIT::rowKey).sorted().toList();
        List<String> bKeys = b.stream().map(IndexSortPropagationIT::rowKey).sorted().toList();
        assertEquals(label + ": row content mismatch", aKeys, bKeys);
    }

    /**
     * Order-sensitive comparison — for queries with a deterministic ORDER BY.
     */
    private static void assertOrderedRowsEqual(String label, List<Object[]> a, List<Object[]> b) {
        assertEquals(label + ": row count mismatch", a.size(), b.size());
        for (int i = 0; i < a.size(); i++) {
            assertEquals(label + ": row " + i + " mismatch", rowKey(a.get(i)), rowKey(b.get(i)));
        }
    }

    private static String rowKey(Object[] row) {
        StringBuilder sb = new StringBuilder();
        for (Object cell : row) {
            sb.append(cell == null ? "<null>" : cell.toString()).append('|');
        }
        return sb.toString();
    }
}
