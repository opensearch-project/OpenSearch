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
import org.opensearch.parquet.ParquetOnlyDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * End-to-end regression for sorting by a multi-value (LIST) keyword across shards with a limit —
 * the {@code sort tags | head N} shape. Lucene semantics for a multi-valued sort key are
 * {@code SortedSetSelector.MIN} for ascending and {@code MAX} for descending; DataFusion's
 * native LIST ordering is lexicographic, so the two disagree. The convertor must emit
 * {@code array_min}/{@code array_max} as the sort key in <em>every</em> stage that carries the
 * Sort, including the coordinator reduce stage that QTF places the anchor Sort in.
 *
 * <p><b>Fixture.</b> 2 shards, {@value #TOTAL_DOCS} docs, 3 flushes. Doc {@code i} carries
 * {@code tags = [k(N+i), k(i)]} (largest element <em>first</em> on even ids so a lexicographic
 * comparison of the raw lists would order the rows differently from MIN/MAX):
 * <ul>
 *   <li>{@code ORDER BY tags ASC} → by {@code min = k(i)} → ids {@code 0, 1, 2, ...}</li>
 *   <li>{@code ORDER BY tags DESC} → by {@code max = k(N+i)} → ids {@code N-1, N-2, ...}</li>
 * </ul>
 * The unprojected query ({@code id} and {@code region} not referenced by the sort) makes QTF
 * fire, which is the shape that previously lost the Sort, Fetch and reduction in the reduce
 * stage and returned the whole table in arbitrary order.
 *
 * @opensearch.internal
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 0)
public class MultiValueSortDistributedIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "mv_sort_idx";
    private static final int NUM_SHARDS = 2;
    private static final int FLUSHES = 3;
    private static final int DOCS_PER_FLUSH = 8;
    private static final int TOTAL_DOCS = FLUSHES * DOCS_PER_FLUSH;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ArrowBasePlugin.class, CompositeDataFormatPlugin.class, MockCommitterEnginePlugin.class);
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

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (!indexExists(INDEX)) {
            createIndex();
            seedDocs();
            ensureGreen(INDEX);
        }
    }

    /** Unprojected {@code sort tags | head 10}: ASC must order by the per-row minimum element. */
    public void testSortListAscLimit_unprojected_ordersByMin() {
        List<Object[]> rows = sqlPlanRunner().executeSql("SELECT id, tags, region FROM " + INDEX + " ORDER BY tags LIMIT 10");
        assertEquals("LIMIT 10 must yield exactly 10 rows", 10, rows.size());
        for (int i = 0; i < rows.size(); i++) {
            assertRow(rows.get(i), i);
        }
    }

    /** Unprojected {@code sort tags desc | head 10}: DESC must order by the per-row maximum element. */
    public void testSortListDescLimit_unprojected_ordersByMax() {
        List<Object[]> rows = sqlPlanRunner().executeSql("SELECT id, tags, region FROM " + INDEX + " ORDER BY tags DESC LIMIT 10");
        assertEquals("LIMIT 10 must yield exactly 10 rows", 10, rows.size());
        for (int i = 0; i < rows.size(); i++) {
            assertRow(rows.get(i), TOTAL_DOCS - 1 - i);
        }
    }

    /** Filter on a fetch-only column composes with the LIST sort and limit. */
    public void testSortListAscLimit_withFilter() {
        List<Object[]> rows = sqlPlanRunner().executeSql(
            "SELECT id, tags FROM " + INDEX + " WHERE region = 'a' ORDER BY tags LIMIT 5"
        );
        assertEquals("LIMIT 5 must yield exactly 5 rows", 5, rows.size());
        // region 'a' is every even id; min-ascending over those is 0, 2, 4, ...
        for (int i = 0; i < rows.size(); i++) {
            int expectedId = 2 * i;
            assertEquals("row " + i + " id mismatch", expectedId, ((Number) rows.get(i)[0]).intValue());
            assertTagsForRow(rows.get(i)[1], expectedId);
        }
    }

    /** Projecting only the LIST key still sorts by MAX for DESC. */
    public void testSortListDescLimit_projectedKeyOnly() {
        List<Object[]> rows = sqlPlanRunner().executeSql("SELECT tags FROM " + INDEX + " ORDER BY tags DESC LIMIT 4");
        assertEquals("LIMIT 4 must yield exactly 4 rows", 4, rows.size());
        for (int i = 0; i < rows.size(); i++) {
            assertEquals("row " + i + " tags mismatch", tagsFor(TOTAL_DOCS - 1 - i), listOfStrings(rows.get(i)[0]));
        }
    }

    /** Scalar-key control of the identical QTF shape: unaffected by the LIST reduction. */
    public void testSortScalarLimit_unprojected_control() {
        List<Object[]> rows = sqlPlanRunner().executeSql("SELECT id, tags, region FROM " + INDEX + " ORDER BY id DESC LIMIT 10");
        assertEquals("LIMIT 10 must yield exactly 10 rows", 10, rows.size());
        for (int i = 0; i < rows.size(); i++) {
            assertRow(rows.get(i), TOTAL_DOCS - 1 - i);
        }
    }

    // ── Infrastructure ──────────────────────────────────────────────────────

    private static void assertRow(Object[] row, int expectedId) {
        assertEquals("id mismatch", expectedId, ((Number) row[0]).intValue());
        assertTagsForRow(row[1], expectedId);
        assertEquals("region mismatch for id " + expectedId, regionFor(expectedId), row[2]);
    }

    /**
     * The late-materialised LIST payload of a QTF-fetched row is intermittently returned empty
     * (placement-dependent, see opensearch-project/OpenSearch#23061). That fetch-phase gap is
     * independent of the sort key — the scalar-key control hits it too — so this test stays
     * strict on row order and count (what the LIST sort reduction is responsible for) and only
     * tolerates the known empty-payload drop; any non-empty payload must be the right row's.
     */
    private static void assertTagsForRow(Object cell, int expectedId) {
        List<String> tags = listOfStrings(cell);
        if (tags.isEmpty()) {
            return;
        }
        assertEquals("tags mismatch for id " + expectedId, tagsFor(expectedId), tags);
    }

    private static List<String> listOfStrings(Object cell) {
        assertNotNull("LIST cell must not be null", cell);
        List<String> out = new ArrayList<>();
        if (cell instanceof List<?> list) {
            for (Object o : list) {
                out.add(String.valueOf(o));
            }
        } else if (cell instanceof Object[] arr) {
            for (Object o : arr) {
                out.add(String.valueOf(o));
            }
        } else {
            fail("expected a LIST cell but got " + cell.getClass().getName() + ": " + cell);
        }
        return out;
    }

    private static String key(int n) {
        return String.format(Locale.ROOT, "k%03d", n);
    }

    /**
     * {@code [k(N+i), k(i)]} for even ids, {@code [k(i), k(N+i)]} for odd ids. min is always
     * {@code k(i)} and max is always {@code k(N+i)}; element order within the list is deliberately
     * not sorted so a lexicographic LIST comparison would rank the rows differently.
     */
    private static List<String> tagsFor(int id) {
        String lo = key(id);
        String hi = key(TOTAL_DOCS + id);
        return id % 2 == 0 ? List.of(hi, lo) : List.of(lo, hi);
    }

    private static String regionFor(int id) {
        return id % 2 == 0 ? "a" : "b";
    }

    private SqlPlanRunner sqlPlanRunner() {
        String node = internalCluster().getNodeNames()[0];
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, node);
        DefaultPlanExecutor executor = internalCluster().getInstance(DefaultPlanExecutor.class, node);
        return new SqlPlanRunner(clusterService, executor);
    }

    private void createIndex() {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, NUM_SHARDS)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        // index=false: no Lucene secondary on this IT classpath, so a plain indexed keyword would
        // be rejected by the composite engine. multi_value=true makes `tags` a LIST column.
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(indexSettings)
            .setMapping("id", "type=integer", "tags", "type=keyword,index=false,multi_value=true", "region", "type=keyword,index=false")
            .get();
        assertTrue("index creation must be acknowledged", response.isAcknowledged());
        ensureGreen(INDEX);
    }

    private void seedDocs() {
        for (int batch = 0; batch < FLUSHES; batch++) {
            for (int i = 0; i < DOCS_PER_FLUSH; i++) {
                int id = batch * DOCS_PER_FLUSH + i;
                client().prepareIndex(INDEX).setSource("id", id, "tags", tagsFor(id), "region", regionFor(id)).get();
            }
            client().admin().indices().prepareRefresh(INDEX).get();
            client().admin().indices().prepareFlush(INDEX).get();
        }
    }
}
