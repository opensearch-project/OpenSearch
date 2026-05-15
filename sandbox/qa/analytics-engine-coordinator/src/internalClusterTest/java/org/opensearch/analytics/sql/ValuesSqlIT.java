/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.sql;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * End-to-end IT for {@code LogicalValues} composed with set/join operators. The Values
 * stage runs locally on the coordinator (LOCAL_COMPUTE); these tests verify it feeds a
 * multi-input parent (Union / Join) alongside a shard-side scan arm without extra
 * wiring beyond the existing MultiInputExchangeSink machinery.
 *
 * @opensearch.internal
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 0)
public class ValuesSqlIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "values_idx";
    private static final int TOTAL_DOCS = 6;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(FlightStreamPlugin.class, CompositeDataFormatPlugin.class, MockCommitterEnginePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetDataFormatPlugin.class, Collections.emptyList()),
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
     * Source-less {@code SELECT 1 + 1, 'x'} — Calcite lowers this to a single-row
     * {@code LogicalValues}. With no shard scan in the fragment, DAGBuilder picks
     * {@code LOCAL_COMPUTE}; LocalComputeStageScheduler hands the Substrait plan to
     * DataFusion's local session with no childInputs. Exercises the pure
     * coordinator-only execution path.
     */
    public void testValues_literalRow() {
        SqlPlanRunner runner = sqlPlanRunner();
        List<Object[]> rows = runner.executeSql("SELECT 1 + 1, 'x'");
        assertEquals("source-less SELECT must produce a single row", 1, rows.size());
        assertEquals(2, ((Number) rows.getFirst()[0]).intValue());
        assertEquals("x", rows.getFirst()[1]);
    }

    /** Values as a Union arm, 1-shard scan. Scan arm and Values arm are both already at
     *  COORDINATOR+SINGLETON (1-shard scan path doesn't need an ER), but the Values arm
     *  still runs as LOCAL_COMPUTE on coord. */
    public void testUnionAll_valuesArm_andScanArm_1shard() {
        createAndSeedIndex(1);
        assertUnionAllValuesAndScan();
    }

    /** Values as a Union arm, 2-shard scan. Scan arm gathers across 2 shards via ER;
     *  Values arm runs as LOCAL_COMPUTE on coord. */
    public void testUnionAll_valuesArm_andScanArm_2shard() {
        createAndSeedIndex(2);
        assertUnionAllValuesAndScan();
    }

    private void assertUnionAllValuesAndScan() {
        SqlPlanRunner runner = sqlPlanRunner();
        List<Object[]> rows = runner.executeSql(
            "SELECT val FROM " + INDEX + " UNION ALL SELECT x FROM (VALUES (99)) AS v(x)"
        );
        assertEquals("UNION ALL of 6 idx rows + 1 values row must yield 7", 7, rows.size());
        boolean foundLiteral = false;
        for (Object[] row : rows) {
            if (((Number) row[0]).intValue() == 99) {
                foundLiteral = true;
                break;
            }
        }
        assertTrue("UNION result must contain the literal 99 from the Values arm", foundLiteral);
    }

    /** Values as a Join arm, 1-shard scan. */
    public void testInnerJoin_scanWithValuesArm_1shard() {
        createAndSeedIndex(1);
        assertInnerJoinScanWithValues();
    }

    /** Values as a Join arm, 2-shard scan. */
    public void testInnerJoin_scanWithValuesArm_2shard() {
        createAndSeedIndex(2);
        assertInnerJoinScanWithValues();
    }

    private void assertInnerJoinScanWithValues() {
        SqlPlanRunner runner = sqlPlanRunner();
        List<Object[]> rows = runner.executeSql(
            "SELECT i.val FROM " + INDEX + " i JOIN (VALUES (1), (2)) AS v(x) ON i.val = v.x"
        );
        assertEquals("JOIN over Values must yield 4 rows (matches: 1,1,2,2)", 4, rows.size());
        List<Integer> vals = new ArrayList<>();
        for (Object[] row : rows) {
            vals.add(((Number) row[0]).intValue());
        }
        java.util.Collections.sort(vals);
        assertEquals("matched rows must be [1,1,2,2]", List.of(1, 1, 2, 2), vals);
    }

    // ── Test infrastructure ─────────────────────────────────────────────────

    private SqlPlanRunner sqlPlanRunner() {
        String node = internalCluster().getNodeNames()[0];
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, node);
        DefaultPlanExecutor executor = internalCluster().getInstance(DefaultPlanExecutor.class, node);
        return new SqlPlanRunner(clusterService, executor);
    }

    private void createAndSeedIndex(int shardCount) {
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shardCount)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(indexSettings)
            .setMapping("val", "type=integer")
            .get();
        assertTrue("index creation must be acknowledged", response.isAcknowledged());
        ensureGreen(INDEX);

        // Three distinct vals, each repeated → 1, 2, 3, 1, 2, 3.
        for (int i = 0; i < TOTAL_DOCS; i++) {
            int val = (i % 3) + 1;
            client().prepareIndex(INDEX).setId(String.valueOf(i)).setSource("val", val).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();
    }
}
