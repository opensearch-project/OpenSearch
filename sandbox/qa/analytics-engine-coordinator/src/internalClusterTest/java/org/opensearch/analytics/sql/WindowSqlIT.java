/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.sql;

import com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.arrow.plugin.ArrowBasePlugin;
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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * End-to-end IT for window functions. Constructs {@code LogicalProject} carrying a
 * {@code RexOver} programmatically via Calcite's {@link RelBuilder} — bypassing the SQL
 * parser, which wraps {@code SUM(...) OVER ()} in a CASE expression for SQL
 * NULL-on-empty-set semantics that DataFusion's physical planner doesn't yet support.
 * This matches the shape PPL's {@code eventstats} produces (clean {@code SUM($0) OVER ()}
 * with no nullability wrapper).
 *
 * @opensearch.internal
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 2, numClientNodes = 0)
public class WindowSqlIT extends OpenSearchIntegTestCase {

    private static final String INDEX = "window_idx";
    private static final int TOTAL_DOCS = 6;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ArrowBasePlugin.class, CompositeDataFormatPlugin.class, MockCommitterEnginePlugin.class);
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(FlightStreamPlugin.class, List.of(ArrowBasePlugin.class.getName())),
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

    /** {@code SUM(val) OVER ()} — unframed grand total over a 2-shard index. RexOver in the
     *  Project triggers the cost gate so the planner inserts a gather; the window evaluates
     *  against the assembled stream. With val ∈ {1,2,3,1,2,3} the per-row total is 12,
     *  repeated on every output row. */
    public void testSumOver_unframed_2shard() {
        createAndSeedIndex(2);
        SqlPlanRunner runner = sqlPlanRunner();
        RelNode plan = buildWindowedProject(runner, SqlStdOperatorTable.SUM, SqlTypeName.BIGINT);
        List<Object[]> rows = runner.executeRel(plan);
        assertEquals("one output row per input row", TOTAL_DOCS, rows.size());
        for (Object[] row : rows) {
            assertEquals("SUM(val) OVER () must equal grand total 12 on every row", 12L, ((Number) row[1]).longValue());
        }
    }

    /** {@code COUNT(val) OVER ()} — unframed count of non-null vals over a 2-shard index.
     *  Should equal {@link #TOTAL_DOCS}. */
    public void testCountOver_unframed_2shard() {
        createAndSeedIndex(2);
        SqlPlanRunner runner = sqlPlanRunner();
        RelNode plan = buildWindowedProject(runner, SqlStdOperatorTable.COUNT, SqlTypeName.BIGINT);
        List<Object[]> rows = runner.executeRel(plan);
        assertEquals(TOTAL_DOCS, rows.size());
        for (Object[] row : rows) {
            assertEquals((long) TOTAL_DOCS, ((Number) row[1]).longValue());
        }
    }

    /**
     * Builds {@code LogicalProject(val=$0, agg=AGG(val) OVER ()) FROM index} via RelBuilder +
     * {@link RexBuilder#makeOver}. {@code nullWhenCountZero=false} suppresses Calcite's
     * SUM-empty-set CASE wrap that the SQL parser would emit — matches the shape PPL's
     * own RelNode lowering produces.
     */
    private RelNode buildWindowedProject(SqlPlanRunner runner, org.apache.calcite.sql.SqlAggFunction agg, SqlTypeName outType) {
        RelBuilder b = runner.relBuilder();
        b.scan(INDEX);
        RexBuilder rex = b.getRexBuilder();
        RelDataType outRelType = b.getTypeFactory().createTypeWithNullability(b.getTypeFactory().createSqlType(outType), true);
        RexNode valRef = rex.makeInputRef(b.peek(), 0);
        RexNode over = rex.makeOver(
            outRelType,
            agg,
            List.of(valRef),
            List.of(),
            ImmutableList.<RexFieldCollation>of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING,
            false,
            true,
            false,
            false,
            false
        );
        b.project(b.field(0), over);
        b.rename(List.of("val", "agg"));
        return b.build();
    }

    // ── Infrastructure ──────────────────────────────────────────────────────

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

        for (int i = 0; i < TOTAL_DOCS; i++) {
            int val = (i % 3) + 1;
            client().prepareIndex(INDEX).setId(String.valueOf(i)).setSource("val", val).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();
    }
}
