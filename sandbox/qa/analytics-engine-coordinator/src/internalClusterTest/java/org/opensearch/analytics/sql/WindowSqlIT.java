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
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
    private static final int[] TIE_VALS = { 1, 2, 2, 3 };
    private static final int TIE_DOCS = TIE_VALS.length;

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

    /**
     * {@code RANK() OVER (ORDER BY val)} over a single-shard index seeded with a tie.
     * With vals {1,2,2,3} the ranks are {1,2,2,4} — the tied rows share rank 2 and the
     * next rank skips to 4 (gap semantics). This is the case that proves RANK is wired
     * distinctly from DENSE_RANK end-to-end.
     */
    public void testRankOver_orderBy_tie_1shard() {
        createAndSeedTieIndex(1);
        SqlPlanRunner runner = sqlPlanRunner();
        RelNode plan = buildRankProject(runner, SqlStdOperatorTable.RANK);
        List<Object[]> rows = runner.executeRel(plan);
        assertEquals(TIE_DOCS, rows.size());
        // Output row order is not guaranteed; assert the val→rank mapping as a set of pairs.
        Map<Integer, Long> valToRank = collectValToRank(rows);
        assertEquals("rank of val=1", 1L, (long) valToRank.get(1));
        assertEquals("rank of tied val=2 (gap semantics)", 2L, (long) valToRank.get(2));
        assertEquals("rank of val=3 skips to 4 after the tie", 4L, (long) valToRank.get(3));
    }

    /**
     * {@code DENSE_RANK() OVER (ORDER BY val)} over the same tied data. With vals {1,2,2,3}
     * the dense ranks are {1,2,2,3} — tied rows share rank 2 and the next rank is 3 with no
     * gap. Contrast with {@link #testRankOver_orderBy_tie_1shard}, where the same data yields
     * a gap (rank 4) — the two assertions together prove the functions are distinct.
     */
    public void testDenseRankOver_orderBy_tie_1shard() {
        createAndSeedTieIndex(1);
        SqlPlanRunner runner = sqlPlanRunner();
        RelNode plan = buildRankProject(runner, SqlStdOperatorTable.DENSE_RANK);
        List<Object[]> rows = runner.executeRel(plan);
        assertEquals(TIE_DOCS, rows.size());
        Map<Integer, Long> valToRank = collectValToRank(rows);
        assertEquals("dense rank of val=1", 1L, (long) valToRank.get(1));
        assertEquals("dense rank of tied val=2", 2L, (long) valToRank.get(2));
        assertEquals("dense rank of val=3 has no gap after the tie", 3L, (long) valToRank.get(3));
    }

    /**
     * {@code RANK() OVER (ORDER BY val)} over the same tied data spread across <b>2 shards</b>.
     * The result must match the 1-shard case exactly — {1,2,2,4} — which only holds if the
     * RexOver cost gate gathered both shards to a single node before the window ran. Were the
     * window evaluated shard-locally, each shard would restart its rank at 1 and the tied 2s
     * (likely on different shards) would no longer share a rank. This is the test that proves
     * the cross-shard gather (validated structurally in {@code WindowPlanShapeTests}) is wired
     * end-to-end for ranking functions.
     */
    public void testRankOver_orderBy_tie_2shard() {
        createAndSeedTieIndex(2);
        SqlPlanRunner runner = sqlPlanRunner();
        RelNode plan = buildRankProject(runner, SqlStdOperatorTable.RANK);
        List<Object[]> rows = runner.executeRel(plan);
        assertEquals(TIE_DOCS, rows.size());
        Map<Integer, Long> valToRank = collectValToRank(rows);
        assertEquals("rank of val=1", 1L, (long) valToRank.get(1));
        assertEquals("rank of tied val=2 — one global rank across shards", 2L, (long) valToRank.get(2));
        assertEquals("rank of val=3 skips to 4 after the global tie", 4L, (long) valToRank.get(3));
    }

    /**
     * {@code DENSE_RANK() OVER (ORDER BY val)} over the tied data across <b>2 shards</b>. Mirror
     * of {@link #testRankOver_orderBy_tie_2shard}: the dense ranks stay {1,2,2,3} only if the
     * gather assembled a single global stream before ranking.
     */
    public void testDenseRankOver_orderBy_tie_2shard() {
        createAndSeedTieIndex(2);
        SqlPlanRunner runner = sqlPlanRunner();
        RelNode plan = buildRankProject(runner, SqlStdOperatorTable.DENSE_RANK);
        List<Object[]> rows = runner.executeRel(plan);
        assertEquals(TIE_DOCS, rows.size());
        Map<Integer, Long> valToRank = collectValToRank(rows);
        assertEquals("dense rank of val=1", 1L, (long) valToRank.get(1));
        assertEquals("dense rank of tied val=2 — one global rank across shards", 2L, (long) valToRank.get(2));
        assertEquals("dense rank of val=3 has no gap after the global tie", 3L, (long) valToRank.get(3));
    }

    /** Folds {@code [val, rank]} output rows into a {@code val → rank} map. The tied rows
     *  carry the same val and the same rank, so collapsing them loses nothing. */
    private static Map<Integer, Long> collectValToRank(List<Object[]> rows) {
        Map<Integer, Long> out = new java.util.HashMap<>();
        for (Object[] row : rows) {
            int val = ((Number) row[0]).intValue();
            long rank = ((Number) row[1]).longValue();
            Long prior = out.put(val, rank);
            if (prior != null) {
                assertEquals("all rows with val=" + val + " must share one rank", (long) prior, rank);
            }
        }
        return out;
    }

    /**
     * Builds {@code LogicalProject(val=$0, rank=RANKFN() OVER (ORDER BY val)) FROM index}. RANK /
     * DENSE_RANK take no operands and require an ORDER BY; with an ORDER BY and no explicit frame
     * the implicit frame is RANGE UNBOUNDED PRECEDING TO CURRENT ROW (rangeBased=false).
     */
    private RelNode buildRankProject(SqlPlanRunner runner, org.apache.calcite.sql.SqlAggFunction rankFn) {
        RelBuilder b = runner.relBuilder();
        b.scan(INDEX);
        RexBuilder rex = b.getRexBuilder();
        RexNode valRef = rex.makeInputRef(b.peek(), 0);
        RexNode over = rex.makeOver(
            b.getTypeFactory().createSqlType(SqlTypeName.BIGINT),
            rankFn,
            List.of(),
            ImmutableList.<RexNode>of(),
            ImmutableList.of(new RexFieldCollation(valRef, java.util.Collections.emptySet())),  // ORDER BY val
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            false,
            true,
            false,
            false,
            false
        );
        b.project(b.field(0), over);
        b.rename(List.of("val", "rank"));
        return b.build();
    }

    private void createAndSeedTieIndex(int shardCount) {
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

        // vals {1, 2, 2, 3} — the duplicated 2 is the tie that distinguishes RANK from DENSE_RANK.
        for (int val : TIE_VALS) {
            client().prepareIndex(INDEX).setSource("val", val).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();
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
            client().prepareIndex(INDEX).setSource("val", val).get();
        }
        client().admin().indices().prepareRefresh(INDEX).get();
        client().admin().indices().prepareFlush(INDEX).get();
    }
}
