/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Plan shape tests for the TopK rewriter. Grouped into Detection (skip cases)
 * and Rewrite (insertion cases).
 */
public class TopKRewriterPlanShapeTests extends PlanShapeTestBase {

    // ── Detection: rewriter should NOT fire ──────────────────────────────────

    /** Factor=0 → no per-partition Sort inserted. */
    public void testDetection_factorZero_skipped() {
        RelNode result = runPlanner(buildSortHeadOverGroupedCount(), contextWithOversampling(0.0));
        String plan = RelOptUtil.toString(result);
        long sortCount = plan.lines().filter(l -> l.contains("OpenSearchSort")).count();
        assertEquals("expected 1 Sort (coord only)", 1, sortCount);
    }

    /** No aggregate in plan → TopK must not fire (sort-pushdown may add a shard Sort, which is fine). */
    public void testDetection_noAggregate_skipped() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RelNode sort = LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING)),
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(sort, contextWithOversampling(2.0));
        String plan = RelOptUtil.toString(result);
        assertFalse("no aggregate → TopK must not fire", plan.contains("OpenSearchAggregate"));
    }

    /** Aggregate without group-by (scalar agg) → skip. */
    public void testDetection_noGroupBy_skipped() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(countStarCall()));
        RelNode sort = LogicalSort.create(
            agg,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING)),
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(sort, contextWithOversampling(2.0));
        String plan = RelOptUtil.toString(result);
        long sortCount = plan.lines().filter(l -> l.contains("OpenSearchSort")).count();
        assertEquals("no per-partition Sort for scalar aggregate", 1, sortCount);
    }

    /** Sort without collation (bare LIMIT) → skip. */
    public void testDetection_bareLimitNoCollation_skipped() {
        RelNode result = runPlanner(buildBareLimitOverGroupedCount(), contextWithOversampling(2.0));
        String plan = RelOptUtil.toString(result);
        long sortCount = plan.lines().filter(l -> l.contains("OpenSearchSort")).count();
        assertTrue("bare LIMIT should not produce a per-partition Sort", sortCount <= 1);
    }

    // ── Rewrite: per-partition Sort IS inserted ──────────────────────────────

    /** Basic: Sort(collation, fetch) above grouped COUNT → per-partition Sort inserted. */
    public void testRewrite_countByGroup_sortInserted() {
        RelNode result = runPlanner(buildSortHeadOverGroupedCount(), contextWithOversampling(2.0));
        String plan = RelOptUtil.toString(result);
        long sortCount = plan.lines().filter(l -> l.contains("OpenSearchSort")).count();
        assertEquals("expected 2 Sorts (coord + per-partition)", 2, sortCount);
        assertTrue(
            "per-partition Sort must be below ER",
            plan.contains("ExchangeReducer") && plan.indexOf("ExchangeReducer") < plan.lastIndexOf("OpenSearchSort")
        );
    }

    /** Two Sorts in plan: only the bottommost (with collation above FINAL) triggers TopK. */
    public void testRewrite_twoSorts_onlyBottomModified() {
        // SystemLimit(no collation) → Sort(collation) → Aggregate
        RelNode innerPlan = buildSortHeadOverGroupedCount();
        RelNode outerLimit = LogicalSort.create(
            innerPlan,
            RelCollations.EMPTY,
            null,
            rexBuilder.makeLiteral(10000, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(outerLimit, contextWithOversampling(2.0));
        String plan = RelOptUtil.toString(result);
        long sortCount = plan.lines().filter(l -> l.contains("OpenSearchSort")).count();
        // 2 = coord collated sort + per-partition sort (outer limit may merge or stay as 3rd)
        assertTrue("at least 2 OpenSearchSort nodes expected", sortCount >= 2);
    }

    /**
     * Collated outer system-limit over a collated inner head: oversampling must honor the INNER
     * fetch, not the outer cap. PPL {@code ... | sort - c | head 10} arrives as
     * {@code SystemLimit(fetch=10000, sort0=$1 DESC) → Sort(fetch=10, sort0=$1 DESC) → ... → FINAL}.
     * The per-partition shard Sort fetch must derive from 10 (→ ceil(10*2)+10 = 30), not from the
     * 10000 system cap (which would over-fetch 30000 rows/shard).
     */
    public void testRewrite_collatedOuterLimit_honorsInnerFetch() {
        RelNode inner = buildSortHeadOverGroupedCount(); // Sort(collation $1 DESC, fetch 10) over grouped count
        // Outer system size-limit with the SAME collation and a large cap (what QueryService wraps).
        RelNode outer = LogicalSort.create(
            inner,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)),
            null,
            rexBuilder.makeLiteral(10000, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(outer, contextWithOversampling(2.0));
        String plan = RelOptUtil.toString(result);
        long sortCount = plan.lines().filter(l -> l.contains("OpenSearchSort")).count();
        assertTrue("a per-partition Sort must be inserted", sortCount >= 2);
        // Shard Sort fetch = offset + ceil(fetch * factor) + fetch = 0 + ceil(10*2) + 10 = 30.
        assertTrue("shard Sort must be sized off the inner fetch (30), got plan:\n" + plan, plan.contains("fetch=[30]"));
        assertFalse("shard Sort must NOT be sized off the 10000 system cap (30000)", plan.contains("fetch=[30000]"));
    }

    /** SUM aggregate: partial SUM is directly sortable, no reduce_eval needed. */
    public void testRewrite_sumByGroup_sortInserted() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        LogicalAggregate agg = LogicalAggregate.create(
            scan,
            List.of(),
            ImmutableBitSet.of(0),
            null,
            List.of(
                AggregateCall.create(
                    SqlStdOperatorTable.SUM,
                    false,
                    false,
                    false,
                    List.of(),
                    List.of(1),
                    -1,
                    null,
                    RelCollations.EMPTY,
                    typeFactory.createSqlType(SqlTypeName.INTEGER),
                    "s"
                )
            )
        );
        RelNode sort = LogicalSort.create(
            agg,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)),
            null,
            rexBuilder.makeLiteral(5, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(sort, contextWithOversampling(1.5));
        String plan = RelOptUtil.toString(result);
        long sortCount = plan.lines().filter(l -> l.contains("OpenSearchSort")).count();
        assertEquals("expected 2 Sorts (coord + per-partition)", 2, sortCount);
    }

    /** dc(x) by group + sort + head: APPROX_COUNT_DISTINCT splits with TopK reduce_eval. */
    public void testRewrite_dcByGroup_splitAndTopK() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(countDistinctCall(scan)));
        RelNode sort = LogicalSort.create(
            agg,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)),
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(sort, contextWithOversampling(2.0));
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$1], dir0=[DESC], fetch=[10], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], dc=[APPROX_COUNT_DISTINCT($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[], partitionCount=0]])
                      OpenSearchProject(status=[$0], dc=[$1], viableBackends=[[mock-parquet]])
                        OpenSearchSort(sort0=[$2], dir0=[DESC], fetch=[30], viableBackends=[[mock-parquet]])
                          OpenSearchProject(status=[$0], dc=[$1], __reduce_eval_1=[reduce_eval('approx_distinct', $1)], viableBackends=[[mock-parquet]])
                            OpenSearchAggregate(group=[{0}], dc=[APPROX_COUNT_DISTINCT($1)], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** Multi-group-by COUNT + TopK: verifies PARTIAL/FINAL split fires. */
    public void testRewrite_multiGroupByCount_splitAndTopK() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0, 1), null, List.of(countStarCall(scan)));
        RelNode sort = LogicalSort.create(
            agg,
            RelCollations.of(new RelFieldCollation(2, RelFieldCollation.Direction.DESCENDING)),
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(sort, contextWithOversampling(2.0));
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$2], dir0=[DESC], fetch=[10], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0, 1}], cnt=[SUM($2)], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[], partitionCount=0]])
                      OpenSearchSort(sort0=[$2], dir0=[DESC], fetch=[30], viableBackends=[[mock-parquet]])
                        OpenSearchAggregate(group=[{0, 1}], cnt=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                          OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * {@code head 10 from 1000}: PPL emits {@code Sort(collation, offset=1000, fetch=10)}. The
     * coordinator skips 1000 then takes 10, so its merged stream must contain the global top-1010
     * — i.e. {@code coordLimit = offset + fetch}. Shard fetch is then the usual oversampling
     * formula applied to that coord limit: {@code ceil(coordLimit * factor) + coordLimit}.
     *
     * <p>Expected shard fetch: {@code ceil(1010*2) + 1010 = 3030}. Without offset handling the
     * rewriter used {@code coordLimit = fetch = 10}, so each shard shipped 30 rows, the
     * coordinator skipped 1000, and the result was empty.
     */
    public void testRewrite_sortWithOffset_shardFetchHonorsOffset() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(countStarCall()));
        RelNode sort = LogicalSort.create(
            agg,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)),
            rexBuilder.makeLiteral(1000, typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(sort, contextWithOversampling(2.0));
        String plan = RelOptUtil.toString(result);
        assertTrue(
            "shard Sort must be sized as ceil((offset+fetch)*factor) + (offset+fetch) = 3030, got plan:\n" + plan,
            plan.contains("fetch=[3030]")
        );
        // Pre-fix bug: shard Sort sized off bare fetch (30) — coordinator's skip-1000 would have emptied the result.
        assertFalse("shard Sort must NOT be sized off fetch alone (30)", plan.contains("fetch=[30]"));
    }

    /**
     * factor=1.0 with offset: oversampling collapses to {@code 2 * coordLimit}. For
     * {@code head 10 from 1000} → 2*1010 = 2020. coordLimit = offset+fetch is still honored;
     * factor=1 just removes the per-coord-row padding multiplier.
     */
    public void testRewrite_factorOne_twiceCoordLimit() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(countStarCall()));
        RelNode sort = LogicalSort.create(
            agg,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)),
            rexBuilder.makeLiteral(1000, typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(sort, contextWithOversampling(1.0));
        String plan = RelOptUtil.toString(result);
        assertTrue("factor=1 → shard fetch = 2 * (offset+fetch) = 2020, got plan:\n" + plan, plan.contains("fetch=[2020]"));
    }

    /**
     * Offset close to {@code Integer.MAX_VALUE} pushes shardSize past int range — rewriter must
     * bail (no per-partition Sort inserted). Without the bail an int overflow would produce a
     * negative or wrong fetch literal.
     */
    public void testRewrite_offsetOverflow_bails() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(countStarCall()));
        RelNode sort = LogicalSort.create(
            agg,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)),
            rexBuilder.makeLiteral(Integer.MAX_VALUE - 5, typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(sort, contextWithOversampling(2.0));
        String plan = RelOptUtil.toString(result);
        long sortCount = plan.lines().filter(l -> l.contains("OpenSearchSort")).count();
        assertEquals("overflow → no per-partition Sort inserted", 1, sortCount);
    }

    /**
     * Non-literal offset (parameterized expression): rewriter bails to no-pushdown rather than
     * guessing a value. Each shard ships everything; correct but slow. PPL emits literal offsets
     * in practice, so this is a defensive check.
     */
    public void testRewrite_nonLiteralOffset_bails() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(countStarCall()));
        RexNode lit5 = rexBuilder.makeLiteral(5, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode lit10 = rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode nonLiteralOffset = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, lit5, lit10);
        RelNode sort = LogicalSort.create(
            agg,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)),
            nonLiteralOffset,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(sort, contextWithOversampling(2.0));
        String plan = RelOptUtil.toString(result);
        long sortCount = plan.lines().filter(l -> l.contains("OpenSearchSort")).count();
        assertEquals("non-literal offset → no per-partition Sort inserted", 1, sortCount);
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private RelNode buildSortHeadOverGroupedCount() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(countStarCall()));
        return LogicalSort.create(
            agg,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)),
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
    }

    private RelNode buildBareLimitOverGroupedCount() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(countStarCall()));
        // Bare LIMIT: empty collation, just fetch
        return LogicalSort.create(
            agg,
            RelCollations.EMPTY,
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
    }

    private PlannerContext contextWithOversampling(double factor) {
        PlannerContext ctx = buildContext("parquet", 2, intFields());
        ctx.setOversamplingFactor(factor);
        return ctx;
    }
}
