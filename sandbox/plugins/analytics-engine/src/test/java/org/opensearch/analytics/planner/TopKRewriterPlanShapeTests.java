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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.common.settings.Settings;

import java.util.List;

import static org.mockito.Mockito.when;

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
        // Shard Sort fetch = ceil(innerFetch * factor) + innerFetch = ceil(10*2)+10 = 30.
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
        Settings clusterSettings = Settings.builder().put("analytics.shard_bucket_oversampling_factor", factor).build();
        when(ctx.getClusterState().metadata().settings()).thenReturn(clusterSettings);
        return ctx;
    }
}
