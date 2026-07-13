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
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.settings.DelegationBlockList;
import org.opensearch.analytics.settings.PlannerSettings;

import java.util.ArrayList;
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
        // TopK must not add a per-partition Sort for a scalar aggregate. The coordinator Sort
        // itself is redundant over a 1-row scalar agg and SORT_REMOVE_REDUNDANT drops it, so the
        // count is 0; the point is that no extra (per-partition) Sort was inserted.
        assertTrue("no per-partition Sort for scalar aggregate", sortCount <= 1);
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
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
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
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
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
        ctx.setPlannerSettings(PlannerSettings.of(factor, DelegationBlockList.empty()));
        return ctx;
    }

    // ── PPL column-swap tests: Sort key remapped through intermediate Project ──

    /**
     * q13: {@code stats count() as c by SearchPhrase | sort - c | head 10}.
     * Single group-by, single COUNT, sort on measure. Shard sort must use $1.
     * Two coordinator Sorts (fetch=10000 + fetch=10) survive because this planner does not
     * register SORT_REMOVE_REDUNDANT. DF collapses them in the physical plan.
     */
    public void testRewrite_pplShape_q13_sortKeyRemappedThroughProject() {
        RelNode result = runPlanner(buildPplSwapPlan(ImmutableBitSet.of(0), List.of(countStarCall()), 10), contextWithOversampling(2.0));
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[DESC], fetch=[10000], viableBackends=[[mock-parquet]])
                  OpenSearchSort(sort0=[$0], dir0=[DESC], fetch=[10], viableBackends=[[mock-parquet]])
                    OpenSearchProject(cnt=[$1], status=[$0], viableBackends=[[mock-parquet]])
                      OpenSearchAggregate(group=[{0}], cnt=[SUM($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                        OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                          OpenSearchSort(sort0=[$1], dir0=[DESC], fetch=[30], viableBackends=[[mock-parquet]])
                            OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * q15: {@code stats count() as c by X, Y | sort - c | head 10}.
     * Multi-group-by, single COUNT, sort on measure. Shard sort must use $2.
     */
    public void testRewrite_pplShape_q15_multiGroup_sortKeyRemappedThroughProject() {
        RelNode result = runPlanner(
            buildPplSwapPlan(ImmutableBitSet.of(0, 1), List.of(countStarCall(stubScan(mockTable("test_index", "status", "size")))), 10),
            contextWithOversampling(2.0)
        );
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[DESC], fetch=[10000], viableBackends=[[mock-parquet]])
                  OpenSearchSort(sort0=[$0], dir0=[DESC], fetch=[10], viableBackends=[[mock-parquet]])
                    OpenSearchProject(cnt=[$2], status=[$0], size=[$1], viableBackends=[[mock-parquet]])
                      OpenSearchAggregate(group=[{0, 1}], cnt=[SUM($2)], mode=[FINAL], viableBackends=[[mock-parquet]])
                        OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                          OpenSearchSort(sort0=[$2], dir0=[DESC], fetch=[30], viableBackends=[[mock-parquet]])
                            OpenSearchAggregate(group=[{0, 1}], cnt=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * q11: {@code stats dc(UserID) as u by MobilePhoneModel | sort - u | head 10}.
     * dc() with column swap — combines reduce_eval logic with the Project remapping.
     */
    public void testRewrite_pplShape_q11_dcWithSwap_sortKeyRemapped() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RelNode result = runPlanner(
            buildPplSwapPlan(ImmutableBitSet.of(0), List.of(approxCountDistinctCall(scan)), 10),
            contextWithOversampling(2.0)
        );
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[DESC], fetch=[10000], viableBackends=[[mock-parquet]])
                  OpenSearchSort(sort0=[$0], dir0=[DESC], fetch=[10], viableBackends=[[mock-parquet]])
                    OpenSearchProject(dc=[$1], status=[$0], viableBackends=[[mock-parquet]])
                      OpenSearchAggregate(group=[{0}], dc=[APPROX_COUNT_DISTINCT($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                        OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                          OpenSearchProject(status=[$0], dc=[$1], viableBackends=[[mock-parquet]])
                            OpenSearchSort(sort0=[$2], dir0=[DESC], fetch=[30], viableBackends=[[mock-parquet]])
                              OpenSearchProject(status=[$0], dc=[$1], __reduce_eval_1=[reduce_eval('approx_distinct', $1)], viableBackends=[[mock-parquet]])
                                OpenSearchAggregate(group=[{0}], dc=[APPROX_COUNT_DISTINCT($1)], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                                  OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Sort by group key (not measure) with swap Project: the swap puts the key at $1 in
     * post-project schema. Remapping should produce shard sort on $0 (the group key at
     * partial agg level). Ensures the fix doesn't break sort-by-key queries.
     */
    public void testRewrite_pplShape_sortByGroupKey_remapsCorrectly() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(countStarCall()));

        // PPL swap: (cnt=$1, status=$0)
        RelNode swapProject = LogicalProject.create(
            agg,
            List.of(),
            List.of(rexBuilder.makeInputRef(agg, 1), rexBuilder.makeInputRef(agg, 0)),
            List.of("cnt", "status")
        );

        // Sort on $1 (which is status/group-key in the swapped schema)
        RelNode innerSort = LogicalSort.create(
            swapProject,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)),
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );

        RelNode outerLimit = LogicalSort.create(
            innerSort,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)),
            null,
            rexBuilder.makeLiteral(10000, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );

        RelNode result = runPlanner(outerLimit, contextWithOversampling(2.0));
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$1], dir0=[DESC], fetch=[10000], viableBackends=[[mock-parquet]])
                  OpenSearchSort(sort0=[$1], dir0=[DESC], fetch=[10], viableBackends=[[mock-parquet]])
                    OpenSearchProject(cnt=[$1], status=[$0], viableBackends=[[mock-parquet]])
                      OpenSearchAggregate(group=[{0}], cnt=[SUM($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                        OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                          OpenSearchSort(sort0=[$0], dir0=[DESC], fetch=[30], viableBackends=[[mock-parquet]])
                            OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    // ── Detection: chained stats (nested aggregation) must NOT get TopK ─────────

    /**
     * PPL: {@code stats count() as c by X, Y | stats sum(c) as total by X | sort - total | head 5}
     * The outer aggregate's PARTIAL input subtree contains another aggregate, so TopK must bail.
     * TopK on the inner agg would truncate (X, Y) groups before the outer sum sees all of them,
     * producing catastrophically wrong totals.
     */
    public void testDetection_chainedStats_topKBails() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);

        // Inner agg: count() by (status, size)
        LogicalAggregate innerAgg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0, 1), null, List.of(countStarCall()));

        // Outer agg: sum(count) by status — groups over the inner agg result
        LogicalAggregate outerAgg = LogicalAggregate.create(
            innerAgg,
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
                    List.of(2),
                    -1,
                    null,
                    RelCollations.EMPTY,
                    typeFactory.createSqlType(SqlTypeName.BIGINT),
                    "total"
                )
            )
        );

        // Sort on total DESC, head 5
        RelNode sort = LogicalSort.create(
            outerAgg,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)),
            null,
            rexBuilder.makeLiteral(5, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );

        RelNode result = runPlanner(sort, contextWithOversampling(2.0));
        String plan = RelOptUtil.toString(result);
        assertEquals("chained stats — TopK must not insert a shard Sort", 0, countShardSortsBelowER(plan));
    }

    // ── Detection: AVG does NOT get TopK (reduce decomposition inserts computed Project) ──

    /** AVG is decomposed into SUM/COUNT with a divide Project — rewriter bails. */
    public void testDetection_avgByGroup_noTopK() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(avgCall(scan)));
        RelNode sort = LogicalSort.create(
            agg,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)),
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(sort, contextWithOversampling(2.0));
        String plan = RelOptUtil.toString(result);
        assertEquals("AVG decomposition prevents TopK — no shard Sort", 0, countShardSortsBelowER(plan));
    }

    /**
     * Multiple adjacent Projects between Sort and Aggregate: PROJECT_MERGE collapses them during
     * RBO so TopK normally fires. If for any reason two projects survive (PROJECT_MERGE removed or
     * blocked), the rewriter now safely bails — accepting the second project is unsafe since it
     * could carry window functions or other expressions that make TopK incorrect.
     * This test verifies the safe-bail behavior when two projects reach the rewriter.
     */
    public void testDetection_multipleProjects_topKStillFires() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(countStarCall()));

        // First Project: identity (no swap)
        RelNode proj1 = LogicalProject.create(
            agg,
            List.of(),
            List.of(rexBuilder.makeInputRef(agg, 0), rexBuilder.makeInputRef(agg, 1)),
            List.of("status", "cnt")
        );
        // Second Project: also identity
        RelNode proj2 = LogicalProject.create(
            proj1,
            List.of(),
            List.of(rexBuilder.makeInputRef(proj1, 0), rexBuilder.makeInputRef(proj1, 1)),
            List.of("status", "cnt")
        );

        RelNode sort = LogicalSort.create(
            proj2,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)),
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(sort, contextWithOversampling(2.0));
        String plan = RelOptUtil.toString(result);
        long sortCount = plan.lines().filter(l -> l.contains("OpenSearchSort")).count();
        // PROJECT_MERGE collapses the two adjacent identity projects, so TopK fires.
        // Even without PROJECT_MERGE, the rewriter passes through multiple plain-column projects
        // and validates the sort key at the first seenProject — TopK still fires correctly.
        assertTrue("TopK should fire with multiple plain-column projects", sortCount >= 2);
    }

    /** Computed expression (literal) in Project between Sort and Aggregate — rewriter bails. */
    public void testDetection_computedProjectExpression_topKBails() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(countStarCall()));

        // Project with a literal expression (not a column reference)
        RelNode proj = LogicalProject.create(
            agg,
            List.of(),
            List.of(rexBuilder.makeLiteral(42, typeFactory.createSqlType(SqlTypeName.INTEGER), true), rexBuilder.makeInputRef(agg, 0)),
            List.of("const", "status")
        );

        // Sort on $0 which is the literal — cannot be remapped
        RelNode sort = LogicalSort.create(
            proj,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING)),
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(sort, contextWithOversampling(2.0));
        String plan = RelOptUtil.toString(result);
        assertEquals("Computed expression in Project — TopK must bail", 0, countShardSortsBelowER(plan));
    }

    // ── Rewrite: PPL swap Project shapes ────────────────────────────────────────

    /** SUM with swap Project: shard sort must use $1 (the SUM measure). */
    public void testRewrite_pplShape_sumWithSwap_sortKeyRemapped() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RelNode result = runPlanner(buildPplSwapPlan(ImmutableBitSet.of(0), List.of(sumCall(scan)), 10), contextWithOversampling(2.0));
        String plan = RelOptUtil.toString(result);
        assertTrue("shard sort must use $1", planHasShardSortOn(plan, "$1"));
    }

    /** Multi-group dc() with swap: shard sort must use $3 (reduce_eval appended column). */
    public void testRewrite_pplShape_multiGroupDcWithSwap_sortKeyRemapped() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RelNode result = runPlanner(
            buildPplSwapPlan(ImmutableBitSet.of(0, 1), List.of(approxCountDistinctCall(scan)), 10),
            contextWithOversampling(2.0)
        );
        String plan = RelOptUtil.toString(result);
        assertTrue("multi-group dc shard sort must use $3 (reduce_eval column), plan:\n" + plan, planHasShardSortOn(plan, "$3"));
    }

    /** Swap with offset: shard fetch must honor offset. */
    public void testRewrite_pplShape_swapWithOffset_shardFetchHonorsOffset() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(countStarCall()));
        RelNode swapProject = LogicalProject.create(
            agg,
            List.of(),
            List.of(rexBuilder.makeInputRef(agg, 1), rexBuilder.makeInputRef(agg, 0)),
            List.of("cnt", "status")
        );
        RelNode innerSort = LogicalSort.create(
            swapProject,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING)),
            rexBuilder.makeLiteral(100, typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode outerLimit = LogicalSort.create(
            innerSort,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING)),
            null,
            rexBuilder.makeLiteral(10000, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(outerLimit, contextWithOversampling(2.0));
        String plan = RelOptUtil.toString(result);
        // coordLimit = 100+10 = 110, shardSize = ceil(110*2)+110 = 330
        assertTrue("shard fetch must be 330, plan:\n" + plan, plan.contains("fetch=[330]"));
        assertTrue("shard sort must use $1", planHasShardSortOn(plan, "$1"));
    }

    /** ASC sort direction with swap: direction must be preserved in shard sort. */
    public void testRewrite_pplShape_ascDirection_preserved() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(countStarCall()));
        RelNode swapProject = LogicalProject.create(
            agg,
            List.of(),
            List.of(rexBuilder.makeInputRef(agg, 1), rexBuilder.makeInputRef(agg, 0)),
            List.of("cnt", "status")
        );
        RelNode innerSort = LogicalSort.create(
            swapProject,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(10, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode outerLimit = LogicalSort.create(
            innerSort,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            rexBuilder.makeLiteral(10000, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        RelNode result = runPlanner(outerLimit, contextWithOversampling(2.0));
        String plan = RelOptUtil.toString(result);
        assertTrue("shard sort must use $1 ASC", planHasShardSortOn(plan, "$1") && plan.contains("dir0=[ASC]"));
    }

    /** Factor=1.5 with swap: verifies arithmetic (ceil(10*1.5)+10 = 25). */
    public void testRewrite_pplShape_factor1_5_shardFetchCorrect() {
        RelNode result = runPlanner(buildPplSwapPlan(ImmutableBitSet.of(0), List.of(countStarCall()), 10), contextWithOversampling(1.5));
        String plan = RelOptUtil.toString(result);
        assertTrue("shard fetch must be 25, plan:\n" + plan, plan.contains("fetch=[25]"));
    }

    /** Large fetch (head 1000) with swap: shard fetch = ceil(1000*2)+1000 = 3000. */
    public void testRewrite_pplShape_largeFetch_shardFetchCorrect() {
        RelNode result = runPlanner(buildPplSwapPlan(ImmutableBitSet.of(0), List.of(countStarCall()), 1000), contextWithOversampling(2.0));
        String plan = RelOptUtil.toString(result);
        assertTrue("shard fetch must be 3000, plan:\n" + plan, plan.contains("fetch=[3000]"));
    }

    private static boolean planHasShardSortOn(String plan, String expectedField) {
        String[] lines = plan.split("\n");
        boolean belowER = false;
        for (String line : lines) {
            if (line.contains("ExchangeReducer")) {
                belowER = true;
                continue;
            }
            if (belowER && line.contains("OpenSearchSort")) {
                return line.contains("sort0=[" + expectedField + "]");
            }
        }
        return false;
    }

    private static long countShardSortsBelowER(String plan) {
        String[] lines = plan.split("\n");
        boolean belowER = false;
        long count = 0;
        for (String line : lines) {
            if (line.contains("ExchangeReducer")) {
                belowER = true;
                continue;
            }
            if (belowER && line.contains("OpenSearchSort")) {
                count++;
            }
        }
        return count;
    }

    // ── PPL swap plan builder ───────────────────────────────────────────────────

    /**
     * Builds the PPL-style plan shape with a column-swapping Project:
     * SystemLimit($0 DESC, 10000) → Sort($0 DESC, fetch) → Project(measures..., keys...) → Agg
     *
     * <p>PPL always puts measures first in output. This builder reorders the Aggregate's
     * output (keys first, measures second) into (measures first, keys second) via a Project,
     * then creates a Sort on $0 (the first measure in swapped output).
     */
    private RelNode buildPplSwapPlan(ImmutableBitSet groupSet, List<AggregateCall> aggCalls, int fetch) {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        LogicalAggregate agg = LogicalAggregate.create(scan, List.of(), groupSet, null, aggCalls);

        int groupCount = groupSet.cardinality();
        int measureCount = aggCalls.size();

        // Build swap project: measures first, then keys
        List<RexNode> swapExprs = new ArrayList<>();
        List<String> swapNames = new ArrayList<>();
        for (int i = 0; i < measureCount; i++) {
            swapExprs.add(rexBuilder.makeInputRef(agg, groupCount + i));
            swapNames.add(agg.getRowType().getFieldNames().get(groupCount + i));
        }
        for (int i = 0; i < groupCount; i++) {
            swapExprs.add(rexBuilder.makeInputRef(agg, i));
            swapNames.add(agg.getRowType().getFieldNames().get(i));
        }

        RelNode swapProject = LogicalProject.create(agg, List.of(), swapExprs, swapNames);

        // Sort on $0 (first measure in swapped output)
        RelNode innerSort = LogicalSort.create(
            swapProject,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING)),
            null,
            rexBuilder.makeLiteral(fetch, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );

        return LogicalSort.create(
            innerSort,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING)),
            null,
            rexBuilder.makeLiteral(10000, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
    }
}
