/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.EngineCapability;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Plan-shape tests for window functions, expressed as {@code RexOver} inside a
 * {@link LogicalProject} (the shape PPL {@code eventstats} / {@code appendcol} emit).
 *
 * <p>The planner detects RexOver inside {@code OpenSearchProjectRule}, narrows viable
 * backends by {@link org.opensearch.analytics.spi.WindowCapability}, and applies a cost
 * gate that requires {@code COORDINATOR+SINGLETON} input on the project when any
 * expression is a RexOver.
 */
public class WindowPlanShapeTests extends PlanShapeTestBase {

    /**
     * 1-shard with empty OVER(). Scan emits SHARD+SINGLETON which satisfies the Project's
     * RexOver cost gate (type=SINGLETON) AND the root's locality-agnostic SINGLETON demand,
     * so no ER is inserted. The whole pipeline runs on the single shard's node.
     */
    public void testCountOverEmpty_1shard() {
        RelNode plan = projectWithCountOverEmpty();
        assertPlanShape("""
            LogicalProject(status=[$0], size=[$1], cnt=[COUNT() OVER ()])
              StubTableScan(table=[[test_index]])
            """, plan);
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape("""
            OpenSearchProject(status=[$0], size=[$1], cnt=[COUNT() OVER ()], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    public void testCountOverEmpty_2shard() {
        RelNode plan = projectWithCountOverEmpty();
        assertPlanShape("""
            LogicalProject(status=[$0], size=[$1], cnt=[COUNT() OVER ()])
              StubTableScan(table=[[test_index]])
            """, plan);
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], cnt=[COUNT() OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Two aggregates with a global window between them (PPL: {@code stats count() by k | eval
     * w = sum(..) over () | stats ...}). The split rule must treat each aggregate independently:
     * the LOWER aggregate scans partitioned shard data → splits PARTIAL/FINAL; the UPPER aggregate
     * sits above the window's gather → stays SINGLE (childGatheredByWindow stops the walk before it
     * would reach the lower split's exchange). Proves the window-skip is per-aggregate, not global.
     */
    public void testAggregateWindowAggregate_2shard_onlyLowerSplits() {
        RelNode lowerAgg = makeAggregate(stubScan(mockTable("test_index", "status", "size")), countStarCall());
        RelNode windowed = projectWithSumOverEmpty(lowerAgg);
        // Outer agg consumes BOTH the passthrough cnt ($1, BIGINT) and the window col s ($2) so
        // neither is dead — keeps cnt in the Project output and keeps the window live.
        AggregateCallOnWindowedProject cntAgg = aggCallOver(windowed, /* cnt */ 1, "total_cnt", SqlTypeName.BIGINT);
        AggregateCallOnWindowedProject sAgg = aggCallOver(windowed, /* window col */ 2, "total_s", SqlTypeName.BIGINT);
        RelNode plan = makeAggregate(windowed, countStarCall(windowed), cntAgg.aggCall, sAgg.aggCall);
        RelNode result = runPlanner(plan, multiShardContext());
        // Upper aggregate stays SINGLE (its input is gathered by the window); lower aggregate splits
        // PARTIAL/FINAL over the partitioned scan. The window's SINGLETON demand is already met by the
        // lower FINAL's coordinator output, so no extra exchange is inserted below the window.
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], cnt=[COUNT()], total_cnt=[SUM($1)], total_s=[SUM($2)], mode=[SINGLE], viableBackends=[[mock-parquet]])
                  OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                    OpenSearchAggregate(group=[{0}], cnt=[SUM($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                      OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                        OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                          OpenSearchProject(status=[$0], viableBackends=[[mock-parquet]])
                            OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Window, then an intermediate {@code where}, then {@code stats} (PPL:
     * {@code eventstats sum(x) as w | where w > 0 | stats count() by k}). The Filter between the
     * window Project and the aggregate forces the {@code childGatheredByWindow} walk to take a
     * {@code getInput(0)} hop — which in Volcano is a RelSubset, not a concrete rel. The walk must
     * resolve the subset (getBestOrOriginal) to still find the window below the Filter; otherwise the
     * aggregate would split PARTIAL/FINAL redundantly over already-gathered rows. Asserts the
     * aggregate stays SINGLE.
     */
    public void testAggregateAfterWindowBehindFilter_2shard_staysSingle() {
        RelNode windowed = projectWithSumOverEmpty();
        // where s > 0 — s is the window output at column index 2.
        RelNode filter = makeFilter(windowed, makeEquals(2, SqlTypeName.BIGINT, 0));
        // Aggregate consumes size ($1) so it stays in the Project output (else trimmed); the filter
        // already keeps the window col s live.
        AggregateCallOnWindowedProject sizeAgg = aggCallOver(filter, /* size */ 1, "total_size");
        RelNode plan = makeAggregate(filter, countStarCall(filter), sizeAgg.aggCall);
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], cnt=[COUNT()], total_size=[SUM($1)], mode=[SINGLE], viableBackends=[[mock-parquet]])
                  OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=0, backends=[mock-parquet], =($2, 0))], viableBackends=[[mock-parquet]])
                    OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                      OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testSumOverEmpty_2shard() {
        RelNode plan = projectWithSumOverEmpty();
        assertPlanShape("""
            LogicalProject(status=[$0], size=[$1], s=[SUM($1) OVER ()])
              StubTableScan(table=[[test_index]])
            """, plan);
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Window after a shard-side Filter (multi-shard). Filter is single-input passthrough,
     * stays at SHARD; the RexOver Project's cost gate forces COORDINATOR input, so an ER
     * sits between Filter and Project.
     */
    public void testSumOverEmpty_afterFilter_2shard() {
        RelNode plan = projectWithSumOverEmpty(
            makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200))
        );
        assertPlanShape("""
            LogicalProject(status=[$0], size=[$1], s=[SUM($1) OVER ()])
              LogicalFilter(condition=[=($0, 200)])
                StubTableScan(table=[[test_index]])
            """, plan);
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=0, backends=[mock-lucene, mock-parquet], =($0, 200))], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Window after a Filter on a 1-shard input. Filter propagates Scan's SHARD+SINGLETON
     * upward, satisfying the Project's RexOver cost gate without an ER. Pipeline stays
     * on the single shard's node.
     */
    public void testSumOverEmpty_afterFilter_1shard() {
        RelNode plan = projectWithSumOverEmpty(
            makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200))
        );
        assertPlanShape("""
            LogicalProject(status=[$0], size=[$1], s=[SUM($1) OVER ()])
              LogicalFilter(condition=[=($0, 200)])
                StubTableScan(table=[[test_index]])
            """, plan);
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=0, backends=[mock-lucene, mock-parquet], =($0, 200))], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Window after a collated Sort (multi-shard). Sort already gathers to SINGLETON via
     * SortSplitRule, so the Project's RexOver cost gate is satisfied without a second ER —
     * Sort's output IS the SINGLETON the Project demands.
     */
    public void testSumOverEmpty_afterSort_2shard() {
        RelNode plan = projectWithSumOverEmpty(makeSort(stubScan(mockTable("test_index", "status", "size")), -1));
        assertPlanShape("""
            LogicalProject(status=[$0], size=[$1], s=[SUM($1) OVER ()])
              LogicalSort(sort0=[$0], dir0=[ASC])
                StubTableScan(table=[[test_index]])
            """, plan);
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchSort(sort0=[$0], dir0=[ASC], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Two RexOver expressions in the same Project — both empty OVER() — share one ER.
     * Verifies the rule emits exactly one OpenSearchProject wrapping both windows and
     * does not duplicate the ER per RexOver.
     */
    public void testMultipleRexOver_2shard() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RexBuilder rb = scan.getCluster().getRexBuilder();

        RexNode countOver = makeOver(
            rb,
            scan,
            SqlStdOperatorTable.COUNT,
            List.of(),
            SqlTypeName.BIGINT,
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING
        );
        RexNode sumOver = makeOver(
            rb,
            scan,
            SqlStdOperatorTable.SUM,
            List.of(rb.makeInputRef(scan, 1)),
            SqlTypeName.BIGINT,
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING
        );

        RelNode plan = LogicalProject.create(
            scan,
            List.of(),
            List.of(rb.makeInputRef(scan, 0), rb.makeInputRef(scan, 1), countOver, sumOver),
            List.of("status", "size", "cnt", "s")
        );
        assertPlanShape("""
            LogicalProject(status=[$0], size=[$1], cnt=[COUNT() OVER ()], s=[SUM($1) OVER ()])
              StubTableScan(table=[[test_index]])
            """, plan);
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], cnt=[COUNT() OVER ()], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Non-default frame: SUM(x) OVER (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) —
     * a running aggregate. The frame bounds change the OVER() rendering but not the plan
     * shape (still ER under Project, same backend narrowing). Today no PPL command on this
     * route emits this frame ({@code streamstats} isn't wired here), but the planner must
     * still handle it correctly because Calcite can construct it directly.
     */
    public void testSumOverRunningFrame_2shard() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RexBuilder rb = scan.getCluster().getRexBuilder();
        RexNode runningSum = makeOver(
            rb,
            scan,
            SqlStdOperatorTable.SUM,
            List.of(rb.makeInputRef(scan, 1)),
            SqlTypeName.BIGINT,
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW
        );

        RelNode plan = LogicalProject.create(
            scan,
            List.of(),
            List.of(rb.makeInputRef(scan, 0), rb.makeInputRef(scan, 1), runningSum),
            List.of("status", "size", "running_s")
        );
        assertPlanShape("""
            LogicalProject(status=[$0], size=[$1], running_s=[SUM($1) OVER (ROWS UNBOUNDED PRECEDING)])
              StubTableScan(table=[[test_index]])
            """, plan);
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], running_s=[SUM($1) OVER (ROWS UNBOUNDED PRECEDING)], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Window after an Aggregate: {@code Project(group, total, SUM(total) OVER ())} over
     * {@code Aggregate(group=[$0], total=SUM($1))} (multi-shard). The aggregate splits into
     * PARTIAL + ER + FINAL. FINAL emits {@code COORDINATOR+SINGLETON}, which the windowed
     * Project's cost gate accepts without a second ER. Mirrors the PPL pattern
     * {@code stats sum(size) by status | eventstats sum(total)}.
     */
    public void testSumOverAfterAggregate_2shard() {
        LogicalAggregate agg = makeAggregate(stubScan(mockTable("test_index", "status", "size")), sumCall());
        RexBuilder rb = agg.getCluster().getRexBuilder();
        RexNode over = makeOver(
            rb,
            agg,
            SqlStdOperatorTable.SUM,
            List.of(rb.makeInputRef(agg, 1)),
            SqlTypeName.BIGINT,
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING
        );
        RelNode plan = LogicalProject.create(
            agg,
            List.of(),
            List.of(rb.makeInputRef(agg, 0), rb.makeInputRef(agg, 1), over),
            List.of("status", "total_size", "grand_total")
        );
        assertPlanShape("""
            LogicalProject(status=[$0], total_size=[$1], grand_total=[SUM($1) OVER ()])
              LogicalAggregate(group=[{0}], total_size=[SUM($1)])
                StubTableScan(table=[[test_index]])
            """, plan);
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], total_size=[$1], grand_total=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Sort placed after a windowed Project: {@code Sort(Project(scan, SUM($1) OVER ()))}.
     * The Project's RexOver cost gate forces an ER below the Project; Sort is collated and
     * doesn't add a second gather because its input is already SINGLETON.
     */
    public void testWindowThenSort_2shard() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RexBuilder rb = scan.getCluster().getRexBuilder();
        RexNode sumOver = makeOver(
            rb,
            scan,
            SqlStdOperatorTable.SUM,
            List.of(rb.makeInputRef(scan, 1)),
            SqlTypeName.BIGINT,
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING
        );
        RelNode project = LogicalProject.create(
            scan,
            List.of(),
            List.of(rb.makeInputRef(scan, 0), rb.makeInputRef(scan, 1), sumOver),
            List.of("status", "size", "s")
        );
        RelNode plan = LogicalSort.create(
            project,
            RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING)),
            null,
            null
        );
        assertPlanShape("""
            LogicalSort(sort0=[$0], dir0=[ASC])
              LogicalProject(status=[$0], size=[$1], s=[SUM($1) OVER ()])
                StubTableScan(table=[[test_index]])
            """, plan);
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchSort(sort0=[$0], dir0=[ASC], viableBackends=[[mock-parquet]])
                  OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Window after a Union: {@code Project(SUM(col) OVER ())} over a Union of two scans.
     * Each Union arm gathers to COORDINATOR via the Union split rule; the Union itself is at
     * COORDINATOR+SINGLETON, which satisfies the windowed Project's cost gate without an
     * extra ER. Mirrors PPL {@code source=t | stats count() as c | append [ source=t | stats count() ] | eventstats sum(c)}.
     */
    public void testWindowAfterUnion_2shard() {
        RelNode leftScan = stubScan(mockTable("test_index", "status", "size"));
        RelNode rightScan = stubScan(mockTable("test_index", "status", "size"));
        RelNode union = LogicalUnion.create(List.of(leftScan, rightScan), /* all */ true);
        RexBuilder rb = union.getCluster().getRexBuilder();
        RexNode sumOver = makeOver(
            rb,
            union,
            SqlStdOperatorTable.SUM,
            List.of(rb.makeInputRef(union, 1)),
            SqlTypeName.BIGINT,
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING
        );
        RelNode plan = LogicalProject.create(
            union,
            List.of(),
            List.of(rb.makeInputRef(union, 0), rb.makeInputRef(union, 1), sumOver),
            List.of("status", "size", "s")
        );
        assertPlanShape("""
            LogicalProject(status=[$0], size=[$1], s=[SUM($1) OVER ()])
              LogicalUnion(all=[true])
                StubTableScan(table=[[test_index]])
                StubTableScan(table=[[test_index]])
            """, plan);
        RelNode result = runPlanner(plan, unionContext("test_index", 2));
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchUnion(all=[true], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * HAVING-like Filter sits ABOVE a windowed Project (1-shard). The Project's cost gate is
     * satisfied by Scan's SHARD+SINGLETON, so no ER. Filter's input is already SINGLETON, so
     * no ER above either. Filter on the windowed column ($2) is derived — viable backends
     * narrow to {@code mock-parquet} only.
     */
    public void testHavingAfterWindow_1shard() {
        RelNode plan = makeFilter(projectWithSumOverEmpty(), makeEquals(2, SqlTypeName.BIGINT, 100L));
        assertPlanShape("""
            LogicalFilter(condition=[=($2, 100)])
              LogicalProject(status=[$0], size=[$1], s=[SUM($1) OVER ()])
                StubTableScan(table=[[test_index]])
            """, plan);
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape("""
            OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=0, backends=[mock-parquet], =($2, 100))], viableBackends=[[mock-parquet]])
              OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    /**
     * HAVING-like Filter above a windowed Project (2-shard). The Project's RexOver gate forces
     * ER below Project; Filter's input is the windowed Project (COORDINATOR+SINGLETON) and
     * needs no ER.
     */
    public void testHavingAfterWindow_2shard() {
        RelNode plan = makeFilter(projectWithSumOverEmpty(), makeEquals(2, SqlTypeName.BIGINT, 100L));
        assertPlanShape("""
            LogicalFilter(condition=[=($2, 100)])
              LogicalProject(status=[$0], size=[$1], s=[SUM($1) OVER ()])
                StubTableScan(table=[[test_index]])
            """, plan);
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=0, backends=[mock-parquet], =($2, 100))], viableBackends=[[mock-parquet]])
                  OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * GROUP BY + HAVING + window: {@code Project(SUM($1) OVER (), Filter(=$1,100, Aggregate))}.
     * 1-shard: aggregate stays SINGLE (no split), Filter and Project layer above without ER.
     */
    public void testWindowAfterAggregateWithHaving_1shard() {
        LogicalAggregate agg = makeAggregate(stubScan(mockTable("test_index", "status", "size")), sumCall());
        RelNode filter = makeFilter(agg, makeEquals(1, SqlTypeName.INTEGER, 100));
        RexBuilder rb = filter.getCluster().getRexBuilder();
        RexNode over = makeOver(
            rb,
            filter,
            SqlStdOperatorTable.SUM,
            List.of(rb.makeInputRef(filter, 1)),
            SqlTypeName.BIGINT,
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING
        );
        RelNode plan = LogicalProject.create(
            filter,
            List.of(),
            List.of(rb.makeInputRef(filter, 0), rb.makeInputRef(filter, 1), over),
            List.of("status", "total_size", "grand_total")
        );
        assertPlanShape("""
            LogicalProject(status=[$0], total_size=[$1], grand_total=[SUM($1) OVER ()])
              LogicalFilter(condition=[=($1, 100)])
                LogicalAggregate(group=[{0}], total_size=[SUM($1)])
                  StubTableScan(table=[[test_index]])
            """, plan);
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape("""
            OpenSearchProject(status=[$0], total_size=[$1], grand_total=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
              OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=1, backends=[mock-parquet], =($1, 100))], viableBackends=[[mock-parquet]])
                OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], mode=[SINGLE], viableBackends=[[mock-parquet]])
                  OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    /**
     * GROUP BY + HAVING + window (2-shard). Aggregate splits to PARTIAL+FINAL with one ER
     * between. Filter sits above FINAL (already COORDINATOR+SINGLETON, no ER); windowed
     * Project on top also needs no additional ER. Single ER total for the whole pipeline.
     */
    public void testWindowAfterAggregateWithHaving_2shard() {
        LogicalAggregate agg = makeAggregate(stubScan(mockTable("test_index", "status", "size")), sumCall());
        RelNode filter = makeFilter(agg, makeEquals(1, SqlTypeName.INTEGER, 100));
        RexBuilder rb = filter.getCluster().getRexBuilder();
        RexNode over = makeOver(
            rb,
            filter,
            SqlStdOperatorTable.SUM,
            List.of(rb.makeInputRef(filter, 1)),
            SqlTypeName.BIGINT,
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING
        );
        RelNode plan = LogicalProject.create(
            filter,
            List.of(),
            List.of(rb.makeInputRef(filter, 0), rb.makeInputRef(filter, 1), over),
            List.of("status", "total_size", "grand_total")
        );
        assertPlanShape("""
            LogicalProject(status=[$0], total_size=[$1], grand_total=[SUM($1) OVER ()])
              LogicalFilter(condition=[=($1, 100)])
                LogicalAggregate(group=[{0}], total_size=[SUM($1)])
                  StubTableScan(table=[[test_index]])
            """, plan);
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], total_size=[$1], grand_total=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=1, backends=[mock-parquet], =($1, 100))], viableBackends=[[mock-parquet]])
                    OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                      OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                        OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                          OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Window above a Join (1-shard self-join). Co-location keeps Join on the shard with no
     * per-arm ER, but unlike a windowed Project directly above a Scan, the Project's cost
     * gate forces an ER above the Join. Documented here so future planner work — making Join
     * propagate SHARD+SINGLETON the same way Scan does — has a regression target.
     */
    public void testWindowAfterJoin_1shard() {
        RelNode join = makeSelfJoin();
        RexBuilder rb = join.getCluster().getRexBuilder();
        RexNode over = makeOver(
            rb,
            join,
            SqlStdOperatorTable.SUM,
            List.of(rb.makeInputRef(join, 1)),
            SqlTypeName.BIGINT,
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING
        );
        RelNode plan = LogicalProject.create(
            join,
            List.of(),
            List.of(rb.makeInputRef(join, 0), rb.makeInputRef(join, 1), over),
            List.of("status", "size", "s")
        );
        assertPlanShape("""
            LogicalProject(status=[$0], size=[$1], s=[SUM($1) OVER ()])
              LogicalJoin(condition=[=($0, $2)], joinType=[inner])
                StubTableScan(table=[[test_index]])
                StubTableScan(table=[[test_index]])
            """, plan);
        RelNode result = runPlanner(plan, perIndexContext(Map.of("test_index", 1)));
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                      OpenSearchProject(status=[$0], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Window above a Join (2-shard different tables). Each Join arm needs SINGLETON, so per-arm
     * ER. Join emits COORDINATOR+SINGLETON which satisfies the windowed Project's cost gate —
     * no second ER above Join.
     */
    public void testWindowAfterJoin_2shard() {
        RelNode join = makeDifferentTablesJoin();
        RexBuilder rb = join.getCluster().getRexBuilder();
        RexNode over = makeOver(
            rb,
            join,
            SqlStdOperatorTable.SUM,
            List.of(rb.makeInputRef(join, 1)),
            SqlTypeName.BIGINT,
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING
        );
        RelNode plan = LogicalProject.create(
            join,
            List.of(),
            List.of(rb.makeInputRef(join, 0), rb.makeInputRef(join, 1), over),
            List.of("status", "size", "s")
        );
        assertPlanShape("""
            LogicalProject(status=[$0], size=[$1], s=[SUM($1) OVER ()])
              LogicalJoin(condition=[=($0, $2)], joinType=[inner])
                StubTableScan(table=[[left_idx]])
                StubTableScan(table=[[right_idx]])
            """, plan);
        RelNode result = runPlanner(plan, perIndexContext(Map.of("left_idx", 2, "right_idx", 2)));
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[left_idx]], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchProject(status=[$0], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[right_idx]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Aggregate stacked on top of a windowed Project (1-shard). The Project's cost gate accepts
     * SHARD+SINGLETON Scan; the follow-on Aggregate stays SINGLE (input is already SINGLETON).
     * Verifies an Aggregate atop a window doesn't re-split into PARTIAL+FINAL.
     */
    public void testAggregateAfterWindow_1shard() {
        RelNode windowedProject = projectWithSumOverEmpty();
        // Aggregate BOTH size ($1) and the window col s ($2) so neither is dead — keeps size in the
        // Project output and keeps the window live (else field trimming drops the dead window/col).
        AggregateCallOnWindowedProject sizeAgg = aggCallOver(windowedProject, /* size */ 1, "total_size");
        AggregateCallOnWindowedProject sAgg = aggCallOver(windowedProject, /* window col */ 2, "total_s", SqlTypeName.BIGINT);
        RelNode plan = makeAggregate(windowedProject, sizeAgg.aggCall, sAgg.aggCall);
        assertPlanShape("""
            LogicalAggregate(group=[{0}], total_size=[SUM($1)], total_s=[SUM($2)])
              LogicalProject(status=[$0], size=[$1], s=[SUM($1) OVER ()])
                StubTableScan(table=[[test_index]])
            """, plan);
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape("""
            OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], total_s=[SUM($2)], mode=[SINGLE], viableBackends=[[mock-parquet]])
              OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    /**
     * Aggregate stacked on top of a windowed Project (2-shard). The Project's cost gate forces
     * an ER below the Project; output is COORDINATOR+SINGLETON. The Aggregate sees an
     * already-gathered input and stays SINGLE — split rule skips because the child is
     * already SINGLETON, so we get one ER for the whole pipeline instead of a redundant
     * PARTIAL+FINAL pair.
     */
    public void testAggregateAfterWindow_2shard() {
        RelNode windowedProject = projectWithSumOverEmpty();
        // Aggregate BOTH size ($1) and window col s ($2) so neither is dead (keeps window live + size).
        AggregateCallOnWindowedProject sizeAgg = aggCallOver(windowedProject, /* size */ 1, "total_size");
        AggregateCallOnWindowedProject sAgg = aggCallOver(windowedProject, /* window col */ 2, "total_s", SqlTypeName.BIGINT);
        RelNode plan = makeAggregate(windowedProject, sizeAgg.aggCall, sAgg.aggCall);
        assertPlanShape("""
            LogicalAggregate(group=[{0}], total_size=[SUM($1)], total_s=[SUM($2)])
              LogicalProject(status=[$0], size=[$1], s=[SUM($1) OVER ()])
                StubTableScan(table=[[test_index]])
            """, plan);
        RelNode result = runPlanner(plan, multiShardContext());
        // The window's global frame (SUM OVER ()) forces a gather (ER) directly below the Project, so
        // the aggregate's input is already on one node. The split rule detects the window-induced
        // gather (childGatheredByWindow) and keeps the aggregate SINGLE — no redundant PARTIAL/FINAL
        // pass over already-gathered rows.
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], total_size=[SUM($1)], total_s=[SUM($2)], mode=[SINGLE], viableBackends=[[mock-parquet]])
                  OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * {@code SUM($1) OVER (PARTITION BY $0)} on a 2-shard scan. PARTITION BY is now accepted —
     * the cost gate on {@link org.opensearch.analytics.planner.rel.OpenSearchProject} forces
     * SINGLETON input on any RexOver-bearing project, so all rows in a partition arrive on the
     * coordinator regardless of whether partition keys span shards. WindowAggExec on the
     * coordinator then groups by the partition key and computes the window correctly.
     *
     * <p>The plan shape is identical to the empty-OVER case (gather then project) — partition
     * keys are encoded inside the RexOver expression and don't affect the surrounding
     * exchange shape.
     */
    public void testRexOverPartitionBy_2shard() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RexBuilder rb = scan.getCluster().getRexBuilder();

        RexNode over = rb.makeOver(
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            (SqlAggFunction) SqlStdOperatorTable.SUM,
            List.of(rb.makeInputRef(scan, 1)),
            ImmutableList.of(rb.makeInputRef(scan, 0)),    // PARTITION BY status
            ImmutableList.of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING,
            true,
            true,
            false,
            false,
            false
        );
        RelNode plan = LogicalProject.create(
            scan,
            List.of(),
            List.of(rb.makeInputRef(scan, 0), rb.makeInputRef(scan, 1), over),
            List.of("status", "size", "s")
        );
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER (PARTITION BY $0)], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * {@code ROW_NUMBER() OVER ()} on a 2-shard scan. PPL's {@code streamstats … by …} lowers
     * to a {@code projectPlus} of {@code ROW_NUMBER() OVER (ROWS UNBOUNDED PRECEDING TO
     * CURRENT ROW)} as a helper sequence column. ROW_NUMBER joined the WindowFunction enum in
     * the same commit that relaxed PARTITION BY — pinning the cost-gate-then-WindowAggExec
     * shape here keeps both changes regression-safe.
     */
    public void testRowNumberOverEmpty_2shard() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RexBuilder rb = scan.getCluster().getRexBuilder();
        RexNode rowNumber = makeOver(
            rb,
            scan,
            (SqlAggFunction) SqlStdOperatorTable.ROW_NUMBER,
            List.of(),
            SqlTypeName.BIGINT,
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW
        );
        RelNode plan = LogicalProject.create(
            scan,
            List.of(),
            List.of(rb.makeInputRef(scan, 0), rb.makeInputRef(scan, 1), rowNumber),
            List.of("status", "size", "rn")
        );
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], rn=[ROW_NUMBER() OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * {@code SUM($1) OVER (PARTITION BY $0 ORDER BY $1)} on a 2-shard scan. ORDER BY is a
     * separate axis from PARTITION BY in {@code RexOver} — the relaxation in
     * {@code OpenSearchProjectRule.collectWindowFunctions} drops the partition-key check but
     * leaves order-by passthrough; this case pins both axes simultaneously.
     *
     * <p>SQL semantics: with ORDER BY but no explicit frame, the implicit frame is RANGE
     * UNBOUNDED PRECEDING TO CURRENT ROW (cumulative). DataFusion's WindowAggExec handles it
     * the same way once SINGLETON-gathered.
     */
    public void testSumOverPartitionByOrderBy_2shard() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RexBuilder rb = scan.getCluster().getRexBuilder();
        RexNode over = rb.makeOver(
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            (SqlAggFunction) SqlStdOperatorTable.SUM,
            List.of(rb.makeInputRef(scan, 1)),
            ImmutableList.of(rb.makeInputRef(scan, 0)),     // PARTITION BY status
            ImmutableList.of(new RexFieldCollation(rb.makeInputRef(scan, 1), Set.of())),  // ORDER BY size
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            false,                                          // rangeBased: range-by-default-when-ORDER-BY
            true,
            false,
            false,
            false
        );
        RelNode plan = LogicalProject.create(
            scan,
            List.of(),
            List.of(rb.makeInputRef(scan, 0), rb.makeInputRef(scan, 1), over),
            List.of("status", "size", "running_sum")
        );
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], running_sum=[SUM($1) OVER (PARTITION BY $0 ORDER BY $1)], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * {@code ROW_NUMBER() OVER (PARTITION BY $0 ORDER BY $1)} on a 2-shard scan. This is the
     * shape PPL's {@code streamstats … by str0 | sort key} produces: a per-partition row
     * sequence pinned by an ORDER BY. The existing tests cover ROW_NUMBER OVER () alone and
     * SUM OVER (PARTITION BY ORDER BY) alone — this one combines them and is the one that
     * matters for streamstats … by … reachability.
     */
    public void testRowNumberOverPartitionByOrderBy_2shard() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RexBuilder rb = scan.getCluster().getRexBuilder();
        RexNode rowNumber = rb.makeOver(
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            (SqlAggFunction) SqlStdOperatorTable.ROW_NUMBER,
            List.of(),
            ImmutableList.of(rb.makeInputRef(scan, 0)),      // PARTITION BY status
            ImmutableList.of(new RexFieldCollation(rb.makeInputRef(scan, 1), Set.of())),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true,                                            // rowBased
            true,
            false,
            false,
            false
        );
        RelNode plan = LogicalProject.create(
            scan,
            List.of(),
            List.of(rb.makeInputRef(scan, 0), rb.makeInputRef(scan, 1), rowNumber),
            List.of("status", "size", "rn_per_status")
        );
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], rn_per_status=[ROW_NUMBER() OVER (PARTITION BY $0 ORDER BY $1)], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * {@code RANK() OVER (PARTITION BY $0 ORDER BY $1)} on a 2-shard scan. RANK joined the
     * WindowFunction enum alongside DENSE_RANK; like ROW_NUMBER it needs no adapter and passes
     * through to DataFusion's native {@code rank()}. Pinning the cost-gate → SINGLETON-gather
     * shape here keeps the wiring regression-safe and confirms the backend stays viable.
     */
    public void testRankOverPartitionByOrderBy_2shard() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RexBuilder rb = scan.getCluster().getRexBuilder();
        RexNode rank = rb.makeOver(
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            (SqlAggFunction) SqlStdOperatorTable.RANK,
            List.of(),
            ImmutableList.of(rb.makeInputRef(scan, 0)),      // PARTITION BY status
            ImmutableList.of(new RexFieldCollation(rb.makeInputRef(scan, 1), Set.of())),  // ORDER BY size
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true,
            true,
            false,
            false,
            false
        );
        RelNode plan = LogicalProject.create(
            scan,
            List.of(),
            List.of(rb.makeInputRef(scan, 0), rb.makeInputRef(scan, 1), rank),
            List.of("status", "size", "rank_per_status")
        );
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], rank_per_status=[RANK() OVER (PARTITION BY $0 ORDER BY $1)], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * {@code DENSE_RANK() OVER (PARTITION BY $0 ORDER BY $1)} on a 2-shard scan. The companion to
     * {@link #testRankOverPartitionByOrderBy_2shard} — confirms DENSE_RANK is independently
     * recognized by {@code WindowFunction.resolveFunction} and advertised by the DataFusion backend.
     */
    public void testDenseRankOverPartitionByOrderBy_2shard() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RexBuilder rb = scan.getCluster().getRexBuilder();
        RexNode denseRank = rb.makeOver(
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            (SqlAggFunction) SqlStdOperatorTable.DENSE_RANK,
            List.of(),
            ImmutableList.of(rb.makeInputRef(scan, 0)),      // PARTITION BY status
            ImmutableList.of(new RexFieldCollation(rb.makeInputRef(scan, 1), Set.of())),  // ORDER BY size
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true,
            true,
            false,
            false,
            false
        );
        RelNode plan = LogicalProject.create(
            scan,
            List.of(),
            List.of(rb.makeInputRef(scan, 0), rb.makeInputRef(scan, 1), denseRank),
            List.of("status", "size", "dense_rank_per_status")
        );
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], dense_rank_per_status=[DENSE_RANK() OVER (PARTITION BY $0 ORDER BY $1)], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    // ── Builders ──────────────────────────────────────────────────────────

    private RelNode projectWithCountOverEmpty() {
        return buildProjectWithOver(SqlStdOperatorTable.COUNT, List.of(), "cnt", SqlTypeName.BIGINT);
    }

    private RelNode projectWithSumOverEmpty() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        return projectWithSumOverEmpty(scan);
    }

    /** Same as {@link #projectWithSumOverEmpty()} but stacked on a caller-supplied input
     *  (Filter, Sort, etc.). Input must have at least 2 fields ($0 status, $1 size). */
    private RelNode projectWithSumOverEmpty(RelNode input) {
        RexBuilder rb = input.getCluster().getRexBuilder();
        RexNode over = makeOver(
            rb,
            input,
            SqlStdOperatorTable.SUM,
            List.of(rb.makeInputRef(input, 1)),
            SqlTypeName.BIGINT,
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING
        );
        return LogicalProject.create(
            input,
            List.of(),
            List.of(rb.makeInputRef(input, 0), rb.makeInputRef(input, 1), over),
            List.of("status", "size", "s")
        );
    }

    /** Build a {@code RexOver} with the given function, operands, output type, and frame bounds.
     *  Empty PARTITION BY and empty ORDER BY. */
    private RexNode makeOver(
        RexBuilder rb,
        RelNode scan,
        SqlAggFunction fn,
        List<RexNode> operands,
        SqlTypeName outType,
        RexWindowBound lowerBound,
        RexWindowBound upperBound
    ) {
        return rb.makeOver(
            typeFactory.createSqlType(outType),
            fn,
            operands,
            ImmutableList.of(),
            ImmutableList.of(),
            lowerBound,
            upperBound,
            true,
            true,
            false,
            false,
            false
        );
    }

    /** Self-join on test_index — used by {@link #testWindowAfterJoin_1shard} for the
     *  co-location fast path on 1-shard. */
    private RelNode makeSelfJoin() {
        RelOptTable left = mockTable("test_index", "status", "size");
        RelOptTable right = mockTable("test_index", "status", "size");
        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        return LogicalJoin.create(stubScan(left), stubScan(right), List.of(), cond, java.util.Set.<CorrelationId>of(), JoinRelType.INNER);
    }

    /** Different-tables equi-join — used by {@link #testWindowAfterJoin_2shard} so the
     *  co-location predicate fails and per-arm ER kicks in. */
    private RelNode makeDifferentTablesJoin() {
        RelOptTable left = mockTable("left_idx", "status", "size");
        RelOptTable right = mockTable("right_idx", "status", "size");
        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        return LogicalJoin.create(stubScan(left), stubScan(right), List.of(), cond, java.util.Set.<CorrelationId>of(), JoinRelType.INNER);
    }

    /** Holder for the AggregateCall used by {@link #testAggregateAfterWindow_1shard}/_2shard. */
    private record AggregateCallOnWindowedProject(org.apache.calcite.rel.core.AggregateCall aggCall) {
    }

    private AggregateCallOnWindowedProject aggCallOver(RelNode input, int sumFieldIndex, String alias) {
        return aggCallOver(input, sumFieldIndex, alias, SqlTypeName.INTEGER);
    }

    /** SUM over {@code sumFieldIndex} with an explicit return type — needed when summing a BIGINT
     *  column (e.g. a window output) where the inferred type differs from the INTEGER default. */
    private AggregateCallOnWindowedProject aggCallOver(RelNode input, int sumFieldIndex, String alias, SqlTypeName returnType) {
        org.apache.calcite.rel.core.AggregateCall call = org.apache.calcite.rel.core.AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(sumFieldIndex),
            -1,
            input,
            typeFactory.createSqlType(returnType),
            alias
        );
        return new AggregateCallOnWindowedProject(call);
    }

    /** Multi-shard context whose only DF backend declares {@link EngineCapability#UNION},
     *  so the Union marker rule accepts the Union shape. */
    private PlannerContext unionContext(String indexName, int shardCount) {
        return buildContextPerIndex("parquet", Map.of(indexName, shardCount), intFields(), List.of(new UnionCapableBackend(), LUCENE));
    }

    /** Mock DF backend with EngineCapability.UNION — needed because UNION is opt-in. */
    private static final class UnionCapableBackend extends MockDataFusionBackend {
        @Override
        protected Set<EngineCapability> supportedEngineCapabilities() {
            Set<EngineCapability> caps = new HashSet<>(super.supportedEngineCapabilities());
            caps.add(EngineCapability.UNION);
            return caps;
        }
    }

    /**
     * Build a LogicalProject that passes through every scan column and adds one RexOver
     * (empty OVER clause) as the final projected expression.
     */
    private RelNode buildProjectWithOver(SqlAggFunction fn, List<RexNode> operands, String outName, SqlTypeName outType) {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RexBuilder rb = scan.getCluster().getRexBuilder();

        RexNode over = rb.makeOver(
            typeFactory.createSqlType(outType),
            fn,
            operands,
            ImmutableList.of(),
            ImmutableList.of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.UNBOUNDED_FOLLOWING,
            true,
            true,
            false,
            false,
            false
        );

        return LogicalProject.create(
            scan,
            List.of(),
            List.of(rb.makeInputRef(scan, 0), rb.makeInputRef(scan, 1), over),
            List.of("status", "size", outName)
        );
    }
}
