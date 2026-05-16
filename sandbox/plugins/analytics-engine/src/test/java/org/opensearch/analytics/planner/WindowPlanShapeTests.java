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
                  OpenSearchAggregate(group=[{0}], total_size=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{0}], total_size=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], mode=[PARTIAL], viableBackends=[[mock-parquet]])
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
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], total_size=[$1], grand_total=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                  OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=1, backends=[mock-parquet], =($1, 100))], viableBackends=[[mock-parquet]])
                    OpenSearchAggregate(group=[{0}], total_size=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], mode=[SINGLE], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
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
                    OpenSearchAggregate(group=[{0}], total_size=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                      OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                        OpenSearchAggregate(group=[{0}], total_size=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], mode=[PARTIAL], viableBackends=[[mock-parquet]])
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
        AggregateCallOnWindowedProject helper = aggCallOver(windowedProject, /* sumFieldIndex */ 1, "total_size");
        RelNode plan = makeAggregate(windowedProject, helper.aggCall);
        assertPlanShape("""
            LogicalAggregate(group=[{0}], total_size=[SUM($1)])
              LogicalProject(status=[$0], size=[$1], s=[SUM($1) OVER ()])
                StubTableScan(table=[[test_index]])
            """, plan);
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], total_size=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], mode=[SINGLE], viableBackends=[[mock-parquet]])
                  OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
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
        AggregateCallOnWindowedProject helper = aggCallOver(windowedProject, /* sumFieldIndex */ 1, "total_size");
        RelNode plan = makeAggregate(windowedProject, helper.aggCall);
        assertPlanShape("""
            LogicalAggregate(group=[{0}], total_size=[SUM($1)])
              LogicalProject(status=[$0], size=[$1], s=[SUM($1) OVER ()])
                StubTableScan(table=[[test_index]])
            """, plan);
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], total_size=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], mode=[SINGLE], viableBackends=[[mock-parquet]])
                  OpenSearchProject(status=[$0], size=[$1], s=[SUM($1) OVER ()], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * OVER (PARTITION BY ...) is rejected at marking time — no shuffle exchange yet.
     */
    public void testRexOverPartitionBy_rejected() {
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
        try {
            runPlanner(plan, multiShardContext());
            fail("Expected planner to reject PARTITION BY");
        } catch (RuntimeException expected) {
            // OK — rule rejected the RexOver.
        }
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
        org.apache.calcite.rel.core.AggregateCall call = org.apache.calcite.rel.core.AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(sumFieldIndex),
            -1,
            input,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
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
