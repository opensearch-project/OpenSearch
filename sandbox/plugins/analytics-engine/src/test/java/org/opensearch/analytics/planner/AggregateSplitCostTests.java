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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.when;

/**
 * Aggregate split decision — driven deterministically by partitioning, NOT cost.
 *
 * <p>{@code OpenSearchAggregateSplitRule} reads the input's distribution trait: partitioned
 * (RANDOM, i.e. multi-shard) input splits into PARTIAL/FINAL across an Exchange; SINGLETON
 * (1 shard / already gathered) input stays SINGLE. No row-count estimates or cost arithmetic
 * are involved — the rule emits exactly one alternative, so Volcano has nothing to cost-compare
 * for placement. This mirrors how the Join/Union split rules choose their shape.
 */
public class AggregateSplitCostTests extends PlanShapeTestBase {

    /**
     * Aggregate over a Filter + Project chain (the shape PPL emits for
     * {@code | where ... | stats count() by k}) at multi-shard: the partitioned input splits
     * into {@code Aggregate(FINAL) → ER → Aggregate(PARTIAL) → Project → Filter → Scan}.
     */
    public void testFilterAndProjectBelowAggregate_2shard_splits() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        LogicalProject project = LogicalProject.create(
            scan,
            List.of(),
            List.of(rexBuilder.makeInputRef(intType, 0), rexBuilder.makeInputRef(intType, 1)),
            List.of("status", "size")
        );
        RelNode filter = makeFilter(project, makeEquals(0, SqlTypeName.INTEGER, 200));
        // Aggregate also SUM(size) ($1) so size stays in the below Project (else trimmed as dead).
        RelNode plan = makeAggregate(filter, countStarCall(filter), sumCall(filter));
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], cnt=[SUM($1)], total_size=[SUM($2)], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchAggregate(group=[{0}], cnt=[COUNT()], total_size=[SUM($1)], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchProject(status=[$0], size=[$1], viableBackends=[[mock-parquet]])
                        OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=0, backends=[mock-lucene, mock-parquet], =($0, 200))], viableBackends=[[mock-parquet]])
                          OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * 1-shard variant of the same shape — must NOT split. The 1-shard scan declares SINGLETON
     * (not RANDOM), so the split rule sees unpartitioned input and keeps a single SINGLE
     * aggregate; a second aggregate adds nothing when the input is already gathered.
     */
    public void testFilterAndProjectBelowAggregate_1shard_doesNotSplit() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        LogicalProject project = LogicalProject.create(
            scan,
            List.of(),
            List.of(rexBuilder.makeInputRef(intType, 0), rexBuilder.makeInputRef(intType, 1)),
            List.of("status", "size")
        );
        RelNode filter = makeFilter(project, makeEquals(0, SqlTypeName.INTEGER, 200));
        // Aggregate also SUM(size) ($1) so size stays in the below Project (else trimmed as dead).
        RelNode plan = makeAggregate(filter, countStarCall(filter), sumCall(filter));
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], cnt=[COUNT()], total_size=[SUM($1)], mode=[SINGLE], viableBackends=[[mock-parquet]])
                  OpenSearchProject(status=[$0], size=[$1], viableBackends=[[mock-parquet]])
                    OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=0, backends=[mock-lucene, mock-parquet], =($0, 200))], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Aggregate over a collated Sort at multi-shard must NOT split. The Sort forces its input to a
     * single node (global order can't run per-shard), so the aggregate's input is already gathered —
     * a PARTIAL below it would be invalid. The rule emits a single SINGLE aggregate over the Sort.
     */
    public void testAggregateOverCollatedSort_2shard_doesNotSplit() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RelNode sort = makeSort(scan, 100);
        RelNode plan = makeAggregate(sort, countStarCall(sort));
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[SINGLE], viableBackends=[[mock-parquet]])
                  OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[100], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchSort(sort0=[$0], dir0=[ASC], fetch=[100], viableBackends=[[mock-parquet]])
                        OpenSearchProject(status=[$0], viableBackends=[[mock-parquet]])
                          OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * {@code where <4 eq-preds> | stats count() by status_code}, multi-shard — the PARTIAL must
     * stay below the Exchange. Two ingredients make the bad coordinator-PARTIAL look cheaper, so
     * this exercises the gate that forbids it:
     * <ul>
     *   <li>Row count 100 + 4 {@code =} conjuncts: default 0.25 selectivity floors the estimate to
     *       1.0 row, erasing the coordinator-PARTIAL's cost margin over the shard-PARTIAL+ER.</li>
     *   <li>Narrowing Project (status_code only): the projected row is narrower than the PARTIAL
     *       state (key + count), so shipping rows below a coordinator-PARTIAL beats shipping the
     *       partial state above a shard-PARTIAL.</li>
     * </ul>
     * Comment out the gate in {@code OpenSearchAggregate.computeSelfCost} and this fails with the
     * FINAL → coordinator-PARTIAL → ER bad shape.
     */
    public void testFourPredicateFilterBelowCountByKey_2shard_partialStaysBelowExchange() {
        // HTTP-access-log shape: count() by status_code, filtered on 4 fields.
        Map<String, Map<String, Object>> fields = new LinkedHashMap<>();
        for (String name : List.of("status_code", "size", "region_id", "endpoint_id")) {
            fields.put(name, Map.of("type", "integer"));
        }
        PlannerContext context = buildContext("parquet", 2, fields);
        RelOptTable table = mockTable("test_index", "status_code", "size", "region_id", "endpoint_id");
        when(table.getRowCount()).thenReturn(100d);
        RelNode scan = stubScan(table);
        RelNode filter = makeFilter(
            scan,
            makeAnd(
                makeEquals(0, SqlTypeName.INTEGER, 200),
                makeEquals(1, SqlTypeName.INTEGER, 1024),
                makeEquals(2, SqlTypeName.INTEGER, 3),
                makeEquals(3, SqlTypeName.INTEGER, 5)
            )
        );
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        LogicalProject project = LogicalProject.create(
            filter,
            List.of(),
            List.of(rexBuilder.makeInputRef(intType, 0)),
            List.of("status_code")
        );
        RelNode plan = makeAggregate(project, ImmutableBitSet.of(0), countStarCall(project));
        RelNode result = runPlanner(plan, context);
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], cnt=[SUM($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchProject(status_code=[$0], viableBackends=[[mock-parquet]])
                        OpenSearchFilter(condition=[AND(ANNOTATED_PREDICATE(id=0, backends=[mock-lucene, mock-parquet], =($0, 200)), ANNOTATED_PREDICATE(id=1, backends=[mock-lucene, mock-parquet], =($1, 1024)), ANNOTATED_PREDICATE(id=2, backends=[mock-lucene, mock-parquet], =($2, 3)), ANNOTATED_PREDICATE(id=3, backends=[mock-lucene, mock-parquet], =($3, 5)))], viableBackends=[[mock-parquet]])
                          OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Same invariant for an aggregate whose PARTIAL state genuinely differs from its output —
     * {@code avg(size) by status_code} decomposes to SUM + COUNT primitives at the shard, reduced
     * additively at the coordinator. 7 {@code =} conjuncts drive the row-count estimate to the 1.0
     * floor; the narrowing Project (status_code, size) keeps the shipped row narrower than the
     * 3-column PARTIAL state, so without the gate the coordinator-PARTIAL wins on cost. The gate
     * keeps the SUM/COUNT PARTIAL below the Exchange.
     */
    public void testSevenPredicateFilterBelowAvgByKey_2shard_partialStaysBelowExchange() {
        Map<String, Map<String, Object>> fields = new LinkedHashMap<>();
        for (String name : List.of("status_code", "size", "region_id", "endpoint_id", "user_id", "method_id", "cache_hit")) {
            fields.put(name, Map.of("type", "integer"));
        }
        PlannerContext context = buildContext("parquet", 2, fields);
        RelOptTable table = mockTable("test_index", "status_code", "size", "region_id", "endpoint_id", "user_id", "method_id", "cache_hit");
        when(table.getRowCount()).thenReturn(100d);
        RelNode scan = stubScan(table);
        RelNode filter = makeFilter(
            scan,
            makeAnd(
                makeEquals(0, SqlTypeName.INTEGER, 200),
                makeEquals(1, SqlTypeName.INTEGER, 1024),
                makeEquals(2, SqlTypeName.INTEGER, 3),
                makeEquals(3, SqlTypeName.INTEGER, 5),
                makeEquals(4, SqlTypeName.INTEGER, 7),
                makeEquals(5, SqlTypeName.INTEGER, 1),
                makeEquals(6, SqlTypeName.INTEGER, 1)
            )
        );
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        LogicalProject project = LogicalProject.create(
            filter,
            List.of(),
            List.of(rexBuilder.makeInputRef(intType, 0), rexBuilder.makeInputRef(intType, 1)),
            List.of("status_code", "size")
        );
        RelNode plan = makeAggregate(project, ImmutableBitSet.of(0), avgCall(project));
        RelNode result = runPlanner(plan, context);
        assertPlanShape(
            """
                OpenSearchProject(status_code=[$0], avg_size=[ANNOTATED_PROJECT_EXPR(id=10, backends=[mock-parquet], CAST(ANNOTATED_PROJECT_EXPR(id=9, backends=[mock-parquet], /($1, $2))):INTEGER NOT NULL)], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], $f1=[SUM($1)], $f2=[SUM($2)], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{0}], agg#0=[SUM($1)], agg#1=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchProject(status_code=[$0], size=[$1], viableBackends=[[mock-parquet]])
                          OpenSearchFilter(condition=[AND(ANNOTATED_PREDICATE(id=0, backends=[mock-lucene, mock-parquet], =($0, 200)), ANNOTATED_PREDICATE(id=1, backends=[mock-lucene, mock-parquet], =($1, 1024)), ANNOTATED_PREDICATE(id=2, backends=[mock-lucene, mock-parquet], =($2, 3)), ANNOTATED_PREDICATE(id=3, backends=[mock-lucene, mock-parquet], =($3, 5)), ANNOTATED_PREDICATE(id=4, backends=[mock-lucene, mock-parquet], =($4, 7)), ANNOTATED_PREDICATE(id=5, backends=[mock-lucene, mock-parquet], =($5, 1)), ANNOTATED_PREDICATE(id=6, backends=[mock-lucene, mock-parquet], =($6, 1)))], viableBackends=[[mock-parquet]])
                            OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Aggregate over a Join at multi-shard must NOT split. A Join gathers both inputs to the
     * coordinator (its {@code computeSelfCost} is infinite over non-SINGLETON input), so the
     * aggregate's input is already singleton — {@code childForcesGather} stops at the Join and the
     * rule emits a single SINGLE aggregate.
     */
    public void testAggregateOverJoin_2shard_doesNotSplit() {
        RelOptTable left = mockTable("test_index", "status", "size");
        RelOptTable right = mockTable("test_index", "status", "size");
        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        RelNode join = LogicalJoin.create(stubScan(left), stubScan(right), List.of(), cond, Set.<CorrelationId>of(), JoinRelType.INNER);
        RelNode plan = makeAggregate(join, ImmutableBitSet.of(0), countStarCall(join));
        RelNode result = runPlanner(plan, multiShardContext());
        assertModeAndNoPartialAboveExchange(result, AggregateMode.SINGLE);
    }

    /**
     * {@code childForcesGather} must walk PAST a pass-through Project (no {@code RexOver}) and a
     * Filter — neither forces a gather — and still split over the partitioned scan beneath. Asserts
     * the walk doesn't false-positive on intermediate ops it's meant to see through.
     */
    public void testAggregateOverPassthroughProjectOverFilter_2shard_splits() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        LogicalProject project = LogicalProject.create(
            scan,
            List.of(),
            List.of(rexBuilder.makeInputRef(intType, 0), rexBuilder.makeInputRef(intType, 1)),
            List.of("status", "size")
        );
        RelNode filter = makeFilter(project, makeEquals(0, SqlTypeName.INTEGER, 200));
        RelNode plan = makeAggregate(filter, countStarCall(filter));
        RelNode result = runPlanner(plan, multiShardContext());
        assertModeAndNoPartialAboveExchange(result, AggregateMode.FINAL);
    }

    /**
     * A Project carrying a window ({@code RexOver}) DOES force a gather — its global frame needs
     * SINGLETON input, so the walk must stop at it (not treat it as pass-through). Aggregate over
     * such a Project at multi-shard stays SINGLE, no split.
     */
    public void testAggregateOverWindowInProject_2shard_doesNotSplit() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RexNode countOverEmpty = rexBuilder.makeOver(
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            SqlStdOperatorTable.COUNT,
            List.of(),
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
        LogicalProject windowProject = LogicalProject.create(
            scan,
            List.of(),
            List.of(rexBuilder.makeInputRef(scan, 0), countOverEmpty),
            List.of("status", "w")
        );
        // Aggregate must CONSUME the window column (avgCall reads slot 1 = `w`); otherwise field
        // trimming drops the dead window and the aggregate legitimately splits. A live window blocks
        // the split — that is what this test guards.
        RelNode plan = makeAggregate(windowProject, ImmutableBitSet.of(0), avgCall(windowProject));
        RelNode result = runPlanner(plan, multiShardContext());
        assertModeAndNoPartialAboveExchange(result, AggregateMode.SINGLE);
    }

    /**
     * Structural assertion shared by the no-split / split cases: the top OpenSearchAggregate carries
     * {@code expectedTopMode}, and no PARTIAL aggregate sits directly above an ExchangeReducer
     * (the bug shape). For SINGLE this confirms no split happened; for FINAL it confirms the split
     * placed PARTIAL below the Exchange.
     */
    private static void assertModeAndNoPartialAboveExchange(RelNode root, AggregateMode expectedTopMode) {
        OpenSearchAggregate top = findTopAggregate(root);
        assertNotNull("expected an OpenSearchAggregate in the plan:\n" + explain(root), top);
        assertEquals("top aggregate mode\n" + explain(root), expectedTopMode, top.getMode());
        assertFalse("PARTIAL sits directly above an ExchangeReducer (bad shape):\n" + explain(root), hasPartialAboveExchange(root));
    }

    private static OpenSearchAggregate findTopAggregate(RelNode node) {
        if (node instanceof OpenSearchAggregate aggregate) {
            return aggregate;
        }
        for (RelNode input : node.getInputs()) {
            OpenSearchAggregate found = findTopAggregate(input);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static boolean hasPartialAboveExchange(RelNode node) {
        if (node instanceof OpenSearchAggregate aggregate
            && aggregate.getMode() == AggregateMode.PARTIAL
            && aggregate.getInput() instanceof OpenSearchExchangeReducer) {
            return true;
        }
        for (RelNode input : node.getInputs()) {
            if (hasPartialAboveExchange(input)) {
                return true;
            }
        }
        return false;
    }

    private static String explain(RelNode node) {
        return RelOptUtil.toString(node);
    }
}
