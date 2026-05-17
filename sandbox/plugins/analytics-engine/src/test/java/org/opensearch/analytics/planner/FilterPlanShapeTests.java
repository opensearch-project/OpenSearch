/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Set;

/**
 * Plan-shape tests for {@link org.opensearch.analytics.planner.rel.OpenSearchFilter}.
 * Filter is single-input passthrough — its trait equals its child's, so the plan shape
 * is {@code ER ← Filter ← Scan} regardless of shard count, with the Filter executed at
 * the shard.
 */
public class FilterPlanShapeTests extends PlanShapeTestBase {

    public void testFilter_1shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeFilter(scan, makeEquals(0, SqlTypeName.INTEGER, 200));
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=0, backends=[mock-lucene, mock-parquet], =($0, 200))], viableBackends=[[mock-parquet]])
                  OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testFilter_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode plan = makeFilter(scan, makeEquals(0, SqlTypeName.INTEGER, 200));
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                  OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=0, backends=[mock-lucene, mock-parquet], =($0, 200))], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    // ---- Filter pushdown via Calcite core transposes ----

    /**
     * Filter sitting above a Project — {@code FilterProjectTransposeRule} should push
     * the Filter through the Project so it sits adjacent to the Scan.
     */
    public void testFilterPastProject_pushed() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        LogicalProject project = LogicalProject.create(
            scan,
            List.of(),
            List.of(rexBuilder.makeInputRef(intType, 0)),
            List.of("status_out")
        );
        RelNode plan = makeFilter(project, makeEquals(0, SqlTypeName.INTEGER, 200));
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status_out=[$0], viableBackends=[[mock-parquet]])
                  OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=0, backends=[mock-lucene, mock-parquet], =($0, 200))], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * HAVING-style Filter on a group-by column above an Aggregate — should push past the
     * Aggregate via {@code FilterAggregateTransposeRule}, ending below it.
     */
    public void testFilterPastAggregateOnGroupKey_pushed() {
        AggregateCall sum = sumCall();
        RelNode aggregate = makeAggregate(sum);
        // Filter on $0 — the group-by column, not the agg result.
        RelNode plan = makeFilter(aggregate, makeEquals(0, SqlTypeName.INTEGER, 200));
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], total_size=[SUM(AGG_CALL_ANNOTATION(id=1, viableBackends=[mock-parquet]), $1)], mode=[SINGLE], viableBackends=[[mock-parquet]])
                  OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=0, backends=[mock-lucene, mock-parquet], =($0, 200))], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * HAVING-style Filter on an aggregate result above an Aggregate — must NOT push, since
     * the predicate references the agg output. Filter stays above Aggregate.
     */
    public void testFilterPastAggregateOnAggResult_notPushed() {
        AggregateCall sum = sumCall();
        RelNode aggregate = makeAggregate(sum);
        // Filter on $1 — the SUM(size) result column, which the rule cannot push below the agg.
        RelNode plan = makeFilter(aggregate, makeEquals(1, SqlTypeName.INTEGER, 1000));
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=1, backends=[mock-parquet], =($1, 1000))], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], total_size=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], mode=[SINGLE], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Filter referencing a column on the left side of a Join — {@code FilterIntoJoinRule}
     * should push the Filter into the left input of the Join.
     */
    public void testFilterPastJoin_pushed() {
        RelOptTable leftTable = mockTable("left_idx", "status", "size");
        RelOptTable rightTable = mockTable("right_idx", "status", "size");
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexNode joinCond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, 2)
        );
        RelNode join = LogicalJoin.create(
            stubScan(leftTable),
            stubScan(rightTable),
            List.of(),
            joinCond,
            Set.<CorrelationId>of(),
            JoinRelType.INNER
        );
        // Filter on $0 — references only the left side; rule should push into left input.
        RelNode plan = makeFilter(join, makeEquals(0, SqlTypeName.INTEGER, 200));
        RelNode result = runPlanner(plan, perIndexContext(java.util.Map.of("left_idx", 1, "right_idx", 1)));
        assertPlanShape(
            """
                OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=0, backends=[mock-lucene, mock-parquet], =($0, 200))], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[left_idx]], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[right_idx]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testFilterWithAnd_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RexNode equalsStatus = makeEquals(0, SqlTypeName.INTEGER, 200);
        RexNode equalsSize = makeEquals(1, SqlTypeName.INTEGER, 1024);
        RexNode andExpr = rexBuilder.makeCall(SqlStdOperatorTable.AND, equalsStatus, equalsSize);
        RelNode plan = makeFilter(scan, andExpr);
        RelNode result = runPlanner(plan, multiShardContext());
        // Both predicates collapse into the same shard-side OpenSearchFilter; one ER above
        // gathers to coord. Annotation IDs are stable because there's exactly one filter rel.
        assertPlanShape(
            """
                OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                  OpenSearchFilter(condition=[AND(ANNOTATED_PREDICATE(id=0, backends=[mock-lucene, mock-parquet], =($0, 200)), ANNOTATED_PREDICATE(id=1, backends=[mock-lucene, mock-parquet], =($1, 1024)))], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }
}
