/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Plan-shape tests for {@link org.opensearch.analytics.planner.rel.OpenSearchAggregate}.
 *
 * <p>1-shard inputs: {@code Aggregate(SINGLE)} runs at the shard, ER above.
 * <p>Multi-shard: {@code OpenSearchAggregateSplitRule} splits into PARTIAL/FINAL with an
 * ER in between.
 */
public class AggregatePlanShapeTests extends PlanShapeTestBase {

    public void testStatsCountStar_1shard() {
        RelNode plan = makeAggregate(stubScan(mockTable("test_index", "status", "size")), countStarCall());
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[SINGLE], viableBackends=[[mock-parquet]])
                  OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testStatsCountStar_2shard() {
        RelNode plan = makeAggregate(stubScan(mockTable("test_index", "status", "size")), countStarCall());
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchAggregate(group=[{0}], cnt=[COUNT(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]))], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testStatsSumByKey_1shard() {
        RelNode plan = makeAggregate(stubScan(mockTable("test_index", "status", "size")), sumCall());
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], total_size=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], mode=[SINGLE], viableBackends=[[mock-parquet]])
                  OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testStatsSumByKey_2shard() {
        RelNode plan = makeAggregate(stubScan(mockTable("test_index", "status", "size")), sumCall());
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], total_size=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchAggregate(group=[{0}], total_size=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testStatsAvgByKey_2shard() {
        // AVG is decomposed during the reduce phase into SUM/COUNT plus a Project
        // computing the quotient. After split, FINAL receives reduced primitive aggs.
        // Use AggregateCall.create with null type so Calcite infers AVG's canonical
        // return type — passing an explicit type can drift from typeMatchesInferred.
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall avg = AggregateCall.create(
            SqlStdOperatorTable.AVG,
            false,
            false,
            false,
            List.of(),
            List.of(1),
            -1,
            null,
            org.apache.calcite.rel.RelCollations.EMPTY,
            1,
            scan,
            null,
            "avg_size"
        );
        RelNode plan = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(avg));
        RelNode result = runPlanner(plan, multiShardContext());
        // Project on top performs CAST(SUM(x) / COUNT()) back to AVG's declared return type.
        // COUNT here has no field operand because the inferred AVG decomposition produces a
        // bare COUNT (counts all rows in the group, equivalent to COUNT(x) when x is not nullable).
        // Skeleton: Project ← FINAL(SUM,COUNT) ← ER ← PARTIAL(SUM,COUNT) ← Scan.
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], avg_size=[ANNOTATED_PROJECT_EXPR(id=3, backends=[mock-parquet], CAST(ANNOTATED_PROJECT_EXPR(id=2, backends=[mock-parquet], /($1, $2))):INTEGER NOT NULL)], viableBackends=[[mock-parquet]])
                  OpenSearchAggregate(group=[{0}], agg#0=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], agg#1=[COUNT(AGG_CALL_ANNOTATION(id=1, viableBackends=[mock-parquet]))], mode=[FINAL], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchAggregate(group=[{0}], agg#0=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], agg#1=[COUNT(AGG_CALL_ANNOTATION(id=1, viableBackends=[mock-parquet]))], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testStatsMultiCall_2shard() {
        // sum + count_star, both grouped by status. Single PARTIAL/FINAL pair carries
        // both calls.
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall sum = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "sum_size"
        );
        AggregateCall cnt = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
        RelNode plan = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(sum, cnt));
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], sum_size=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], cnt=[COUNT(AGG_CALL_ANNOTATION(id=1, viableBackends=[mock-parquet]))], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchAggregate(group=[{0}], sum_size=[SUM(AGG_CALL_ANNOTATION(id=0, viableBackends=[mock-parquet]), $1)], cnt=[COUNT(AGG_CALL_ANNOTATION(id=1, viableBackends=[mock-parquet]))], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }
}
