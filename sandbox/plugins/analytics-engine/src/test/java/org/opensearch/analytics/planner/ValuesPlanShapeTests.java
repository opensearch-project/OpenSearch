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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.EngineCapability;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Plan-shape tests for {@link org.opensearch.analytics.planner.rel.OpenSearchValues}.
 * Values is a coordinator-local literal-row source; the planner stamps it at
 * COORDINATOR+SINGLETON so the root demand satisfies directly with no ER above it.
 */
public class ValuesPlanShapeTests extends PlanShapeTestBase {

    /** Single literal row — the {@code SELECT 1+1, NOW()} shape after Calcite unwraps. */
    public void testSingleRowValues() {
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType rowType = typeFactory.builder().add("a", intType).add("b", intType).build();
        RexLiteral one = (RexLiteral) rexBuilder.makeLiteral(1, intType, true);
        RexLiteral two = (RexLiteral) rexBuilder.makeLiteral(2, intType, true);
        ImmutableList<ImmutableList<RexLiteral>> tuples = ImmutableList.of(ImmutableList.of(one, two));
        RelNode plan = LogicalValues.create(cluster, rowType, tuples);
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchValues(tuples=[[{ 1, 2 }]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Values as a Union arm — the literal-row source is already at COORDINATOR+SINGLETON,
     * so no ER is added on the Values arm; the multi-shard scan arm gathers via ER, and
     * the Union itself runs at COORDINATOR+SINGLETON. Mirrors the SQL pattern
     * {@code SELECT v FROM idx UNION ALL SELECT x FROM (VALUES (99))}.
     */
    public void testValuesAsUnionArm_2shard() {
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RexLiteral ninetyNine = (RexLiteral) rexBuilder.makeLiteral(99, intType, true);
        // Values row type must match the scan's output row type for Union to type-check.
        RelDataType valuesRowType = typeFactory.builder().add("status", intType).add("size", intType).build();
        RexLiteral one = (RexLiteral) rexBuilder.makeLiteral(1, intType, true);
        ImmutableList<ImmutableList<RexLiteral>> tuples = ImmutableList.of(ImmutableList.of(ninetyNine, one));
        RelNode values = LogicalValues.create(cluster, valuesRowType, tuples);
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RelNode union = LogicalUnion.create(List.of(scan, values), /* all */ true);
        RelNode result = runPlanner(union, unionAndValuesContext("test_index", 2));
        assertPlanShape(
            """
                OpenSearchUnion(all=[true], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                  OpenSearchValues(tuples=[[{ 99, 1 }]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Values as a Join arm — the literal-row source is already at COORDINATOR+SINGLETON, so
     * no ER is added on the Values side; the multi-shard scan side gathers via ER. The Join
     * runs at COORDINATOR+SINGLETON. Mirrors {@code SELECT i.val FROM idx i JOIN (VALUES …) v
     * ON i.val = v.x}.
     */
    public void testValuesAsJoinArm_2shard() {
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelOptTable scanTable = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(scanTable);
        // Values: single int column "x" with one row; type matches scan's $0 ("status").
        RelDataType valuesRowType = typeFactory.builder().add("x", intType).build();
        RexLiteral one = (RexLiteral) rexBuilder.makeLiteral(1, intType, true);
        ImmutableList<ImmutableList<RexLiteral>> tuples = ImmutableList.of(ImmutableList.of(one));
        RelNode values = LogicalValues.create(cluster, valuesRowType, tuples);
        // Equi-join on scan.$0 == values.$0 (which is scan-side index 2 after the join row type).
        RexNode cond = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(intType, 0),
            rexBuilder.makeInputRef(intType, 2)
        );
        RelNode join = LogicalJoin.create(scan, values, List.of(), cond, Set.<CorrelationId>of(), JoinRelType.INNER);
        RelNode result = runPlanner(join, perIndexContext(Map.of("test_index", 2)));
        assertPlanShape(
            """
                OpenSearchJoin(condition=[=($0, $2)], joinType=[inner], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                  OpenSearchValues(tuples=[[{ 1 }]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    private PlannerContext unionAndValuesContext(String indexName, int shardCount) {
        return buildContextPerIndex("parquet", Map.of(indexName, shardCount), intFields(), List.of(new UnionAndValuesCapableBackend(), LUCENE));
    }

    /** Mock DF backend with both UNION and VALUES engine capabilities for tests that
     *  exercise a Union over a Values arm. */
    private static final class UnionAndValuesCapableBackend extends MockDataFusionBackend {
        @Override
        protected Set<EngineCapability> supportedEngineCapabilities() {
            Set<EngineCapability> caps = new HashSet<>(super.supportedEngineCapabilities());
            caps.add(EngineCapability.UNION);
            return caps;
        }
    }

    /** Values feeding a Project — confirms the rel composes cleanly downstream. */
    public void testProjectOverValues() {
        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType rowType = typeFactory.builder().add("a", intType).add("b", intType).build();
        RexLiteral one = (RexLiteral) rexBuilder.makeLiteral(1, intType, true);
        RexLiteral two = (RexLiteral) rexBuilder.makeLiteral(2, intType, true);
        ImmutableList<ImmutableList<RexLiteral>> tuples = ImmutableList.of(ImmutableList.of(one, two));
        RelNode values = LogicalValues.create(cluster, rowType, tuples);
        RexNode sum = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, rexBuilder.makeInputRef(values, 0), rexBuilder.makeInputRef(values, 1));
        RelNode plan = LogicalProject.create(values, List.of(), List.of(sum), List.of("c"));
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchProject(c=[ANNOTATED_PROJECT_EXPR(id=0, backends=[mock-parquet], +($0, $1))], viableBackends=[[mock-parquet]])
                  OpenSearchValues(tuples=[[{ 1, 2 }]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

}
