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
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Optionality;

import java.math.BigDecimal;
import java.util.List;

/**
 * Plan-shape tests for {@link org.opensearch.analytics.planner.rules.OpenSearchAggLiteralArgProjectSplitRule}.
 *
 * <p>Multi-shard: the literal-bearing Project is duplicated into a pinned upper copy (keeping the literal
 * with the aggregate) over an unpinned lower copy (physical-only), with the CBO-inserted ExchangeReducer
 * landing between them — so the literal stays coordinator-side while the scan narrows.
 *
 * <p>1-shard: no exchange is needed; the planner leaves a (harmless) stacked Project — DataFusion's physical
 * optimizer folds it at execution (verified separately on a live node).
 */
public class AggLiteralArgProjectSplitPlanShapeTests extends PlanShapeTestBase {

    /** {@code Aggregate(<aggName>($0,$1)) over Project(status=$0, $f1=50) over scan(status,size)}. */
    private RelNode aggOverLiteralProject(String aggName) {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RexNode statusRef = rexBuilder.makeInputRef(scan, 0);
        RexNode fifty = rexBuilder.makeLiteral(BigDecimal.valueOf(50), typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RelDataType projectType = typeFactory.builder()
            .add("status", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .add("$f1", typeFactory.createSqlType(SqlTypeName.INTEGER))
            .build();
        LogicalProject project = (LogicalProject) LogicalProject.create(scan, List.of(), List.of(statusRef, fifty), projectType);

        AggregateCall call = AggregateCall.create(
            udaf(aggName),
            false,
            false,
            false,
            List.of(),
            List.of(0, 1),
            -1,
            null,
            org.apache.calcite.rel.RelCollations.EMPTY,
            0,
            project,
            typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            aggName
        );
        return LogicalAggregate.create(project, List.of(), ImmutableBitSet.of(), null, List.of(call));
    }

    public void testPercentileLiteralArg_2shard_splitsWithErBetween() {
        RelNode result = runPlanner(aggOverLiteralProject("PERCENTILE_APPROX"), multiShardContext());
        // SINGLE percentile over a PINNED upper Project (literal $f1=50) over the ER over the
        // physical-only lower Project (status) over the scan.
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{}], PERCENTILE_APPROX=[PERCENTILE_APPROX($0, $1)], mode=[SINGLE], viableBackends=[[mock-parquet]])
                  OpenSearchProject(status=[$0], $f1=[50], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchProject(status=[$0], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testPercentileLiteralArg_1shard_noExchangeNoSplit() {
        RelNode result = runPlanner(aggOverLiteralProject("PERCENTILE_APPROX"), singleShardContext());
        // 1-shard: SINGLETON already satisfied, no ER. The rule still duplicates the Project (the pinned
        // upper copy's SINGLETON requirement is trivially met), leaving a stacked Project that DataFusion
        // folds at execution.
        assertPlanShape("""
            OpenSearchAggregate(group=[{}], PERCENTILE_APPROX=[PERCENTILE_APPROX($0, $1)], mode=[SINGLE], viableBackends=[[mock-parquet]])
              OpenSearchProject(status=[$0], $f1=[50], viableBackends=[[mock-parquet]])
                OpenSearchProject(status=[$0], viableBackends=[[mock-parquet]])
                  OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    public void testTakeLiteralArg_2shard_splitsWithErBetween() {
        RelNode result = runPlanner(aggOverLiteralProject("TAKE"), multiShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{}], TAKE=[TAKE($0, $1)], mode=[SINGLE], viableBackends=[[mock-parquet]])
                  OpenSearchProject(status=[$0], $f1=[50], viableBackends=[[mock-parquet]])
                    OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                      OpenSearchProject(status=[$0], viableBackends=[[mock-parquet]])
                        OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /** A minimal user-defined agg function named {@code name} (two ANY operands), resolved by the SPI by name. */
    private static SqlAggFunction udaf(String name) {
        return new SqlAggFunction(
            name,
            null,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.ARG0_FORCE_NULLABLE,
            null,
            OperandTypes.ANY_ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION,
            false,
            false,
            Optionality.FORBIDDEN
        ) {
        };
    }
}
