/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.List;

/**
 * Plan-shape tests for {@link org.opensearch.analytics.planner.rel.OpenSearchProject} —
 * passthrough projection (field refs only) and {@code eval}-style scalar expressions.
 */
public class ProjectPlanShapeTests extends PlanShapeTestBase {

    public void testFieldsProject_1shard() {
        RelNode plan = identityFieldsProject();
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape("""
            OpenSearchProject(status=[$0], size=[$1], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    public void testFieldsProject_2shard() {
        RelNode plan = identityFieldsProject();
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                  OpenSearchProject(status=[$0], size=[$1], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testProjectWithScalarExpression_2shard() {
        // status + size — primitive arithmetic. PLUS now goes through the capability
        // registry so it gets wrapped in ANNOTATED_PROJECT_EXPR.
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RexNode plus = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, rexBuilder.makeInputRef(scan, 0), rexBuilder.makeInputRef(scan, 1));
        RelNode plan = LogicalProject.create(scan, List.of(), List.of(rexBuilder.makeInputRef(scan, 0), plus), List.of("status", "sum"));
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                  OpenSearchProject(status=[$0], sum=[ANNOTATED_PROJECT_EXPR(id=0, backends=[mock-parquet], +($0, $1))], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    private RelNode identityFieldsProject() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        return LogicalProject.create(
            scan,
            List.of(),
            List.of(rexBuilder.makeInputRef(scan, 0), rexBuilder.makeInputRef(scan, 1)),
            List.of("status", "size")
        );
    }
}
