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

    /**
     * A NARROWING projection must stay BELOW the gather, so the reducer transports 1 column instead of 3.
     *
     * <p>This is the property the ER's additive width term exists for
     * ({@code OpenSearchExchangeReducer.computeSelfCost}): both placements move the same rows, so only width
     * breaks the tie. Worth pinning explicitly because the sibling {@code testFieldsProject_2shard} CANNOT
     * pin it — its projection is an identity over a 2-column table, so both sides have equal width and which
     * one wins is arbitrary tie-break order rather than a cost decision.
     */
    public void testNarrowingProject_2shard_staysBelowGather() {
        PlannerContext context = buildContext("parquet", 2, threeIntFields());
        RelNode scan = stubScan(mockTable("test_index", "status", "size", "extra"));
        RelNode plan = LogicalProject.create(scan, List.of(), List.of(rexBuilder.makeInputRef(scan, 0)), List.of("status"));
        RelNode result = runPlanner(plan, context);
        assertPlanShape(
            """
                OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[], partitionCount=0]])
                  OpenSearchProject(status=[$0], viableBackends=[[mock-parquet]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testFieldsProject_1shard() {
        RelNode plan = identityFieldsProject();
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape("""
            OpenSearchProject(status=[$0], size=[$1], viableBackends=[[mock-parquet]])
              OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
            """, result);
    }

    /**
     * An IDENTITY projection is width-neutral, so its side of the gather is an arbitrary tie-break, not a
     * cost decision — see {@link #testNarrowingProject_2shard_staysBelowGather} for the case the cost model
     * actually decides. Pinned as-is only to keep the shape visible in review.
     */
    public void testFieldsProject_2shard() {
        RelNode plan = identityFieldsProject();
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], size=[$1], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[], partitionCount=0]])
                    OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    public void testProjectWithScalarExpression_2shard() {
        // status + size — primitive arithmetic. PLUS now goes through the capability
        // registry so it gets wrapped in ANNOTATED_PROJECT_EXPR.
        // Two columns in, two out: width-neutral, so the Project's side of the gather is a tie-break here
        // too (see testNarrowingProject_2shard_staysBelowGather).
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        RexNode plus = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, rexBuilder.makeInputRef(scan, 0), rexBuilder.makeInputRef(scan, 1));
        RelNode plan = LogicalProject.create(scan, List.of(), List.of(rexBuilder.makeInputRef(scan, 0), plus), List.of("status", "sum"));
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchProject(status=[$0], sum=[ANNOTATED_PROJECT_EXPR(id=0, backends=[mock-parquet], +($0, $1))], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[], partitionCount=0]])
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
