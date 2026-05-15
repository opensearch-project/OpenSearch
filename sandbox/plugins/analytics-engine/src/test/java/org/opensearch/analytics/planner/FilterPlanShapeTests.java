/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

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
