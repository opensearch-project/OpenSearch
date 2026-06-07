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
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;

import java.util.List;

/**
 * Cost-driven tests for {@code OpenSearchAggregate} split decision. Two row-count
 * overrides combine to drive the cost arithmetic toward the right plan:
 *
 * <ul>
 *   <li>{@code OpenSearchTableScan.estimateRowCount} returns
 *       {@code shards × DEFAULT_ROWS_PER_SHARD} so the cost model sees the scan as "big"
 *       rather than Calcite's default of 100.</li>
 *   <li>{@code OpenSearchAggregate.estimateRowCount} returns {@code input / 100} so the
 *       split alternative ships ~100× fewer rows over the Exchange than the non-split
 *       alternative — making split the cheaper plan when there's a group-by.</li>
 * </ul>
 *
 * <p>Without these overrides, every alternative looks cost-equivalent at row-count=100,
 * and Volcano's tie-break picks the first plausible plan it sees. That can be a wedged
 * placement where an ER sits below a Project/Filter that itself sits below a SINGLE
 * aggregate — every input row gets shipped to the coordinator, which is the failure
 * shape we're guarding against here.
 */
public class AggregateSplitCostTests extends PlanShapeTestBase {

    /**
     * Aggregate over a Filter + Project chain (the shape PPL emits for
     * {@code | where ... | stats count() by k}). The filter introduces an extra wedge
     * point for ER placement — without proper cost arithmetic, Volcano can choose
     * {@code Aggregate(SINGLE) → Project → ER → Filter → Scan} instead of
     * {@code Aggregate(FINAL) → ER → Aggregate(PARTIAL) → Project → Filter → Scan}.
     * This test asserts the latter (correct) shape.
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
        RelNode plan = makeAggregate(filter, countStarCall(filter));
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], cnt=[SUM($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[]]])
                    OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
                      OpenSearchProject(status=[$0], size=[$1], viableBackends=[[mock-parquet]])
                        OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=0, backends=[mock-lucene, mock-parquet], =($0, 200))], viableBackends=[[mock-parquet]])
                          OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * 1-shard variant of the same shape — must NOT split. The 1-shard scan declares
     * SINGLETON type (not RANDOM) so the SINGLE-cost gate passes, and the row-count
     * arithmetic doesn't matter (no second aggregate adds value when the input is
     * already gathered).
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
        RelNode plan = makeAggregate(filter, countStarCall(filter));
        RelNode result = runPlanner(plan, singleShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[SINGLE], viableBackends=[[mock-parquet]])
                  OpenSearchProject(status=[$0], size=[$1], viableBackends=[[mock-parquet]])
                    OpenSearchFilter(condition=[ANNOTATED_PREDICATE(id=0, backends=[mock-lucene, mock-parquet], =($0, 200))], viableBackends=[[mock-parquet]])
                      OpenSearchTableScan(table=[[test_index]], viableBackends=[[mock-parquet]])
                """,
            result
        );
    }

    /**
     * Direct cost-arithmetic check: the marked OpenSearchTableScan reports a non-trivial
     * row count (shards × DEFAULT_ROWS_PER_SHARD when no real stat is available), and the
     * marked OpenSearchAggregate reports a meaningfully-smaller row count
     * ({@code input / GROUP_REDUCTION_FACTOR}).
     *
     * <p>If either invariant breaks, plan-shape tests would still pass for some inputs but
     * Volcano's split decision becomes fragile — every plan looks cost-equivalent and the
     * tie-breaker decides. Asserting the row-count gap directly locks in the property that
     * the cost arithmetic relies on.
     */
    public void testRowCountEstimatesProduceSplitGap() {
        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode scan = stubScan(table);
        RelNode plan = makeAggregate(scan, countStarCall(scan));
        RelNode marked = runPlanner(plan, multiShardContext());

        OpenSearchTableScan markedScan = findScan(marked);
        OpenSearchAggregate finalAgg = findAggregate(marked, AggregateMode.FINAL);
        OpenSearchAggregate partialAgg = findAggregate(marked, AggregateMode.PARTIAL);
        assertNotNull("marked plan should contain an OpenSearchTableScan", markedScan);
        assertNotNull("marked plan should split into FINAL/PARTIAL", finalAgg);
        assertNotNull("marked plan should split into FINAL/PARTIAL", partialAgg);

        RelMetadataQuery mq = marked.getCluster().getMetadataQuery();
        double scanRows = mq.getRowCount(markedScan);
        double partialRows = mq.getRowCount(partialAgg);

        // Scan must report >> Calcite's default of 100 so cost arithmetic differentiates plans.
        assertTrue("scan row count must be non-trivial: " + scanRows, scanRows > 1_000);
        // Aggregate output must be strictly smaller than its input — that's the gap that
        // makes the split alternative win on cost. Calcite's metadata query may apply its
        // own dampening, so we don't assert a specific ratio, just monotone shrinkage.
        assertTrue("aggregate output (" + partialRows + ") must be < scan rows (" + scanRows + ")", partialRows < scanRows);
    }

    private static OpenSearchTableScan findScan(RelNode root) {
        if (root instanceof OpenSearchTableScan ts) {
            return ts;
        }
        for (RelNode child : root.getInputs()) {
            OpenSearchTableScan found = findScan(child);
            if (found != null) return found;
        }
        return null;
    }

    private static OpenSearchAggregate findAggregate(RelNode root, AggregateMode mode) {
        if (root instanceof OpenSearchAggregate agg && agg.getMode() == mode) {
            return agg;
        }
        for (RelNode child : root.getInputs()) {
            OpenSearchAggregate found = findAggregate(child, mode);
            if (found != null) return found;
        }
        return null;
    }
}
