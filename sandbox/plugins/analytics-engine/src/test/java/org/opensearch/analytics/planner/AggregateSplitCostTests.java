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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

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
        RelNode plan = makeAggregate(filter, countStarCall(filter));
        RelNode result = runPlanner(plan, multiShardContext());
        assertPlanShape(
            """
                OpenSearchAggregate(group=[{0}], cnt=[SUM($1)], mode=[FINAL], viableBackends=[[mock-parquet]])
                  OpenSearchExchangeReducer(viableBackends=[[mock-parquet]], exchange=[ExchangeInfo[distributionType=SINGLETON, partitionKeyIndices=[], partitionCount=0]])
                    OpenSearchAggregate(group=[{0}], cnt=[COUNT()], mode=[PARTIAL], viableBackends=[[mock-parquet]])
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
}
