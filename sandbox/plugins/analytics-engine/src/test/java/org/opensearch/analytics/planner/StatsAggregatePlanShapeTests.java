/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.spi.OpenSearchAggregateOperators;

import java.util.List;

/**
 * Plan-shape tests for the STATS aggregate after the full {@code PlannerImpl} pipeline
 * (HEP decompose → marking → CBO).
 *
 * <p>By the time these assertions run, {@code OpenSearchStatsReduceRule} has expanded
 * STATS into the four primitive aggregate calls plus a Project that builds the
 * {@code STRUCT(count, min, max, avg, sum)} result. Subsequent phases (marking, Volcano
 * split, distribute) operate on that primitive plan exactly the way they operate on any
 * other AVG/SUM/COUNT aggregate. The assertion shape is therefore the same as
 * {@link AggregatePlanShapeTests}'s — the structure proves STATS routes through every
 * phase correctly.
 */
public class StatsAggregatePlanShapeTests extends PlanShapeTestBase {

    public void testStatsSingleField_1shard_yieldsProjectOverSingleAggregate() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall stats = makeStatsCall(scan, 1, "stats_size");
        RelNode plan = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(stats));

        RelNode result = runPlanner(plan, singleShardContext());

        // Top-level: ExchangeReducer wrapping a Project that builds the struct from a
        // single-mode Aggregate's primitives. Peel the ER to inspect Project + Aggregate.
        RelNode top = unwrapRootReducer(result);
        assertTrue("Top must be OpenSearchProject", top instanceof OpenSearchProject);
        OpenSearchProject project = (OpenSearchProject) top;
        assertEquals(1, project.getProjects().size());

        RelNode child = unwrapExchange(project.getInput());
        assertTrue("Project input must be OpenSearchAggregate", child instanceof OpenSearchAggregate);
        OpenSearchAggregate agg = (OpenSearchAggregate) child;
        assertEquals("Decomposed to 4 primitive calls", 4, agg.getAggCallList().size());
    }

    public void testStatsSingleField_2shard_splitsIntoPartialFinalAroundProject() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall stats = makeStatsCall(scan, 1, "stats_size");
        RelNode plan = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(), null, List.of(stats));

        RelNode result = runPlanner(plan, multiShardContext());

        // Plan shape after the multi-shard split:
        //   Project [stats = STRUCT(count, min, max, avg, sum)]
        //     └── Aggregate(FINAL) [SUM, MIN, MAX, COUNT]
        //          └── ExchangeReducer
        //               └── Aggregate(PARTIAL) [SUM, MIN, MAX, COUNT]
        //                    └── Scan
        RelNode top = unwrapRootReducer(result);
        assertTrue("Top must be OpenSearchProject", top instanceof OpenSearchProject);
        OpenSearchProject project = (OpenSearchProject) top;

        RelNode finalAgg = unwrapExchange(project.getInput());
        assertTrue("Below project must be Aggregate(FINAL)", finalAgg instanceof OpenSearchAggregate);
        assertEquals(4, ((OpenSearchAggregate) finalAgg).getAggCallList().size());

        RelNode reducer = finalAgg.getInputs().get(0);
        assertTrue("Below FINAL must be ExchangeReducer", reducer instanceof OpenSearchExchangeReducer);
    }

    public void testStatsWithGroupBy_2shard() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall stats = makeStatsCall(scan, 1, "stats_size");
        RelNode plan = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(stats));

        RelNode result = runPlanner(plan, multiShardContext());

        RelNode top = unwrapRootReducer(result);
        assertTrue("Top must be OpenSearchProject", top instanceof OpenSearchProject);
        OpenSearchProject project = (OpenSearchProject) top;
        assertEquals(2, project.getProjects().size());
        assertEquals("status", project.getRowType().getFieldList().get(0).getName());
        assertEquals("stats_size", project.getRowType().getFieldList().get(1).getName());
    }

    public void testStatsMixedWithCount_2shard_bothFlowThroughSamePartialFinal() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall stats = makeStatsCall(scan, 1, "stats_size");
        AggregateCall countStar = AggregateCall.create(
            org.apache.calcite.sql.fun.SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            List.of(),
            List.of(),
            -1,
            null,
            RelCollations.EMPTY,
            0,
            scan,
            null,
            "cnt_all"
        );
        RelNode plan = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(stats, countStar));

        RelNode result = runPlanner(plan, multiShardContext());

        RelNode top = unwrapRootReducer(result);
        assertTrue("Top must be OpenSearchProject", top instanceof OpenSearchProject);
        OpenSearchProject project = (OpenSearchProject) top;
        assertEquals(3, project.getProjects().size());

        RelNode finalAgg = unwrapExchange(project.getInput());
        assertTrue("Below project must be Aggregate(FINAL)", finalAgg instanceof OpenSearchAggregate);
        // 4 primitives for STATS + 1 for cnt_all = 5.
        assertEquals(5, ((OpenSearchAggregate) finalAgg).getAggCallList().size());
    }

    private AggregateCall makeStatsCall(RelNode input, int colIdx, String name) {
        return AggregateCall.create(
            OpenSearchAggregateOperators.STATS,
            false,
            false,
            false,
            List.of(),
            List.of(colIdx),
            -1,
            null,
            RelCollations.EMPTY,
            0,
            input,
            null,
            name
        );
    }
}
