/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.PlannerContext;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchStageInputScan;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Sub-plan reuse in {@link DAGBuilder} ({@code analytics.planner.subplan_reuse.enabled}).
 *
 * <p>Why this matters beyond saving work: a plan that computes the same aggregate twice returns the WRONG
 * ANSWER when the two copies are compared for exact equality, because {@code SUM(double)} is not associative
 * and the copies' partial sums merge in different orders. That is TPC-H q15, which returns 1 row or 0 rows at
 * random. Sharing one evaluation makes the comparison hold by construction. See {@link SharedSubplanReuse}.
 */
public class SharedSubplanReuseTests extends BasePlannerRulesTests {

    /** Off (the default): the duplicated aggregate is cut twice, and nothing references a shared build. */
    public void testReuseDisabled_duplicateAggregateIsComputedTwice() {
        PlannerContext context = buildContext("parquet", 2, intFields());
        RelNode cbo = runPlanner(joinOfTwoIdenticalAggregates(), context);

        QueryDAG dag = DAGBuilder.build(cbo, context.getCapabilityRegistry(), mockClusterService(), TEST_RESOLVER);

        assertEquals("both copies of the aggregate are cut as their own stages with reuse off", 2, stageInputScans(dag).size());
        assertEquals("both copies of the aggregate are still computed", 2, completeAggregateCount(dag));
    }

    /**
     * The consumer would have the shared aggregate as its ONLY input, so it would be served by the streaming
     * (once-consumable) reduce sink and the second read would come back empty. {@code DAGBuilder} must detect
     * that and rebuild without sub-plan reuse — a missed reuse is slower, a double read of a once-consumable input is wrong.
     */
    public void testReuseFallsBackWhenTheSharedInputWouldNotBeBuffered() {
        PlannerContext context = buildContext("parquet", 2, intFields());
        RelNode cbo = runPlanner(joinOfTwoIdenticalAggregates(), context);

        QueryDAG dag = DAGBuilder.build(
            cbo,
            context.getCapabilityRegistry(),
            mockClusterService(),
            TEST_RESOLVER,
            /* subplanReuseEnabled */ true
        );

        List<OpenSearchStageInputScan> scans = stageInputScans(dag);
        assertEquals("the two copies stay on separate stages", 2, scans.size());
        assertNotEquals(
            "sharing must NOT happen when the consumer would not buffer the shared input",
            scans.get(0).getChildStageId(),
            scans.get(1).getChildStageId()
        );
        assertEquals("both copies are still computed (the fallback)", 2, completeAggregateCount(dag));
    }

    /**
     * On, with the consumer keeping another input: the aggregate is cut ONCE and BOTH consumers scan that one
     * child stage, so the buffered memtable input is read twice and every consumer sees identical rows.
     */
    public void testReuseEnabled_duplicateAggregateIsSharedByChildStageId() {
        PlannerContext context = buildContextPerIndex("parquet", Map.of("test_index", 2, "other_idx", 2));
        RelNode cbo = runPlanner(joinKeepingAnotherInputBesideTheSharedAggregate(), context);

        QueryDAG dag = DAGBuilder.build(
            cbo,
            context.getCapabilityRegistry(),
            mockClusterService(),
            TEST_RESOLVER,
            /* subplanReuseEnabled */ true
        );

        // THE point of the feature: one evaluation, so both consumers read identical rows and an exact-equality
        // comparison between them cannot fall foul of float accumulation order.
        assertEquals("the duplicated aggregate is computed exactly ONCE", 1, completeAggregateCount(dag));

        Map<Integer, Long> refsByChildStage = stageInputScans(dag).stream()
            .collect(
                java.util.stream.Collectors.groupingBy(OpenSearchStageInputScan::getChildStageId, java.util.stream.Collectors.counting())
            );
        assertTrue(
            "exactly one child stage must be scanned twice (the shared aggregate), got " + refsByChildStage,
            refsByChildStage.values().stream().filter(c -> c == 2L).count() == 1
        );
    }

    // ─── helpers ───────────────────────────────────────────────────────────────

    /**
     * {@code Join(Aggregate(scan), Aggregate(scan))} where the two aggregates are IDENTICAL — the shape q15
     * produces by inlining one subquery twice.
     */
    private RelNode joinOfTwoIdenticalAggregates() {
        RelNode left = identicalAggregate();
        RelNode right = identicalAggregate();
        RexNode condition = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
        );
        return LogicalJoin.create(left, right, List.of(), condition, Set.<CorrelationId>of(), JoinRelType.INNER);
    }

    /**
     * {@code Join(Join(otherScan, sharedAgg), sharedAgg)} — the q15 skeleton. The consumer keeps a second input
     * (the other scan's gather) besides the shared aggregate, so it buffers its inputs and sharing is sound.
     */
    private RelNode joinKeepingAnotherInputBesideTheSharedAggregate() {
        RelNode other = stubScan(mockTable("other_idx", "status", "size"));
        RelNode inner = LogicalJoin.create(
            other,
            identicalAggregate(),
            List.of(),
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 2)
            ),
            Set.<CorrelationId>of(),
            JoinRelType.INNER
        );
        return LogicalJoin.create(
            inner,
            identicalAggregate(),
            List.of(),
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0),
                rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 4)
            ),
            Set.<CorrelationId>of(),
            JoinRelType.INNER
        );
    }

    private RelNode identicalAggregate() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall sum = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false,
            List.of(1),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "total"
        );
        return LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of(sum));
    }

    private static List<OpenSearchStageInputScan> stageInputScans(QueryDAG dag) {
        List<OpenSearchStageInputScan> found = new ArrayList<>();
        collectScans(dag.rootStage(), found);
        return found;
    }

    private static void collectScans(Stage stage, List<OpenSearchStageInputScan> found) {
        if (stage.getFragment() != null) {
            found.addAll(RelNodeUtils.findNodes(stage.getFragment(), OpenSearchStageInputScan.class));
        }
        for (Stage child : stage.getChildStages()) {
            collectScans(child, found);
        }
    }

    /** Counts FINAL/SINGLE aggregates across every stage — one per surviving evaluation. */
    private static int completeAggregateCount(QueryDAG dag) {
        return countCompleteAggregates(dag.rootStage());
    }

    private static int countCompleteAggregates(Stage stage) {
        int count = 0;
        if (stage.getFragment() != null) {
            for (OpenSearchAggregate aggregate : RelNodeUtils.findNodes(stage.getFragment(), OpenSearchAggregate.class)) {
                if (aggregate.getMode() == AggregateMode.FINAL || aggregate.getMode() == AggregateMode.SINGLE) {
                    count++;
                }
            }
        }
        for (Stage child : stage.getChildStages()) {
            count += countCompleteAggregates(child);
        }
        return count;
    }

}
