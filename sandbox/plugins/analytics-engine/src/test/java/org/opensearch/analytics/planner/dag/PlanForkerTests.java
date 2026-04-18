/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.BasePlannerRulesTests;
import org.opensearch.analytics.planner.MockDataFusionBackend;
import org.opensearch.analytics.planner.MockLuceneBackend;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.ScanCapability;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link PlanForker} — verifies plan alternatives are generated correctly
 * for different query shapes with two viable backends.
 *
 * <p>All tests use duplicated doc values (both parquet and lucene) so both backends
 * are viable, verifying that forking produces two alternatives and each is narrowed
 * to exactly one backend.
 */
public class PlanForkerTests extends BasePlannerRulesTests {

    private static final Logger LOGGER = LogManager.getLogger(PlanForkerTests.class);

    private static final Set<FieldType> SUPPORTED_TYPES = Set.of(
        FieldType.INTEGER, FieldType.LONG, FieldType.KEYWORD, FieldType.DATE, FieldType.BOOLEAN
    );

    private QueryDAG buildAndFork(int shardCount, RelNode logicalPlan) {
        MockLuceneBackend luceneWithScanAndAgg = new MockLuceneBackend() {
            @Override protected Set<ScanCapability> scanCapabilities() {
                return Set.of(new ScanCapability.DocValues(
                    Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), SUPPORTED_TYPES));
            }
            @Override protected Set<AggregateCapability> aggregateCapabilities() {
                return aggCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT),
                    Map.of(AggregateFunction.SUM, Set.of(FieldType.INTEGER),
                           AggregateFunction.COUNT, Set.of(FieldType.INTEGER)));
            }
        };
        var context = buildContextWithExplicitStorage(shardCount,
            duplicatedIntFields(),
            List.of(DATAFUSION, luceneWithScanAndAgg));
        LOGGER.info("Input RelNode:\n{}", RelOptUtil.toString(logicalPlan));
        RelNode cboOutput = runPlanner(logicalPlan, context);
        LOGGER.info("Marked+CBO RelNode:\n{}", RelOptUtil.toString(cboOutput));
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        LOGGER.info("QueryDAG after forking:\n{}", dag);
        return dag;
    }

    /**
     * Asserts a stage has exactly two alternatives (one per backend), each narrowed to a single backend.
     * TODO: extend with randomized tests that pick N backends from a pool and assert exactly N alternatives.
     */
    private static void assertTwoAlternatives(Stage stage, Class<? extends OpenSearchRelNode> expectedRootType) {
        List<StagePlan> alternatives = stage.getPlanAlternatives();
        assertEquals("expected two alternatives (one per viable backend)", 2, alternatives.size());
        for (StagePlan plan : alternatives) {
            assertNotNull(plan.resolvedFragment());
            assertTrue("resolved fragment root must be " + expectedRootType.getSimpleName(),
                expectedRootType.isInstance(plan.resolvedFragment()));
            assertEquals("viableBackends must be narrowed to single backend", 1,
                ((OpenSearchRelNode) plan.resolvedFragment()).getViableBackends().size());
            assertEquals(plan.backendId(),
                ((OpenSearchRelNode) plan.resolvedFragment()).getViableBackends().getFirst());
        }
        assertNotEquals("both alternatives must have distinct backends",
            alternatives.get(0).backendId(), alternatives.get(1).backendId());
    }

    /** Single-shard scan, filter, and aggregate — two alternatives each, one per backend. */
    public void testSingleStageQueryShapes() {
        QueryDAG scanDag = buildAndFork(1, stubScan(mockTable("test_index", "status", "size")));
        assertTwoAlternatives(scanDag.rootStage(), OpenSearchTableScan.class);

        QueryDAG filterDag = buildAndFork(1, LogicalFilter.create(
            stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)));
        assertTwoAlternatives(filterDag.rootStage(), OpenSearchFilter.class);

        QueryDAG aggDag = buildAndFork(1, makeAggregate(sumCall()));
        assertTwoAlternatives(aggDag.rootStage(), OpenSearchAggregate.class);
    }

    /** Multi-shard aggregate — child stage gets two alternatives, root gets one (only DF has ExchangeSinkProvider). */
    public void testMultiShardAggregateForksAllStages() {
        QueryDAG dag = buildAndFork(2, makeAggregate(sumCall()));

        // Root coordinator stage — only DF has ExchangeSinkProvider
        assertEquals(1, dag.rootStage().getPlanAlternatives().size());

        // Child data node stage — two alternatives
        Stage child = dag.rootStage().getChildStages().getFirst();
        assertTwoAlternatives(child, OpenSearchAggregate.class);
    }

    /**
     * Mixed aggregate: SUM(size), COUNT(*), SUM(status) — COUNT(*) has no field args.
     * All three get annotated. Verifies forking handles the mix without index misalignment.
     */
    public void testMixedAggCallsWithAndWithoutFieldArgs() {
        QueryDAG dag = buildAndFork(1,
            makeMultiCallAggregate(sumCall(), countStarCall(),
                AggregateCall.create(SqlStdOperatorTable.SUM, false, List.of(0), 0,
                    stubScan(mockTable("test_index", "status", "size")),
                    typeFactory.createSqlType(SqlTypeName.INTEGER), "total_status")));
        assertTwoAlternatives(dag.rootStage(), OpenSearchAggregate.class);
    }

    /**
     * Filter with AND of two annotated predicates — verifies tree-walk annotation
     * collection and replacement are consistent across multiple predicates.
     */
    public void testFilterWithMultipleAnnotatedPredicates() {
        QueryDAG dag = buildAndFork(1,
            LogicalFilter.create(stubScan(mockTable("test_index", "status", "size")),
                makeAnd(makeEquals(0, SqlTypeName.INTEGER, 200),
                    makeCall(SqlStdOperatorTable.GREATER_THAN,
                        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1),
                        rexBuilder.makeLiteral(100, typeFactory.createSqlType(SqlTypeName.INTEGER), true)))));
        assertTwoAlternatives(dag.rootStage(), OpenSearchFilter.class);
    }

    /**
     * Constant predicate (1=1) must be eliminated by ReduceExpressionsRule before marking.
     * The filter disappears entirely — the root of the marked tree is the scan, not a filter.
     */
    public void testConstantPredicateEliminated() {
        var context = buildContext("parquet", 1, intFields());
        RexNode constant = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), true));
        LogicalFilter filter = LogicalFilter.create(stubScan(mockTable("test_index", "status", "size")), constant);
        RelNode result = runPlanner(filter, context);
        // ReduceExpressionsRule folds 1=1 → TRUE, then filter on TRUE is removed
        assertFalse("filter on constant true must be eliminated", result instanceof OpenSearchFilter);
        assertTrue("root must be the scan after filter elimination", result instanceof OpenSearchTableScan);
    }
}
