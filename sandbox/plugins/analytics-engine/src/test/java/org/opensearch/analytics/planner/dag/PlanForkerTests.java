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
import org.opensearch.analytics.planner.MockLuceneBackend;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchRelNode;
import org.opensearch.analytics.planner.rel.OpenSearchSort;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.EngineCapability;
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
        FieldType.INTEGER,
        FieldType.LONG,
        FieldType.KEYWORD,
        FieldType.DATE,
        FieldType.BOOLEAN
    );

    private QueryDAG buildAndFork(int shardCount, RelNode logicalPlan) {
        MockLuceneBackend luceneWithScanAndAgg = new MockLuceneBackend() {
            @Override
            protected Set<ScanCapability> scanCapabilities() {
                return Set.of(new ScanCapability.DocValues(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), SUPPORTED_TYPES));
            }

            @Override
            protected Set<AggregateCapability> aggregateCapabilities() {
                return aggCaps(
                    Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT),
                    Map.of(AggregateFunction.SUM, Set.of(FieldType.INTEGER), AggregateFunction.COUNT, Set.of(FieldType.INTEGER))
                );
            }

            @Override
            protected Set<EngineCapability> supportedEngineCapabilities() {
                return Set.of(EngineCapability.SORT);
            }
        };
        var context = buildContextWithExplicitStorage(shardCount, duplicatedIntFields(), List.of(DATAFUSION, luceneWithScanAndAgg));
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
     * TODO: add delegation-aware forking tests once delegation is implemented — verify that when
     * annotation viable backends differ from operator backend, one plan per annotation target is generated
     * (e.g. DF operator with Lucene annotation for filter delegation produces a separate alternative).
     */
    private static void assertTwoAlternatives(Stage stage, Class<? extends OpenSearchRelNode> expectedRootType) {
        List<StagePlan> alternatives = stage.getPlanAlternatives();
        assertEquals("expected two alternatives (one per viable backend)", 2, alternatives.size());
        for (StagePlan plan : alternatives) {
            assertNotNull(plan.resolvedFragment());
            assertTrue(
                "resolved fragment root must be " + expectedRootType.getSimpleName(),
                expectedRootType.isInstance(plan.resolvedFragment())
            );
            assertEquals(
                "viableBackends must be narrowed to single backend",
                1,
                ((OpenSearchRelNode) plan.resolvedFragment()).getViableBackends().size()
            );
            assertEquals(plan.backendId(), ((OpenSearchRelNode) plan.resolvedFragment()).getViableBackends().getFirst());
        }
        assertNotEquals("both alternatives must have distinct backends", alternatives.get(0).backendId(), alternatives.get(1).backendId());
    }

    /** Single-shard scan, filter, and aggregate — two alternatives each, one per backend. */
    public void testSingleStageQueryShapes() {
        QueryDAG scanDag = buildAndFork(1, stubScan(mockTable("test_index", "status", "size")));
        assertTwoAlternatives(scanDag.rootStage(), OpenSearchTableScan.class);

        QueryDAG filterDag = buildAndFork(
            1,
            LogicalFilter.create(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200))
        );
        assertTwoAlternatives(filterDag.rootStage(), OpenSearchFilter.class);

        QueryDAG aggDag = buildAndFork(1, makeAggregate(sumCall()));
        assertTwoAlternatives(aggDag.rootStage(), OpenSearchAggregate.class);
    }

    /**
     * Sort(Filter(Scan)) and Sort(Agg(Filter(Scan))) with limit — collated Sort always requires
     * EXECUTION(SINGLETON), so the plan has an ER between the Sort and the scan subtree.
     * Forking at the root stage is limited to backends that can reduce (only mock-parquet in
     * these tests) → one alternative per stage, not two.
     */
    public void testSortQueryShapes() {
        // Sort(Filter(Scan)) with limit — multi-shard so root-demand SINGLETON requires a gather,
        // which narrows the root stage to reduce-capable backends.
        QueryDAG sortFilterDag = buildAndFork(
            3,
            makeSort(makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)), 10)
        );
        assertEquals(1, sortFilterDag.rootStage().getPlanAlternatives().size());
        for (StagePlan plan : sortFilterDag.rootStage().getPlanAlternatives()) {
            assertTrue(plan.resolvedFragment() instanceof OpenSearchSort);
        }

        // Sort(Agg(Filter(Scan))) with limit — multi-shard so split fires, root stage is FINAL+Sort
        // over an ER, narrowed to reduce-capable backends.
        QueryDAG sortAggDag = buildAndFork(
            3,
            makeSort(
                makeAggregate(
                    makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
                    sumCall()
                ),
                10
            )
        );
        assertEquals(1, sortAggDag.rootStage().getPlanAlternatives().size());
        for (StagePlan plan : sortAggDag.rootStage().getPlanAlternatives()) {
            assertTrue(plan.resolvedFragment() instanceof OpenSearchSort);
        }
    }

    /**
     * Aggregate(Filter(Scan)) — most common OLAP shape. Verifies that forking narrows
     * annotations consistently through the entire tree: both the aggregate root and the
     * filter child in each alternative must be narrowed to the same single backend.
     *
     * TODO: with delegation, a DF aggregate over a Lucene-delegated filter would produce
     * alternatives where operator backend ≠ annotation backend — this assertion will need
     * to be relaxed or split per delegation strategy.
     */
    public void testComposedPipelineForking() {
        RelNode pipeline = makeAggregate(
            makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
            sumCall()
        );
        QueryDAG dag = buildAndFork(1, pipeline);
        assertTwoAlternatives(dag.rootStage(), OpenSearchAggregate.class);

        // Each alternative's full pipeline must be narrowed to the same single backend
        for (StagePlan plan : dag.rootStage().getPlanAlternatives()) {
            assertPipelineViableBackends(
                plan.resolvedFragment(),
                List.of(OpenSearchAggregate.class, OpenSearchFilter.class, OpenSearchTableScan.class),
                Set.of(plan.backendId())
            );
        }
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
        QueryDAG dag = buildAndFork(
            1,
            makeMultiCallAggregate(
                sumCall(),
                countStarCall(),
                AggregateCall.create(
                    SqlStdOperatorTable.SUM,
                    false,
                    List.of(0),
                    0,
                    stubScan(mockTable("test_index", "status", "size")),
                    typeFactory.createSqlType(SqlTypeName.INTEGER),
                    "total_status"
                )
            )
        );
        assertTwoAlternatives(dag.rootStage(), OpenSearchAggregate.class);
    }

    /**
     * Filter with AND of two annotated predicates — verifies tree-walk annotation
     * collection and replacement are consistent across multiple predicates.
     */
    public void testFilterWithMultipleAnnotatedPredicates() {
        QueryDAG dag = buildAndFork(
            1,
            LogicalFilter.create(
                stubScan(mockTable("test_index", "status", "size")),
                makeAnd(
                    makeEquals(0, SqlTypeName.INTEGER, 200),
                    makeCall(
                        SqlStdOperatorTable.GREATER_THAN,
                        rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1),
                        rexBuilder.makeLiteral(100, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
                    )
                )
            )
        );
        assertTwoAlternatives(dag.rootStage(), OpenSearchFilter.class);
    }

    /**
     * Constant predicate (1=1) must be eliminated by ReduceExpressionsRule before marking.
     * The filter disappears entirely — the root of the marked tree is the scan, not a filter.
     */
    public void testConstantPredicateEliminated() {
        var context = buildContext("parquet", 1, intFields());
        RexNode constant = rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        LogicalFilter filter = LogicalFilter.create(stubScan(mockTable("test_index", "status", "size")), constant);
        RelNode result = runPlanner(filter, context);
        // ReduceExpressionsRule folds 1=1 → TRUE, then filter on TRUE is removed.
        assertFalse("filter on constant true must be eliminated", result instanceof OpenSearchFilter);
        assertTrue("root must be the scan after filter elimination", result instanceof OpenSearchTableScan);
    }
}
