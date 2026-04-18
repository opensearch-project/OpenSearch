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
import org.opensearch.analytics.planner.FieldStorageInfo;
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
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.routing.GroupShardsIterator;
import org.opensearch.cluster.routing.OperationRouting;
import org.opensearch.cluster.routing.ShardIterator;
import org.opensearch.cluster.service.ClusterService;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    private ClusterService mockClusterService() {
        ClusterService clusterService = mock(ClusterService.class);
        ClusterState clusterState = mock(ClusterState.class);
        OperationRouting routing = mock(OperationRouting.class);
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.operationRouting()).thenReturn(routing);
        when(routing.searchShards(any(), any(), any(), any()))
            .thenReturn(new GroupShardsIterator<ShardIterator>(List.of()));
        return clusterService;
    }

    /**
     * Builds with two backends (DF + Lucene with scan + aggregate) using duplicated doc values.
     * Both backends are viable for scan and aggregate — forking should produce two alternatives.
     */
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
            duplicatedIntFields(MockDataFusionBackend.PARQUET_DATA_FORMAT, MockLuceneBackend.LUCENE_DATA_FORMAT),
            List.of(DATAFUSION, luceneWithScanAndAgg));
        LOGGER.info("Input RelNode:\n{}", RelOptUtil.toString(logicalPlan));
        RelNode cboOutput = runPlanner(logicalPlan, context);
        LOGGER.info("Marked+CBO RelNode:\n{}", RelOptUtil.toString(cboOutput));
        QueryDAG dag = DAGBuilder.build(cboOutput, context.getCapabilityRegistry(), mockClusterService());
        PlanForker.forkAll(dag, context.getCapabilityRegistry());
        LOGGER.info("QueryDAG after forking:\n{}", dag);
        return dag;
    }

    /** Duplicated int fields — doc values in both parquet and lucene. */
    private static Map<String, FieldStorageInfo> duplicatedIntFields(String format1, String format2) {
        return Map.of(
            "status", new FieldStorageInfo("status", "integer", FieldType.INTEGER,
                List.of(format1, format2), List.of(), List.of(), false),
            "size", new FieldStorageInfo("size", "integer", FieldType.INTEGER,
                List.of(format1, format2), List.of(), List.of(), false)
        );
    }

    /**
     * Asserts a stage has exactly two alternatives (one per backend), each narrowed to a single backend.
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

    /** Single-shard scan — two alternatives (one per backend), each narrowed. */
    public void testSingleShardScanTwoAlternatives() {
        QueryDAG dag = buildAndFork(1, stubScan(mockTable("test_index", "status", "size")));
        assertTwoAlternatives(dag.rootStage(), OpenSearchTableScan.class);
    }

    /** Single-shard filter — two alternatives, each narrowed to one backend. */
    public void testSingleShardFilterTwoAlternatives() {
        QueryDAG dag = buildAndFork(1,
            LogicalFilter.create(stubScan(mockTable("test_index", "status", "size")),
                makeEquals(0, SqlTypeName.INTEGER, 200)));
        assertTwoAlternatives(dag.rootStage(), OpenSearchFilter.class);
    }

    /** Single-shard aggregate — two alternatives, each narrowed to one backend. */
    public void testSingleShardAggregateTwoAlternatives() {
        QueryDAG dag = buildAndFork(1, makeAggregate(1, sumCall()));
        assertTwoAlternatives(dag.rootStage(), OpenSearchAggregate.class);
    }

    /** Multi-shard aggregate — child stage gets two alternatives, root gets one (only DF has ExchangeSinkProvider). */
    public void testMultiShardAggregateForksAllStages() {
        QueryDAG dag = buildAndFork(2, makeAggregate(2, sumCall()));

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
            makeMultiCallAggregate(1, sumCall(), countStarCall(),
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
     * Constant predicate (1=1) must throw — ReduceExpressionsRule should eliminate it
     * before marking. Verifies the gap is surfaced rather than silently misrouted.
     */
    public void testConstantPredicateThrows() {
        var context = buildContext("parquet", 1, Map.of(
            "status", Map.of("type", "integer"), "size", Map.of("type", "integer")));
        RexNode constant = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), true),
            rexBuilder.makeLiteral(1, typeFactory.createSqlType(SqlTypeName.INTEGER), true));
        LogicalFilter filter = LogicalFilter.create(stubScan(mockTable("test_index", "status", "size")), constant);
        expectThrows(UnsupportedOperationException.class, () -> runPlanner(filter, context));
    }
}
