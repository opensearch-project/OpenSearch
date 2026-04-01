/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.AggregateCallAnnotation;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.OperatorCapability;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for aggregate rule: per-call annotation, viable backends,
 * split behavior, and delegation.
 */
public class AggregateRuleTests extends BasePlannerRulesTests {

    // ---- Per-call annotation ----

    public void testPerCallAnnotation() {
        OpenSearchAggregate agg = runAggregate(1, sumCall());

        AggregateCallAnnotation annotation = AggregateCallAnnotation.find(agg.getAggCallList().getFirst());
        assertNotNull("AggregateCall should have annotation", annotation);
        assertTrue(annotation.getViableBackends().contains(MockDataFusionBackend.NAME));
    }

    public void testViableBackendsPopulated() {
        OpenSearchAggregate agg = runAggregate(1, sumCall());

        assertEquals(MockDataFusionBackend.NAME, agg.getBackend());
        assertFalse(agg.getViableBackends().isEmpty());
        assertTrue(agg.getViableBackends().contains(MockDataFusionBackend.NAME));
    }

    // ---- Split behavior ----

    public void testSplitOnMultiShard() {
        RelNode result = unwrapExchange(runPlanner(makeAggregate(5, sumCall()), defaultContext(5)));
        logger.info("Plan:\n{}", RelOptUtil.toString(result));

        assertTrue(result instanceof OpenSearchAggregate);
        OpenSearchAggregate finalAgg = (OpenSearchAggregate) result;
        assertEquals(AggregateMode.FINAL, finalAgg.getMode());

        assertTrue(finalAgg.getInput() instanceof OpenSearchExchangeReducer);
        OpenSearchExchangeReducer reducer = (OpenSearchExchangeReducer) finalAgg.getInput();

        assertTrue(reducer.getInput() instanceof OpenSearchAggregate);
        OpenSearchAggregate partialAgg = (OpenSearchAggregate) reducer.getInput();
        assertEquals(AggregateMode.PARTIAL, partialAgg.getMode());
        assertTrue(partialAgg.getInput() instanceof OpenSearchTableScan);
    }

    public void testSplitPreservesViableBackends() {
        RelNode result = unwrapExchange(runPlanner(makeAggregate(5, sumCall()), defaultContext(5)));

        OpenSearchAggregate finalAgg = (OpenSearchAggregate) result;
        OpenSearchAggregate partialAgg = (OpenSearchAggregate) ((OpenSearchExchangeReducer) finalAgg.getInput()).getInput();
        assertEquals(finalAgg.getViableBackends(), partialAgg.getViableBackends());
    }

    public void testNoSplitOnSingleShard() {
        OpenSearchAggregate agg = runAggregate(1, sumCall());

        assertEquals(AggregateMode.SINGLE, agg.getMode());
        assertTrue(agg.getInput() instanceof OpenSearchTableScan);
    }

    // ---- Delegation ----

    public void testAggregateErrorsWhenNoBackendSupportsFunction() {
        // Lucene-only backend with no AGGREGATE capability
        PlannerContext context = buildContext("parquet", 1, Map.of(
            "status", Map.of("type", "integer"),
            "size", Map.of("type", "integer")
        ), List.of(new MockDataFusionBackend() {
            @Override
            public Set<AggregateFunction> supportedAggregateFunctions() {
                return Set.of(); // supports AGGREGATE capability but no functions
            }
        }));

        LogicalAggregate aggregate = makeAggregate(1, sumCall());

        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> runPlanner(aggregate, context));
        assertTrue(exception.getMessage().contains("No backend supports aggregate function"));
    }

    public void testAggregateViableBackendsIntersection() {
        // Both backends support AGGREGATE + SUM
        MockLuceneBackend luceneWithAgg = new MockLuceneBackend() {
            @Override
            public Set<OperatorCapability> supportedOperators() {
                return Set.of(OperatorCapability.SCAN, OperatorCapability.FILTER, OperatorCapability.AGGREGATE);
            }
            @Override
            public Set<AggregateFunction> supportedAggregateFunctions() {
                return EnumSet.of(AggregateFunction.SUM, AggregateFunction.COUNT);
            }
        };

        PlannerContext context = buildContext("parquet", 1, Map.of(
            "status", Map.of("type", "integer"),
            "size", Map.of("type", "integer")
        ), List.of(DATAFUSION, luceneWithAgg));

        RelNode result = runPlanner(makeAggregate(1, sumCall()), context);
        assertTrue(result instanceof OpenSearchAggregate);
        OpenSearchAggregate agg = (OpenSearchAggregate) result;

        assertTrue(agg.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertTrue(agg.getViableBackends().contains(MockLuceneBackend.NAME));
    }

    // ---- Scan ----

    public void testTableScanResolvesBackendAndFieldStorage() {
        PlannerContext context = buildContext("parquet", Map.of(
            "status", Map.of("type", "integer"),
            "size", Map.of("type", "integer")
        ));

        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode result = unwrapExchange(runPlanner(stubScan(table), context));

        assertTrue(result instanceof OpenSearchTableScan);
        OpenSearchTableScan scan = (OpenSearchTableScan) result;
        assertEquals(MockDataFusionBackend.NAME, scan.getBackend());
        assertEquals(2, scan.getOutputFieldStorage().size());
        assertEquals("status", scan.getOutputFieldStorage().get(0).getFieldName());
        assertFalse(scan.getViableBackends().isEmpty());
    }

    // ---- Helpers ----

    private AggregateCall sumCall() {
        RelOptTable table = mockTable("test_index", "status", "size");
        return AggregateCall.create(
            SqlStdOperatorTable.SUM, false, List.of(1), 1, stubScan(table),
            typeFactory.createSqlType(SqlTypeName.INTEGER), "total_size"
        );
    }

    private LogicalAggregate makeAggregate(int shardCount, AggregateCall aggCall) {
        RelOptTable table = mockTable("test_index", "status", "size");
        return LogicalAggregate.create(
            stubScan(table), List.of(), ImmutableBitSet.of(0), null, List.of(aggCall)
        );
    }

    private PlannerContext defaultContext(int shardCount) {
        return buildContext("parquet", shardCount, Map.of(
            "status", Map.of("type", "integer"),
            "size", Map.of("type", "integer")
        ));
    }

    private OpenSearchAggregate runAggregate(int shardCount, AggregateCall aggCall) {
        PlannerContext context = defaultContext(shardCount);
        RelNode result = runPlanner(makeAggregate(shardCount, aggCall), context);
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        assertTrue("Expected OpenSearchAggregate, got " + result.getClass().getSimpleName(),
            result instanceof OpenSearchAggregate);
        return (OpenSearchAggregate) result;
    }

    private LogicalAggregate makeMultiCallAggregate(int shardCount, AggregateCall... aggCalls) {
        RelOptTable table = mockTable("test_index", "status", "size");
        return LogicalAggregate.create(
            stubScan(table), List.of(), ImmutableBitSet.of(0), null, List.of(aggCalls)
        );
    }

    private AggregateCall countCall() {
        RelOptTable table = mockTable("test_index", "status", "size");
        return AggregateCall.create(
            SqlStdOperatorTable.COUNT, false, List.of(1), 1, stubScan(table),
            typeFactory.createSqlType(SqlTypeName.BIGINT), "cnt"
        );
    }

    private AggregateCall stddevCall() {
        RelOptTable table = mockTable("test_index", "status", "size");
        return AggregateCall.create(
            SqlStdOperatorTable.STDDEV_POP, false, List.of(1), 1, stubScan(table),
            typeFactory.createSqlType(SqlTypeName.INTEGER), "stddev"
        );
    }

    // ---- Edge case: mixed viable backends per call ----

    /** SUM (both backends) + COUNT (DataFusion only if Lucene lacks AGGREGATE) → operator viable = DataFusion only. */
    public void testMixedPerCallViableBackends() {
        // Lucene with AGGREGATE + SUM only (no COUNT)
        MockLuceneBackend lucenePartialAgg = new MockLuceneBackend() {
            @Override
            public Set<OperatorCapability> supportedOperators() {
                return Set.of(OperatorCapability.SCAN, OperatorCapability.FILTER, OperatorCapability.AGGREGATE);
            }
            @Override
            public Set<AggregateFunction> supportedAggregateFunctions() {
                return EnumSet.of(AggregateFunction.SUM);  // no COUNT
            }
        };

        PlannerContext context = buildContext("parquet", 1, Map.of(
            "status", Map.of("type", "integer"),
            "size", Map.of("type", "integer")
        ), List.of(DATAFUSION, lucenePartialAgg));

        RelNode result = runPlanner(makeMultiCallAggregate(1, sumCall(), countCall()), context);
        assertTrue(result instanceof OpenSearchAggregate);
        OpenSearchAggregate agg = (OpenSearchAggregate) result;

        // SUM: both viable. COUNT: only DataFusion. Intersection = DataFusion only.
        assertTrue(agg.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse("Lucene should not be viable (missing COUNT)",
            agg.getViableBackends().contains(MockLuceneBackend.NAME));

        // Per-call: SUM should have both, COUNT should have only DataFusion
        AggregateCallAnnotation sumAnnotation = AggregateCallAnnotation.find(agg.getAggCallList().get(0));
        AggregateCallAnnotation countAnnotation = AggregateCallAnnotation.find(agg.getAggCallList().get(1));
        assertNotNull(sumAnnotation);
        assertNotNull(countAnnotation);
        assertEquals(2, sumAnnotation.getViableBackends().size());
        assertEquals(1, countAnnotation.getViableBackends().size());
    }

    // ---- Edge case: exchange passthrough ----

    /** Reducer should pass through child's viable backends. */
    public void testReducerPassthroughViableBackends() {
        PlannerContext context = buildContext("parquet", 5, Map.of(
            "status", Map.of("type", "integer"),
            "size", Map.of("type", "integer")
        ));

        RelNode result = unwrapExchange(runPlanner(makeAggregate(5, sumCall()), context));
        assertTrue(result instanceof OpenSearchAggregate);
        OpenSearchAggregate finalAgg = (OpenSearchAggregate) result;

        assertTrue(finalAgg.getInput() instanceof OpenSearchExchangeReducer);
        OpenSearchExchangeReducer reducer = (OpenSearchExchangeReducer) finalAgg.getInput();

        assertFalse(reducer.getViableBackends().isEmpty());
    }

    // ---- Delegation ----

    /** SUM (DF only) + STDDEV_POP (Lucene only) — with delegation, DF delegates STDDEV to Lucene. */
    public void testAggregateViableWithDelegation() {
        MockDataFusionBackend dfWithDelegation = new MockDataFusionBackend() {
            @Override
            public Set<AggregateFunction> supportedAggregateFunctions() {
                return EnumSet.of(AggregateFunction.SUM);
            }
            @Override
            public Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.AGGREGATE);
            }
        };
        MockLuceneBackend luceneAccepting = new MockLuceneBackend() {
            @Override
            public Set<OperatorCapability> supportedOperators() {
                return Set.of(OperatorCapability.SCAN, OperatorCapability.FILTER, OperatorCapability.AGGREGATE);
            }
            @Override
            public Set<AggregateFunction> supportedAggregateFunctions() {
                return EnumSet.of(AggregateFunction.STDDEV_POP);
            }
            @Override
            public Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.AGGREGATE);
            }
        };

        LogicalAggregate aggregate = makeMultiCallAggregate(1, sumCall(), stddevCall());

        PlannerContext context = buildContext("parquet", 1, Map.of(
            "status", Map.of("type", "integer"),
            "size", Map.of("type", "integer")
        ), List.of(dfWithDelegation, luceneAccepting));

        RelNode result = runPlanner(aggregate, context);
        assertTrue(result instanceof OpenSearchAggregate);
        OpenSearchAggregate agg = (OpenSearchAggregate) result;

        assertTrue(agg.getViableBackends().contains(MockDataFusionBackend.NAME));
    }

    /** Same setup without delegation — no backend can handle all calls → error. */
    public void testAggregateErrorsWithoutDelegation() {
        MockLuceneBackend luceneWithStddev = new MockLuceneBackend() {
            @Override
            public Set<OperatorCapability> supportedOperators() {
                return Set.of(OperatorCapability.SCAN, OperatorCapability.FILTER, OperatorCapability.AGGREGATE);
            }
            @Override
            public Set<AggregateFunction> supportedAggregateFunctions() {
                return EnumSet.of(AggregateFunction.STDDEV_POP);
            }
        };

        LogicalAggregate aggregate = makeMultiCallAggregate(1, sumCall(), stddevCall());

        PlannerContext context = buildContext("parquet", 1, Map.of(
            "status", Map.of("type", "integer"),
            "size", Map.of("type", "integer")
        ), List.of(DATAFUSION, luceneWithStddev));

        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> runPlanner(aggregate, context));
        assertTrue(exception.getMessage().contains("no delegation path exists"));
    }
}
