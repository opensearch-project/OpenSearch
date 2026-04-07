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
import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.OperatorCapability;

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

        assertTrue(agg.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse(agg.getViableBackends().isEmpty());
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

    // ---- Error cases ----

    public void testAggregateErrorsWhenNoBackendSupportsFunction() {
        BackendCapabilityProvider noAggFunctions = new BackendCapabilityProvider() {
            @Override public Set<OperatorCapability> supportedOperators() { return DATAFUSION.getCapabilityProvider().supportedOperators(); }
            @Override public Set<FilterCapability> filterCapabilities() { return DATAFUSION.getCapabilityProvider().filterCapabilities(); }
            @Override public Set<AggregateCapability> aggregateCapabilities() { return Set.of(); }
        };

        PlannerContext context = buildContext(
            "parquet", 1,
            Map.of("status", Map.of("type", "integer"), "size", Map.of("type", "integer")),
            List.of(DATAFUSION.withCapabilityProvider(noAggFunctions))
        );

        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> runPlanner(makeAggregate(1, sumCall()), context));
        assertTrue(exception.getMessage().contains("No backend supports aggregate function"));
    }

    public void testAggregateViableBackendsIntersection() {
        BackendCapabilityProvider luceneWithAgg = new BackendCapabilityProvider() {
            @Override public Set<OperatorCapability> supportedOperators() {
                return Set.of(OperatorCapability.SCAN, OperatorCapability.FILTER, OperatorCapability.AGGREGATE);
            }
            @Override public Set<FilterCapability> filterCapabilities() { return LUCENE.getCapabilityProvider().filterCapabilities(); }
            @Override public Set<AggregateCapability> aggregateCapabilities() {
                return aggCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT),
                    Map.of(AggregateFunction.SUM, Set.of(FieldType.INTEGER), AggregateFunction.COUNT, Set.of(FieldType.INTEGER)));
            }
        };

        PlannerContext context = buildContext(
            "parquet", 1,
            Map.of("status", Map.of("type", "integer"), "size", Map.of("type", "integer")),
            List.of(DATAFUSION, LUCENE.withCapabilityProvider(luceneWithAgg))
        );

        RelNode result = runPlanner(makeAggregate(1, sumCall()), context);
        assertTrue(result instanceof OpenSearchAggregate);
        OpenSearchAggregate agg = (OpenSearchAggregate) result;

        assertTrue(agg.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse(agg.getViableBackends().contains(MockLuceneBackend.NAME));
        AggregateCallAnnotation annotation = AggregateCallAnnotation.find(agg.getAggCallList().get(0));
        assertNotNull(annotation);
        assertTrue(annotation.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertTrue(annotation.getViableBackends().contains(MockLuceneBackend.NAME));
    }

    // ---- Scan ----

    public void testTableScanResolvesBackendAndFieldStorage() {
        PlannerContext context = buildContext("parquet",
            Map.of("status", Map.of("type", "integer"), "size", Map.of("type", "integer")));

        RelOptTable table = mockTable("test_index", "status", "size");
        RelNode result = unwrapExchange(runPlanner(stubScan(table), context));

        assertTrue(result instanceof OpenSearchTableScan);
        OpenSearchTableScan scan = (OpenSearchTableScan) result;
        assertTrue(scan.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertEquals(2, scan.getOutputFieldStorage().size());
        assertEquals("status", scan.getOutputFieldStorage().get(0).getFieldName());
    }

    // ---- Mixed per-call viable backends ----

    public void testMixedPerCallViableBackends() {
        BackendCapabilityProvider lucenePartialAgg = new BackendCapabilityProvider() {
            @Override public Set<OperatorCapability> supportedOperators() {
                return Set.of(OperatorCapability.SCAN, OperatorCapability.FILTER, OperatorCapability.AGGREGATE);
            }
            @Override public Set<FilterCapability> filterCapabilities() { return LUCENE.getCapabilityProvider().filterCapabilities(); }
            @Override public Set<AggregateCapability> aggregateCapabilities() {
                return aggCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT),
                    Map.of(AggregateFunction.SUM, Set.of(FieldType.INTEGER)));
            }
        };

        PlannerContext context = buildContext(
            "parquet", 1,
            Map.of("status", Map.of("type", "integer"), "size", Map.of("type", "integer")),
            List.of(DATAFUSION, LUCENE.withCapabilityProvider(lucenePartialAgg))
        );

        RelNode result = runPlanner(makeMultiCallAggregate(1, sumCall(), countCall()), context);
        assertTrue(result instanceof OpenSearchAggregate);
        OpenSearchAggregate agg = (OpenSearchAggregate) result;

        assertTrue(agg.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse("Lucene should not be viable (missing COUNT)", agg.getViableBackends().contains(MockLuceneBackend.NAME));

        AggregateCallAnnotation sumAnnotation = AggregateCallAnnotation.find(agg.getAggCallList().get(0));
        AggregateCallAnnotation countAnnotation = AggregateCallAnnotation.find(agg.getAggCallList().get(1));
        assertNotNull(sumAnnotation);
        assertNotNull(countAnnotation);
        assertEquals(2, sumAnnotation.getViableBackends().size());
        assertEquals(1, countAnnotation.getViableBackends().size());
    }

    // ---- Exchange passthrough ----

    public void testReducerPassthroughViableBackends() {
        PlannerContext context = buildContext("parquet", 5,
            Map.of("status", Map.of("type", "integer"), "size", Map.of("type", "integer")));

        RelNode result = unwrapExchange(runPlanner(makeAggregate(5, sumCall()), context));
        OpenSearchAggregate finalAgg = (OpenSearchAggregate) result;
        OpenSearchExchangeReducer reducer = (OpenSearchExchangeReducer) finalAgg.getInput();
        assertFalse(reducer.getViableBackends().isEmpty());
    }

    // ---- Delegation ----

    public void testAggregateViableWithDelegation() {
        BackendCapabilityProvider dfWithDelegation = new BackendCapabilityProvider() {
            @Override public Set<OperatorCapability> supportedOperators() { return DATAFUSION.getCapabilityProvider().supportedOperators(); }
            @Override public Set<FilterCapability> filterCapabilities() { return DATAFUSION.getCapabilityProvider().filterCapabilities(); }
            @Override public Set<AggregateCapability> aggregateCapabilities() {
                return aggCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT),
                    Map.of(AggregateFunction.SUM, Set.of(FieldType.INTEGER)));
            }
            @Override public Set<DelegationType> supportedDelegations() { return Set.of(DelegationType.AGGREGATE); }
        };
        BackendCapabilityProvider luceneAccepting = new BackendCapabilityProvider() {
            @Override public Set<OperatorCapability> supportedOperators() {
                return Set.of(OperatorCapability.SCAN, OperatorCapability.FILTER, OperatorCapability.AGGREGATE);
            }
            @Override public Set<FilterCapability> filterCapabilities() { return LUCENE.getCapabilityProvider().filterCapabilities(); }
            @Override public Set<AggregateCapability> aggregateCapabilities() {
                return aggCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT),
                    Map.of(AggregateFunction.STDDEV_POP, Set.of(FieldType.INTEGER)));
            }
            @Override public Set<DelegationType> acceptedDelegations() { return Set.of(DelegationType.AGGREGATE); }
        };

        PlannerContext context = buildContext(
            "parquet", 1,
            Map.of("status", Map.of("type", "integer"), "size", Map.of("type", "integer")),
            List.of(DATAFUSION.withCapabilityProvider(dfWithDelegation), LUCENE.withCapabilityProvider(luceneAccepting))
        );

        RelNode result = runPlanner(makeMultiCallAggregate(1, sumCall(), stddevCall()), context);
        assertTrue(result instanceof OpenSearchAggregate);
        assertTrue(((OpenSearchAggregate) result).getViableBackends().contains(MockDataFusionBackend.NAME));
    }

    public void testAggregateErrorsWithoutDelegation() {
        BackendCapabilityProvider luceneWithStddev = new BackendCapabilityProvider() {
            @Override public Set<OperatorCapability> supportedOperators() {
                return Set.of(OperatorCapability.SCAN, OperatorCapability.FILTER, OperatorCapability.AGGREGATE);
            }
            @Override public Set<FilterCapability> filterCapabilities() { return LUCENE.getCapabilityProvider().filterCapabilities(); }
            @Override public Set<AggregateCapability> aggregateCapabilities() {
                return aggCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT),
                    Map.of(AggregateFunction.STDDEV_POP, Set.of(FieldType.INTEGER)));
            }
        };

        PlannerContext context = buildContext(
            "parquet", 1,
            Map.of("status", Map.of("type", "integer"), "size", Map.of("type", "integer")),
            List.of(DATAFUSION, LUCENE.withCapabilityProvider(luceneWithStddev))
        );

        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> runPlanner(makeMultiCallAggregate(1, sumCall(), stddevCall()), context));
        assertTrue(exception.getMessage().contains("not supported by any viable backend"));
    }

    // ---- Helpers ----

    private AggregateCall sumCall() {
        RelOptTable table = mockTable("test_index", "status", "size");
        return AggregateCall.create(SqlStdOperatorTable.SUM, false, List.of(1), 1, stubScan(table),
            typeFactory.createSqlType(SqlTypeName.INTEGER), "total_size");
    }

    private AggregateCall countCall() {
        RelOptTable table = mockTable("test_index", "status", "size");
        return AggregateCall.create(SqlStdOperatorTable.COUNT, false, List.of(1), 1, stubScan(table),
            typeFactory.createSqlType(SqlTypeName.BIGINT), "cnt");
    }

    private AggregateCall stddevCall() {
        RelOptTable table = mockTable("test_index", "status", "size");
        return AggregateCall.create(SqlStdOperatorTable.STDDEV_POP, false, List.of(1), 1, stubScan(table),
            typeFactory.createSqlType(SqlTypeName.INTEGER), "stddev");
    }

    private LogicalAggregate makeAggregate(int shardCount, AggregateCall aggCall) {
        RelOptTable table = mockTable("test_index", "status", "size");
        return LogicalAggregate.create(stubScan(table), List.of(), ImmutableBitSet.of(0), null, List.of(aggCall));
    }

    private LogicalAggregate makeMultiCallAggregate(int shardCount, AggregateCall... aggCalls) {
        RelOptTable table = mockTable("test_index", "status", "size");
        return LogicalAggregate.create(stubScan(table), List.of(), ImmutableBitSet.of(0), null, List.of(aggCalls));
    }

    private PlannerContext defaultContext(int shardCount) {
        return buildContext("parquet", shardCount,
            Map.of("status", Map.of("type", "integer"), "size", Map.of("type", "integer")));
    }

    private OpenSearchAggregate runAggregate(int shardCount, AggregateCall aggCall) {
        RelNode result = runPlanner(makeAggregate(shardCount, aggCall), defaultContext(shardCount));
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        assertTrue("Expected OpenSearchAggregate", result instanceof OpenSearchAggregate);
        return (OpenSearchAggregate) result;
    }

    private static Set<AggregateCapability> aggCaps(Set<String> formats, Map<AggregateFunction, Set<FieldType>> funcToTypes) {
        Set<AggregateCapability> caps = new java.util.HashSet<>();
        for (var entry : funcToTypes.entrySet()) {
            for (FieldType type : entry.getValue()) {
                caps.add(new AggregateCapability(entry.getKey(), type, formats));
            }
        }
        return caps;
    }
}
