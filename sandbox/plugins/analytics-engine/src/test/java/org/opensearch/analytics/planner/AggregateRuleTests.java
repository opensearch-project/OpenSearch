/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.rel.AggregateCallAnnotation;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchProject;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.ScanCapability;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for aggregate rule: per-call annotation, viable backends,
 * split behavior, and delegation.
 */
public class AggregateRuleTests extends BasePlannerRulesTests {

    // ---- Per-call annotation ----

    /** Every agg call must have an annotation with non-empty viableBackends. */
    public void testPerCallAnnotation() {
        OpenSearchAggregate agg = runAggregate(1, sumCall());
        for (AggregateCall call : agg.getAggCallList()) {
            AggregateCallAnnotation annotation = AggregateCallAnnotation.find(call);
            assertNotNull("Every AggregateCall must have an annotation", annotation);
            assertFalse("Annotation viableBackends must not be empty", annotation.getViableBackends().isEmpty());
            assertTrue(annotation.getViableBackends().contains(MockDataFusionBackend.NAME));
        }
    }

    public void testViableBackendsPopulated() {
        OpenSearchAggregate agg = runAggregate(1, sumCall());
        assertPipelineViableBackends(
            agg,
            List.of(OpenSearchAggregate.class, OpenSearchTableScan.class),
            Set.of(MockDataFusionBackend.NAME)
        );
    }

    // ---- Split behavior ----

    public void testSplitOnMultiShard() {
        RelNode result = unwrapExchange(runPlanner(makeAggregate(sumCall()), defaultContext(5)));
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        // Full pipeline: FinalAgg → ExchangeReducer → PartialAgg → Scan
        assertPipelineViableBackends(
            result,
            List.of(OpenSearchAggregate.class, OpenSearchExchangeReducer.class, OpenSearchAggregate.class, OpenSearchTableScan.class),
            Set.of(MockDataFusionBackend.NAME)
        );
        OpenSearchAggregate finalAgg = (OpenSearchAggregate) unwrapRootReducer(result);
        assertEquals(AggregateMode.FINAL, finalAgg.getMode());
        OpenSearchAggregate partialAgg = (OpenSearchAggregate) finalAgg.getInputs().get(0).getInputs().get(0);
        assertEquals(AggregateMode.PARTIAL, partialAgg.getMode());
    }

    public void testNoSplitOnSingleShard() {
        OpenSearchAggregate agg = runAggregate(1, sumCall());
        assertEquals(AggregateMode.SINGLE, agg.getMode());
        assertPipelineViableBackends(
            agg,
            List.of(OpenSearchAggregate.class, OpenSearchTableScan.class),
            Set.of(MockDataFusionBackend.NAME)
        );
    }

    // ---- Error cases ----

    public void testAggregateErrorsWhenNoBackendSupportsFunction() {
        MockDataFusionBackend noAggFunctions = new MockDataFusionBackend() {
            @Override
            protected Set<AggregateCapability> aggregateCapabilities() {
                return Set.of();
            }
        };
        PlannerContext context = buildContext("parquet", 1, intFields(), List.of(noAggFunctions));
        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(makeAggregate(sumCall()), context));
        assertTrue(exception.getMessage().contains("No backend supports aggregate function"));
    }

    public void testAggregateViableBackendsIntersection() {
        MockLuceneBackend luceneWithAgg = new MockLuceneBackend() {
            @Override
            protected Set<AggregateCapability> aggregateCapabilities() {
                return aggCaps(
                    Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT),
                    Map.of(AggregateFunction.SUM, Set.of(FieldType.INTEGER), AggregateFunction.COUNT, Set.of(FieldType.INTEGER))
                );
            }
        };
        PlannerContext context = buildContextWithExplicitStorage(1, duplicatedIntFields(), List.of(DATAFUSION, luceneWithAgg));
        RelNode result = runPlanner(makeAggregate(sumCall()), context);
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        assertPipelineViableBackends(
            result,
            List.of(OpenSearchAggregate.class, OpenSearchTableScan.class),
            Set.of(MockDataFusionBackend.NAME)
        );
        OpenSearchAggregate agg = (OpenSearchAggregate) unwrapRootReducer(result);
        assertFalse(agg.getViableBackends().contains(MockLuceneBackend.NAME));
        // Per-call annotation includes both — Lucene is viable for SUM on this field
        assertCallAnnotation(agg.getAggCallList().get(0), MockDataFusionBackend.NAME, MockLuceneBackend.NAME);
    }

    /**
     * Variation of testAggregateViableBackendsIntersection where Lucene also declares
     * scan capability — both backends viable for scan AND agg → operator-level includes both.
     */
    public void testAggregateViableBackendsIntersectionWithLuceneScan() {
        MockLuceneBackend luceneWithScanAndAgg = new MockLuceneBackend() {
            @Override
            protected Set<ScanCapability> scanCapabilities() {
                return Set.of(new ScanCapability.DocValues(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), Set.of(FieldType.INTEGER)));
            }

            @Override
            protected Set<AggregateCapability> aggregateCapabilities() {
                return aggCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), Map.of(AggregateFunction.SUM, Set.of(FieldType.INTEGER)));
            }
        };
        PlannerContext context = buildContextWithExplicitStorage(1, duplicatedIntFields(), List.of(DATAFUSION, luceneWithScanAndAgg));
        RelNode result = runPlanner(makeAggregate(sumCall()), context);
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        assertPipelineViableBackends(
            result,
            List.of(OpenSearchAggregate.class, OpenSearchTableScan.class),
            Set.of(MockDataFusionBackend.NAME, MockLuceneBackend.NAME)
        );
        OpenSearchAggregate agg = (OpenSearchAggregate) unwrapRootReducer(result);
        assertTrue(agg.getViableBackends().contains(MockLuceneBackend.NAME));
        assertCallAnnotation(agg.getAggCallList().get(0), MockDataFusionBackend.NAME, MockLuceneBackend.NAME);
    }

    // ---- Composed pipeline shapes ----

    /**
     * Aggregate(Filter(Scan)) — most common OLAP shape. Verifies annotation propagation
     * through filter→aggregate at every level.
     */
    public void testAggregateOnFilteredScan() {
        RelNode result = runPlanner(
            makeAggregate(
                makeFilter(stubScan(mockTable("test_index", "status", "size")), makeEquals(0, SqlTypeName.INTEGER, 200)),
                sumCall()
            ),
            defaultContext(1)
        );
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        assertPipelineViableBackends(
            result,
            List.of(OpenSearchAggregate.class, OpenSearchFilter.class, OpenSearchTableScan.class),
            Set.of(MockDataFusionBackend.NAME)
        );
    }

    // ---- Mixed per-call viable backends ----

    public void testMixedPerCallViableBackends() {
        MockLuceneBackend lucenePartialAgg = new MockLuceneBackend() {
            @Override
            protected Set<AggregateCapability> aggregateCapabilities() {
                return aggCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), Map.of(AggregateFunction.SUM, Set.of(FieldType.INTEGER)));
            }
        };
        PlannerContext context = buildContextWithExplicitStorage(1, duplicatedIntFields(), List.of(DATAFUSION, lucenePartialAgg));
        RelNode result = runPlanner(makeMultiCallAggregate(sumCall(), countCall()), context);
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        assertPipelineViableBackends(
            result,
            List.of(OpenSearchAggregate.class, OpenSearchTableScan.class),
            Set.of(MockDataFusionBackend.NAME)
        );
        OpenSearchAggregate agg = (OpenSearchAggregate) unwrapRootReducer(result);
        assertFalse(
            "Lucene not viable at operator level — can handle SUM but not COUNT",
            agg.getViableBackends().contains(MockLuceneBackend.NAME)
        );
        assertCallAnnotation(agg.getAggCallList().get(0), MockDataFusionBackend.NAME, MockLuceneBackend.NAME);
        assertCallAnnotation(agg.getAggCallList().get(1), MockDataFusionBackend.NAME);
        assertEquals(
            "SUM viable for both backends",
            2,
            AggregateCallAnnotation.find(agg.getAggCallList().get(0)).getViableBackends().size()
        );
        assertEquals(
            "COUNT viable for DF only (Lucene not declared)",
            1,
            AggregateCallAnnotation.find(agg.getAggCallList().get(1)).getViableBackends().size()
        );
    }

    // ---- Delegation ----

    public void testAggregateViableWithDelegation() {
        MockDataFusionBackend dfWithDelegation = new MockDataFusionBackend() {
            @Override
            protected Set<AggregateCapability> aggregateCapabilities() {
                return aggCaps(Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT), Map.of(AggregateFunction.SUM, Set.of(FieldType.INTEGER)));
            }

            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.AGGREGATE);
            }
        };
        MockLuceneBackend luceneAccepting = new MockLuceneBackend() {
            @Override
            protected Set<AggregateCapability> aggregateCapabilities() {
                return aggCaps(
                    Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT),
                    Map.of(AggregateFunction.STDDEV_POP, Set.of(FieldType.INTEGER))
                );
            }

            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.AGGREGATE);
            }
        };
        PlannerContext context = buildContext("parquet", 1, intFields(), List.of(dfWithDelegation, luceneAccepting));
        RelNode result = runPlanner(makeMultiCallAggregate(sumCall(), stddevCall()), context);
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        // OpenSearchAggregateReduceRule decomposes STDDEV_POP into SUM+COUNT wrapped in
        // Project(sqrt) above / Project(squared-inputs) below the Aggregate.
        assertPipelineViableBackends(
            result,
            List.of(OpenSearchProject.class, OpenSearchAggregate.class, OpenSearchProject.class, OpenSearchTableScan.class),
            Set.of(MockDataFusionBackend.NAME)
        );
    }

    public void testAggregateErrorsWithoutDelegation() {
        // DF declares only COUNT — can't satisfy STDDEV_POP's reduction (needs SUM(x) and
        // SUM(x*x)) on its own. Lucene has SUM but refuses delegation.
        MockDataFusionBackend dfNoSum = new MockDataFusionBackend() {
            @Override
            protected Set<AggregateCapability> aggregateCapabilities() {
                return aggCaps(
                    Set.of(MockDataFusionBackend.PARQUET_DATA_FORMAT),
                    Map.of(AggregateFunction.COUNT, Set.of(FieldType.INTEGER))
                );
            }
        };
        MockLuceneBackend luceneWithSum = new MockLuceneBackend() {
            @Override
            protected Set<AggregateCapability> aggregateCapabilities() {
                return aggCaps(Set.of(MockLuceneBackend.LUCENE_DATA_FORMAT), Map.of(AggregateFunction.SUM, Set.of(FieldType.INTEGER)));
            }
            // No acceptedDelegations() override → delegation is refused.
        };
        PlannerContext context = buildContext("parquet", 1, intFields(), List.of(dfNoSum, luceneWithSum));
        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> runPlanner(makeMultiCallAggregate(sumCall(), stddevCall()), context)
        );
        assertTrue(exception.getMessage().contains("No backend supports aggregate function"));
    }

    // ---- Helpers ----

    private AggregateCall countCall() {
        return AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            List.of(1),
            1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "cnt"
        );
    }

    private AggregateCall stddevCall() {
        return AggregateCall.create(
            SqlStdOperatorTable.STDDEV_POP,
            false,
            List.of(1),
            1,
            stubScan(mockTable("test_index", "status", "size")),
            typeFactory.createSqlType(SqlTypeName.INTEGER),
            "stddev"
        );
    }

    private PlannerContext defaultContext(int shardCount) {
        return buildContext("parquet", shardCount, intFields());
    }

    private void assertCallAnnotation(AggregateCall call, String... expectedBackends) {
        AggregateCallAnnotation annotation = AggregateCallAnnotation.find(call);
        assertNotNull("AggregateCall must have annotation", annotation);
        for (String backend : expectedBackends)
            assertTrue("Annotation must contain backend " + backend, annotation.getViableBackends().contains(backend));
    }

    private OpenSearchAggregate runAggregate(int shardCount, AggregateCall aggCall) {
        RelNode result = runPlanner(makeAggregate(aggCall), defaultContext(shardCount));
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        // The planner now emits a top-level ExchangeReducer to materialize the coord-side
        // EXECUTION(SINGLETON) requirement. Peel it off for aggregate-centric assertions.
        if (result instanceof org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer er) {
            result = er.getInput();
        }
        assertTrue("Expected OpenSearchAggregate, got " + result.getClass().getSimpleName(), result instanceof OpenSearchAggregate);
        return (OpenSearchAggregate) unwrapRootReducer(result);
    }
}
