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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlBasicAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
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
        for (int i = 0; i < agg.getAggCallList().size(); i++) {
            AggregateCallAnnotation annotation = agg.getCallAnnotations().get(i);
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
        UnsupportedFunctionException exception = expectThrows(
            UnsupportedFunctionException.class,
            () -> runPlanner(makeAggregate(sumCall()), context)
        );
        assertTrue(exception.getMessage().contains("is not currently supported"));
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
        assertCallAnnotation(agg, 0, MockDataFusionBackend.NAME, MockLuceneBackend.NAME);
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
        assertCallAnnotation(agg, 0, MockDataFusionBackend.NAME, MockLuceneBackend.NAME);
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
        assertCallAnnotation(agg, 0, MockDataFusionBackend.NAME, MockLuceneBackend.NAME);
        assertCallAnnotation(agg, 1, MockDataFusionBackend.NAME);
        assertEquals("SUM viable for both backends", 2, agg.getCallAnnotations().get(0).getViableBackends().size());
        assertEquals("COUNT viable for DF only (Lucene not declared)", 1, agg.getCallAnnotations().get(1).getViableBackends().size());
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
        UnsupportedFunctionException exception = expectThrows(
            UnsupportedFunctionException.class,
            () -> runPlanner(makeMultiCallAggregate(sumCall(), stddevCall()), context)
        );
        assertTrue(exception.getMessage().contains("is not currently supported"));
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

    private void assertCallAnnotation(OpenSearchAggregate agg, int callIndex, String... expectedBackends) {
        AggregateCallAnnotation annotation = agg.getCallAnnotations().get(callIndex);
        assertNotNull("AggregateCall must have annotation", annotation);
        for (String backend : expectedBackends)
            assertTrue("Annotation must contain backend " + backend, annotation.getViableBackends().contains(backend));
    }

    // ---- Operator resolution ----

    /**
     * Exact {@code COUNT(DISTINCT x)} is rewritten to {@code APPROX_COUNT_DISTINCT(x)} on the
     * analytics-engine route — the resulting aggregate uses the standard approximate operator and
     * is no longer marked {@code isDistinct}, so it routes through the APPROXIMATE
     * single-stage gather path rather than the additive partial/final split.
     */
    public void testCountDistinctRewrittenToApproxCountDistinct() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall countDistinct = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            true,
            List.of(1),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "dc"
        );
        OpenSearchAggregate agg = runAggregate(1, countDistinct);
        assertEquals(1, agg.getAggCallList().size());
        AggregateCall rebuilt = agg.getAggCallList().get(0);
        assertSame(SqlStdOperatorTable.APPROX_COUNT_DISTINCT, rebuilt.getAggregation());
        assertFalse("isDistinct must be cleared on the rewritten APPROX_COUNT_DISTINCT call", rebuilt.isDistinct());
    }

    /**
     * PPL's {@code distinct_count_approx} UDAF marker — a {@code SqlAggFunction} named
     * {@code "APPROX_COUNT_DISTINCT"} but not the Calcite stdop, returning a NULLABLE BIGINT —
     * is rewritten to {@code SqlStdOperatorTable.APPROX_COUNT_DISTINCT} (which infers BIGINT
     * NOT NULL) and wrapped in a casting {@link OpenSearchProject} that restores the original
     * nullable row type. The Project bridge is required because {@code Aggregate.typeMatchesInferred}
     * pins the new aggCall's type to its operator's inferred type while {@code HepPlanner} pins
     * the replacement's row type to the original's.
     */
    public void testPplDistinctCountApproxUdfRewrittenWithCastProject() {
        SqlAggFunction pplUdfMarker = SqlBasicAggFunction.create(
            "APPROX_COUNT_DISTINCT",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BIGINT_FORCE_NULLABLE,
            OperandTypes.ANY
        );
        RelDataType nullableBigint = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true);
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        AggregateCall pplApprox = AggregateCall.create(pplUdfMarker, /* distinct */ false, List.of(1), -1, scan, nullableBigint, "dca");

        RelNode result = runPlanner(makeAggregate(pplApprox), defaultContext(1));
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        RelNode unwrapped = unwrapRootReducer(result);
        assertTrue(
            "Expected OpenSearchProject(OpenSearchAggregate(...)) wrap, got " + unwrapped.getClass().getSimpleName(),
            unwrapped instanceof OpenSearchProject
        );
        OpenSearchProject project = (OpenSearchProject) unwrapped;
        // Project preserves the original nullable BIGINT for the aggregated column.
        RelDataType dcaType = project.getRowType().getFieldList().get(1).getType();
        assertEquals("Project must restore original nullable BIGINT", nullableBigint, dcaType);
        // Field 1 must be a non-trivial expression (the cast); field 0 stays as a passthrough ref.
        // OpenSearchProjectRule wraps scalar calls in AnnotatedProjectExpression, so we search
        // recursively for a CAST node.
        assertTrue("Project must contain a CAST to bridge nullability", containsCast(project.getProjects().get(1)));

        RelNode innerAgg = RelNodeUtils.unwrapHep(project.getInput());
        assertTrue(
            "Inner node must be OpenSearchAggregate, got " + innerAgg.getClass().getSimpleName(),
            innerAgg instanceof OpenSearchAggregate
        );
        OpenSearchAggregate agg = (OpenSearchAggregate) innerAgg;
        AggregateCall rebuilt = agg.getAggCallList().get(0);
        assertSame(
            "Inner aggregate must use SqlStdOperatorTable.APPROX_COUNT_DISTINCT",
            SqlStdOperatorTable.APPROX_COUNT_DISTINCT,
            rebuilt.getAggregation()
        );
        assertFalse("isDistinct must be cleared on the rewritten call", rebuilt.isDistinct());
    }

    /**
     * COUNT(DISTINCT smallint_col) inserts a widening CAST(col AS INTEGER) below the aggregate
     * so DataFusion uses HLL (Binary state) instead of bitmap (List state).
     */
    public void testCountDistinctOnSmallintWidensToInteger() {
        RelNode scan = stubScan(
            mockTable("test_index", new String[] { "status", "size" }, new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.SMALLINT })
        );
        AggregateCall countDistinct = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            true,
            List.of(1),
            -1,
            scan,
            typeFactory.createSqlType(SqlTypeName.BIGINT),
            "dc"
        );
        RelNode result = runPlanner(makeAggregate(scan, countDistinct), defaultContext(1));
        String plan = RelOptUtil.toString(result);
        logger.info("Full plan with SMALLINT dc:\n{}", plan);
        assertTrue("Plan must contain CAST to widen SMALLINT to INTEGER", plan.contains("CAST") && plan.contains("INTEGER"));
    }

    /**
     * Stdop {@code APPROX_COUNT_DISTINCT} (already canonical) must not be rewritten — the rule's
     * predicate excludes the stdop, so no Project wrap is added and the result is a plain
     * {@link OpenSearchAggregate}.
     */
    public void testStdopApproxCountDistinctNotRewritten() {
        RelNode scan = stubScan(mockTable("test_index", "status", "size"));
        OpenSearchAggregate agg = runAggregate(1, approxCountDistinctCall(scan));
        AggregateCall call = agg.getAggCallList().get(0);
        assertSame(SqlStdOperatorTable.APPROX_COUNT_DISTINCT, call.getAggregation());
    }

    /** Recursive search for a CAST node — Project rule wraps scalar calls in AnnotatedProjectExpression. */
    private static boolean containsCast(RexNode node) {
        if (node.getKind() == SqlKind.CAST) return true;
        if (node instanceof RexCall call) {
            for (RexNode operand : call.getOperands()) {
                if (containsCast(operand)) return true;
            }
        }
        return false;
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
