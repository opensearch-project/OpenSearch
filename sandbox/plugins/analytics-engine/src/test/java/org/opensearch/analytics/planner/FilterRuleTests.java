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
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
import org.opensearch.analytics.planner.rel.OpenSearchTableScan;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.DelegationType;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Tests for filter rule: predicate annotation, viable backends computation,
 * delegation, and derived column handling.
 */
public class FilterRuleTests extends BasePlannerRulesTests {

    private static SqlFunction fullTextSqlFunction(String name) {
        return new SqlFunction(
            name,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BOOLEAN,
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
    }

    // ---- Per-predicate annotation tests ----

    /** Integer equality — both backends can evaluate natively. */
    public void testNativePredicateAnnotatedWithBothBackends() {
        OpenSearchFilter result = runFilter(
            "parquet",
            Map.of("status", Map.of("type", "integer", "index", true), "size", Map.of("type", "integer", "index", true)),
            new String[] { "status", "size" },
            new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.INTEGER },
            makeEquals(0, SqlTypeName.INTEGER, 200)
        );

        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        AnnotatedPredicate annotated = (AnnotatedPredicate) result.getCondition();
        assertPredicateAnnotation(annotated, MockDataFusionBackend.NAME, MockLuceneBackend.NAME);
    }

    /** Keyword equality — both backends viable per-predicate, operator-level only child. */
    public void testKeywordEqualsAnnotatedWithBothBackends() {
        OpenSearchFilter result = runFilter(
            "parquet",
            Map.of("country_name", Map.of("type", "keyword", "index", true)),
            new String[] { "country_name" },
            new SqlTypeName[] { SqlTypeName.VARCHAR },
            makeEquals(0, SqlTypeName.VARCHAR, "US")
        );

        AnnotatedPredicate annotated = (AnnotatedPredicate) result.getCondition();
        assertEquals(2, annotated.getViableBackends().size());
        // Operator-level: only child backend (no delegation configured)
        assertEquals(1, result.getViableBackends().size());
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
    }

    // ---- Viable backends with delegation ----

    /** Full-text with delegation — both backends viable at operator level. */
    public void testFullTextViableWithDelegation() {
        OpenSearchFilter result = runFilterWithDelegation(
            "parquet",
            Map.of("message", Map.of("type", "keyword", "index", true)),
            new String[] { "message" },
            new SqlTypeName[] { SqlTypeName.VARCHAR },
            makeFullTextCall(fullTextSqlFunction("MATCH_PHRASE"), 0, "hello world")
        );

        // DF is viable at operator level (has doc values in parquet)
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        // Lucene not viable at operator level — only delegation target
        assertFalse(result.getViableBackends().contains(MockLuceneBackend.NAME));
        // MATCH_PHRASE predicate has Lucene as delegation target
        AnnotatedPredicate predicate = (AnnotatedPredicate) result.getCondition();
        assertPredicateAnnotation(predicate, MockLuceneBackend.NAME);
        assertTrue(predicate.getOriginal().toString().contains("MATCH_PHRASE"));
    }

    /** AND with delegation — DF viable at operator, equals viable for both, MATCH_PHRASE delegated to Lucene. */
    public void testAndWithDelegationBothViable() {
        OpenSearchFilter result = runFilterWithDelegation(
            "parquet",
            Map.of("status", Map.of("type", "integer", "index", true), "message", Map.of("type", "keyword", "index", true)),
            new String[] { "status", "message" },
            new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.VARCHAR },
            makeAnd(makeEquals(0, SqlTypeName.INTEGER, 200), makeFullTextCall(fullTextSqlFunction("MATCH_PHRASE"), 1, "timeout error"))
        );

        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse(result.getViableBackends().contains(MockLuceneBackend.NAME));
        RexCall andCondition = (RexCall) result.getCondition();
        AnnotatedPredicate equalsPred = (AnnotatedPredicate) andCondition.getOperands().get(0);
        AnnotatedPredicate matchPred = (AnnotatedPredicate) andCondition.getOperands().get(1);
        assertPredicateAnnotation(equalsPred, MockDataFusionBackend.NAME, MockLuceneBackend.NAME);
        assertPredicateAnnotation(matchPred, MockLuceneBackend.NAME);
        assertTrue(matchPred.getOriginal().toString().contains("MATCH_PHRASE"));
    }

    /** OR across backends — DF viable at operator, equals viable for both, MATCH delegated to Lucene. */
    public void testOrAcrossBackendsWithDelegation() {
        OpenSearchFilter result = runFilterWithDelegation(
            "parquet",
            Map.of("status", Map.of("type", "integer", "index", true), "message", Map.of("type", "keyword", "index", true)),
            new String[] { "status", "message" },
            new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.VARCHAR },
            makeCall(
                SqlStdOperatorTable.OR,
                makeEquals(0, SqlTypeName.INTEGER, 200),
                makeFullTextCall(fullTextSqlFunction("MATCH"), 1, "error")
            )
        );

        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse(result.getViableBackends().contains(MockLuceneBackend.NAME));
        RexCall orCondition = (RexCall) result.getCondition();
        AnnotatedPredicate equalsPred = (AnnotatedPredicate) orCondition.getOperands().get(0);
        AnnotatedPredicate matchPred = (AnnotatedPredicate) orCondition.getOperands().get(1);
        assertPredicateAnnotation(equalsPred, MockDataFusionBackend.NAME, MockLuceneBackend.NAME);
        assertPredicateAnnotation(matchPred, MockLuceneBackend.NAME);
        assertTrue(matchPred.getOriginal().toString().contains("MATCH"));
    }

    /** OR of two full-text predicates — DF viable at operator, both predicates delegated to Lucene. */
    public void testMultipleFullTextOrWithDelegation() {
        OpenSearchFilter result = runFilterWithDelegation(
            "parquet",
            Map.of("title", Map.of("type", "keyword", "index", true), "body", Map.of("type", "keyword", "index", true)),
            new String[] { "title", "body" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.VARCHAR },
            makeCall(
                SqlStdOperatorTable.OR,
                makeFullTextCall(fullTextSqlFunction("MATCH"), 0, "hello"),
                makeFullTextCall(fullTextSqlFunction("MATCH_PHRASE"), 1, "world")
            )
        );

        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse(result.getViableBackends().contains(MockLuceneBackend.NAME));
        RexCall orCondition = (RexCall) result.getCondition();
        AnnotatedPredicate matchPred = (AnnotatedPredicate) orCondition.getOperands().get(0);
        AnnotatedPredicate phrasePred = (AnnotatedPredicate) orCondition.getOperands().get(1);
        assertPredicateAnnotation(matchPred, MockLuceneBackend.NAME);
        assertTrue(matchPred.getOriginal().toString().contains("MATCH"));
        assertPredicateAnnotation(phrasePred, MockLuceneBackend.NAME);
        assertTrue(phrasePred.getOriginal().toString().contains("MATCH_PHRASE"));
    }

    // ---- Error cases ----

    /** Full-text without delegation — errors. */
    public void testFullTextErrorsWithoutDelegation() {
        RelOptTable table = mockTable("test_index", new String[] { "message" }, new SqlTypeName[] { SqlTypeName.VARCHAR });
        RexNode condition = makeFullTextCall(fullTextSqlFunction("MATCH_PHRASE"), 0, "hello world");
        LogicalFilter filter = LogicalFilter.create(stubScan(table), condition);

        // index=false strips the inverted index so no backend can satisfy the full-text predicate
        // natively, forcing the "without delegation" code path under test.
        PlannerContext context = buildContext("parquet", Map.of("message", Map.of("type", "keyword", "index", false)));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(filter, context));
        assertTrue(exception.getMessage().contains("No backend can evaluate filter predicate"));
    }

    /** Unsupported field type for operator — errors. */
    public void testErrorForUnsupportedFieldTypeOperatorCombo() {
        RelOptTable table = mockTable("test_index", new String[] { "location" }, new SqlTypeName[] { SqlTypeName.OTHER });
        RexNode condition = makeCall(
            SqlStdOperatorTable.IS_NOT_NULL,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.OTHER), 0)
        );
        LogicalFilter filter = LogicalFilter.create(stubScan(table), condition);

        // doc_values=false, index=false, store=false → no storage in any format → error
        PlannerContext context = buildContext(
            "parquet",
            Map.of("location", Map.of("type", "geo_point", "doc_values", false, "index", false, "store", false))
        );

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(filter, context));
        assertTrue(exception.getMessage().contains("has no storage"));
    }

    // ---- Derived columns ----

    /**
     * HAVING on derived column must throw — marking on derived/expression columns
     * is not yet implemented. Verifies the planner fails fast with a clear message
     * rather than silently producing incorrect viableBackends.
     *
     * TODO: add testFilterOnAggregateOutput — Filter(Aggregate(Scan)) where the filter
     * is on a non-derived column (e.g. group-by key) should succeed and propagate
     * viableBackends correctly through the composed pipeline.
     */
    public void testFilterOnDerivedColumnsAfterAggregateThrows() {
        PlannerContext context = buildContext("parquet", 1, Map.of("status", Map.of("type", "integer"), "size", Map.of("type", "integer")));

        RelOptTable table = mockTable("test_index", "status", "size");
        LogicalAggregate aggregate = LogicalAggregate.create(
            stubScan(table),
            List.of(),
            ImmutableBitSet.of(0),
            null,
            List.of(
                AggregateCall.create(
                    SqlStdOperatorTable.SUM,
                    false,
                    List.of(1),
                    1,
                    stubScan(table),
                    typeFactory.createSqlType(SqlTypeName.INTEGER),
                    "total_size"
                )
            )
        );

        RexNode havingCondition = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1),
            rexBuilder.makeLiteral(1000000, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        LogicalFilter having = LogicalFilter.create(aggregate, havingCondition);

        UnsupportedOperationException ex = expectThrows(UnsupportedOperationException.class, () -> runPlanner(having, context));
        assertTrue("Expected message about derived column, got: " + ex.getMessage(), ex.getMessage().contains("derived column"));
    }

    // ---- Helpers ----

    private OpenSearchFilter runFilter(
        String format,
        Map<String, Map<String, Object>> fields,
        String[] fieldNames,
        SqlTypeName[] fieldTypes,
        RexNode condition
    ) {
        return runFilter(
            format,
            fields,
            fieldNames,
            fieldTypes,
            condition,
            List.of(DATAFUSION, LUCENE),
            Set.of(MockDataFusionBackend.NAME)
        );
    }

    private OpenSearchFilter runFilterWithDelegation(
        String format,
        Map<String, Map<String, Object>> fields,
        String[] fieldNames,
        SqlTypeName[] fieldTypes,
        RexNode condition
    ) {
        return runFilter(format, fields, fieldNames, fieldTypes, condition, delegationBackends(), Set.of(MockDataFusionBackend.NAME));
    }

    private OpenSearchFilter runFilter(
        String format,
        Map<String, Map<String, Object>> fields,
        String[] fieldNames,
        SqlTypeName[] fieldTypes,
        RexNode condition,
        List<AnalyticsSearchBackendPlugin> backends,
        Set<String> expectedViable
    ) {
        PlannerContext context = buildContext(format, fields, backends);
        RelOptTable table = mockTable("test_index", fieldNames, fieldTypes);
        LogicalFilter filter = LogicalFilter.create(stubScan(table), condition);
        RelNode result = unwrapExchange(runPlanner(filter, context));
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        assertTrue("Expected OpenSearchFilter, got " + result.getClass().getSimpleName(), result instanceof OpenSearchFilter);
        assertPipelineViableBackends(result, List.of(OpenSearchFilter.class, OpenSearchTableScan.class), expectedViable);
        return (OpenSearchFilter) result;
    }

    private void assertPredicateAnnotation(AnnotatedPredicate predicate, String... expectedBackends) {
        for (String backend : expectedBackends)
            assertTrue("Predicate annotation must contain " + backend, predicate.getViableBackends().contains(backend));
    }

    private List<AnalyticsSearchBackendPlugin> delegationBackends() {
        MockDataFusionBackend df = new MockDataFusionBackend() {
            @Override
            protected Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.FILTER);
            }
        };
        MockLuceneBackend lucene = new MockLuceneBackend() {
            @Override
            protected Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }
        };
        return List.of(df, lucene);
    }
}
