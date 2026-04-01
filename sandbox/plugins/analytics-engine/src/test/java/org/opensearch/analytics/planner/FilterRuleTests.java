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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.FullTextFunctions;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;
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

    // ---- Per-predicate annotation tests ----

    /** Integer equality — both backends can evaluate natively. */
    public void testNativePredicateAnnotatedWithBothBackends() {
        OpenSearchFilter result = runFilter("parquet", Map.of(
            "status", Map.of("type", "integer"),
            "size", Map.of("type", "integer")
        ), new String[]{"status", "size"}, new SqlTypeName[]{SqlTypeName.INTEGER, SqlTypeName.INTEGER},
            makeEquals(0, SqlTypeName.INTEGER, 200));

        assertEquals(MockDataFusionBackend.NAME, result.getBackend());
        assertTrue(result.getCondition() instanceof AnnotatedPredicate);
        AnnotatedPredicate annotated = (AnnotatedPredicate) result.getCondition();
        assertTrue(annotated.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertTrue(annotated.getViableBackends().contains(MockLuceneBackend.NAME));
    }

    /** Keyword equality — both backends viable per-predicate, operator-level only child. */
    public void testKeywordEqualsAnnotatedWithBothBackends() {
        OpenSearchFilter result = runFilter("parquet", Map.of(
            "country_name", Map.of("type", "keyword")
        ), new String[]{"country_name"}, new SqlTypeName[]{SqlTypeName.VARCHAR},
            makeEquals(0, SqlTypeName.VARCHAR, "US"));

        assertTrue(result.getCondition() instanceof AnnotatedPredicate);
        AnnotatedPredicate annotated = (AnnotatedPredicate) result.getCondition();
        assertEquals(2, annotated.getViableBackends().size());
        // Operator-level: only child backend (no delegation configured)
        assertEquals(1, result.getViableBackends().size());
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
    }

    // ---- Viable backends with delegation ----

    /** Full-text with delegation — both backends viable at operator level. */
    public void testFullTextViableWithDelegation() {
        OpenSearchFilter result = runFilterWithDelegation("parquet", Map.of(
            "message", Map.of("type", "text")
        ), new String[]{"message"}, new SqlTypeName[]{SqlTypeName.VARCHAR},
            makeFullTextCall(FullTextFunctions.MATCH_PHRASE, 0, "hello world"));

        assertEquals(MockDataFusionBackend.NAME, result.getBackend());
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertTrue(result.getViableBackends().contains(MockLuceneBackend.NAME));
    }

    /** AND with delegation — both backends viable. */
    public void testAndWithDelegationBothViable() {
        OpenSearchFilter result = runFilterWithDelegation("parquet", Map.of(
            "status", Map.of("type", "integer"),
            "message", Map.of("type", "text")
        ), new String[]{"status", "message"}, new SqlTypeName[]{SqlTypeName.INTEGER, SqlTypeName.VARCHAR},
            makeAnd(
                makeEquals(0, SqlTypeName.INTEGER, 200),
                makeFullTextCall(FullTextFunctions.MATCH_PHRASE, 1, "timeout error")
            ));

        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertTrue(result.getViableBackends().contains(MockLuceneBackend.NAME));
    }

    /** OR across backends with delegation — both viable. */
    public void testOrAcrossBackendsWithDelegation() {
        OpenSearchFilter result = runFilterWithDelegation("parquet", Map.of(
            "status", Map.of("type", "integer"),
            "message", Map.of("type", "text")
        ), new String[]{"status", "message"}, new SqlTypeName[]{SqlTypeName.INTEGER, SqlTypeName.VARCHAR},
            makeCall(SqlStdOperatorTable.OR,
                makeEquals(0, SqlTypeName.INTEGER, 200),
                makeFullTextCall(FullTextFunctions.MATCH, 1, "error")
            ));

        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertTrue(result.getViableBackends().contains(MockLuceneBackend.NAME));
    }

    /** OR of two full-text predicates with delegation — both viable. */
    public void testMultipleFullTextOrWithDelegation() {
        OpenSearchFilter result = runFilterWithDelegation("parquet", Map.of(
            "title", Map.of("type", "text"),
            "body", Map.of("type", "text")
        ), new String[]{"title", "body"}, new SqlTypeName[]{SqlTypeName.VARCHAR, SqlTypeName.VARCHAR},
            makeCall(SqlStdOperatorTable.OR,
                makeFullTextCall(FullTextFunctions.MATCH, 0, "hello"),
                makeFullTextCall(FullTextFunctions.MATCH_PHRASE, 1, "world")
            ));

        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertTrue(result.getViableBackends().contains(MockLuceneBackend.NAME));
    }

    // ---- Error cases ----

    /** Full-text without delegation — errors. */
    public void testFullTextErrorsWithoutDelegation() {
        RelOptTable table = mockTable("test_index", new String[]{"message"}, new SqlTypeName[]{SqlTypeName.VARCHAR});
        RexNode condition = makeFullTextCall(FullTextFunctions.MATCH_PHRASE, 0, "hello world");
        LogicalFilter filter = LogicalFilter.create(stubScan(table), condition);

        PlannerContext context = buildContext("parquet", Map.of(
            "message", Map.of("type", "text")
        ));

        IllegalStateException exception = expectThrows(IllegalStateException.class,
            () -> runPlanner(filter, context));
        assertTrue(exception.getMessage().contains("No backend can execute filter"));
    }

    /** Unsupported field type for operator — errors. */
    public void testErrorForUnsupportedFieldTypeOperatorCombo() {
        RelOptTable table = mockTable("test_index", new String[]{"location"}, new SqlTypeName[]{SqlTypeName.OTHER});
        RexNode condition = makeCall(SqlStdOperatorTable.IS_NOT_NULL,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.OTHER), 0)
        );
        LogicalFilter filter = LogicalFilter.create(stubScan(table), condition);

        PlannerContext context = buildContext("parquet", Map.of(
            "location", Map.of("type", "geo_point")
        ));

        try {
            runPlanner(filter, context);
            fail("Expected IllegalStateException for unsupported field type");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("No backend can"));
        }
    }

    // ---- Derived columns ----

    /** HAVING on derived column — only FILTER_ON_EXPRESSIONS backends viable. */
    public void testFilterOnDerivedColumnsAfterAggregate() {
        PlannerContext context = buildContext("parquet", 1, Map.of(
            "status", Map.of("type", "integer"),
            "size", Map.of("type", "integer")
        ));

        RelOptTable table = mockTable("test_index", "status", "size");
        LogicalAggregate aggregate = LogicalAggregate.create(
            stubScan(table), List.of(), ImmutableBitSet.of(0), null,
            List.of(AggregateCall.create(
                SqlStdOperatorTable.SUM, false, List.of(1), 1, stubScan(table),
                typeFactory.createSqlType(SqlTypeName.INTEGER), "total_size"
            ))
        );

        RexNode havingCondition = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1),
            rexBuilder.makeLiteral(1000000, typeFactory.createSqlType(SqlTypeName.INTEGER), true)
        );
        LogicalFilter having = LogicalFilter.create(aggregate, havingCondition);

        RelNode result = runPlanner(having, context);
        logger.info("Plan:\n{}", RelOptUtil.toString(result));

        result = unwrapExchange(result);
        assertTrue(result instanceof OpenSearchFilter);
        OpenSearchFilter filter = (OpenSearchFilter) result;

        assertTrue(filter.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse(filter.getViableBackends().contains(MockLuceneBackend.NAME));

        assertTrue(filter.getCondition() instanceof AnnotatedPredicate);
        AnnotatedPredicate annotated = (AnnotatedPredicate) filter.getCondition();
        assertEquals(1, annotated.getViableBackends().size());
        assertTrue(annotated.getViableBackends().contains(MockDataFusionBackend.NAME));
    }

    // ---- Helpers ----

    private OpenSearchFilter runFilter(String format, Map<String, Map<String, Object>> fields,
                                       String[] fieldNames, SqlTypeName[] fieldTypes,
                                       RexNode condition) {
        return runFilter(format, fields, fieldNames, fieldTypes, condition, List.of(DATAFUSION, LUCENE));
    }

    private OpenSearchFilter runFilterWithDelegation(String format, Map<String, Map<String, Object>> fields,
                                                     String[] fieldNames, SqlTypeName[] fieldTypes,
                                                     RexNode condition) {
        return runFilter(format, fields, fieldNames, fieldTypes, condition, delegationBackends());
    }

    private OpenSearchFilter runFilter(String format, Map<String, Map<String, Object>> fields,
                                       String[] fieldNames, SqlTypeName[] fieldTypes,
                                       RexNode condition, List<AnalyticsSearchBackendPlugin> backends) {
        PlannerContext context = buildContext(format, fields, backends);
        RelOptTable table = mockTable("test_index", fieldNames, fieldTypes);
        LogicalFilter filter = LogicalFilter.create(stubScan(table), condition);
        RelNode result = unwrapExchange(runPlanner(filter, context));
        logger.info("Plan:\n{}", RelOptUtil.toString(result));
        assertTrue("Expected OpenSearchFilter, got " + result.getClass().getSimpleName(),
            result instanceof OpenSearchFilter);
        return (OpenSearchFilter) result;
    }

    private List<AnalyticsSearchBackendPlugin> delegationBackends() {
        MockDataFusionBackend df = new MockDataFusionBackend() {
            @Override
            public Set<DelegationType> supportedDelegations() {
                return Set.of(DelegationType.FILTER);
            }
        };
        MockLuceneBackend lucene = new MockLuceneBackend() {
            @Override
            public Set<DelegationType> acceptedDelegations() {
                return Set.of(DelegationType.FILTER);
            }
        };
        return List.of(df, lucene);
    }
}
