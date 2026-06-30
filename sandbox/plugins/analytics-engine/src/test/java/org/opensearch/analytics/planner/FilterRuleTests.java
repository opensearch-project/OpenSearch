/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
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
import org.opensearch.analytics.settings.DelegationBlockList;
import org.opensearch.analytics.settings.PlannerSettings;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.DelegatedPredicateSerializer;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.EngineCapability;
import org.opensearch.analytics.spi.FieldReferences;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunction;

import java.math.BigDecimal;
import java.util.ArrayList;
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

    /**
     * Keyword equality is normally viable for both backends per-predicate (see
     * {@link #testKeywordEqualsAnnotatedWithBothBackends}). Blocking EQUALS for the Lucene backend
     * via the delegation block-list must drop Lucene from the predicate's viable set, leaving only
     * DataFusion — so the predicate is never delegated to Lucene.
     */
    public void testBlockListDropsBlockedBackendFromPredicate() {
        DelegationBlockList blockList = DelegationBlockList.fromMap(Map.of(MockLuceneBackend.NAME, List.of(ScalarFunction.EQUALS)));
        OpenSearchFilter result = runFilterWithBlockList(
            "parquet",
            Map.of("country_name", Map.of("type", "keyword", "index", true)),
            new String[] { "country_name" },
            new SqlTypeName[] { SqlTypeName.VARCHAR },
            makeEquals(0, SqlTypeName.VARCHAR, "US"),
            blockList
        );

        AnnotatedPredicate annotated = (AnnotatedPredicate) result.getCondition();
        assertTrue("DataFusion stays viable", annotated.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse("Lucene dropped by block-list", annotated.getViableBackends().contains(MockLuceneBackend.NAME));
        assertEquals(1, annotated.getViableBackends().size());
    }

    /**
     * Safety invariant: blocking must never make a predicate unexecutable. If EVERY viable backend
     * blocks the shape, the block-list is ignored and all backends remain (there is nowhere else to
     * run the predicate). Here both backends block EQUALS, so the annotation keeps both.
     */
    public void testBlockListIgnoredWhenAllViableBackendsBlock() {
        DelegationBlockList blockList = DelegationBlockList.fromMap(
            Map.of(MockLuceneBackend.NAME, List.of(ScalarFunction.EQUALS), MockDataFusionBackend.NAME, List.of(ScalarFunction.EQUALS))
        );
        OpenSearchFilter result = runFilterWithBlockList(
            "parquet",
            Map.of("country_name", Map.of("type", "keyword", "index", true)),
            new String[] { "country_name" },
            new SqlTypeName[] { SqlTypeName.VARCHAR },
            makeEquals(0, SqlTypeName.VARCHAR, "US"),
            blockList
        );

        AnnotatedPredicate annotated = (AnnotatedPredicate) result.getCondition();
        assertEquals("both backends retained — blocking can't strand the predicate", 2, annotated.getViableBackends().size());
    }

    /**
     * Blocking a predicate for one backend must not affect a different, non-blocked predicate shape:
     * blocking EQUALS for Lucene leaves a keyword-equality dual-viable annotation untouched when the
     * block targets a different backend namespace that doesn't exist.
     */
    public void testBlockListUnknownBackendIsNoOp() {
        DelegationBlockList blockList = DelegationBlockList.fromMap(Map.of("not-a-backend", List.of(ScalarFunction.EQUALS)));
        OpenSearchFilter result = runFilterWithBlockList(
            "parquet",
            Map.of("country_name", Map.of("type", "keyword", "index", true)),
            new String[] { "country_name" },
            new SqlTypeName[] { SqlTypeName.VARCHAR },
            makeEquals(0, SqlTypeName.VARCHAR, "US"),
            blockList
        );

        AnnotatedPredicate annotated = (AnnotatedPredicate) result.getCondition();
        assertEquals("both backends still viable", 2, annotated.getViableBackends().size());
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

    /**
     * Multi-index alias MATCH delegation: when a scan resolves to two backing indices, the
     * unioned {@link org.opensearch.analytics.spi.FieldStorageInfo} must still expose the field's
     * Lucene index format so a {@code MATCH} predicate on it delegates to the Lucene backend.
     *
     * <p>This pins the contract called out in the multi-index PR description: MATCH on an alias
     * spanning multiple concrete indices must behave the same as MATCH on a single index.
     *
     * <p>Implementation note: with the current per-index storage helpers, both backings declare
     * the field with identical (keyword, indexed) settings, and
     * {@link org.opensearch.analytics.planner.FieldStorageResolver#merged} preserves Lucene index
     * formats via {@code putIfAbsent}. A future regression that drops index formats during the
     * union would surface as the assertion below failing.
     */
    public void testMatchDelegationOverMultiIndexAlias() {
        OpenSearchFilter result = runMultiIndexFilter(
            new String[] { "idx_a", "idx_b" },
            "parquet",
            Map.of("message", Map.of("type", "keyword", "index", true)),
            new String[] { "message" },
            new SqlTypeName[] { SqlTypeName.VARCHAR },
            makeFullTextCall(fullTextSqlFunction("MATCH_PHRASE"), 0, "hello world")
        );

        assertTrue("DataFusion stays viable at the operator level", result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse("Lucene is delegation-only here", result.getViableBackends().contains(MockLuceneBackend.NAME));
        AnnotatedPredicate predicate = (AnnotatedPredicate) result.getCondition();
        assertPredicateAnnotation(predicate, MockLuceneBackend.NAME);
        assertTrue(predicate.getOriginal().toString().contains("MATCH_PHRASE"));
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

    // ---- Text-relevance function on non-text field type-check ----

    /**
     * {@code query_string(['severityNumber'], '...')} on a {@code long} field is rejected
     * at planning with a precise, actionable error message — text-relevance functions
     * cannot be applied to numeric fields.
     */
    public void testQueryStringOnNumericFieldIsRejected() {
        RelOptTable table = mockTable("test_index", new String[] { "severityNumber" }, new SqlTypeName[] { SqlTypeName.BIGINT });
        RexNode condition = makeMultiFieldFullTextCall(
            fullTextSqlFunction("QUERY_STRING"),
            List.of("severityNumber"),
            "severityNumber:>15"
        );
        LogicalFilter filter = LogicalFilter.create(stubScan(table), condition);
        PlannerContext context = buildContext("parquet", Map.of("severityNumber", Map.of("type", "long")));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> runPlanner(filter, context));
        assertTrue("Error must name the function: " + exception.getMessage(), exception.getMessage().contains("QUERY_STRING"));
        assertTrue("Error must name the offending field: " + exception.getMessage(), exception.getMessage().contains("severityNumber"));
        assertTrue("Error must name the field type: " + exception.getMessage(), exception.getMessage().contains("long"));
        assertTrue("Error must hint at typed comparison: " + exception.getMessage(), exception.getMessage().contains("typed comparison"));
    }

    /**
     * {@code query_string(['eventTime'], '...')} on a {@code date} field is rejected — text-relevance
     * functions only apply to text/keyword.
     */
    public void testQueryStringOnDateFieldIsRejected() {
        RelOptTable table = mockTable("test_index", new String[] { "eventTime" }, new SqlTypeName[] { SqlTypeName.DATE });
        RexNode condition = makeMultiFieldFullTextCall(fullTextSqlFunction("QUERY_STRING"), List.of("eventTime"), "2026-01-01");
        LogicalFilter filter = LogicalFilter.create(stubScan(table), condition);
        PlannerContext context = buildContext("parquet", Map.of("eventTime", Map.of("type", "date")));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> runPlanner(filter, context));
        assertTrue(exception.getMessage().contains("QUERY_STRING"));
        assertTrue(exception.getMessage().contains("eventTime"));
        assertTrue(exception.getMessage().contains("date"));
    }

    /**
     * {@code query_string(['status'], '...')} on a {@code keyword} field is accepted —
     * keyword fields are valid for text-relevance functions.
     *
     * <p>Note: this fixture's mock DataFusion backend declares scan support only for
     * numeric/keyword/date/boolean types (not {@code text}), and the mock Lucene backend
     * has no value-producing scan capability, so a pure {@code text} field can't be scanned
     * here at all — the table-scan rule throws before the filter rule runs. This test therefore
     * exercises the {@code keyword} acceptance path.
     */
    public void testQueryStringOnKeywordFieldIsAccepted() {
        OpenSearchFilter result = runFilterWithDelegation(
            "parquet",
            Map.of("status", Map.of("type", "keyword", "index", true)),
            new String[] { "status" },
            new SqlTypeName[] { SqlTypeName.VARCHAR },
            makeMultiFieldFullTextCall(fullTextSqlFunction("QUERY_STRING"), List.of("status"), "active")
        );

        AnnotatedPredicate predicate = (AnnotatedPredicate) result.getCondition();
        assertPredicateAnnotation(predicate, MockLuceneBackend.NAME);
    }

    /**
     * {@code query_string(['status', 'severityNumber'], '...')} with a mixed field list
     * (keyword + long) is rejected — every named field must be text/keyword. The keyword
     * field keeps the scan viable (see note on {@link #testQueryStringOnKeywordFieldIsAccepted}),
     * so planning reaches the filter rule and the {@code long} field triggers the rejection.
     */
    public void testQueryStringOnMixedFieldsRejectsBecauseOfNonTextField() {
        RelOptTable table = mockTable(
            "test_index",
            new String[] { "status", "severityNumber" },
            new SqlTypeName[] { SqlTypeName.VARCHAR, SqlTypeName.BIGINT }
        );
        RexNode condition = makeMultiFieldFullTextCall(fullTextSqlFunction("QUERY_STRING"), List.of("status", "severityNumber"), "error");
        LogicalFilter filter = LogicalFilter.create(stubScan(table), condition);
        PlannerContext context = buildContext(
            "parquet",
            Map.of("status", Map.of("type", "keyword", "index", true), "severityNumber", Map.of("type", "long"))
        );

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> runPlanner(filter, context));
        assertTrue(exception.getMessage().contains("severityNumber"));
        assertTrue(exception.getMessage().contains("long"));
    }

    /**
     * Field-less {@code query_string('category:A')} — the form the PPL {@code search} command lowers
     * {@code search source=idx category=A} into. There is no {@code fields} MAP, so no literal field
     * names are extracted; the field reference lives inside the Lucene query-string syntax. This must
     * NOT be rejected — it falls back to the TEXT-type assumption and routes to the full-text backend.
     * Regression test for the {@code search}-command 500 (no {@code fields} MAP present).
     */
    public void testFieldlessQueryStringIsAccepted() {
        OpenSearchFilter result = runFilterWithDelegation(
            "parquet",
            Map.of("category", Map.of("type", "keyword", "index", true)),
            new String[] { "category" },
            new SqlTypeName[] { SqlTypeName.VARCHAR },
            makeFieldlessFullTextCall(fullTextSqlFunction("QUERY_STRING"), "category:A")
        );

        AnnotatedPredicate predicate = (AnnotatedPredicate) result.getCondition();
        assertPredicateAnnotation(predicate, MockLuceneBackend.NAME);
    }

    // ---- referencedFields-driven validation (Wave 4) ----

    /**
     * With a backend serializer that reports field references, a field named only inside the
     * query string (no {@code fields} MAP) is now validated. The stub reports a literal {@code long}
     * field with {@code lenient=false}, so the planner rejects it — coverage the old MAP-literal
     * path missed (field-less queries previously fell back to the TEXT assumption and were accepted).
     */
    public void testInStringLiteralNonTextRejectedViaExtractor() {
        FieldReferences refs = new FieldReferences(List.of("severityNumber"), List.of(), false);
        RelOptTable table = mockTable("test_index", new String[] { "severityNumber" }, new SqlTypeName[] { SqlTypeName.BIGINT });
        RexNode condition = makeFieldlessFullTextCall(fullTextSqlFunction("QUERY_STRING"), "severityNumber:>15");
        LogicalFilter filter = LogicalFilter.create(stubScan(table), condition);
        PlannerContext context = buildContext("parquet", Map.of("severityNumber", Map.of("type", "long")), backendsWithExtractor(refs));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> runPlanner(filter, context));
        assertTrue(exception.getMessage().contains("severityNumber"));
        assertTrue(exception.getMessage().contains("text and keyword"));
    }

    /**
     * An explicit {@code lenient=true} (reported by the extractor) suppresses the eager type
     * rejection. The query still fails — no backend supports {@code query_string} on a {@code long}
     * field — but via the capability-viability path, not the friendly type-rejection. The distinct
     * error proves the lenient gate skipped {@code rejectNonTextFieldsForTextFunction}.
     *
     * <p>The viability failure is a query-level (client) error, so it surfaces as an
     * {@link IllegalArgumentException} (HTTP 400 with the actionable message preserved) rather
     * than an {@link IllegalStateException} (which would be redacted as a 500).
     */
    public void testLenientTrueSuppressesEagerRejectionViaExtractor() {
        FieldReferences refs = new FieldReferences(List.of("severityNumber"), List.of(), true);
        RelOptTable table = mockTable("test_index", new String[] { "severityNumber" }, new SqlTypeName[] { SqlTypeName.BIGINT });
        RexNode condition = makeFieldlessFullTextCall(fullTextSqlFunction("QUERY_STRING"), "severityNumber:>15");
        LogicalFilter filter = LogicalFilter.create(stubScan(table), condition);
        PlannerContext context = buildContext("parquet", Map.of("severityNumber", Map.of("type", "long")), backendsWithExtractor(refs));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> runPlanner(filter, context));
        assertTrue(
            "Should fail via viability, not eager rejection: " + exception.getMessage(),
            exception.getMessage().contains("No backend can evaluate")
        );
        assertFalse(
            "Eager type-rejection must have been skipped: " + exception.getMessage(),
            exception.getMessage().contains("text and keyword")
        );
    }

    /**
     * A field named only inside the query string that resolves to a {@code keyword} field is
     * accepted via the extractor path and routes to the full-text backend — the extractor surfaces
     * {@code category} as a literal, which the planner type-checks as keyword (valid).
     */
    public void testInStringLiteralKeywordAcceptedViaExtractor() {
        FieldReferences refs = new FieldReferences(List.of("category"), List.of(), false);
        OpenSearchFilter result = runFilter(
            "parquet",
            Map.of("category", Map.of("type", "keyword", "index", true)),
            new String[] { "category" },
            new SqlTypeName[] { SqlTypeName.VARCHAR },
            makeFieldlessFullTextCall(fullTextSqlFunction("QUERY_STRING"), "category:A"),
            backendsWithExtractor(refs),
            Set.of(MockDataFusionBackend.NAME)
        );

        AnnotatedPredicate predicate = (AnnotatedPredicate) result.getCondition();
        assertPredicateAnnotation(predicate, MockLuceneBackend.NAME);
    }

    /**
     * A literal field named in {@code query_string} that is absent from the scan's schema is an
     * unknown field — rejected with a "field not found" error, matching how a normal predicate on an
     * unknown field fails, rather than silently assuming TEXT and matching nothing. (Wildcard/regex
     * tokens are classified as patterns and are unaffected — see
     * {@link #testQueryStringWildcardFieldPatternIsAccepted}.)
     */
    public void testQueryStringOnUnknownLiteralFieldIsRejected() {
        RelOptTable table = mockTable("test_index", new String[] { "status" }, new SqlTypeName[] { SqlTypeName.VARCHAR });
        RexNode condition = makeMultiFieldFullTextCall(fullTextSqlFunction("QUERY_STRING"), List.of("nosuchfield"), "active");
        LogicalFilter filter = LogicalFilter.create(stubScan(table), condition);
        PlannerContext context = buildContext("parquet", Map.of("status", Map.of("type", "keyword", "index", true)));

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> runPlanner(filter, context));
        assertTrue("Error must name the unknown field: " + exception.getMessage(), exception.getMessage().contains("nosuchfield"));
        assertTrue("Error must say not found: " + exception.getMessage(), exception.getMessage().contains("not found"));
    }

    /**
     * A wildcard field pattern (e.g. {@code ser*}) is classified as a pattern, not a literal, so it
     * is never type-checked or treated as an unknown field — it routes to the full-text backend and
     * is expanded at execution. Guards the empty-literals fallback against the unknown-field rejection
     * added in {@link #testQueryStringOnUnknownLiteralFieldIsRejected}.
     */
    public void testQueryStringWildcardFieldPatternIsAccepted() {
        OpenSearchFilter result = runFilterWithDelegation(
            "parquet",
            Map.of("status", Map.of("type", "keyword", "index", true)),
            new String[] { "status" },
            new SqlTypeName[] { SqlTypeName.VARCHAR },
            makeMultiFieldFullTextCall(fullTextSqlFunction("QUERY_STRING"), List.of("ser*"), "active")
        );

        AnnotatedPredicate predicate = (AnnotatedPredicate) result.getCondition();
        assertPredicateAnnotation(predicate, MockLuceneBackend.NAME);
    }

    /**
     * DataFusion (FILTER delegation) + Lucene (accepts FILTER delegation, registers a stub
     * {@link DelegatedPredicateSerializer} for {@code QUERY_STRING} whose {@code referencedFields}
     * returns {@code refs}).
     */
    private List<AnalyticsSearchBackendPlugin> backendsWithExtractor(FieldReferences refs) {
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

            @Override
            public Map<ScalarFunction, DelegatedPredicateSerializer> delegatedPredicateSerializers() {
                DelegatedPredicateSerializer stub = new DelegatedPredicateSerializer() {
                    @Override
                    public byte[] serialize(RexCall call, List<FieldStorageInfo> fieldStorage) {
                        throw new UnsupportedOperationException("stub");
                    }

                    @Override
                    public FieldReferences referencedFields(RexCall call, List<FieldStorageInfo> fieldStorage) {
                        return refs;
                    }
                };
                return Map.of(ScalarFunction.QUERY_STRING, stub);
            }
        };
        return List.of(df, lucene);
    }

    /**
     * Builds a multi-field full-text {@code RexCall} matching the shape that the SQL plugin
     * lowers {@code query_string(['fieldName'], 'queryText')} into:
     * <pre>
     *   QUERY_STRING(
     *     MAP('fields', MAP('fieldName':VARCHAR, 1.0:DOUBLE)),
     *     MAP('query', 'queryText':VARCHAR)
     *   )
     * </pre>
     */
    private RexNode makeMultiFieldFullTextCall(SqlFunction function, List<String> fieldNames, String query) {
        List<RexNode> innerMapOperands = new ArrayList<>();
        for (String fieldName : fieldNames) {
            innerMapOperands.add(rexBuilder.makeLiteral(fieldName));
            innerMapOperands.add(rexBuilder.makeApproxLiteral(BigDecimal.valueOf(1.0)));
        }
        RexNode innerMap = rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, innerMapOperands);
        RexNode fieldsMap = rexBuilder.makeCall(SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR, rexBuilder.makeLiteral("fields"), innerMap);
        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral(query)
        );
        return rexBuilder.makeCall(function, fieldsMap, queryMap);
    }

    /**
     * Builds a field-less full-text {@code RexCall} matching the shape the SQL plugin lowers
     * {@code query_string('category:A')} (and the PPL {@code search} command's {@code category=A})
     * into:
     * <pre>
     *   QUERY_STRING(MAP('query', 'category:A':VARCHAR))
     * </pre>
     * There is no {@code fields} MAP — the field names live inside the query-string value itself.
     */
    private RexNode makeFieldlessFullTextCall(SqlFunction function, String query) {
        RexNode queryMap = rexBuilder.makeCall(
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            rexBuilder.makeLiteral("query"),
            rexBuilder.makeLiteral(query)
        );
        return rexBuilder.makeCall(function, queryMap);
    }

    // ---- Derived columns ----

    /**
     * HAVING on a derived column (the aggregate's {@code total_size} output) plans without
     * throwing. The filter has no per-field storage to narrow on, so its viable backends are
     * just the upstream aggregate's. The filter runs on the same backend that produced the
     * derived column.
     */
    public void testFilterOnDerivedColumnPlansSuccessfully() {
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

        RelNode result = runPlanner(having, context);
        assertNotNull("Planner must produce a plan for HAVING on derived column", result);
    }

    // ---- Scalar-function capability narrowing ----

    /**
     * Baseline: {@code country = 'US'} has no nested scalar function, so both backends
     * stay viable. Sets the expected shape that the next four tests narrow against.
     */
    public void testNoScalarFunctionsKeepsLuceneViable() {
        OpenSearchFilter result = runFilter(
            "parquet",
            Map.of("country_name", Map.of("type", "keyword", "index", true)),
            new String[] { "country_name" },
            new SqlTypeName[] { SqlTypeName.VARCHAR },
            makeEquals(0, SqlTypeName.VARCHAR, "US")
        );

        AnnotatedPredicate annotated = (AnnotatedPredicate) result.getCondition();
        assertPredicateAnnotation(annotated, MockDataFusionBackend.NAME, MockLuceneBackend.NAME);
    }

    /**
     * {@code UPPER(country) = 'US'}: Lucene supports EQUALS on KEYWORD but does not
     * support UPPER, so the predicate is no longer viable for Lucene.
     */
    public void testNestedScalarFunctionDropsLuceneFromPredicateAnnotation() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode upperCall = rexBuilder.makeCall(SqlStdOperatorTable.UPPER, rexBuilder.makeInputRef(varchar, 0));
        RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, upperCall, rexBuilder.makeLiteral("US"));

        OpenSearchFilter result = runFilter(
            "parquet",
            Map.of("country_name", Map.of("type", "keyword", "index", true)),
            new String[] { "country_name" },
            new SqlTypeName[] { SqlTypeName.VARCHAR },
            predicate
        );

        AnnotatedPredicate annotated = (AnnotatedPredicate) result.getCondition();
        assertEquals("Only DataFusion remains viable", 1, annotated.getViableBackends().size());
        assertTrue(annotated.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse(
            "Lucene must be excluded — no scalar capability for UPPER",
            annotated.getViableBackends().contains(MockLuceneBackend.NAME)
        );
    }

    /**
     * Four-level nesting: {@code UPPER(CONCAT(UPPER(CONCAT(name, '_a')), '_b')) = 'FOO'}.
     * Confirms the walk recurses through arbitrary depth; Lucene drops at any level it can't evaluate.
     */
    public void testDeeplyNestedScalarFunctionsDropLucene() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode level4 = rexBuilder.makeCall(SqlStdOperatorTable.CONCAT, rexBuilder.makeInputRef(varchar, 0), rexBuilder.makeLiteral("_a"));
        RexNode level3 = rexBuilder.makeCall(SqlStdOperatorTable.UPPER, level4);
        RexNode level2 = rexBuilder.makeCall(SqlStdOperatorTable.CONCAT, level3, rexBuilder.makeLiteral("_b"));
        RexNode level1 = rexBuilder.makeCall(SqlStdOperatorTable.UPPER, level2);
        RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, level1, rexBuilder.makeLiteral("FOO"));

        OpenSearchFilter result = runFilter(
            "parquet",
            Map.of("name", Map.of("type", "keyword", "index", true)),
            new String[] { "name" },
            new SqlTypeName[] { SqlTypeName.VARCHAR },
            predicate
        );

        AnnotatedPredicate annotated = (AnnotatedPredicate) result.getCondition();
        assertEquals("Only DataFusion remains viable", 1, annotated.getViableBackends().size());
        assertTrue(annotated.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse(
            "Lucene must be excluded across deeply nested scalar-function calls",
            annotated.getViableBackends().contains(MockLuceneBackend.NAME)
        );
    }

    /**
     * Predicate with a nested scalar function ({@code EXP}) that NO mock backend declares
     * scalar capability for: viableSet collapses to empty after the inner-call intersection,
     * and the existing "no backend can evaluate filter predicate" throw fires. Verifies the
     * error message names the outer comparator's SqlKind and the field, so the failure is
     * pin-pointable from logs.
     */
    public void testInnerScalarWithNoBackendSupportThrows() {
        SqlFunction expFn = SqlStdOperatorTable.EXP;
        RexNode innerCall = rexBuilder.makeCall(expFn, rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0));
        RexNode predicate = rexBuilder.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            innerCall,
            rexBuilder.makeLiteral(1.0, typeFactory.createSqlType(SqlTypeName.DOUBLE), true)
        );

        RelOptTable table = mockTable("test_index", new String[] { "n" }, new SqlTypeName[] { SqlTypeName.INTEGER });
        LogicalFilter filter = LogicalFilter.create(stubScan(table), predicate);
        PlannerContext context = buildContext("parquet", Map.of("n", Map.of("type", "integer", "index", true)));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(filter, context));
        assertTrue(
            "Message must indicate no backend can evaluate the predicate, got: " + exception.getMessage(),
            exception.getMessage().contains("No backend can evaluate filter predicate")
        );
        assertTrue("Message must name the outer comparator's kind", exception.getMessage().contains("GREATER_THAN"));
        assertTrue("Message must name the field", exception.getMessage().contains("n:integer"));
    }

    /**
     * Nested boolean tree {@code AND( $0 = 200 , OR( UPPER($s) = 'X' , >(EXP($num), 1.0) ) )}.
     * The deepest leaf has an inner {@code EXP} no backend declares; the throw fires from
     * that leaf and propagates up through OR and AND. Verifies (a) sibling leaves don't
     * suppress the un-evaluable leaf's failure and (b) the error message stays per-leaf
     * (names {@code GREATER_THAN} on {@code num:integer}, not the surrounding shape).
     */
    public void testNestedAndOrWithUnevaluableLeafThrows() {
        RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);

        // status = 200
        RexNode goodLeaf = makeEquals(0, SqlTypeName.INTEGER, 200);

        // UPPER(s) = 'X'
        RexNode upperCall = rexBuilder.makeCall(SqlStdOperatorTable.UPPER, rexBuilder.makeInputRef(varchar, 1));
        RexNode upperLeaf = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, upperCall, rexBuilder.makeLiteral("X"));

        // EXP(num) > 1.0 (no backend declares EXP)
        RexNode expCall = rexBuilder.makeCall(SqlStdOperatorTable.EXP, rexBuilder.makeInputRef(integer, 2));
        RexNode badLeaf = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, expCall, rexBuilder.makeLiteral(1.0, doubleType, true));

        RexNode condition = makeCall(SqlStdOperatorTable.AND, goodLeaf, makeCall(SqlStdOperatorTable.OR, upperLeaf, badLeaf));

        RelOptTable table = mockTable(
            "test_index",
            new String[] { "status", "s", "num" },
            new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.VARCHAR, SqlTypeName.INTEGER }
        );
        LogicalFilter filter = LogicalFilter.create(stubScan(table), condition);
        PlannerContext context = buildContext(
            "parquet",
            Map.of(
                "status",
                Map.of("type", "integer", "index", true),
                "s",
                Map.of("type", "keyword", "index", true),
                "num",
                Map.of("type", "integer", "index", true)
            )
        );

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(filter, context));
        assertTrue(
            "Message must indicate no backend can evaluate the predicate, got: " + exception.getMessage(),
            exception.getMessage().contains("No backend can evaluate filter predicate")
        );
        assertTrue("Message must name the offending leaf's comparator", exception.getMessage().contains("GREATER_THAN"));
        assertTrue("Message must name the offending leaf's field", exception.getMessage().contains("num:integer"));
        assertFalse(
            "Message must not conflate sibling leaves' fields",
            exception.getMessage().contains("status:") || exception.getMessage().contains("s:keyword")
        );
    }

    /**
     * Inter-leaf "same backend" check: {@code (MATCH(s, 'foo') OR status = 200) AND >(SIN(num), 0.5)}.
     * Each leaf is individually viable on different single backends — P1 (MATCH) is Lucene-only,
     * P3 (SIN inner call) is DataFusion-only — and no FILTER delegation is registered. The
     * operator-level intersection at {@code computeFilterViableBackends} finds no backend that
     * can evaluate every leaf, throwing the "No backend can execute filter" error. This pins
     * the cross-leaf constraint distinct from the per-leaf throw above.
     */
    public void testFilterWithMixedBackendLeavesAndNoDelegationThrows() {
        RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);

        // P1: MATCH(s, 'foo') — full-text predicate, Lucene-only (DataFusion declares no FULL_TEXT caps).
        RexNode p1 = makeFullTextCall(fullTextSqlFunction("MATCH"), 1, "foo");

        // P2: status = 200 — dual-viable on its own, doesn't help bridge P1 and P3.
        RexNode p2 = makeEquals(0, SqlTypeName.INTEGER, 200);

        // P3: SIN(num) > 0.5 — inner SIN is DataFusion-only (Lucene declares no ProjectCapability),
        // so the leaf narrows to [datafusion] via the new inner-call walk.
        RexNode sinCall = rexBuilder.makeCall(SqlStdOperatorTable.SIN, rexBuilder.makeInputRef(integer, 2));
        RexNode p3 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, sinCall, rexBuilder.makeLiteral(0.5, doubleType, true));

        RexNode condition = makeCall(SqlStdOperatorTable.AND, makeCall(SqlStdOperatorTable.OR, p1, p2), p3);

        RelOptTable table = mockTable(
            "test_index",
            new String[] { "status", "s", "num" },
            new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.VARCHAR, SqlTypeName.INTEGER }
        );
        LogicalFilter filter = LogicalFilter.create(stubScan(table), condition);
        PlannerContext context = buildContext(
            "parquet",
            Map.of(
                "status",
                Map.of("type", "integer", "index", true),
                "s",
                Map.of("type", "keyword", "index", true),
                "num",
                Map.of("type", "integer", "index", true)
            )
        );

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(filter, context));
        assertTrue(
            "Message must indicate the operator-level mismatch, got: " + exception.getMessage(),
            exception.getMessage().contains("No backend can execute filter")
        );
        assertTrue("Message must mention the missing delegation path", exception.getMessage().contains("no delegation path exists"));
    }

    /**
     * Symmetric happy path: same {@code (MATCH(s, 'foo') OR status = 200) AND >(SIN(num), 0.5)}
     * shape, but with FILTER delegation registered (DataFusion drives, Lucene accepts).
     * The operator-level intersection finds DataFusion as a viable driver — it evaluates
     * P2 and P3 natively and delegates the MATCH leaf (P1) to Lucene. No throw; the filter
     * marks {@code [datafusion]} at the operator level and P1's annotation lists Lucene as
     * the delegation target.
     */
    public void testFilterWithMixedBackendLeavesAndDelegationSucceeds() {
        RelDataType integer = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType doubleType = typeFactory.createSqlType(SqlTypeName.DOUBLE);

        RexNode p1 = makeFullTextCall(fullTextSqlFunction("MATCH"), 1, "foo");
        RexNode p2 = makeEquals(0, SqlTypeName.INTEGER, 200);
        RexNode sinCall = rexBuilder.makeCall(SqlStdOperatorTable.SIN, rexBuilder.makeInputRef(integer, 2));
        RexNode p3 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, sinCall, rexBuilder.makeLiteral(0.5, doubleType, true));
        RexNode condition = makeCall(SqlStdOperatorTable.AND, makeCall(SqlStdOperatorTable.OR, p1, p2), p3);

        OpenSearchFilter result = runFilterWithDelegation(
            "parquet",
            Map.of(
                "status",
                Map.of("type", "integer", "index", true),
                "s",
                Map.of("type", "keyword", "index", true),
                "num",
                Map.of("type", "integer", "index", true)
            ),
            new String[] { "status", "s", "num" },
            new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.VARCHAR, SqlTypeName.INTEGER },
            condition
        );

        // Operator-level: DataFusion drives. Lucene is a delegation target only, never the driver.
        assertEquals("Operator-level viable backends must be exactly [datafusion]", 1, result.getViableBackends().size());
        assertTrue(result.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse(
            "Lucene must not appear at the operator level — it accepts delegated leaves but doesn't drive",
            result.getViableBackends().contains(MockLuceneBackend.NAME)
        );

        RexCall andCondition = (RexCall) result.getCondition();
        RexCall orBranch = (RexCall) andCondition.getOperands().get(0);
        AnnotatedPredicate matchPred = (AnnotatedPredicate) orBranch.getOperands().get(0);
        AnnotatedPredicate equalsPred = (AnnotatedPredicate) orBranch.getOperands().get(1);
        AnnotatedPredicate sinPred = (AnnotatedPredicate) andCondition.getOperands().get(1);

        // P1 (MATCH) — exactly Lucene; DataFusion will delegate it.
        assertEquals("P1 (MATCH) viable backends must be exactly [lucene]", 1, matchPred.getViableBackends().size());
        assertTrue(matchPred.getViableBackends().contains(MockLuceneBackend.NAME));

        // P2 (EQUALS on integer) — exactly both backends, dual-viable.
        assertEquals("P2 (EQUALS) viable backends must be exactly [datafusion, lucene]", 2, equalsPred.getViableBackends().size());
        assertTrue(equalsPred.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertTrue(equalsPred.getViableBackends().contains(MockLuceneBackend.NAME));

        // P3 (SIN > 0.5) — exactly DataFusion; Lucene declares no scalar capability for SIN
        // so the inner-call walk drops it.
        assertEquals("P3 (SIN) viable backends must be exactly [datafusion]", 1, sinPred.getViableBackends().size());
        assertTrue(sinPred.getViableBackends().contains(MockDataFusionBackend.NAME));
    }

    /**
     * AND of {@code status = 200} (no nested function) and {@code UPPER(country) = 'US'}
     * (nested UPPER). Each leaf is annotated independently: Lucene stays on the first
     * leaf and drops only on the second.
     */
    public void testScalarFunctionNarrowingIsPerLeaf() {
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode upperCall = rexBuilder.makeCall(SqlStdOperatorTable.UPPER, rexBuilder.makeInputRef(varchar, 1));
        RexNode upperEq = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, upperCall, rexBuilder.makeLiteral("US"));

        OpenSearchFilter result = runFilter(
            "parquet",
            Map.of("status", Map.of("type", "integer", "index", true), "country_name", Map.of("type", "keyword", "index", true)),
            new String[] { "status", "country_name" },
            new SqlTypeName[] { SqlTypeName.INTEGER, SqlTypeName.VARCHAR },
            makeAnd(makeEquals(0, SqlTypeName.INTEGER, 200), upperEq)
        );

        RexCall andCondition = (RexCall) result.getCondition();
        AnnotatedPredicate plainEq = (AnnotatedPredicate) andCondition.getOperands().get(0);
        AnnotatedPredicate upperEqAnnotated = (AnnotatedPredicate) andCondition.getOperands().get(1);

        assertPredicateAnnotation(plainEq, MockDataFusionBackend.NAME, MockLuceneBackend.NAME);
        assertEquals("Only DataFusion remains viable for the scalar-function leaf", 1, upperEqAnnotated.getViableBackends().size());
        assertTrue(upperEqAnnotated.getViableBackends().contains(MockDataFusionBackend.NAME));
        assertFalse(
            "Lucene must be excluded only on the scalar-function leaf",
            upperEqAnnotated.getViableBackends().contains(MockLuceneBackend.NAME)
        );
    }

    /**
     * A nested scalar function that the framework doesn't know about (no entry in the
     * {@link org.opensearch.analytics.spi.ScalarFunction} enum) makes the predicate
     * unevaluable on every backend. Marking should fail fast and the error should name
     * the unrecognized function.
     */
    public void testUnrecognizedScalarFunctionThrows() {
        SqlFunction unknown = new SqlFunction(
            "FAKE_UDF",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000,
            null,
            OperandTypes.ANY,
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
        RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        RexNode unknownCall = rexBuilder.makeCall(unknown, rexBuilder.makeInputRef(varchar, 0));
        RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, unknownCall, rexBuilder.makeLiteral("X"));

        RelOptTable table = mockTable("test_index", new String[] { "name" }, new SqlTypeName[] { SqlTypeName.VARCHAR });
        LogicalFilter filter = LogicalFilter.create(stubScan(table), predicate);
        PlannerContext context = buildContext("parquet", Map.of("name", Map.of("type", "keyword", "index", true)));

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> runPlanner(filter, context));
        assertTrue(
            "Message must name the unrecognized scalar function",
            exception.getMessage().contains("Unrecognized scalar function [FAKE_UDF]")
        );
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

    /** Mirrors {@code runFilter} but injects a {@link DelegationBlockList} into the planner context. */
    private OpenSearchFilter runFilterWithBlockList(
        String format,
        Map<String, Map<String, Object>> fields,
        String[] fieldNames,
        SqlTypeName[] fieldTypes,
        RexNode condition,
        DelegationBlockList blockList
    ) {
        PlannerContext context = buildContext(format, fields, List.of(DATAFUSION, LUCENE));
        context.setPlannerSettings(PlannerSettings.of(0.0, blockList));
        RelOptTable table = mockTable("test_index", fieldNames, fieldTypes);
        LogicalFilter filter = LogicalFilter.create(stubScan(table), condition);
        RelNode result = unwrapExchange(runPlanner(filter, context));
        assertTrue("Expected OpenSearchFilter, got " + result.getClass().getSimpleName(), result instanceof OpenSearchFilter);
        return (OpenSearchFilter) result;
    }

    /**
     * Variant of {@code runFilter} that resolves to {@code multipleIndexNames} concrete indices
     * sharing identical mapping/settings, simulating an alias scan. Uses
     * {@link BasePlannerRulesTests#buildContextPerIndex} to register all indices under the same
     * table name. The fragment's table name is the first index in the list — that's what the
     * scan rule looks up; since all backings have the same mapping, the unioned field storage
     * matches single-index behavior.
     */
    private OpenSearchFilter runMultiIndexFilter(
        String[] indexNames,
        String format,
        Map<String, Map<String, Object>> fields,
        String[] fieldNames,
        SqlTypeName[] fieldTypes,
        RexNode condition
    ) {
        java.util.Map<String, Integer> shardCounts = new java.util.LinkedHashMap<>();
        for (String name : indexNames) {
            shardCounts.put(name, 1);
        }
        PlannerContext context = buildContextPerIndex(format, shardCounts, fields, delegationBackends());
        RelOptTable table = mockTable(indexNames[0], fieldNames, fieldTypes);
        LogicalFilter filter = LogicalFilter.create(stubScan(table), condition);
        RelNode result = unwrapExchange(runPlanner(filter, context));
        assertTrue("Expected OpenSearchFilter, got " + result.getClass().getSimpleName(), result instanceof OpenSearchFilter);
        assertPipelineViableBackends(
            result,
            List.of(OpenSearchFilter.class, OpenSearchTableScan.class),
            Set.of(MockDataFusionBackend.NAME)
        );
        return (OpenSearchFilter) result;
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

    public void testBackendWithFilterDelegationButNoFactory_throws() {
        AnalyticsSearchBackendPlugin badBackend = new AnalyticsSearchBackendPlugin() {
            @Override
            public String name() {
                return "bad-backend";
            }

            @Override
            public BackendCapabilityProvider getCapabilityProvider() {
                return new BackendCapabilityProvider() {
                    @Override
                    public Set<EngineCapability> supportedEngineCapabilities() {
                        return Set.of();
                    }

                    @Override
                    public Set<DelegationType> supportedDelegations() {
                        return Set.of(DelegationType.FILTER);
                    }
                };
            }
        };

        IllegalStateException exception = expectThrows(
            IllegalStateException.class,
            () -> new CapabilityRegistry(List.of(badBackend), idx -> null)
        );
        assertTrue(exception.getMessage().contains("bad-backend"));
        assertTrue(exception.getMessage().contains("getInstructionHandlerFactory"));
    }
}
