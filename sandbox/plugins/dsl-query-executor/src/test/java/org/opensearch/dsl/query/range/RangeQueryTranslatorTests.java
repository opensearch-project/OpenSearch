/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query.range;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class RangeQueryTranslatorTests extends OpenSearchTestCase {

    private final RangeQueryTranslator translator = new RangeQueryTranslator();
    private final ConversionContext ctx = TestUtils.createContext();

    public void testGte() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").gte(100), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertEquals(2, call.getOperands().size());

        RexInputRef fieldRef = (RexInputRef) call.getOperands().get(0);
        assertEquals(1, fieldRef.getIndex());
        assertEquals("price", ctx.getRowType().getFieldList().get(1).getName());

        assertNotNull(call.getOperands().get(1));
    }

    public void testGt() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").gt(100), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN, call.getKind());
        assertEquals(2, call.getOperands().size());

        RexInputRef fieldRef = (RexInputRef) call.getOperands().get(0);
        assertEquals(1, fieldRef.getIndex());
    }

    public void testLte() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").lte(500), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, call.getKind());
        assertEquals(2, call.getOperands().size());

        RexInputRef fieldRef = (RexInputRef) call.getOperands().get(0);
        assertEquals(1, fieldRef.getIndex());
    }

    public void testLt() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").lt(500), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LESS_THAN, call.getKind());
        assertEquals(2, call.getOperands().size());

        RexInputRef fieldRef = (RexInputRef) call.getOperands().get(0);
        assertEquals(1, fieldRef.getIndex());
    }

    public void testBothBounds() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").gte(100).lte(500), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.AND, call.getKind());
        assertEquals(2, call.getOperands().size());

        RexCall lowerBound = (RexCall) call.getOperands().get(0);
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, lowerBound.getKind());
        assertEquals(2, lowerBound.getOperands().size());
        assertEquals(1, ((RexInputRef) lowerBound.getOperands().get(0)).getIndex());

        RexCall upperBound = (RexCall) call.getOperands().get(1);
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, upperBound.getKind());
        assertEquals(2, upperBound.getOperands().size());
        assertEquals(1, ((RexInputRef) upperBound.getOperands().get(0)).getIndex());
    }

    public void testWithFormat() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.rangeQuery("rating").gte("01/01/2022").lte("31/12/2022").format("dd/MM/yyyy"),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.AND, call.getKind());
        assertEquals(2, call.getOperands().size());

        RexCall lowerBound = (RexCall) call.getOperands().get(0);
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, lowerBound.getKind());
        assertEquals(3, ((RexInputRef) lowerBound.getOperands().get(0)).getIndex());
    }

    public void testWithTimeZone() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.rangeQuery("rating").gte("2022-01-01T00:00:00").timeZone("America/New_York"),
            ctx
        );

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
    }

    public void testWithFormatAndTimeZone() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("rating").gte("01/01/2022").format("dd/MM/yyyy").timeZone("UTC"), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
    }

    public void testDateMathNow() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("rating").gte("now"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertEquals(2, call.getOperands().size());
        assertEquals(3, ((RexInputRef) call.getOperands().get(0)).getIndex());
        assertNotNull(call.getOperands().get(1));
    }

    public void testDateMathSubtraction() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("rating").gte("now-7d"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertEquals(2, call.getOperands().size());
        assertEquals(3, ((RexInputRef) call.getOperands().get(0)).getIndex());
    }

    public void testDateMathAddition() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("rating").lte("now+1M"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, call.getKind());
        assertEquals(2, call.getOperands().size());
        assertEquals(3, ((RexInputRef) call.getOperands().get(0)).getIndex());
    }

    public void testDateMathRounding() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("rating").gte("now-1d/d"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertEquals(2, call.getOperands().size());
        assertEquals(3, ((RexInputRef) call.getOperands().get(0)).getIndex());
        assertNotNull(call.getOperands().get(1));
    }

    public void testDateMathWithFormat() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("rating").gte("01/01/2022||+1M").format("dd/MM/yyyy"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertEquals(2, call.getOperands().size());
        assertEquals(3, ((RexInputRef) call.getOperands().get(0)).getIndex());
    }

    public void testRoundingWithGte() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("rating").gte("now-1d/d"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertEquals(2, call.getOperands().size());

        RexInputRef fieldRef = (RexInputRef) call.getOperands().get(0);
        assertEquals(3, fieldRef.getIndex());
        assertEquals("rating", ctx.getRowType().getFieldList().get(3).getName());
    }

    public void testRoundingWithGt() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("rating").gt("now-1d/d"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN, call.getKind());
        assertEquals(2, call.getOperands().size());
        assertEquals(3, ((RexInputRef) call.getOperands().get(0)).getIndex());
    }

    public void testRoundingWithLte() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("rating").lte("now/d"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, call.getKind());
        assertEquals(2, call.getOperands().size());
        assertEquals(3, ((RexInputRef) call.getOperands().get(0)).getIndex());
    }

    public void testRoundingWithLt() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("rating").lt("now/d"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LESS_THAN, call.getKind());
        assertEquals(2, call.getOperands().size());
        assertEquals(3, ((RexInputRef) call.getOperands().get(0)).getIndex());
    }

    public void testRoundingBothBounds() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("rating").gte("now-7d/d").lte("now/d"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.AND, call.getKind());
        assertEquals(2, call.getOperands().size());

        RexCall lowerBound = (RexCall) call.getOperands().get(0);
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, lowerBound.getKind());
        assertEquals(2, lowerBound.getOperands().size());
        assertEquals(3, ((RexInputRef) lowerBound.getOperands().get(0)).getIndex());

        RexCall upperBound = (RexCall) call.getOperands().get(1);
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, upperBound.getKind());
        assertEquals(2, upperBound.getOperands().size());
        assertEquals(3, ((RexInputRef) upperBound.getOperands().get(0)).getIndex());
    }

    public void testRoundingMonthWithGte() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("rating").gte("now-1M/M"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertEquals(2, call.getOperands().size());
        assertEquals(3, ((RexInputRef) call.getOperands().get(0)).getIndex());
    }

    public void testRoundingYearWithLt() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("rating").lt("now/y"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LESS_THAN, call.getKind());
        assertEquals(2, call.getOperands().size());
        assertEquals(3, ((RexInputRef) call.getOperands().get(0)).getIndex());
    }

    public void testWithIntersectsRelation() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").gte(100).relation("INTERSECTS"), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
    }

    /**
     * Verifies CONTAINS relation is silently ignored on scalar fields, matching legacy behavior.
     * In legacy OpenSearch, SimpleMappedFieldType.rangeQuery() and DateFieldType.rangeQuery()
     * silently drop the relation parameter for scalar fields — the built query is identical
     * regardless of INTERSECTS, CONTAINS, or WITHIN.
     */
    public void testContainsRelationIgnoredOnScalarField() throws ConversionException {
        RexNode withContains = translator.convert(QueryBuilders.rangeQuery("price").gte(100).relation("CONTAINS"), ctx);
        RexNode withoutRelation = translator.convert(QueryBuilders.rangeQuery("price").gte(100), ctx);

        assertEquals(
            "CONTAINS relation must produce identical expression to no-relation (legacy scalar behavior)",
            withoutRelation.toString(),
            withContains.toString()
        );
    }

    /**
     * Verifies WITHIN relation is silently ignored on scalar fields, matching legacy behavior.
     * In legacy OpenSearch, SimpleMappedFieldType.rangeQuery() and DateFieldType.rangeQuery()
     * silently drop the relation parameter for scalar fields — the built query is identical
     * regardless of INTERSECTS, CONTAINS, or WITHIN.
     */
    public void testWithinRelationIgnoredOnScalarField() throws ConversionException {
        RexNode withWithin = translator.convert(QueryBuilders.rangeQuery("price").gte(100).relation("WITHIN"), ctx);
        RexNode withoutRelation = translator.convert(QueryBuilders.rangeQuery("price").gte(100), ctx);

        assertEquals(
            "WITHIN relation must produce identical expression to no-relation (legacy scalar behavior)",
            withoutRelation.toString(),
            withWithin.toString()
        );
    }

    /**
     * Verifies DISJOINT relation is rejected. In legacy OpenSearch, SimpleMappedFieldType.rangeQuery()
     * and DateFieldType.rangeQuery() reject DISJOINT with IllegalArgumentException. However,
     * RangeQueryBuilder.relation("DISJOINT") itself rejects DISJOINT at the builder level
     * (isRelationAllowed returns false), so our translator never sees it. This test asserts
     * the builder-level rejection that enforces the legacy contract earlier in the stack.
     */
    public void testDisjointRelationRejected() {
        // RangeQueryBuilder.relation("DISJOINT") throws IllegalArgumentException because
        // isRelationAllowed() only permits INTERSECTS, CONTAINS, and WITHIN.
        // Legacy scalar mappers also reject DISJOINT, so the contract is preserved.
        expectThrows(IllegalArgumentException.class, () -> QueryBuilders.rangeQuery("price").gte(100).relation("DISJOINT"));
    }

    /**
     * Unmapped field returns literal false (match-none), matching legacy DISJOINT for null fieldType.
     */
    public void testUnknownFieldMatchesNone() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("unknown").gte(1), ctx);
        assertTrue("Unmapped field should produce literal false", result instanceof RexLiteral);
        RexLiteral literal = (RexLiteral) result;
        assertEquals(Boolean.FALSE, literal.getValueAs(Boolean.class));
    }

    public void testThrowsForBoost() {
        RangeQueryBuilder query = QueryBuilders.rangeQuery("price").gte(100);
        query.boost(2.0f);
        expectThrows(ConversionException.class, () -> translator.convert(query, ctx));
    }

    /**
     * No bounds produces IS_NOT_NULL, matching legacy RangeQueryBuilder.doToQuery exists rewrite.
     */
    public void testNoBoundsBecomesExists() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price"), ctx);

        assertTrue("No-bounds should produce IS NOT NULL call", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.IS_NOT_NULL, call.getKind());
        assertEquals(1, call.getOperands().size());
        RexInputRef fieldRef = (RexInputRef) call.getOperands().get(0);
        assertEquals(1, fieldRef.getIndex());
    }

    public void testReportsCorrectQueryType() {
        assertEquals(RangeQueryBuilder.class, translator.getQueryType());
    }

    // ========== HELPER METHODS FOR LITERAL VALUE EXTRACTION ==========

    /**
     * Unwraps a RexNode to get the underlying RexLiteral, handling CAST wrappers.
     * Calcite may wrap literals in CAST when the type doesn't exactly match.
     */
    private RexLiteral unwrapLiteral(RexNode node) {
        if (node instanceof RexLiteral) {
            return (RexLiteral) node;
        }
        if (node instanceof RexCall) {
            RexCall call = (RexCall) node;
            if (call.getKind() == SqlKind.CAST) {
                return unwrapLiteral(call.getOperands().get(0));
            }
        }
        fail("Expected RexLiteral or CAST(RexLiteral), got: " + node.getClass().getSimpleName() + " = " + node);
        return null; // unreachable
    }

    /**
     * Asserts that the literal operand (operand 1) of a comparison RexCall represents
     * the given epoch millis value. Handles both direct RexLiteral and CAST-wrapped forms.
     * Calcite stores TIMESTAMP(3) literals as millis-since-epoch internally.
     */
    private void assertLiteralEpoch(RexNode comparison, long expectedEpochMillis) {
        assertTrue("Expected RexCall comparison, got: " + comparison.getClass(), comparison instanceof RexCall);
        RexCall call = (RexCall) comparison;
        RexNode operand1 = call.getOperands().get(1);
        RexLiteral literal = unwrapLiteral(operand1);
        assertNotNull("Could not unwrap literal from: " + operand1, literal);
        // For TIMESTAMP(3) literals, Calcite stores the value as millis from epoch
        // getValueAs(Long.class) returns the internal millis representation
        Long actualValue = literal.getValueAs(Long.class);
        assertNotNull("Literal value is null", actualValue);
        assertEquals("Epoch millis mismatch", expectedEpochMillis, actualValue.longValue());
    }

    /**
     * Asserts that the literal operand (operand 1) of a comparison RexCall has the given
     * numeric value and that its type matches the expected SqlTypeName.
     */
    private void assertLiteralNumber(RexNode comparison, Number expectedValue, SqlTypeName expectedType) {
        assertTrue("Expected RexCall comparison, got: " + comparison.getClass(), comparison instanceof RexCall);
        RexCall call = (RexCall) comparison;
        RexNode operand1 = call.getOperands().get(1);
        RexLiteral literal = unwrapLiteral(operand1);
        assertNotNull("Could not unwrap literal from: " + operand1, literal);
        if (expectedType != null) {
            assertEquals("Type mismatch", expectedType, literal.getTypeName());
        }
        Number actualValue = literal.getValueAs(expectedValue.getClass());
        assertNotNull("Literal numeric value is null", actualValue);
        assertEquals("Numeric value mismatch", expectedValue, actualValue);
    }

    /**
     * Asserts that the literal operand (operand 1) of a comparison RexCall has the given string value.
     */
    private void assertLiteralString(RexNode comparison, String expectedValue) {
        assertTrue("Expected RexCall comparison, got: " + comparison.getClass(), comparison instanceof RexCall);
        RexCall call = (RexCall) comparison;
        RexNode operand1 = call.getOperands().get(1);
        RexLiteral literal = unwrapLiteral(operand1);
        assertNotNull("Could not unwrap literal from: " + operand1, literal);
        String actualValue = literal.getValueAs(String.class);
        assertEquals("String literal mismatch", expectedValue, actualValue);
    }

    // ========== GROUP A - VALUE CORRECTNESS ==========

    /** gte(100) on numeric field produces literal 100 with Calcite canonical DECIMAL type. */
    public void testGteNumericLiteralValue() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").gte(100), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        RexNode operand1 = call.getOperands().get(1);
        RexLiteral literal = unwrapLiteral(operand1);
        assertNotNull(literal);
        // Value must be exactly 100; Calcite makeLiteral with INTEGER field type stores as DECIMAL
        assertEquals(Integer.valueOf(100), literal.getValueAs(Integer.class));
        assertEquals(SqlTypeName.DECIMAL, literal.getTypeName());
    }

    /** Custom format dd/MM/yyyy parses "01/01/2022" to epoch 1640995200000 (2022-01-01T00:00:00Z). */
    public void testFormatParsesToExactEpoch() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("event_time").gte("01/01/2022").format("dd/MM/yyyy"), ctx);

        assertLiteralEpoch(result, 1640995200000L);
    }

    /** timeZone("America/New_York") shifts the parsed epoch by UTC-5 offset. */
    public void testTimeZoneShiftsEpoch() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.rangeQuery("event_time").gte("2022-01-01T00:00:00").timeZone("America/New_York"),
            ctx
        );

        // 2022-01-01T00:00:00 in America/New_York = 2022-01-01T05:00:00Z = 1641013200000L
        assertLiteralEpoch(result, 1641013200000L);
    }

    /** lte("2022-01-01") on date field rounds up to end-of-day (23:59:59.999Z). */
    public void testDateOnlyUpperBoundRoundsUp() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("event_time").lte("2022-01-01"), ctx);

        // End of 2022-01-01 = 2022-01-01T23:59:59.999Z = 1641081599999L
        assertLiteralEpoch(result, 1641081599999L);
    }

    // ========== GROUP B - EPOCH_MILLIS (P0) ==========

    /** format("epoch_millis") with string "1640995200000" produces literal == 1640995200000L. */
    public void testEpochMillisFormatGte() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("event_time").gte("1640995200000").format("epoch_millis"), ctx);

        assertLiteralEpoch(result, 1640995200000L);
    }

    /** epoch_millis is absolute; timeZone parameter does not shift the value. */
    public void testEpochMillisIgnoresTimeZone() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.rangeQuery("event_time").gte("1640995200000").format("epoch_millis").timeZone("America/New_York"),
            ctx
        );

        // Epoch millis is absolute; timezone should be irrelevant
        assertLiteralEpoch(result, 1640995200000L);
    }

    /** epoch_millis with non-numeric string "abc" throws ConversionException. */
    public void testEpochMillisInvalidValueThrows() {
        expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("event_time").gte("abc").format("epoch_millis"), ctx)
        );
    }

    /** Raw long value on date field produces timestamp literal at the given epoch millis. */
    public void testEpochMillisLongBoundOnDateField() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("event_time").gte(1640995200000L), ctx);

        assertLiteralEpoch(result, 1640995200000L);
    }

    // ========== GROUP C - NON-DATE FIELDS ==========

    /**
     * String "100" on numeric field coerces to numeric 100, not TIMESTAMP.
     * Calcite canonically types exact-numeric literals as DECIMAL.
     */
    public void testNumericStringOnNumericField() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").gte("100"), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        RexNode operand1 = call.getOperands().get(1);
        RexLiteral literal = unwrapLiteral(operand1);
        assertNotNull(literal);
        // Value must be 100
        assertEquals("Value should be 100", Integer.valueOf(100), literal.getValueAs(Integer.class));
        // Must NOT be mistyped as TIMESTAMP — DECIMAL/INTEGER/BIGINT are all acceptable
        assertNotEquals("Numeric string on numeric field must NOT produce TIMESTAMP type", SqlTypeName.TIMESTAMP, literal.getTypeName());
    }

    /**
     * String bounds on VARCHAR field produce lexicographic comparisons without date parsing.
     */
    public void testLexicographicRangeOnKeywordField() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("brand").gte("apple").lte("dell"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.AND, call.getKind());

        RexCall lowerBound = (RexCall) call.getOperands().get(0);
        assertLiteralString(lowerBound, "apple");

        RexCall upperBound = (RexCall) call.getOperands().get(1);
        assertLiteralString(upperBound, "dell");

        // Assert operators
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, lowerBound.getKind());
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, upperBound.getKind());

        // Assert column reference (brand is index 2 in TestUtils schema)
        RexInputRef lowerRef = (RexInputRef) lowerBound.getOperands().get(0);
        assertEquals(2, lowerRef.getIndex());
        RexInputRef upperRef = (RexInputRef) upperBound.getOperands().get(0);
        assertEquals(2, upperRef.getIndex());

        // Assert literal type (Calcite produces CHAR for string literals)
        RexLiteral lowerLit = unwrapLiteral(lowerBound.getOperands().get(1));
        assertEquals(SqlTypeName.CHAR, lowerLit.getTypeName());
        RexLiteral upperLit = unwrapLiteral(upperBound.getOperands().get(1));
        assertEquals(SqlTypeName.CHAR, upperLit.getTypeName());
    }

    /**
     * Exclusive string bounds on keyword field produce GREATER_THAN / LESS_THAN operators.
     */
    public void testExclusiveLexicographicRangeOnKeywordField() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("brand").gt("apple").lt("dell"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.AND, call.getKind());

        RexCall lowerBound = (RexCall) call.getOperands().get(0);
        RexCall upperBound = (RexCall) call.getOperands().get(1);

        // Assert exclusive operators
        assertEquals(SqlKind.GREATER_THAN, lowerBound.getKind());
        assertEquals(SqlKind.LESS_THAN, upperBound.getKind());

        // Assert column reference
        RexInputRef lowerRef = (RexInputRef) lowerBound.getOperands().get(0);
        assertEquals(2, lowerRef.getIndex());
        RexInputRef upperRef = (RexInputRef) upperBound.getOperands().get(0);
        assertEquals(2, upperRef.getIndex());

        // Assert literal values and type
        assertLiteralString(lowerBound, "apple");
        assertLiteralString(upperBound, "dell");
        RexLiteral lowerLit = unwrapLiteral(lowerBound.getOperands().get(1));
        assertEquals(SqlTypeName.CHAR, lowerLit.getTypeName());
        RexLiteral upperLit = unwrapLiteral(upperBound.getOperands().get(1));
        assertEquals(SqlTypeName.CHAR, upperLit.getTypeName());
    }

    /**
     * Long value on numeric field produces numeric literal, not TIMESTAMP(3).
     * Calcite's canonical DECIMAL type is acceptable.
     */
    public void testLongValueOnNumericFieldKeepsFieldType() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").gte(100L), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        RexNode operand1 = call.getOperands().get(1);
        RexLiteral literal = unwrapLiteral(operand1);
        assertNotNull(literal);
        // Must NOT be mistyped as TIMESTAMP — DECIMAL/INTEGER/BIGINT are all acceptable
        assertNotEquals("Long value on numeric field should NOT produce TIMESTAMP type", SqlTypeName.TIMESTAMP, literal.getTypeName());
        assertEquals("Value should be 100", Long.valueOf(100L), literal.getValueAs(Long.class));
    }

    /**
     * Double value on DOUBLE field produces DOUBLE-typed literal with exact value.
     */
    public void testDoubleValueOnNumericField() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("rating").gte(99.5), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        RexNode operand1 = call.getOperands().get(1);
        RexLiteral literal = unwrapLiteral(operand1);
        assertNotNull(literal);
        assertEquals("Expected DOUBLE type for double value on DOUBLE field", SqlTypeName.DOUBLE, literal.getTypeName());
        assertEquals(Double.valueOf(99.5), literal.getValueAs(Double.class));
    }

    // ========== GROUP D - ROUNDING EDGE CASES ==========

    /**
     * lte("31/12/2022") with format("dd/MM/yyyy") rounds up to end-of-day.
     * Expected: 2022-12-31T23:59:59.999Z = 1672531199999L.
     */
    public void testFormatWithSlashSeparatorsStillRoundsUp() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("event_time").lte("31/12/2022").format("dd/MM/yyyy"), ctx);

        // End of 2022-12-31 = 1672531199999L
        assertLiteralEpoch(result, 1672531199999L);
    }

    /** lte("now/d") with explicit rounding operator produces expected shape (nondeterministic value). */
    public void testExplicitRoundingNotDoubleRounded() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("event_time").lte("now/d"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, call.getKind());
        assertEquals(2, call.getOperands().size());
        // event_time is index 10 in the schema
        assertEquals(10, ((RexInputRef) call.getOperands().get(0)).getIndex());
        assertNotNull(call.getOperands().get(1));
    }

    /**
     * gte("2022-06-15||/M") rounds down to start of month: 2022-06-01T00:00:00Z = 1654041600000L.
     */
    public void testFixedDateMathRounding() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("event_time").gte("2022-06-15||/M"), ctx);

        assertLiteralEpoch(result, 1654041600000L);
    }

    // ========== GROUP E - PARAMETER AUDIT ==========

    /**
     * queryName("my_range") throws ConversionException; unsupported parameters are rejected.
     */
    public void testThrowsForQueryName() {
        RangeQueryBuilder query = QueryBuilders.rangeQuery("price").gte(100);
        query.queryName("my_range");
        expectThrows(ConversionException.class, () -> translator.convert(query, ctx));
    }

    /**
     * Invalid timezone string throws IllegalArgumentException at builder construction time
     * since RangeQueryBuilder.timeZone() calls ZoneId.of() which validates immediately.
     */
    public void testInvalidTimeZoneThrows() {
        // RangeQueryBuilder.timeZone() calls ZoneId.of() which throws for invalid zones,
        // wrapped in IllegalArgumentException by the builder
        expectThrows(IllegalArgumentException.class, () -> QueryBuilders.rangeQuery("price").gte(100).timeZone("Invalid/NotAZone"));
    }

    // ========== GROUP G - DATE ROUNDING KEYED ON INCLUSIVITY ==========

    /**
     * gt "2022-01-01" on date field rounds UP (end-of-day) because exclusive lower bound
     * uses roundUp=true per DateFieldMapper.dateRangeQuery (roundUp=!includeLower).
     * Expected: 2022-01-01T23:59:59.999Z = 1641081599999L, operator GREATER_THAN.
     */
    public void testGtDateRoundsUpExclusive() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("event_time").gt("2022-01-01"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN, call.getKind());
        assertLiteralEpoch(result, 1641081599999L);
    }

    /**
     * lt "2022-01-01" on date field rounds DOWN (start-of-day) because exclusive upper bound
     * uses roundUp=false per DateFieldMapper.dateRangeQuery (roundUp=includeUpper=false).
     * Expected: 2022-01-01T00:00:00.000Z = 1640995200000L, operator LESS_THAN.
     */
    public void testLtDateRoundsDownExclusive() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("event_time").lt("2022-01-01"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LESS_THAN, call.getKind());
        assertLiteralEpoch(result, 1640995200000L);
    }

    // ========== GROUP H - DECIMAL BOUNDS ON INTEGER FIELDS ==========

    /**
     * gt 10.5 on INTEGER field produces gte 11. Per NumberFieldMapper INTEGER.rangeQuery: positive decimal lower bound increments.
     */
    public void testGtDecimalOnIntegerFieldPositive() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").gt(10.5), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertLiteralNumber(result, Integer.valueOf(11), null);
    }

    /**
     * gte 10.5 on INTEGER field produces gte 11. Per NumberFieldMapper INTEGER.rangeQuery: positive decimal lower bound increments.
     */
    public void testGteDecimalOnIntegerFieldPositive() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").gte(10.5), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertLiteralNumber(result, Integer.valueOf(11), null);
    }

    /**
     * lt 10.5 on INTEGER field produces lte 10. Per NumberFieldMapper INTEGER.rangeQuery: positive decimal upper bound, no decrement.
     */
    public void testLtDecimalOnIntegerFieldPositive() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").lt(10.5), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, call.getKind());
        assertLiteralNumber(result, Integer.valueOf(10), null);
    }

    /**
     * lte 10.5 on INTEGER field produces lte 10. Per NumberFieldMapper INTEGER.rangeQuery: positive decimal upper bound, no decrement.
     */
    public void testLteDecimalOnIntegerFieldPositive() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").lte(10.5), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, call.getKind());
        assertLiteralNumber(result, Integer.valueOf(10), null);
    }

    /**
     * gt -10.5 on INTEGER field produces gte -10. Per NumberFieldMapper INTEGER.rangeQuery: negative decimal lower bound, no increment.
     */
    public void testGtDecimalOnIntegerFieldNegative() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").gt(-10.5), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertLiteralNumber(result, Integer.valueOf(-10), null);
    }

    /**
     * gte -10.5 on INTEGER field produces gte -10. Per NumberFieldMapper INTEGER.rangeQuery: negative decimal lower bound, no increment.
     */
    public void testGteDecimalOnIntegerFieldNegative() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").gte(-10.5), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertLiteralNumber(result, Integer.valueOf(-10), null);
    }

    /**
     * lt -10.5 on INTEGER field produces lte -11. Per NumberFieldMapper INTEGER.rangeQuery: negative decimal upper bound decrements.
     */
    public void testLtDecimalOnIntegerFieldNegative() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").lt(-10.5), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, call.getKind());
        assertLiteralNumber(result, Integer.valueOf(-11), null);
    }

    /**
     * lte -10.5 on INTEGER field produces lte -11. Per NumberFieldMapper INTEGER.rangeQuery: negative decimal upper bound decrements.
     */
    public void testLteDecimalOnIntegerFieldNegative() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").lte(-10.5), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, call.getKind());
        assertLiteralNumber(result, Integer.valueOf(-11), null);
    }

    // ========== GROUP I - OVERFLOW GUARDS ==========

    /**
     * gt 2147483647.5 on INTEGER field returns FALSE (match-none). Overflow guard:
     * truncate to Integer.MAX_VALUE, increment would overflow, matching NumberFieldMapper MatchNoDocsQuery.
     */
    public void testGtDecimalAtIntegerMaxMatchesNone() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").gt(2147483647.5), ctx);

        assertTrue("Should produce literal false (match-none) at Integer.MAX_VALUE boundary", result instanceof RexLiteral);
        RexLiteral literal = (RexLiteral) result;
        assertEquals(Boolean.FALSE, literal.getValueAs(Boolean.class));
    }

    /**
     * lt -2147483648.5 on INTEGER field returns FALSE (match-none). Overflow guard:
     * truncate to Integer.MIN_VALUE, decrement would overflow, matching NumberFieldMapper MatchNoDocsQuery.
     */
    public void testLtDecimalAtIntegerMinMatchesNone() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").lt(-2147483648.5), ctx);

        assertTrue("Should produce literal false (match-none) at Integer.MIN_VALUE boundary", result instanceof RexLiteral);
        RexLiteral literal = (RexLiteral) result;
        assertEquals(Boolean.FALSE, literal.getValueAs(Boolean.class));
    }

    /**
     * gt decimal on BIGINT field performs correct decimal-adjust with increment.
     * Uses a value within double's exact-integer range (below 2^52) where the fractional part is preserved.
     */
    public void testGtDecimalAtLongMaxMatchesNone() throws ConversionException {
        // 4503599627370495.5 has exact double representation (below 2^52 boundary)
        // Decimal-adjust: truncate -> 4503599627370495, positive -> increment -> >= 4503599627370496
        RexNode result = translator.convert(QueryBuilders.rangeQuery("timestamp").gt(4503599627370495.5), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertLiteralNumber(result, Long.valueOf(4503599627370496L), null);
    }

    /**
     * lt negative decimal on BIGINT field performs correct decimal-adjust with decrement.
     * Uses a value within double's exact-integer range where the fractional part is preserved.
     */
    public void testLtDecimalAtLongMinMatchesNone() throws ConversionException {
        // -4503599627370495.5 has exact double representation, negative -> decrement
        // Decimal-adjust: truncate -> -4503599627370495, negative upper -> decrement -> <= -4503599627370496
        RexNode result = translator.convert(QueryBuilders.rangeQuery("timestamp").lt(-4503599627370495.5), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, call.getKind());
        assertLiteralNumber(result, Long.valueOf(-4503599627370496L), null);
    }

    // ========== GROUP J - STRING-ORIGIN DECIMALS ON INTEGER FIELDS ==========

    /**
     * gt "10.5" as String on INTEGER field behaves identically to gt 10.5 raw double: produces gte 11.
     */
    public void testGtDecimalStringOnIntegerField() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").gt("10.5"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertLiteralNumber(result, Integer.valueOf(11), null);
    }

    // ========== GROUP K - DOCUMENTATION TESTS ==========

    /**
     * gt 10.0 (whole double) on integer field uses exclusive > 10 without adjustment.
     * Only non-zero fractional parts trigger decimal-adjust, matching NumberFieldMapper.hasDecimalPart.
     */
    public void testGtWholeDoubleOnIntegerField() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").gt(10.0), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        // 10.0 has no decimal part, so no adjustment; stays exclusive GREATER_THAN
        assertEquals(SqlKind.GREATER_THAN, call.getKind());
        assertLiteralNumber(result, Integer.valueOf(10), null);
    }

    /**
     * gt "2022-01-01||/M" on date field: exclusive lower bound -> roundUp=true per
     * DateFieldMapper.dateRangeQuery, DateMathParser rounds to end-of-month.
     * 2022-01-31T23:59:59.999Z = 1643673599999L.
     */
    public void testGtDateMathExplicitRounding() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("event_time").gt("2022-01-01||/M"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN, call.getKind());
        // gt with roundUp=true: "2022-01-01||/M" rounds to end of month = 2022-01-31T23:59:59.999Z
        assertLiteralEpoch(result, 1643673599999L);
    }

    // ========== GROUP L - FIELD TYPE GUARDS AND IP RANGE SUPPORT ==========

    /**
     * Range query on ip-typed field produces VARBINARY byte-range comparison.
     * Legacy IpFieldMapper.rangeQuery uses InetAddress-order (16-byte IPv6-mapped encoding).
     */
    public void testRangeOnIpFieldProducesByteComparison() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("ip_address").gte("192.168.0.1"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertEquals(2, call.getOperands().size());
        // Field ref at index 11 (ip_address)
        assertEquals(11, ((RexInputRef) call.getOperands().get(0)).getIndex());
    }

    /**
     * Range query on binary_data field (plain VARBINARY, not IpType) throws ConversionException.
     * Legacy BinaryFieldMapper has no rangeQuery implementation.
     */
    public void testRangeOnBinaryFieldThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("binary_data").gte("abc"), ctx)
        );
        assertTrue("Message should mention binary fields: " + ex.getMessage(), ex.getMessage().contains("binary"));
    }

    // ========== GROUP N - DATE_NANOS (TIMESTAMP(9)) RANGE SUPPORT ==========

    /**
     * Range query on nanosecond-precision date field (TIMESTAMP(9)) produces a valid comparison.
     * The guard that previously threw ConversionException has been removed; date_nanos is now
     * supported via nanosecond-resolution parsing and TIMESTAMP(9) literals.
     */
    public void testRangeOnDateNanosFieldSucceeds() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("event_nanos").gte("2022-01-01"), ctx);

        assertTrue("Expected RexCall comparison, got: " + result.getClass(), result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        // event_nanos is field index 12
        assertEquals(12, ((RexInputRef) call.getOperands().get(0)).getIndex());
    }

    /**
     * Bound "2026-07-28T00:00:00.123456789" on event_nanos (precision 9) must produce a literal
     * whose epoch-nanosecond value ends in ...123456789 exactly. This verifies no truncation to millis.
     * Validation is via the literal's TimestampString which preserves all 9 fractional digits,
     * and the literal's type which must be TIMESTAMP(9).
     */
    public void testDateNanosLiteralPreservesFullNanoPrecision() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("event_nanos").gte("2026-07-28T00:00:00.123456789"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        RexLiteral literal = unwrapLiteral(call.getOperands().get(1));
        assertNotNull(literal);
        // Type must be TIMESTAMP(9)
        assertEquals(SqlTypeName.TIMESTAMP, literal.getType().getSqlTypeName());
        assertEquals(9, literal.getType().getPrecision());
        // The TimestampString must preserve all 9 fractional digits
        org.apache.calcite.util.TimestampString ts = literal.getValueAs(org.apache.calcite.util.TimestampString.class);
        assertNotNull("TimestampString value must not be null", ts);
        String tsStr = ts.toString();
        // Must contain .123456789 (all 9 digits preserved)
        assertTrue("TimestampString must contain 9-digit fraction .123456789, got: " + tsStr, tsStr.contains(".123456789"));
        // Verify the date part
        assertTrue("TimestampString must start with 2026-07-28, got: " + tsStr, tsStr.startsWith("2026-07-28"));
    }

    /**
     * gt vs gte at a nanosecond boundary must differ by rounding.
     * gt "2026-07-28" uses roundUp=true (exclusive lower), producing end-of-day nanos.
     * gte uses roundUp=false (inclusive lower), producing start-of-day nanos.
     */
    public void testDateNanosGtVsGteDifferByRounding() throws ConversionException {
        // gte "2026-07-28" -> roundUp=false -> start of day = .000000000
        RexNode gteResult = translator.convert(QueryBuilders.rangeQuery("event_nanos").gte("2026-07-28"), ctx);
        RexCall gteCall = (RexCall) gteResult;
        RexLiteral gteLiteral = unwrapLiteral(gteCall.getOperands().get(1));
        org.apache.calcite.util.TimestampString gteTs = gteLiteral.getValueAs(org.apache.calcite.util.TimestampString.class);

        // gt "2026-07-28" -> roundUp=true -> end of day = .999999999
        RexNode gtResult = translator.convert(QueryBuilders.rangeQuery("event_nanos").gt("2026-07-28"), ctx);
        RexCall gtCall = (RexCall) gtResult;
        RexLiteral gtLiteral = unwrapLiteral(gtCall.getOperands().get(1));
        org.apache.calcite.util.TimestampString gtTs = gtLiteral.getValueAs(org.apache.calcite.util.TimestampString.class);

        assertNotNull(gteTs);
        assertNotNull(gtTs);
        // gte rounds down to start of day (no fractional part or all zeros)
        String gteStr = gteTs.toString();
        assertTrue("gte should be start of 2026-07-28: " + gteStr, gteStr.startsWith("2026-07-28 00:00:00"));
        // gt rounds up to end of day: 23:59:59.999999999
        String gtStr = gtTs.toString();
        assertTrue("gt should be end of 2026-07-28 with .999999999: " + gtStr, gtStr.contains("23:59:59.999999999"));
    }

    /**
     * Upper bound rounding on date_nanos: lte "2026-07-28" rounds UP to end-of-day at nano
     * granularity (23:59:59.999999999).
     */
    public void testDateNanosUpperBoundRoundsToNanos() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("event_nanos").lte("2026-07-28"), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, call.getKind());
        RexLiteral literal = unwrapLiteral(call.getOperands().get(1));
        org.apache.calcite.util.TimestampString ts = literal.getValueAs(org.apache.calcite.util.TimestampString.class);
        assertNotNull(ts);
        String tsStr = ts.toString();
        // End of 2026-07-28 in nanos: 2026-07-28 23:59:59.999999999
        assertTrue("lte should round up to end of day at nano granularity (.999999999): " + tsStr, tsStr.contains("23:59:59.999999999"));
    }

    /**
     * Year-2262 clamping: date beyond MAX_NANOSECOND_INSTANT (~2262-04-11) is clamped
     * to the max nanosecond epoch value, not overflowing.
     */
    public void testDateNanosYear2262Clamp() throws ConversionException {
        // 2300-01-01 is beyond MAX_NANOSECOND_INSTANT (2262-04-11T23:47:16.854775807Z)
        RexNode result = translator.convert(QueryBuilders.rangeQuery("event_nanos").gte("2300-01-01"), ctx);

        RexCall call = (RexCall) result;
        RexLiteral literal = unwrapLiteral(call.getOperands().get(1));
        assertEquals(SqlTypeName.TIMESTAMP, literal.getType().getSqlTypeName());
        assertEquals(9, literal.getType().getPrecision());
        // The value must be clamped to MAX_NANOSECOND_INSTANT
        org.apache.calcite.util.TimestampString ts = literal.getValueAs(org.apache.calcite.util.TimestampString.class);
        assertNotNull(ts);
        String tsStr = ts.toString();
        // MAX_NANOSECOND_INSTANT = 2262-04-11T23:47:16.854775807Z
        assertTrue("Should be clamped to 2262-04-11: " + tsStr, tsStr.startsWith("2262-04-11"));
        assertTrue("Should contain .854775807: " + tsStr, tsStr.contains(".854775807"));
    }

    /**
     * REGRESSION: plain date field (event_time, precision 3) still yields precision-3 millis literal.
     * Ensures the precision-9 path does not break existing millisecond date handling.
     */
    public void testRegressionPlainDateFieldStillUsesMillis() throws ConversionException {
        // Use epoch_millis format which is timezone-independent (epoch is absolute)
        RexNode result = translator.convert(QueryBuilders.rangeQuery("event_time").gte("1785369600123").format("epoch_millis"), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        RexLiteral literal = unwrapLiteral(call.getOperands().get(1));
        // Type should be TIMESTAMP(3)
        assertEquals(SqlTypeName.TIMESTAMP, literal.getType().getSqlTypeName());
        assertEquals(3, literal.getType().getPrecision());
        // Value as Long should be epoch millis
        Long millis = literal.getValueAs(Long.class);
        assertNotNull(millis);
        assertEquals("Plain date field should use millis precision", 1785369600123L, millis.longValue());
    }

    // ========== GROUP M - IP RANGE VALUE CORRECTNESS ==========

    /**
     * gte("192.168.0.1") on ip field produces VARBINARY literal with IPv4-mapped-IPv6 encoding:
     * 10 zero bytes + 0xff 0xff + 192(0xc0) 168(0xa8) 0(0x00) 1(0x01).
     */
    public void testIpRangeIpv4EncodingValue() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("ip_address").gte("192.168.0.1"), ctx);

        RexCall call = (RexCall) result;
        RexLiteral literal = unwrapLiteral(call.getOperands().get(1));
        assertNotNull(literal);
        org.apache.calcite.avatica.util.ByteString bs = literal.getValueAs(org.apache.calcite.avatica.util.ByteString.class);
        assertNotNull("IP literal should be ByteString", bs);
        byte[] bytes = bs.getBytes();
        assertEquals("IPv6-mapped encoding must be 16 bytes", 16, bytes.length);
        // Verify IPv4-mapped structure: 10 zeros + 0xff 0xff + 192.168.0.1
        for (int i = 0; i < 10; i++) {
            assertEquals("Byte " + i + " should be 0x00", 0, bytes[i]);
        }
        assertEquals((byte) 0xff, bytes[10]);
        assertEquals((byte) 0xff, bytes[11]);
        assertEquals((byte) 192, bytes[12]);
        assertEquals((byte) 168, bytes[13]);
        assertEquals((byte) 0, bytes[14]);
        assertEquals((byte) 1, bytes[15]);
    }

    /**
     * gte("::1") on ip field produces VARBINARY literal with native IPv6 encoding:
     * 15 zero bytes + 0x01.
     */
    public void testIpRangeIpv6EncodingValue() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("ip_address").gte("::1"), ctx);

        RexCall call = (RexCall) result;
        RexLiteral literal = unwrapLiteral(call.getOperands().get(1));
        assertNotNull(literal);
        org.apache.calcite.avatica.util.ByteString bs = literal.getValueAs(org.apache.calcite.avatica.util.ByteString.class);
        assertNotNull("IP literal should be ByteString", bs);
        byte[] bytes = bs.getBytes();
        assertEquals("IPv6 encoding must be 16 bytes", 16, bytes.length);
        // ::1 = 15 zero bytes + 0x01
        for (int i = 0; i < 15; i++) {
            assertEquals("Byte " + i + " should be 0x00", 0, bytes[i]);
        }
        assertEquals((byte) 0x01, bytes[15]);
    }

    /**
     * Both bounds on ip field produce AND of two byte comparisons with correct operators.
     */
    public void testIpRangeBothBounds() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("ip_address").gte("10.0.0.0").lte("10.0.0.255"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.AND, call.getKind());
        assertEquals(2, call.getOperands().size());

        RexCall lower = (RexCall) call.getOperands().get(0);
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, lower.getKind());

        RexCall upper = (RexCall) call.getOperands().get(1);
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, upper.getKind());
    }

    /**
     * gt on ip field produces GREATER_THAN (exclusive).
     */
    public void testIpRangeExclusiveLowerBound() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("ip_address").gt("10.0.0.1"), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN, call.getKind());
    }

    /**
     * Invalid IP string throws ConversionException.
     */
    public void testIpRangeInvalidIpThrows() {
        expectThrows(ConversionException.class, () -> translator.convert(QueryBuilders.rangeQuery("ip_address").gte("not_an_ip"), ctx));
    }

    /**
     * Hostname input (e.g. "localhost") must be rejected with ConversionException.
     * DNS resolution must never be attempted on user-supplied range bounds.
     */
    public void testIpRangeHostnameLocalhostThrows() {
        expectThrows(ConversionException.class, () -> translator.convert(QueryBuilders.rangeQuery("ip_address").gte("localhost"), ctx));
    }

    /**
     * Arbitrary hostname input must be rejected with ConversionException,
     * not passed to DNS resolution.
     */
    public void testIpRangeHostnameArbitraryThrows() {
        expectThrows(ConversionException.class, () -> translator.convert(QueryBuilders.rangeQuery("ip_address").gte("evil.example"), ctx));
    }

    // ========== SCALED_FLOAT RANGE TESTS ==========

    public void testGtOnScaledFloat() throws ConversionException {
        // gt 10.5 with factor 10 -> Math.round(10.5 * 10) = 105, +1 for exclusive = 106, GTE
        // Per NumberFieldMapper.longRangeQuery: exclusive lower increments to make inclusive GTE.
        RexNode result = translator.convert(QueryBuilders.rangeQuery("scaled_price").gt(10.5), ctx);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertEquals(13, ((RexInputRef) call.getOperands().get(0)).getIndex());
        assertEquals(Long.valueOf(106L), extractLiteralLong(call.getOperands().get(1)));
    }

    public void testGteOnScaledFloat() throws ConversionException {
        // gte 10.5 with factor 10 -> Math.round(10.5 * 10) = 105, inclusive, GTE
        RexNode result = translator.convert(QueryBuilders.rangeQuery("scaled_price").gte(10.5), ctx);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertEquals(Long.valueOf(105L), extractLiteralLong(call.getOperands().get(1)));
    }

    public void testLtOnScaledFloat() throws ConversionException {
        // lt 10.5 with factor 10 -> 105, -1 for exclusive = 104, LTE
        RexNode result = translator.convert(QueryBuilders.rangeQuery("scaled_price").lt(10.5), ctx);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, call.getKind());
        assertEquals(Long.valueOf(104L), extractLiteralLong(call.getOperands().get(1)));
    }

    public void testLteOnScaledFloat() throws ConversionException {
        // lte 10.5 with factor 10 -> 105, inclusive, LTE
        RexNode result = translator.convert(QueryBuilders.rangeQuery("scaled_price").lte(10.5), ctx);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, call.getKind());
        assertEquals(Long.valueOf(105L), extractLiteralLong(call.getOperands().get(1)));
    }

    public void testHalfBoundaryGteScaledFloatRoundsUp() throws ConversionException {
        // gte 10.55 with factor 10 -> Math.round(105.5) = 106, inclusive, GTE
        RexNode result = translator.convert(QueryBuilders.rangeQuery("scaled_price").gte(10.55), ctx);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertEquals(Long.valueOf(106L), extractLiteralLong(call.getOperands().get(1)));
    }

    public void testNegativeValueOnScaledFloat() throws ConversionException {
        // gt -5.3 with factor 10 -> Math.round(-53.0) = -53, +1 for exclusive = -52, GTE
        RexNode result = translator.convert(QueryBuilders.rangeQuery("scaled_price").gt(-5.3), ctx);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertEquals(Long.valueOf(-52L), extractLiteralLong(call.getOperands().get(1)));
    }

    public void testBothBoundsScaledFloatProducesAnd() throws ConversionException {
        // gte 5.0 AND lte 20.0 with factor 10 -> 50 (inclusive) AND 200 (inclusive)
        RexNode result = translator.convert(QueryBuilders.rangeQuery("scaled_price").gte(5.0).lte(20.0), ctx);
        RexCall andCall = (RexCall) result;
        assertEquals(SqlKind.AND, andCall.getKind());

        RexCall lower = (RexCall) andCall.getOperands().get(0);
        RexCall upper = (RexCall) andCall.getOperands().get(1);

        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, lower.getKind());
        assertEquals(Long.valueOf(50L), extractLiteralLong(lower.getOperands().get(1)));

        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, upper.getKind());
        assertEquals(Long.valueOf(200L), extractLiteralLong(upper.getOperands().get(1)));
    }

    public void testOverflowBoundScaledFloatReturnsFalse() {
        // Value that overflows Long when scaled: 1e18 * 10 > Long.MAX_VALUE
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("scaled_price").gt(1e18), ctx)
        );
        assertTrue(ex.getMessage().contains("overflows"));
    }

    public void testNonNumericBoundScaledFloatThrowsConversionException() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("scaled_price").gt("not-a-number"), ctx)
        );
        assertTrue(ex.getMessage().contains("not-a-number"));
    }

    /** Unwraps a nullable CAST around a RexLiteral produced by makeLiteral on a nullable type. */
    private static Long extractLiteralLong(RexNode node) {
        if (node instanceof RexLiteral lit) {
            return lit.getValueAs(Long.class);
        }
        // makeLiteral wraps nullable types in CAST
        if (node instanceof RexCall cast && cast.getKind() == SqlKind.CAST) {
            return ((RexLiteral) cast.getOperands().get(0)).getValueAs(Long.class);
        }
        throw new AssertionError("Expected RexLiteral or CAST(RexLiteral), got: " + node.getClass().getSimpleName());
    }

    // ========== UNSIGNED_LONG RANGE TESTS ==========

    public void testGtDecimalOnUnsignedLong() throws ConversionException {
        // gt 10.5 on unsigned_long: positive decimal lower → truncate(10) + 1 = 11, GTE
        // Mirrors NumberFieldMapper.unsignedLongRangeQuery: "if lowerTerm=1.5 then the
        // (inclusive) bound becomes 2" — positive decimal lower increments after truncation.
        RexNode result = translator.convert(QueryBuilders.rangeQuery("unsigned_counter").gt(10.5), ctx);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertEquals(14, ((RexInputRef) call.getOperands().get(0)).getIndex());
        assertEquals(Long.valueOf(11L), extractLiteralLong(call.getOperands().get(1)));
    }

    public void testGteNegativeLowerOnUnsignedLong() throws ConversionException {
        // gte -5 on unsigned_long: negative lower bound → clamp to 0, effectively unbounded-low.
        // Per NumberFieldMapper.objectToUnsignedLong(lenientBound=true): values below 0 return
        // MIN_UNSIGNED_LONG_VALUE (0). With lower defaulting to 0 and no upper, this becomes
        // IS_NOT_NULL (exists semantics) since the lower condition is omitted.
        RexNode result = translator.convert(QueryBuilders.rangeQuery("unsigned_counter").gte(-5), ctx);
        // With only a lower bound that is negative → null returned from translateBound → no conditions → IS_NOT_NULL
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.IS_NOT_NULL, call.getKind());
    }

    public void testLtNegativeUpperOnUnsignedLong() throws ConversionException {
        // lt -1 on unsigned_long: negative upper → match-none.
        // Per NumberFieldMapper.objectToUnsignedLong(lenientBound=true): negative upper clamps to 0,
        // then l(0) > u(0 after exclusive decrement = underflow) → MatchNoDocsQuery.
        RexNode result = translator.convert(QueryBuilders.rangeQuery("unsigned_counter").lt(-1), ctx);
        assertTrue("Expected literal false (match-none)", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testBoundAboveLongMaxOnUnsignedLongThrows() {
        // Bound 9223372036854775808 (Long.MAX_VALUE + 1) → ConversionException.
        // Values above Long.MAX_VALUE are not representable due to schema_coerce.rs UInt64→Int64 narrowing.
        // The error may come from coercion (Long overflow) or from our explicit check.
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("unsigned_counter").gte("9223372036854775808"), ctx)
        );
        assertNotNull(ex.getMessage());
    }

    public void testGteOnUnsignedLongInRange() throws ConversionException {
        // gte 100 (whole, inclusive) → GTE 100
        RexNode result = translator.convert(QueryBuilders.rangeQuery("unsigned_counter").gte(100), ctx);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertEquals(Long.valueOf(100L), extractLiteralLong(call.getOperands().get(1)));
    }

    public void testGtOnUnsignedLongExclusiveWholeNumber() throws ConversionException {
        // gt 100 (whole, exclusive) → exclusive adjusts: +1 = 101, GTE
        RexNode result = translator.convert(QueryBuilders.rangeQuery("unsigned_counter").gt(100), ctx);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertEquals(Long.valueOf(101L), extractLiteralLong(call.getOperands().get(1)));
    }

    public void testLtOnUnsignedLongExclusiveWholeNumber() throws ConversionException {
        // lt 100 (whole, exclusive) → -1 = 99, LTE
        RexNode result = translator.convert(QueryBuilders.rangeQuery("unsigned_counter").lt(100), ctx);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, call.getKind());
        assertEquals(Long.valueOf(99L), extractLiteralLong(call.getOperands().get(1)));
    }

    public void testLteNegativeUpperOnUnsignedLong() throws ConversionException {
        // lte -5 on unsigned_long: negative upper → match-none.
        RexNode result = translator.convert(QueryBuilders.rangeQuery("unsigned_counter").lte(-5), ctx);
        assertTrue("Expected literal false (match-none)", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testNonNumericBoundOnUnsignedLongThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("unsigned_counter").gte("not-a-number"), ctx)
        );
        assertTrue(ex.getMessage().contains("coerce") || ex.getMessage().contains("Non-numeric"));
    }

    public void testExistsOnUnsignedLong() throws ConversionException {
        // No bounds → IS_NOT_NULL (exists semantics), same as other types.
        RexNode result = translator.convert(QueryBuilders.rangeQuery("unsigned_counter"), ctx);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.IS_NOT_NULL, call.getKind());
    }

    // ========== STRING-COERCED NUMERIC BOUND TESTS ==========

    public void testGteStringDecimalOnScaledFloat() throws ConversionException {
        // gte "1.5" (string) with factor 10 -> Math.round(1.5 * 10) = 15, inclusive, GTE
        RexNode result = translator.convert(QueryBuilders.rangeQuery("scaled_price").gte("1.5"), ctx);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertEquals(Long.valueOf(15L), extractLiteralLong(call.getOperands().get(1)));
    }

    public void testGteStringWholeNumberOnUnsignedLong() throws ConversionException {
        // gte "100" (string) on unsigned_long: whole number within Long range -> GTE 100
        RexNode result = translator.convert(QueryBuilders.rangeQuery("unsigned_counter").gte("100"), ctx);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertEquals(Long.valueOf(100L), extractLiteralLong(call.getOperands().get(1)));
    }

    // ========== FIX 1: NaN/Infinity rejection on scaled_float range ==========

    /** NaN string bound on scaled_float must throw ConversionException, not silently produce 0. */
    public void testNaNStringOnScaledFloatThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("scaled_price").gte("NaN"), ctx)
        );
        assertTrue(ex.getMessage().contains("NaN") || ex.getMessage().contains("non-finite"));
    }

    /** Double.NaN bound on scaled_float must throw ConversionException. */
    public void testNaNDoubleOnScaledFloatThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("scaled_price").gte(Double.NaN), ctx)
        );
        assertTrue(ex.getMessage().contains("NaN") || ex.getMessage().contains("non-finite"));
    }

    /** Infinity string bound on scaled_float must throw ConversionException. */
    public void testInfinityStringOnScaledFloatRangeThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("scaled_price").gte("Infinity"), ctx)
        );
        assertTrue(ex.getMessage().contains("Infinity") || ex.getMessage().contains("non-finite"));
    }

    // ========== GROUP P - CHARACTERIZATION: MILLIS vs NANOS BOUNDARY BEHAVIOUR ==========

    /** Pre-1970 date on millisecond field produces a negative epoch-millis literal (1969-07-20 = -14256000000). */
    public void testPreEpochDateOnMillisFieldProducesNegativeValue() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("event_time").gte("1969-07-20T00:00:00Z"), ctx);

        assertLiteralEpoch(result, -14256000000L);
    }

    /** Negative epoch_millis value on date_nanos field throws ConversionException. */
    public void testNegativeEpochMillisOnDateNanosFieldThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("event_nanos").gte("-1").format("epoch_millis"), ctx)
        );
        assertTrue(
            "Expected message about value before epoch, got: " + ex.getMessage(),
            ex.getMessage().contains("before epoch not representable in nanos")
        );
    }

    /** Far-future bound on date_nanos clamps to MAX_NANOSECOND_INSTANT (2262-04-11T23:47:16.854775807Z). */
    public void testFarFutureDateOnNanosFieldClampsToMax() throws ConversionException {
        // 2300-01-01 is far beyond MAX_NANOSECOND_INSTANT (2262-04-11T23:47:16.854775807Z)
        RexNode result = translator.convert(QueryBuilders.rangeQuery("event_nanos").gte("2300-01-01"), ctx);

        RexCall call = (RexCall) result;
        RexLiteral literal = unwrapLiteral(call.getOperands().get(1));
        assertNotNull(literal);
        assertEquals(9, literal.getType().getPrecision());
        // Verify clamped to MAX_NANOSECOND_INSTANT via TimestampString
        org.apache.calcite.util.TimestampString ts = literal.getValueAs(org.apache.calcite.util.TimestampString.class);
        assertNotNull("TimestampString must not be null", ts);
        String tsStr = ts.toString();
        assertTrue("Must clamp to 2262-04-11, got: " + tsStr, tsStr.startsWith("2262-04-11"));
        assertTrue("Must contain .854775807, got: " + tsStr, tsStr.contains(".854775807"));
    }

    // ========== GROUP Q - BOOLEAN RANGE PARITY ==========
    // Parity with BooleanFieldMapper.BooleanFieldType.rangeQuery:309 which converts bounds to
    // BytesRef "T"/"F", collapses lower==upper to termQuery, and returns MatchNoDocsQuery for
    // impossible ranges. Vanilla's collapse-to-term and match-none special cases are expressed
    // here as ordinary comparisons that SQL evaluates identically.

    /** gte(true) AND lte(true) selects only true; vanilla collapses to termQuery(true). */
    public void testBooleanRangeGteTrueAndLteTrue() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("is_active").gte(true).lte(true), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.AND, call.getKind());
        assertEquals(2, call.getOperands().size());

        RexCall lower = (RexCall) call.getOperands().get(0);
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, lower.getKind());
        assertEquals(5, ((RexInputRef) lower.getOperands().get(0)).getIndex());
        RexLiteral lowerLit = unwrapLiteral(lower.getOperands().get(1));
        assertEquals(Boolean.TRUE, lowerLit.getValueAs(Boolean.class));

        RexCall upper = (RexCall) call.getOperands().get(1);
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, upper.getKind());
        assertEquals(5, ((RexInputRef) upper.getOperands().get(0)).getIndex());
        RexLiteral upperLit = unwrapLiteral(upper.getOperands().get(1));
        assertEquals(Boolean.TRUE, upperLit.getValueAs(Boolean.class));
    }

    /** gte(false) AND lte(true) selects both values; vanilla returns an existsQuery equivalent. */
    public void testBooleanRangeGteFalseAndLteTrue() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("is_active").gte(false).lte(true), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.AND, call.getKind());
        assertEquals(2, call.getOperands().size());

        RexCall lower = (RexCall) call.getOperands().get(0);
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, lower.getKind());
        assertEquals(5, ((RexInputRef) lower.getOperands().get(0)).getIndex());
        RexLiteral lowerLit = unwrapLiteral(lower.getOperands().get(1));
        assertEquals(Boolean.FALSE, lowerLit.getValueAs(Boolean.class));

        RexCall upper = (RexCall) call.getOperands().get(1);
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, upper.getKind());
        assertEquals(5, ((RexInputRef) upper.getOperands().get(0)).getIndex());
        RexLiteral upperLit = unwrapLiteral(upper.getOperands().get(1));
        assertEquals(Boolean.TRUE, upperLit.getValueAs(Boolean.class));
    }

    /** gt(true) can never be satisfied; vanilla returns MatchNoDocsQuery for this impossible range. */
    public void testBooleanRangeGtTrue() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("is_active").gt(true), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN, call.getKind());
        assertEquals(5, ((RexInputRef) call.getOperands().get(0)).getIndex());
        RexLiteral lit = unwrapLiteral(call.getOperands().get(1));
        assertEquals(Boolean.TRUE, lit.getValueAs(Boolean.class));
    }

    /** lt(false) can never be satisfied; vanilla returns MatchNoDocsQuery for this impossible range. */
    public void testBooleanRangeLtFalse() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("is_active").lt(false), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LESS_THAN, call.getKind());
        assertEquals(5, ((RexInputRef) call.getOperands().get(0)).getIndex());
        RexLiteral lit = unwrapLiteral(call.getOperands().get(1));
        assertEquals(Boolean.FALSE, lit.getValueAs(Boolean.class));
    }

    // ========== GROUP R - NON-FINITE BOUNDS ON INTEGER FAMILY ==========

    /** BUG 1 proof: NaN as raw Double on INTEGER silently produces bound of 0 instead of throwing. */
    public void testNaNDoubleOnIntegerThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("price").gte(Double.NaN), ctx)
        );
        assertTrue(
            "Message should mention non-finite or NaN: " + ex.getMessage(),
            ex.getMessage().contains("non-finite") || ex.getMessage().contains("NaN")
        );
    }

    /** NaN as String "NaN" on INTEGER must throw ConversionException. */
    public void testNaNStringOnIntegerThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("price").gte("NaN"), ctx)
        );
        assertTrue(
            "Message should mention non-finite or NaN: " + ex.getMessage(),
            ex.getMessage().contains("non-finite") || ex.getMessage().contains("NaN")
        );
    }

    /** Positive Infinity as raw Double on INTEGER must throw ConversionException. */
    public void testPositiveInfinityDoubleOnIntegerThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("price").gte(Double.POSITIVE_INFINITY), ctx)
        );
        assertTrue(
            "Message should mention non-finite or Infinity: " + ex.getMessage(),
            ex.getMessage().contains("non-finite") || ex.getMessage().contains("Infinity")
        );
    }

    /** "Infinity" as String on INTEGER must throw ConversionException. */
    public void testInfinityStringOnIntegerThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("price").gte("Infinity"), ctx)
        );
        assertTrue(
            "Message should mention non-finite or Infinity: " + ex.getMessage(),
            ex.getMessage().contains("non-finite") || ex.getMessage().contains("Infinity")
        );
    }

    /** NaN as raw Double on BIGINT must throw ConversionException. */
    public void testNaNDoubleOnBigintThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("timestamp").gte(Double.NaN), ctx)
        );
        assertTrue(
            "Message should mention non-finite or NaN: " + ex.getMessage(),
            ex.getMessage().contains("non-finite") || ex.getMessage().contains("NaN")
        );
    }

    /** "NaN" as String on BIGINT must throw ConversionException. */
    public void testNaNStringOnBigintThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("timestamp").gte("NaN"), ctx)
        );
        assertTrue(
            "Message should mention non-finite or NaN: " + ex.getMessage(),
            ex.getMessage().contains("non-finite") || ex.getMessage().contains("NaN")
        );
    }

    /** Infinity as raw Double on BIGINT must throw ConversionException. */
    public void testPositiveInfinityDoubleOnBigintThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("timestamp").gte(Double.POSITIVE_INFINITY), ctx)
        );
        assertTrue(
            "Message should mention non-finite or Infinity: " + ex.getMessage(),
            ex.getMessage().contains("non-finite") || ex.getMessage().contains("Infinity")
        );
    }

    /** "Infinity" as String on BIGINT must throw ConversionException. */
    public void testInfinityStringOnBigintThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("timestamp").gte("Infinity"), ctx)
        );
        assertTrue(
            "Message should mention non-finite or Infinity: " + ex.getMessage(),
            ex.getMessage().contains("non-finite") || ex.getMessage().contains("Infinity")
        );
    }

    // ========== GROUP R2 - NON-FINITE ON UNSIGNED_LONG ==========

    /** NaN as raw Double on unsigned_long must throw ConversionException. */
    public void testNaNDoubleOnUnsignedLongThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("unsigned_counter").gte(Double.NaN), ctx)
        );
        assertTrue(
            "Message should mention non-finite or NaN: " + ex.getMessage(),
            ex.getMessage().contains("non-finite") || ex.getMessage().contains("NaN")
        );
    }

    /** "NaN" as String on unsigned_long must throw ConversionException. */
    public void testNaNStringOnUnsignedLongThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("unsigned_counter").gte("NaN"), ctx)
        );
        assertTrue(
            "Message should name the rejected value and the coercion failure: " + ex.getMessage(),
            ex.getMessage().contains("NaN") && ex.getMessage().contains("coerce")
        );
    }

    /** Infinity as raw Double on unsigned_long must throw ConversionException. */
    public void testInfinityDoubleOnUnsignedLongThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("unsigned_counter").gte(Double.POSITIVE_INFINITY), ctx)
        );
        assertTrue(
            "Message should mention non-finite or Infinity or above: " + ex.getMessage(),
            ex.getMessage().contains("non-finite") || ex.getMessage().contains("Infinity") || ex.getMessage().contains("above")
        );
    }

    /** "-Infinity" as String on unsigned_long must throw ConversionException. */
    public void testNegativeInfinityStringOnUnsignedLongThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("unsigned_counter").gte("-Infinity"), ctx)
        );
        assertTrue(
            "Message should name the rejected value and the coercion failure: " + ex.getMessage(),
            ex.getMessage().contains("-Infinity") && ex.getMessage().contains("coerce")
        );
    }

    // ========== GROUP R3 - NON-FINITE ON REAL (FLOAT) ==========

    /** NaN on REAL (float_val) must throw ConversionException. */
    public void testNaNDoubleOnRealThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("float_val").gte(Double.NaN), ctx)
        );
        assertTrue(
            "Message should mention non-finite or NaN: " + ex.getMessage(),
            ex.getMessage().contains("non-finite") || ex.getMessage().contains("NaN")
        );
    }

    /** "Infinity" on REAL (float_val) must throw ConversionException. */
    public void testInfinityStringOnRealThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("float_val").gte("Infinity"), ctx)
        );
        assertTrue(
            "Message should mention non-finite or Infinity: " + ex.getMessage(),
            ex.getMessage().contains("non-finite") || ex.getMessage().contains("Infinity")
        );
    }

    // ========== GROUP R4 - NON-FINITE ON DOUBLE (must NOT throw) ==========

    /** NaN on DOUBLE field must NOT throw -- legacy accepts non-finite doubles. */
    public void testNaNDoubleOnDoubleFieldAccepted() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("rating").gte(Double.NaN), ctx);
        // Must produce a comparison, not throw
        assertTrue("Non-finite on DOUBLE should produce a RexCall, not throw", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
    }

    /** Infinity on DOUBLE field must NOT throw -- legacy accepts non-finite doubles. */
    public void testInfinityDoubleOnDoubleFieldAccepted() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("rating").gte(Double.POSITIVE_INFINITY), ctx);
        assertTrue("Non-finite on DOUBLE should produce a RexCall, not throw", result instanceof RexCall);
    }

    /** Negative Infinity on DOUBLE field must NOT throw. */
    public void testNegativeInfinityDoubleOnDoubleFieldAccepted() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("rating").lte(Double.NEGATIVE_INFINITY), ctx);
        assertTrue("Non-finite on DOUBLE should produce a RexCall, not throw", result instanceof RexCall);
    }

    // ========== GROUP R5 - SCALED_FLOAT NAN/INFINITY REGRESSION PIN ==========

    /** Regression: NaN on scaled_float still throws (existing guard). */
    public void testNaNOnScaledFloatRegressionPin() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("scaled_price").gte(Double.NaN), ctx)
        );
        assertTrue(
            "Message should mention non-finite: " + ex.getMessage(),
            ex.getMessage().contains("non-finite") || ex.getMessage().contains("Non-finite")
        );
    }

    // ========== GROUP S - UNCHECKED NARROWING OVERFLOW ==========

    /** BUG 2 proof: gte 2147483648L on INTEGER silently narrows to -2147483648 instead of throwing. */
    public void testGteAboveIntegerMaxThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("price").gte(2147483648L), ctx)
        );
        assertTrue(
            "Message should mention out of range or overflow: " + ex.getMessage(),
            ex.getMessage().contains("out of range") || ex.getMessage().contains("overflow")
        );
    }

    /** lte -2147483649L on INTEGER must throw ConversionException. */
    public void testLteBelowIntegerMinThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("price").lte(-2147483649L), ctx)
        );
        assertTrue(
            "Message should mention out of range or overflow: " + ex.getMessage(),
            ex.getMessage().contains("out of range") || ex.getMessage().contains("overflow")
        );
    }

    /** Bound past Long range on BIGINT (as String) must throw ConversionException. */
    public void testBoundPastLongRangeOnBigintThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("timestamp").gte("9223372036854775808"), ctx)
        );
        assertTrue(
            "Message should mention out of range or overflow or numeric: " + ex.getMessage(),
            ex.getMessage().contains("out of range")
                || ex.getMessage().contains("overflow")
                || ex.getMessage().contains("numeric")
                || ex.getMessage().contains("NumberFormat")
        );
    }

    // ========== GROUP T - TINYINT/SMALLINT BOUNDARY (MATCH-NONE / NO-CONSTRAINT) ==========

    /** gte 200 on TINYINT (max=127) -> match-none (FALSE literal). */
    public void testGteAboveTinyintMaxMatchesNone() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("tiny_val").gte(200), ctx);
        assertTrue("Lower bound above TINYINT max must produce literal false (match-none)", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    /** lte 200 on TINYINT (max=127) -> no constraint (upper bound above max is unbounded). */
    public void testLteAboveTinyintMaxNoConstraint() throws ConversionException {
        // Only an upper bound that exceeds the type max; upper becomes null => IS_NOT_NULL (exists)
        RexNode result = translator.convert(QueryBuilders.rangeQuery("tiny_val").lte(200), ctx);
        // When the only bound returns null, translator produces IS_NOT_NULL (exists semantics)
        assertTrue("Upper bound above TINYINT max should produce IS_NOT_NULL (no constraint)", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.IS_NOT_NULL, call.getKind());
    }

    /** gte 40000 on SMALLINT (max=32767) -> match-none. */
    public void testGteAboveSmallintMaxMatchesNone() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("small_val").gte(40000), ctx);
        assertTrue("Lower bound above SMALLINT max must produce literal false (match-none)", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    /** lte -40000 on SMALLINT (min=-32768) -> match-none. */
    public void testLteBelowSmallintMinMatchesNone() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("small_val").lte(-40000), ctx);
        assertTrue("Upper bound below SMALLINT min must produce literal false (match-none)", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    /** gte -200 on TINYINT (min=-128) -> no constraint (lower below min is unbounded). */
    public void testGteBelowTinyintMinNoConstraint() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("tiny_val").gte(-200), ctx);
        assertTrue("Lower bound below TINYINT min should produce IS_NOT_NULL (no constraint)", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.IS_NOT_NULL, call.getKind());
    }

    /** gte 40000 on SMALLINT with lte -40000 -> match-none for both => AND of false? Actually each bound returns separately. */
    public void testLteAboveSmallintMaxNoConstraint() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("small_val").lte(40000), ctx);
        assertTrue("Upper bound above SMALLINT max should produce IS_NOT_NULL (no constraint)", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.IS_NOT_NULL, call.getKind());
    }

    // ========== GROUP U - REGRESSION: IN-RANGE BOUNDARY VALUES STILL WORK ==========

    /** Integer.MAX_VALUE on INTEGER must still produce a valid comparison. */
    public void testIntegerMaxValueStillWorks() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").gte(Integer.MAX_VALUE), ctx);
        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
        assertLiteralNumber(result, Integer.valueOf(Integer.MAX_VALUE), null);
    }

    /** Integer.MIN_VALUE on INTEGER must still produce a valid comparison. */
    public void testIntegerMinValueStillWorks() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("price").lte(Integer.MIN_VALUE), ctx);
        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, call.getKind());
        assertLiteralNumber(result, Integer.valueOf(Integer.MIN_VALUE), null);
    }

    /** Byte.MAX_VALUE (127) on TINYINT must still produce a valid comparison. */
    public void testByteMaxValueOnTinyintStillWorks() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("tiny_val").gte(127), ctx);
        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, call.getKind());
    }

    /** Short.MIN_VALUE (-32768) on SMALLINT must still produce a valid comparison. */
    public void testShortMinValueOnSmallintStillWorks() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.rangeQuery("small_val").lte(-32768), ctx);
        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.LESS_THAN_OR_EQUAL, call.getKind());
    }

    // ========== END OF TESTS ==========
}
