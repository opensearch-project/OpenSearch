/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

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

    // ========== END OF TESTS ==========
}
