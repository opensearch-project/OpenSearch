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
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.indices.TermsLookup;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Date;

public class TermsQueryTranslatorTests extends OpenSearchTestCase {

    private final TermsQueryTranslator translator = new TermsQueryTranslator();
    private final ConversionContext ctx = TestUtils.createContext();

    public void testSingleValueUsesEquals() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.termsQuery("name", "laptop"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(2, call.getOperands().size());
    }

    public void testMultipleStringValuesUsesSearch() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.termsQuery("name", "laptop", "phone"), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());
    }

    public void testResolvesCorrectFieldIndex() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.termsQuery("brand", "brandX", "brandY"), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());
        // OR expression has nested structure, get field from first operand
        RexCall firstEquals = (RexCall) call.getOperands().get(0);
        RexInputRef fieldRef = (RexInputRef) firstEquals.getOperands().get(0);
        assertEquals(2, fieldRef.getIndex());
    }

    public void testIntegerValues() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.termsQuery("price", new Object[] { 1200, 1500 }), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());
        // OR expression has nested structure, get field from first operand
        RexCall firstEquals = (RexCall) call.getOperands().get(0);
        RexInputRef fieldRef = (RexInputRef) firstEquals.getOperands().get(0);
        assertEquals(1, fieldRef.getIndex());
    }

    public void testDoubleValuesUsesSearch() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.termsQuery("rating", new Object[] { 4.5, 4.8, 5.0 }), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());
    }

    public void testThrowsForUnknownField() {
        expectThrows(ConversionException.class, () -> translator.convert(QueryBuilders.termsQuery("nonexistent", "value"), ctx));
    }

    public void testThrowsForEmptyValues() {
        expectThrows(IllegalArgumentException.class, () -> translator.convert(QueryBuilders.termsQuery("name", (Object[]) null), ctx));
    }

    public void testThrowsForBoost() {
        expectThrows(ConversionException.class, () -> translator.convert(QueryBuilders.termsQuery("name", "laptop").boost(2.0f), ctx));
    }

    public void testThrowsForQueryName() {
        expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termsQuery("name", "laptop").queryName("my_query"), ctx)
        );
    }

    public void testThrowsForTermsLookup() {
        TermsLookup termsLookup = new TermsLookup("lookup_index", "1", "terms");
        expectThrows(ConversionException.class, () -> translator.convert(QueryBuilders.termsLookupQuery("name", termsLookup), ctx));
    }

    public void testThrowsForValueType() {
        expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termsQuery("name", "laptop").valueType(TermsQueryBuilder.ValueType.BITMAP), ctx)
        );
    }

    public void testReportsCorrectQueryType() {
        assertEquals(TermsQueryBuilder.class, translator.getQueryType());
    }

    // Supported types: VARCHAR, INTEGER, DOUBLE, BOOLEAN, BIGINT
    // Date type still throws ClassCastException from Calcite's RexBuilder.makeLiteral()

    // TODO: Enable when date type support is added — legacy DateFieldMapper.DateFieldType.termQuery
    // (line 505) supports term on date by delegating to rangeQuery; our rejection is a known
    // divergence until parity-verified date term support is implemented.
    public void testDateType() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(
                QueryBuilders.termsQuery("created_date", new Object[] { new Date(1704067200000L), new Date(1706745600000L) }),
                ctx
            )
        );
        assertTrue(ex.getMessage().contains("not yet supported"));
    }

    public void testBooleanType() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.termsQuery("is_active", new Object[] { true, false }), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());
    }

    public void testLongType() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.termsQuery("timestamp", new Object[] { 1234567890L, 9876543210L }), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());
    }

    public void testGeoPointType() {
        expectThrows(
            IllegalArgumentException.class,
            () -> translator.convert(QueryBuilders.termsQuery("location", new Object[] { "40.7128,-74.0060", "34.0522,-118.2437" }), ctx)
        );
    }

    public void testKeywordType() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.termsQuery("status", new Object[] { "active", "pending" }), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());
    }

    // TODO: Enable when binary type support is added — legacy BinaryFieldMapper.BinaryFieldType.termQuery
    // (line 172) throws QueryShardException("Binary fields do not support searching");
    // ConversionException now MATCHES legacy rejection behaviour.
    public void testBinaryType() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(
                QueryBuilders.termsQuery("binary_data", new Object[] { "U29tZSBiaW5hcnkgYmxvYg==", "QW5vdGhlciBibG9i" }),
                ctx
            )
        );
        assertTrue(ex.getMessage().contains("does not support term queries"));
    }

    public void testScaledFloatTermsQuery() throws ConversionException {
        // terms scaled_price = [10.5, 20.3] with factor 10 -> [105, 203]
        RexNode result = translator.convert(QueryBuilders.termsQuery("scaled_price", new Object[] { 10.5, 20.3 }), ctx);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());

        RexCall eq0 = (RexCall) call.getOperands().get(0);
        assertEquals(SqlKind.EQUALS, eq0.getKind());
        RexNode lit0 = eq0.getOperands().get(1);
        Long val0 = (lit0 instanceof RexLiteral l)
            ? l.getValueAs(Long.class)
            : ((RexLiteral) ((RexCall) lit0).getOperands().get(0)).getValueAs(Long.class);
        assertEquals(Long.valueOf(105L), val0);

        RexCall eq1 = (RexCall) call.getOperands().get(1);
        assertEquals(SqlKind.EQUALS, eq1.getKind());
        RexNode lit1 = eq1.getOperands().get(1);
        Long val1 = (lit1 instanceof RexLiteral l)
            ? l.getValueAs(Long.class)
            : ((RexLiteral) ((RexCall) lit1).getOperands().get(0)).getValueAs(Long.class);
        assertEquals(Long.valueOf(203L), val1);
    }

    // ========== UNSIGNED_LONG TERMS TESTS ==========

    public void testUnsignedLongTermsInRange() throws ConversionException {
        // terms unsigned_counter = [100, 200] → IN(100, 200)
        RexNode result = translator.convert(QueryBuilders.termsQuery("unsigned_counter", new Object[] { 100, 200 }), ctx);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());

        RexCall eq0 = (RexCall) call.getOperands().get(0);
        assertEquals(SqlKind.EQUALS, eq0.getKind());
        RexNode lit0 = eq0.getOperands().get(1);
        Long val0 = (lit0 instanceof RexLiteral l)
            ? l.getValueAs(Long.class)
            : ((RexLiteral) ((RexCall) lit0).getOperands().get(0)).getValueAs(Long.class);
        assertEquals(Long.valueOf(100L), val0);
    }

    public void testUnsignedLongTermsAllNegativeMatchNone() throws ConversionException {
        // All values negative → match-none (literal false)
        RexNode result = translator.convert(QueryBuilders.termsQuery("unsigned_counter", new Object[] { -1, -5 }), ctx);
        assertTrue("Expected literal false", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testUnsignedLongTermsAboveLongMaxThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termsQuery("unsigned_counter", new Object[] { "9223372036854775808" }), ctx)
        );
        assertTrue(ex.getMessage().contains("not representable"));
    }

    // ========== FIX 3: NaN/Infinity on scaled_float terms must throw ==========

    public void testScaledFloatTermsInfinityThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termsQuery("scaled_price", new Object[] { "Infinity" }), ctx)
        );
        assertTrue(ex.getMessage().contains("Infinity") || ex.getMessage().contains("non-finite"));
    }

    public void testScaledFloatTermsNaNThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termsQuery("scaled_price", new Object[] { "NaN" }), ctx)
        );
        assertTrue(ex.getMessage().contains("NaN") || ex.getMessage().contains("non-finite"));
    }

    public void testScaledFloatTermsDoubleNaNThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termsQuery("scaled_price", new Object[] { Double.NaN }), ctx)
        );
        assertTrue(ex.getMessage().contains("NaN") || ex.getMessage().contains("non-finite"));
    }

    // ========== FIX 5: decimal on unsigned_long terms skips decimal values ==========

    public void testUnsignedLongTermsDecimalOnlyMatchNone() throws ConversionException {
        // terms [2.5] on unsigned_long → all have decimal → match-none
        RexNode result = translator.convert(QueryBuilders.termsQuery("unsigned_counter", new Object[] { 2.5 }), ctx);
        assertTrue("Expected literal false (match-none) for decimal-only terms", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testUnsignedLongTermsMixedDecimalAndWholeKeepsOnlyWhole() throws ConversionException {
        // terms [2.5, 100] on unsigned_long → skip 2.5, keep 100
        RexNode result = translator.convert(QueryBuilders.termsQuery("unsigned_counter", new Object[] { 2.5, 100 }), ctx);
        // Should produce equality on 100 only (single value = EQUALS, not OR)
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        RexNode lit = call.getOperands().get(1);
        Long val = (lit instanceof RexLiteral l)
            ? l.getValueAs(Long.class)
            : ((RexLiteral) ((RexCall) lit).getOperands().get(0)).getValueAs(Long.class);
        assertEquals(Long.valueOf(100L), val);
    }
}
