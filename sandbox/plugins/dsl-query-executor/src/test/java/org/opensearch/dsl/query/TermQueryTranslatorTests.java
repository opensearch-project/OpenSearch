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
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

public class TermQueryTranslatorTests extends OpenSearchTestCase {

    private final TermQueryTranslator translator = new TermQueryTranslator();
    private final ConversionContext ctx = TestUtils.createContext();

    public void testConvertsTermQueryToEquals() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.termQuery("name", "laptop"), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        // name is the 1st field (index 0) in TestUtils schema: name, price, brand, rating
        assertEquals(0, ((RexInputRef) call.getOperands().get(0)).getIndex());
        // makeLiteral wraps nullable VARCHAR in a CAST, so unwrap to get the inner literal
        RexCall cast = (RexCall) call.getOperands().get(1);
        assertEquals("laptop", ((RexLiteral) cast.getOperands().get(0)).getValueAs(String.class));
    }

    public void testResolvesCorrectFieldIndex() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.termQuery("brand", "brandX"), ctx);

        RexCall call = (RexCall) result;
        RexInputRef fieldRef = (RexInputRef) call.getOperands().get(0);
        // brand is the 3rd field (index 2) in TestUtils schema: name, price, brand, rating
        assertEquals(2, fieldRef.getIndex());
    }

    public void testIntegerValue() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.termQuery("price", 1200), ctx);

        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        // price is the 2nd field (index 1)
        assertEquals(1, ((RexInputRef) call.getOperands().get(0)).getIndex());
    }

    public void testThrowsForUnknownField() {
        expectThrows(ConversionException.class, () -> translator.convert(QueryBuilders.termQuery("nonexistent", "value"), ctx));
    }

    public void testReportsCorrectQueryType() {
        assertEquals(TermQueryBuilder.class, translator.getQueryType());
    }

    public void testScaledFloatTermQuery() throws ConversionException {
        // term scaled_price = 10.5 with factor 10 -> Math.round(10.5 * 10) = 105
        RexNode result = translator.convert(QueryBuilders.termQuery("scaled_price", 10.5), ctx);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(13, ((RexInputRef) call.getOperands().get(0)).getIndex());
        // makeLiteral wraps nullable BIGINT in a CAST
        RexNode literalNode = call.getOperands().get(1);
        Long literalValue;
        if (literalNode instanceof RexLiteral lit) {
            literalValue = lit.getValueAs(Long.class);
        } else {
            RexCall cast = (RexCall) literalNode;
            literalValue = ((RexLiteral) cast.getOperands().get(0)).getValueAs(Long.class);
        }
        assertEquals(Long.valueOf(105L), literalValue);
    }

    public void testScaledFloatTermQueryNonNumericThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("scaled_price", "abc"), ctx)
        );
        assertTrue(ex.getMessage().contains("Non-numeric"));
    }

    // ========== UNSIGNED_LONG TERM TESTS ==========

    public void testUnsignedLongTermInRange() throws ConversionException {
        // term unsigned_counter = 100 → literal 100, EQUALS
        RexNode result = translator.convert(QueryBuilders.termQuery("unsigned_counter", 100), ctx);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(14, ((RexInputRef) call.getOperands().get(0)).getIndex());
        RexNode literalNode = call.getOperands().get(1);
        Long literalValue;
        if (literalNode instanceof RexLiteral lit) {
            literalValue = lit.getValueAs(Long.class);
        } else {
            RexCall cast = (RexCall) literalNode;
            literalValue = ((RexLiteral) cast.getOperands().get(0)).getValueAs(Long.class);
        }
        assertEquals(Long.valueOf(100L), literalValue);
    }

    public void testUnsignedLongTermAboveLongMaxThrows() {
        // term above Long.MAX_VALUE → ConversionException
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("unsigned_counter", "9223372036854775808"), ctx)
        );
        assertTrue(ex.getMessage().contains("not representable"));
    }

    public void testUnsignedLongTermNegativeMatchNone() throws ConversionException {
        // term -5 on unsigned_long → match-none (literal false)
        RexNode result = translator.convert(QueryBuilders.termQuery("unsigned_counter", -5), ctx);
        assertTrue("Expected literal false (match-none)", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testUnsignedLongTermNonNumericThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("unsigned_counter", "abc"), ctx)
        );
        assertTrue(ex.getMessage().contains("Non-numeric"));
    }

    // ========== FIX 2: NaN/Infinity on scaled_float term must throw ==========

    public void testScaledFloatTermInfinityThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("scaled_price", "Infinity"), ctx)
        );
        assertTrue(ex.getMessage().contains("Infinity") || ex.getMessage().contains("non-finite"));
    }

    public void testScaledFloatTermNaNThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("scaled_price", "NaN"), ctx)
        );
        assertTrue(ex.getMessage().contains("NaN") || ex.getMessage().contains("non-finite"));
    }

    public void testScaledFloatTermDoubleNaNThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("scaled_price", Double.NaN), ctx)
        );
        assertTrue(ex.getMessage().contains("NaN") || ex.getMessage().contains("non-finite"));
    }

    public void testScaledFloatTermDoubleInfinityThrows() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("scaled_price", Double.POSITIVE_INFINITY), ctx)
        );
        assertTrue(ex.getMessage().contains("Infinity") || ex.getMessage().contains("non-finite"));
    }

    // ========== FIX 4+5: decimal on unsigned_long term must match-none ==========

    public void testUnsignedLongTermDecimalMatchNone() throws ConversionException {
        // term 2.5 on unsigned_long → match-none (literal false), per legacy MatchNoDocsQuery
        RexNode result = translator.convert(QueryBuilders.termQuery("unsigned_counter", 2.5), ctx);
        assertTrue("Expected literal false (match-none) for decimal term on unsigned_long", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testUnsignedLongTermDecimalStringMatchNone() throws ConversionException {
        // term "2.5" on unsigned_long → match-none
        RexNode result = translator.convert(QueryBuilders.termQuery("unsigned_counter", "2.5"), ctx);
        assertTrue("Expected literal false (match-none) for decimal string term on unsigned_long", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    // ========== IP FIELD TERM TEST ==========

    public void testIpTermThrowsConversionException() {
        // Legacy IpFieldMapper.termQuery supports IP terms, but implementing without verified
        // parity would replace a loud crash with a possibly silently-wrong answer.
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("ip_address", "192.168.1.1"), ctx)
        );
        assertTrue(ex.getMessage().contains("not yet supported"));
    }

    // ========== DATE FIELD TERM TEST ==========

    public void testDateTermThrowsConversionException() {
        // Legacy DateFieldMapper.DateFieldType.termQuery (line 505) supports date terms by
        // delegating to rangeQuery; our rejection is a known divergence until parity-verified
        // date term support is implemented.
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("created_date", "2024-01-01"), ctx)
        );
        assertTrue(ex.getMessage().contains("not yet supported"));
    }
}
