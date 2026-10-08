/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.lucene.util.BytesRef;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigDecimal;
import java.util.Optional;

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

    public void testThrowsForBoost() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("name", "laptop").boost(2.0f), ctx)
        );
        assertEquals("Term query parameter 'boost' is not supported", ex.getMessage());
    }

    public void testThrowsForName() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("name", "laptop").queryName("my_term"), ctx)
        );
        assertEquals("Term query parameter '_name' is not supported", ex.getMessage());
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

    // ========== FRACTIONAL VALUE ON EXACT-INTEGER FIELD MUST MATCH-NONE ==========

    public void testIntegerTermDecimalMatchNone() throws ConversionException {
        // term 30.5 on INTEGER field (price) → match-none, per NumberFieldMapper INTEGER.termQuery
        RexNode result = translator.convert(QueryBuilders.termQuery("price", 30.5), ctx);
        assertTrue("Expected literal false (match-none) for decimal term on INTEGER", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testBigintTermDecimalMatchNone() throws ConversionException {
        // term 30.5 on BIGINT field (timestamp) → match-none
        RexNode result = translator.convert(QueryBuilders.termQuery("timestamp", 30.5), ctx);
        assertTrue("Expected literal false (match-none) for decimal term on BIGINT", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testSmallintTermDecimalMatchNone() throws ConversionException {
        // term 30.5 on SMALLINT field (small_val) → match-none
        RexNode result = translator.convert(QueryBuilders.termQuery("small_val", 30.5), ctx);
        assertTrue("Expected literal false (match-none) for decimal term on SMALLINT", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testTinyintTermDecimalMatchNone() throws ConversionException {
        // term 30.5 on TINYINT field (tiny_val) → match-none
        RexNode result = translator.convert(QueryBuilders.termQuery("tiny_val", 30.5), ctx);
        assertTrue("Expected literal false (match-none) for decimal term on TINYINT", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testIntegerTermDecimalStringMatchNone() throws ConversionException {
        // term "30.5" (String) on INTEGER field → match-none; hasDecimalPart parses String
        RexNode result = translator.convert(QueryBuilders.termQuery("price", "30.5"), ctx);
        assertTrue("Expected literal false (match-none) for decimal string term on INTEGER", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testIntegerTermIntegralStillEquals() throws ConversionException {
        // NEGATIVE CONTROL: term 30 (integral) on INTEGER field → normal EQUALS, unchanged
        RexNode result = translator.convert(QueryBuilders.termQuery("price", 30), ctx);
        assertTrue("Expected EQUALS for integral term on INTEGER", result instanceof RexCall);
        assertEquals(SqlKind.EQUALS, ((RexCall) result).getKind());
    }

    public void testDoubleTermDecimalStillEquals() throws ConversionException {
        // NEGATIVE CONTROL: term 30.5 on DOUBLE field (rating) → normal EQUALS, NOT match-none.
        // Proves the guard is scoped to exact-integer types, not blanket.
        RexNode result = translator.convert(QueryBuilders.termQuery("rating", 30.5), ctx);
        assertTrue("Expected EQUALS for decimal term on DOUBLE", result instanceof RexCall);
        assertEquals(SqlKind.EQUALS, ((RexCall) result).getKind());
    }

    public void testFloatTermDecimalStillEquals() throws ConversionException {
        // NEGATIVE CONTROL: term 30.5 on REAL/float field (float_val) → normal EQUALS, NOT match-none.
        RexNode result = translator.convert(QueryBuilders.termQuery("float_val", 30.5), ctx);
        assertTrue("Expected EQUALS for decimal term on REAL", result instanceof RexCall);
        assertEquals(SqlKind.EQUALS, ((RexCall) result).getKind());
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

    // ========== OUT-OF-RANGE WHOLE VALUE ON EXACT-INTEGER FIELD ==========

    public void testIntegerTermAboveIntMaxThrows() {
        // term 2147483648 on INTEGER field (price) → out of INTEGER domain → ConversionException (HTTP 400),
        // matching legacy NumberFieldMapper INTEGER.termQuery IllegalArgumentException.
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("price", 2147483648L), ctx)
        );
        assertTrue(ex.getMessage().contains("out of range"));
        assertTrue(ex.getMessage().contains("2147483648"));
    }

    public void testIntegerTermBelowIntMinThrows() {
        // term -2147483649 on INTEGER field (price) → out of INTEGER domain → ConversionException.
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("price", -2147483649L), ctx)
        );
        assertTrue(ex.getMessage().contains("out of range"));
        assertTrue(ex.getMessage().contains("-2147483649"));
    }

    public void testBigintTermAboveLongMaxThrows() {
        // term 9223372036854775808 (Long.MAX_VALUE + 1) on BIGINT field (timestamp) → out of BIGINT domain
        // → ConversionException, matching legacy NumberFieldMapper LONG.termQuery "out of range for a long".
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("timestamp", new BigDecimal("9223372036854775808")), ctx)
        );
        assertTrue(ex.getMessage().contains("out of range for a long"));
        assertTrue(ex.getMessage().contains("9223372036854775808"));
    }

    public void testSmallintTermAboveShortMaxMatchNone() throws ConversionException {
        // term 32768 on SMALLINT field (small_val): in INTEGER range but outside SMALLINT domain → match-none,
        // matching legacy where SHORT.termQuery delegates to INTEGER.termQuery and returns 0 hits (HTTP 200).
        RexNode result = translator.convert(QueryBuilders.termQuery("small_val", 32768), ctx);
        assertTrue("Expected literal false (match-none) for out-of-range term on SMALLINT", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testTinyintTermAboveByteMaxMatchNone() throws ConversionException {
        // term 128 on TINYINT field (tiny_val): in INTEGER range but outside TINYINT domain → match-none.
        RexNode result = translator.convert(QueryBuilders.termQuery("tiny_val", 128), ctx);
        assertTrue("Expected literal false (match-none) for out-of-range term on TINYINT", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testIntegerTermMaxInRangeStillEquals() throws ConversionException {
        // NEGATIVE CONTROL: term 2147483647 (Integer.MAX_VALUE, in range) on INTEGER → normal EQUALS.
        RexNode result = translator.convert(QueryBuilders.termQuery("price", 2147483647), ctx);
        assertTrue("Expected EQUALS for in-range max term on INTEGER", result instanceof RexCall);
        assertEquals(SqlKind.EQUALS, ((RexCall) result).getKind());
    }

    public void testIntegerTermMinInRangeStillEquals() throws ConversionException {
        // NEGATIVE CONTROL: term -2147483648 (Integer.MIN_VALUE, in range) on INTEGER → normal EQUALS.
        RexNode result = translator.convert(QueryBuilders.termQuery("price", -2147483648), ctx);
        assertTrue("Expected EQUALS for in-range min term on INTEGER", result instanceof RexCall);
        assertEquals(SqlKind.EQUALS, ((RexCall) result).getKind());
    }

    public void testBigintTermLongMaxStillEquals() throws ConversionException {
        // NEGATIVE CONTROL: term Long.MAX_VALUE (in range) on BIGINT → normal EQUALS.
        RexNode result = translator.convert(QueryBuilders.termQuery("timestamp", Long.MAX_VALUE), ctx);
        assertTrue("Expected EQUALS for in-range max term on BIGINT", result instanceof RexCall);
        assertEquals(SqlKind.EQUALS, ((RexCall) result).getKind());
    }

    public void testDoubleTermHugeValueStillEquals() throws ConversionException {
        // NEGATIVE CONTROL: a huge value on DOUBLE field (rating) is unaffected by the integer guard → normal EQUALS.
        RexNode result = translator.convert(QueryBuilders.termQuery("rating", 1.0e300), ctx);
        assertTrue("Expected EQUALS for huge term on DOUBLE", result instanceof RexCall);
        assertEquals(SqlKind.EQUALS, ((RexCall) result).getKind());
    }

    public void testBigintTermHugeDoubleAboveLongMaxThrows() {
        // term 1e19 (whole Double beyond Long.MAX_VALUE) on BIGINT field (timestamp): double-to-long
        // narrowing saturates to Long.MAX_VALUE, so it must be rejected rather than silently accepted.
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("timestamp", 1e19), ctx)
        );
        assertTrue(ex.getMessage().contains("out of range for a long"));
    }

    public void testBigintTermHugeNegativeDoubleBelowLongMinThrows() {
        // term -1e19 (whole Double beyond Long.MIN_VALUE) on BIGINT field (timestamp) → ConversionException.
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("timestamp", -1e19), ctx)
        );
        assertTrue(ex.getMessage().contains("out of range for a long"));
    }

    public void testBigintTermLongMinStillEquals() throws ConversionException {
        // NEGATIVE CONTROL: term Long.MIN_VALUE (in range) on BIGINT → normal EQUALS.
        RexNode result = translator.convert(QueryBuilders.termQuery("timestamp", Long.MIN_VALUE), ctx);
        assertTrue("Expected EQUALS for in-range min term on BIGINT", result instanceof RexCall);
        assertEquals(SqlKind.EQUALS, ((RexCall) result).getKind());
    }

    public void testBigintTermNaNMatchNone() throws ConversionException {
        // REGRESSION GUARD: NaN on BIGINT reaches the fractional guard first → match-none, not an exception.
        RexNode result = translator.convert(QueryBuilders.termQuery("timestamp", Double.NaN), ctx);
        assertTrue("Expected literal false (match-none) for NaN term on BIGINT", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testBigintTermInfinityMatchNone() throws ConversionException {
        // REGRESSION GUARD: +Infinity on BIGINT reaches the fractional guard first → match-none.
        RexNode result = translator.convert(QueryBuilders.termQuery("timestamp", Double.POSITIVE_INFINITY), ctx);
        assertTrue("Expected literal false (match-none) for Infinity term on BIGINT", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    // ========== IN-RANGE WHOLE Double/Float ON EXACT-INTEGER FIELD MUST EQUAL ==========

    private static Integer intLiteral(RexNode literalNode) {
        return (literalNode instanceof RexLiteral l)
            ? l.getValueAs(Integer.class)
            : ((RexLiteral) ((RexCall) literalNode).getOperands().get(0)).getValueAs(Integer.class);
    }

    private static Long longLiteral(RexNode literalNode) {
        return (literalNode instanceof RexLiteral l)
            ? l.getValueAs(Long.class)
            : ((RexLiteral) ((RexCall) literalNode).getOperands().get(0)).getValueAs(Long.class);
    }

    public void testIntegerTermWholeDoubleEquals() throws ConversionException {
        // term 30.0d (whole Double) on INTEGER field (price) → normal EQUALS whose literal equals 30.
        RexNode result = translator.convert(QueryBuilders.termQuery("price", 30.0d), ctx);
        assertTrue("Expected EQUALS for whole-double term on INTEGER", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(Integer.valueOf(30), intLiteral(call.getOperands().get(1)));
    }

    public void testIntegerTermWholeFloatEquals() throws ConversionException {
        // term 30.0f (whole Float) on INTEGER field (price) → normal EQUALS, literal equals 30 (no binary artefact).
        RexNode result = translator.convert(QueryBuilders.termQuery("price", 30.0f), ctx);
        assertTrue("Expected EQUALS for whole-float term on INTEGER", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(Integer.valueOf(30), intLiteral(call.getOperands().get(1)));
    }

    public void testBigintTermWholeDoubleEquals() throws ConversionException {
        // term 30.0d (whole Double) on BIGINT field (timestamp) → normal EQUALS, literal equals 30.
        RexNode result = translator.convert(QueryBuilders.termQuery("timestamp", 30.0d), ctx);
        assertTrue("Expected EQUALS for whole-double term on BIGINT", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(Long.valueOf(30L), longLiteral(call.getOperands().get(1)));
    }

    public void testSmallintTermWholeDoubleEquals() throws ConversionException {
        // term 30.0d (whole Double) on SMALLINT field (small_val) → normal EQUALS, literal equals 30.
        RexNode result = translator.convert(QueryBuilders.termQuery("small_val", 30.0d), ctx);
        assertTrue("Expected EQUALS for whole-double term on SMALLINT", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(Integer.valueOf(30), intLiteral(call.getOperands().get(1)));
    }

    public void testTinyintTermWholeDoubleEquals() throws ConversionException {
        // term 30.0d (whole Double) on TINYINT field (tiny_val) → normal EQUALS, literal equals 30.
        RexNode result = translator.convert(QueryBuilders.termQuery("tiny_val", 30.0d), ctx);
        assertTrue("Expected EQUALS for whole-double term on TINYINT", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(Integer.valueOf(30), intLiteral(call.getOperands().get(1)));
    }

    public void testIntegerTermWholeDoubleAtIntMaxEquals() throws ConversionException {
        // BOUNDARY: term 2147483647.0d (whole Double at Integer.MAX_VALUE) on INTEGER → EQUALS, literal equals 2147483647.
        RexNode result = translator.convert(QueryBuilders.termQuery("price", 2147483647.0d), ctx);
        assertTrue("Expected EQUALS for in-range max whole-double on INTEGER", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(Integer.valueOf(2147483647), intLiteral(call.getOperands().get(1)));
    }

    public void testBigintTermLargeWholeDoubleEquals() throws ConversionException {
        // term 1.0E15d (exactly representable whole Double) on BIGINT → EQUALS, literal equals 1000000000000000 (no artefact).
        RexNode result = translator.convert(QueryBuilders.termQuery("timestamp", 1.0E15d), ctx);
        assertTrue("Expected EQUALS for large whole-double on BIGINT", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(Long.valueOf(1000000000000000L), longLiteral(call.getOperands().get(1)));
    }

    public void testDoubleTermWholeDoubleStillEquals() throws ConversionException {
        // REGRESSION GUARD: term 30.0d on DOUBLE field (rating) is unaffected by the coercion → normal EQUALS.
        RexNode result = translator.convert(QueryBuilders.termQuery("rating", 30.0d), ctx);
        assertTrue("Expected EQUALS for whole-double term on DOUBLE", result instanceof RexCall);
        assertEquals(SqlKind.EQUALS, ((RexCall) result).getKind());
    }

    public void testIntegerTermLongStillEquals() throws ConversionException {
        // REGRESSION GUARD: term 30L (Long) on INTEGER field → unchanged normal EQUALS, literal equals 30.
        RexNode result = translator.convert(QueryBuilders.termQuery("price", 30L), ctx);
        assertTrue("Expected EQUALS for Long term on INTEGER", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(Integer.valueOf(30), intLiteral(call.getOperands().get(1)));
    }

    // ========== STRING-TYPED TERM VALUE ON EXACT-INTEGER FIELD ==========

    // Invokes the mapper directly so a genuine BytesRef reaches toTermLiteral: TermQueryBuilder.value()
    // converts the stored BytesRef back to String, so the translator path can only exercise String.
    private Optional<RexNode> termLiteral(String fieldName, Object value) throws ConversionException {
        RelDataTypeField field = ctx.getField(fieldName);
        return TranslatorMapperRegistry.INSTANCE.resolve(field.getType()).toTermLiteral(value, field, ctx);
    }

    public void testIntegerTermStringWholeEquals() throws ConversionException {
        // term "30" (String) on INTEGER field (price) → EQUALS, literal equals 30 (vanilla parses the string and matches).
        RexNode result = translator.convert(QueryBuilders.termQuery("price", "30"), ctx);
        assertTrue("Expected EQUALS for whole string term on INTEGER", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(Integer.valueOf(30), intLiteral(call.getOperands().get(1)));
    }

    public void testIntegerTermBytesRefWholeEquals() throws ConversionException {
        // new BytesRef("30") on INTEGER field (price) → EQUALS literal 30: the production wire shape is BytesRef.
        Optional<RexNode> literal = termLiteral("price", new BytesRef("30"));
        assertTrue("Expected present literal for whole BytesRef term on INTEGER", literal.isPresent());
        assertEquals(Integer.valueOf(30), intLiteral(literal.get()));
    }

    public void testBigintTermStringWholeEquals() throws ConversionException {
        // term "30" (String) on BIGINT field (timestamp) → EQUALS, literal equals 30.
        RexNode result = translator.convert(QueryBuilders.termQuery("timestamp", "30"), ctx);
        assertTrue("Expected EQUALS for whole string term on BIGINT", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(Long.valueOf(30L), longLiteral(call.getOperands().get(1)));
    }

    public void testSmallintTermStringWholeEquals() throws ConversionException {
        // term "30" (String) on SMALLINT field (small_val) → EQUALS, literal equals 30.
        RexNode result = translator.convert(QueryBuilders.termQuery("small_val", "30"), ctx);
        assertTrue("Expected EQUALS for whole string term on SMALLINT", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(Integer.valueOf(30), intLiteral(call.getOperands().get(1)));
    }

    public void testTinyintTermStringWholeEquals() throws ConversionException {
        // term "30" (String) on TINYINT field (tiny_val) → EQUALS, literal equals 30.
        RexNode result = translator.convert(QueryBuilders.termQuery("tiny_val", "30"), ctx);
        assertTrue("Expected EQUALS for whole string term on TINYINT", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(Integer.valueOf(30), intLiteral(call.getOperands().get(1)));
    }

    public void testIntegerTermStringAboveIntMaxThrows() {
        // term "2147483648" (String) on INTEGER field (price) → out of INTEGER domain → ConversionException (HTTP 400).
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("price", "2147483648"), ctx)
        );
        assertTrue(ex.getMessage().contains("out of range"));
        assertTrue(ex.getMessage().contains("2147483648"));
    }

    public void testIntegerTermBytesRefAboveIntMaxThrows() {
        // new BytesRef("2147483648") on INTEGER field (price) → ConversionException, same as the String shape.
        ConversionException ex = expectThrows(ConversionException.class, () -> termLiteral("price", new BytesRef("2147483648")));
        assertTrue(ex.getMessage().contains("out of range"));
        assertTrue(ex.getMessage().contains("2147483648"));
    }

    public void testBigintTermStringAboveLongMaxThrows() {
        // term "9223372036854775808" (Long.MAX_VALUE + 1, String) on BIGINT field (timestamp) → ConversionException.
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("timestamp", "9223372036854775808"), ctx)
        );
        assertTrue(ex.getMessage().contains("out of range for a long"));
        assertTrue(ex.getMessage().contains("9223372036854775808"));
    }

    public void testSmallintTermStringAboveShortMaxMatchNone() throws ConversionException {
        // term "32768" (String) on SMALLINT field (small_val): in INTEGER range but outside SMALLINT domain → match-none,
        // preserving the two-tier asymmetry where SHORT.termQuery delegates to INTEGER.termQuery (HTTP 200, 0 hits).
        RexNode result = translator.convert(QueryBuilders.termQuery("small_val", "32768"), ctx);
        assertTrue("Expected literal false (match-none) for out-of-range string term on SMALLINT", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testIntegerTermStringNonNumericThrows() {
        // term "abc" (non-numeric String) on INTEGER field (price): vanilla objectToDouble → Double.parseDouble →
        // NumberFormatException (an IllegalArgumentException) → HTTP 400. We pre-validate and raise a clean ConversionException.
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("price", "abc"), ctx)
        );
        assertTrue(ex.getMessage().contains("not a valid number"));
        assertTrue(ex.getMessage().contains("abc"));
    }

    public void testIntegerTermBytesRefDecimalMatchNone() throws ConversionException {
        // REGRESSION GUARD: new BytesRef("30.5") on INTEGER field → match-none; normalisation preserves the fractional part.
        Optional<RexNode> literal = termLiteral("price", new BytesRef("30.5"));
        assertTrue("Expected match-none (empty) for fractional BytesRef term on INTEGER", literal.isEmpty());
    }

    public void testIntegerTermStringWhitespaceTrimmedEquals() throws ConversionException {
        // term " 30 " on INTEGER field (price) → EQUALS 30; vanilla Double.parseDouble trims surrounding whitespace.
        RexNode result = translator.convert(QueryBuilders.termQuery("price", "  30  "), ctx);
        assertTrue("Expected EQUALS for whitespace-padded string term on INTEGER", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(Integer.valueOf(30), intLiteral(call.getOperands().get(1)));
    }

    public void testIntegerTermStringDoubleSuffixEquals() throws ConversionException {
        // term "30d" on INTEGER field (price) → EQUALS 30; vanilla Double.parseDouble accepts the 'd' type suffix.
        RexNode result = translator.convert(QueryBuilders.termQuery("price", "30d"), ctx);
        assertTrue("Expected EQUALS for 'd'-suffixed string term on INTEGER", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(Integer.valueOf(30), intLiteral(call.getOperands().get(1)));
    }

    public void testIntegerTermStringFloatSuffixEquals() throws ConversionException {
        // term "30f" on INTEGER field (price) → EQUALS 30; vanilla Double.parseDouble accepts the 'f' type suffix.
        RexNode result = translator.convert(QueryBuilders.termQuery("price", "30f"), ctx);
        assertTrue("Expected EQUALS for 'f'-suffixed string term on INTEGER", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(Integer.valueOf(30), intLiteral(call.getOperands().get(1)));
    }

    public void testIntegerTermStringInfinityMatchNone() throws ConversionException {
        // term "Infinity" on INTEGER field (price) → match-none; vanilla parses +Inf, hasDecimalPart treats it as fractional.
        RexNode result = translator.convert(QueryBuilders.termQuery("price", "Infinity"), ctx);
        assertTrue("Expected literal false (match-none) for Infinity string term on INTEGER", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testIntegerTermStringNegativeInfinityMatchNone() throws ConversionException {
        // term "-Infinity" on INTEGER field (price) → match-none.
        RexNode result = translator.convert(QueryBuilders.termQuery("price", "-Infinity"), ctx);
        assertTrue("Expected literal false (match-none) for -Infinity string term on INTEGER", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testIntegerTermStringNaNMatchNone() throws ConversionException {
        // term "NaN" on INTEGER field (price) → match-none.
        RexNode result = translator.convert(QueryBuilders.termQuery("price", "NaN"), ctx);
        assertTrue("Expected literal false (match-none) for NaN string term on INTEGER", result instanceof RexLiteral);
        assertEquals(Boolean.FALSE, ((RexLiteral) result).getValueAs(Boolean.class));
    }

    public void testBigintTermStringLongMaxExactEquals() throws ConversionException {
        // PRECISION GUARD: term "9223372036854775807" on BIGINT field (timestamp) → EQUALS whose literal is exactly
        // Long.MAX_VALUE. Fails if parsing is ever routed through double, which cannot represent this value exactly.
        RexNode result = translator.convert(QueryBuilders.termQuery("timestamp", "9223372036854775807"), ctx);
        assertTrue("Expected EQUALS for Long.MAX_VALUE string term on BIGINT", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(Long.valueOf(Long.MAX_VALUE), longLiteral(call.getOperands().get(1)));
    }

    public void testBigintTermStringAbove2Pow53ExactEquals() throws ConversionException {
        // PRECISION GUARD: term "9007199254740993" (2^53 + 1, not representable as a double) on BIGINT field (timestamp)
        // → EQUALS whose literal is exactly 9007199254740993.
        RexNode result = translator.convert(QueryBuilders.termQuery("timestamp", "9007199254740993"), ctx);
        assertTrue("Expected EQUALS for 2^53+1 string term on BIGINT", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(Long.valueOf(9007199254740993L), longLiteral(call.getOperands().get(1)));
    }

    public void testIntegerTermStringWhitespaceAboveIntMaxThrows() {
        // term " 2147483648 " on INTEGER field (price) → trimmed, parsed, but still out of INTEGER domain → ConversionException.
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("price", "  2147483648  "), ctx)
        );
        assertTrue(ex.getMessage().contains("out of range"));
    }

    public void testIntegerTermStringHexThrows() {
        // term "0x1E" on INTEGER field (price) → ConversionException; parity with vanilla Double.parseDouble, which rejects hex.
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("price", "0x1E"), ctx)
        );
        assertTrue(ex.getMessage().contains("not a valid number"));
    }

    public void testIntegerTermStringEmptyThrows() {
        // term "" on INTEGER field (price) → ConversionException; parity with vanilla Double.parseDouble, which rejects empty.
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("price", ""), ctx)
        );
        assertTrue(ex.getMessage().contains("not a valid number"));
    }

    public void testIntegerTermStringLongNonNumericMessageCapped() {
        // term with a long non-numeric string on INTEGER field (price) → ConversionException whose message is capped so it
        // does not echo the full attacker-controlled input.
        String longInput = "z".repeat(200);
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.termQuery("price", longInput), ctx)
        );
        assertTrue("Expected capped message with ellipsis", ex.getMessage().contains("..."));
        assertFalse("Message must not echo the full oversized input", ex.getMessage().contains(longInput));
    }

    public void testIntegerTermStringScientificEquals() throws ConversionException {
        // REGRESSION GUARD: term "1e3" on INTEGER field (price) → EQUALS 1000; BigDecimal already parses scientific notation.
        RexNode result = translator.convert(QueryBuilders.termQuery("price", "1e3"), ctx);
        assertTrue("Expected EQUALS for scientific string term on INTEGER", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(Integer.valueOf(1000), intLiteral(call.getOperands().get(1)));
    }

    public void testIntegerTermStringLeadingPlusEquals() throws ConversionException {
        // REGRESSION GUARD: term "+30" on INTEGER field (price) → EQUALS 30; BigDecimal already parses a leading sign.
        RexNode result = translator.convert(QueryBuilders.termQuery("price", "+30"), ctx);
        assertTrue("Expected EQUALS for leading-plus string term on INTEGER", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
        assertEquals(Integer.valueOf(30), intLiteral(call.getOperands().get(1)));
    }
}
