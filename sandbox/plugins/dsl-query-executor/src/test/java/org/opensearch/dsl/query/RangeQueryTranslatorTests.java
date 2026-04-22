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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
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

    public void testThrowsForNonIntersectsRelation() {
        expectThrows(
            ConversionException.class,
            () -> translator.convert(QueryBuilders.rangeQuery("price").gte(100).relation("CONTAINS"), ctx)
        );
    }

    public void testThrowsForUnknownField() {
        expectThrows(ConversionException.class, () -> translator.convert(QueryBuilders.rangeQuery("unknown").gte(1), ctx));
    }

    public void testThrowsForBoost() {
        RangeQueryBuilder query = QueryBuilders.rangeQuery("price").gte(100);
        query.boost(2.0f);
        expectThrows(ConversionException.class, () -> translator.convert(query, ctx));
    }

    public void testThrowsForNoBounds() {
        expectThrows(ConversionException.class, () -> translator.convert(QueryBuilders.rangeQuery("price"), ctx));
    }

    public void testReportsCorrectQueryType() {
        assertEquals(RangeQueryBuilder.class, translator.getQueryType());
    }
}
