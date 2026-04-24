/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchTestCase;

public class BoolQueryTranslatorTests extends OpenSearchTestCase {

    private final QueryRegistry registry = QueryRegistryFactory.create();
    private final BoolQueryTranslator translator = new BoolQueryTranslator(registry);
    private final ConversionContext ctx = TestUtils.createContext();

    // Basic bool query tests

    public void testMustClause() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("name", "test")), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
    }

    public void testShouldClauseWithoutMust() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.boolQuery().should(QueryBuilders.termQuery("name", "test1")).should(QueryBuilders.termQuery("name", "test2")),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());
        assertEquals(2, call.getOperands().size());
    }

    public void testMustNotClause() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("name", "test")), ctx);

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.NOT, call.getKind());
    }

    // minimum_should_match: Non-negative integer

    public void testMinimumShouldMatchInteger2() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("name", "a"))
                .should(QueryBuilders.termQuery("name", "b"))
                .should(QueryBuilders.termQuery("name", "c"))
                .minimumShouldMatch("2"),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());
        // Should generate: (a AND b) OR (a AND c) OR (b AND c)
        assertEquals(3, call.getOperands().size());
    }

    public void testMinimumShouldMatchInteger1() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("name", "a"))
                .should(QueryBuilders.termQuery("name", "b"))
                .minimumShouldMatch("1"),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());
        assertEquals(2, call.getOperands().size());
    }

    // minimum_should_match: Negative integer

    public void testMinimumShouldMatchNegativeInteger() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("name", "a"))
                .should(QueryBuilders.termQuery("name", "b"))
                .should(QueryBuilders.termQuery("name", "c"))
                .minimumShouldMatch("-1"),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());
        // -1 means total - 1 = 3 - 1 = 2 required
        assertEquals(3, call.getOperands().size());
    }

    // minimum_should_match: Non-negative percentage

    public void testMinimumShouldMatchPercentage70() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("name", "a"))
                .should(QueryBuilders.termQuery("name", "b"))
                .should(QueryBuilders.termQuery("name", "c"))
                .should(QueryBuilders.termQuery("name", "d"))
                .minimumShouldMatch("70%"),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());
        // 70% of 4 = 2.8, floor = 2 required
    }

    public void testMinimumShouldMatchPercentage50() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("name", "a"))
                .should(QueryBuilders.termQuery("name", "b"))
                .should(QueryBuilders.termQuery("name", "c"))
                .should(QueryBuilders.termQuery("name", "d"))
                .minimumShouldMatch("50%"),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());
        // 50% of 4 = 2 required
    }

    // minimum_should_match: Negative percentage

    public void testMinimumShouldMatchNegativePercentage() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("name", "a"))
                .should(QueryBuilders.termQuery("name", "b"))
                .should(QueryBuilders.termQuery("name", "c"))
                .should(QueryBuilders.termQuery("name", "d"))
                .minimumShouldMatch("-30%"),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());
        // -30% means can miss 30% = 1.2, floor = 1, so 4 - 1 = 3 required
    }

    // minimum_should_match: Single combination

    public void testMinimumShouldMatchCombination2Less75Percent() throws ConversionException {
        // 2<75% means: if total <= 2, match all; if total > 2, match 75%
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("name", "a"))
                .should(QueryBuilders.termQuery("name", "b"))
                .minimumShouldMatch("2<75%"),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        // total = 2, so should match all (2)
        assertEquals(SqlKind.AND, call.getKind());
    }

    public void testMinimumShouldMatchCombinationWithMoreClauses() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("name", "a"))
                .should(QueryBuilders.termQuery("name", "b"))
                .should(QueryBuilders.termQuery("name", "c"))
                .should(QueryBuilders.termQuery("name", "d"))
                .minimumShouldMatch("2<75%"),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        // total = 4 > 2, so 75% of 4 = 3 required
        assertEquals(SqlKind.OR, call.getKind());
    }

    // minimum_should_match: Multiple combinations

    public void testMinimumShouldMatchMultipleCombinations() throws ConversionException {
        // 3<-1 5<50% means:
        // if total <= 3: match all
        // if 3 < total <= 5: match all but 1
        // if total > 5: match 50%
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("name", "a"))
                .should(QueryBuilders.termQuery("name", "b"))
                .should(QueryBuilders.termQuery("name", "c"))
                .should(QueryBuilders.termQuery("name", "d"))
                .minimumShouldMatch("3<-1 5<50%"),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        // total = 4, which is 3 < 4 <= 5, so -1 = 4 - 1 = 3 required
        assertEquals(SqlKind.OR, call.getKind());
    }

    public void testMinimumShouldMatchMultipleCombinationsWithSixClauses() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("name", "a"))
                .should(QueryBuilders.termQuery("name", "b"))
                .should(QueryBuilders.termQuery("name", "c"))
                .should(QueryBuilders.termQuery("name", "d"))
                .should(QueryBuilders.termQuery("name", "e"))
                .should(QueryBuilders.termQuery("name", "f"))
                .minimumShouldMatch("3<-1 5<50%"),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        // total = 6 > 5, so 50% of 6 = 3 required
        assertEquals(SqlKind.OR, call.getKind());
    }

    // Edge cases

    public void testShouldWithMustClause() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("name", "required"))
                .should(QueryBuilders.termQuery("name", "a"))
                .should(QueryBuilders.termQuery("name", "b")),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        // Should clauses are optional when must is present (no minimum_should_match)
        assertEquals(SqlKind.EQUALS, call.getKind());
    }

    public void testShouldWithMustAndMinimumShouldMatch() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("name", "required"))
                .should(QueryBuilders.termQuery("name", "a"))
                .should(QueryBuilders.termQuery("name", "b"))
                .minimumShouldMatch("1"),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.AND, call.getKind());
        assertEquals(2, call.getOperands().size());
    }

    public void testComplexBoolQuery() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("name", "active"))
                .should(QueryBuilders.termQuery("brand", "high"))
                .should(QueryBuilders.termQuery("brand", "medium"))
                .mustNot(QueryBuilders.termQuery("name", "deleted"))
                .minimumShouldMatch("1"),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.AND, call.getKind());
        assertEquals(3, call.getOperands().size());
    }

    public void testReportsCorrectQueryType() {
        assertEquals(BoolQueryBuilder.class, translator.getQueryType());
    }

    public void testNestedBoolQueryFlattening() throws ConversionException {
        // Nested bool: bool(must: [term1, bool(must: [term2, term3])])
        // Should flatten to: AND(term1, term2, term3)
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("name", "value1"))
                .must(
                    QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("brand", "value2"))
                        .must(QueryBuilders.termQuery("rating", 3.0))
                ),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.AND, call.getKind());
        // Should be flattened to 3 operands, not nested
        assertEquals(3, call.getOperands().size());
    }

    public void testNestedShouldQueryFlattening() throws ConversionException {
        // Nested should: bool(should: [term1, bool(should: [term2, term3])])
        // Should flatten to: OR(term1, term2, term3)
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("name", "value1"))
                .should(
                    QueryBuilders.boolQuery()
                        .should(QueryBuilders.termQuery("brand", "value2"))
                        .should(QueryBuilders.termQuery("rating", 3.0))
                ),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());
        // Should be flattened to 3 operands, not nested
        assertEquals(3, call.getOperands().size());
    }

    public void testDoubleNegationElimination() throws ConversionException {
        // bool(must_not: [bool(must_not: [term])])
        // Should eliminate double negation: NOT(NOT(term)) -> term
        RexNode result = translator.convert(
            QueryBuilders.boolQuery().mustNot(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("name", "value"))),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        // Should be the term itself, not wrapped in NOT
        assertEquals(SqlKind.EQUALS, call.getKind());
    }
}
