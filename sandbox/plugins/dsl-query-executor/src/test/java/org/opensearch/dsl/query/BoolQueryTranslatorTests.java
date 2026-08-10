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
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.dsl.TestUtils;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

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
        // Pins: must_not emits IS_NOT_TRUE so missing-field rows (NULL) are retained.
        assertEquals(SqlKind.IS_NOT_TRUE, call.getKind());
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
        // C(3,2)=3 subsets in enumerated form
        assertEquals(SqlKind.OR, call.getKind());
        assertEquals(3, call.getOperands().size());
        for (RexNode operand : call.getOperands()) {
            assertTrue(operand instanceof RexCall);
            assertEquals(SqlKind.AND, ((RexCall) operand).getKind());
            assertEquals(2, ((RexCall) operand).getOperands().size());
        }
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
        // -1 on 3 clauses: 3-1=2 required, C(3,2)=3 subsets
        assertEquals(SqlKind.OR, call.getKind());
        assertEquals(3, call.getOperands().size());
        for (RexNode operand : call.getOperands()) {
            assertTrue(operand instanceof RexCall);
            assertEquals(SqlKind.AND, ((RexCall) operand).getKind());
            assertEquals(2, ((RexCall) operand).getOperands().size());
        }
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
        // 70% of 4 = floor(2.8) = 2, C(4,2)=6 subsets
        assertEquals(SqlKind.OR, call.getKind());
        assertEquals(6, call.getOperands().size());
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
        // 50% of 4 = 2, C(4,2)=6 subsets
        assertEquals(SqlKind.OR, call.getKind());
        assertEquals(6, call.getOperands().size());
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
        // -30% of 4: miss floor(1.2)=1, so 4-1=3 required, C(4,3)=4 subsets
        assertEquals(SqlKind.OR, call.getKind());
        assertEquals(4, call.getOperands().size());
        for (RexNode operand : call.getOperands()) {
            assertTrue(operand instanceof RexCall);
            assertEquals(SqlKind.AND, ((RexCall) operand).getKind());
            assertEquals(3, ((RexCall) operand).getOperands().size());
        }
    }

    // minimum_should_match: Single combination

    public void testMinimumShouldMatchCombination2Less75Percent() throws ConversionException {
        // 2<75%: if total <= 2 match all; if total > 2 match 75%
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("name", "a"))
                .should(QueryBuilders.termQuery("name", "b"))
                .minimumShouldMatch("2<75%"),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        // total=2 so all required → AND
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
        // total=4 > 2, 75% of 4 = 3 required, C(4,3)=4 subsets
        assertEquals(SqlKind.OR, call.getKind());
        assertEquals(4, call.getOperands().size());
    }

    // minimum_should_match: Multiple combinations

    public void testMinimumShouldMatchMultipleCombinations() throws ConversionException {
        // "3<-1 5<50%": total=4 in range (3,5] → -1 → 3 required
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
        // 3 required, C(4,3)=4 subsets
        assertEquals(SqlKind.OR, call.getKind());
        assertEquals(4, call.getOperands().size());
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
        // total=6 > 5, 50% of 6 = 3 required, C(6,3)=20 subsets
        assertEquals(SqlKind.OR, call.getKind());
        assertEquals(20, call.getOperands().size());
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
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("name", "value1"))
                .must(
                    QueryBuilders.boolQuery().must(QueryBuilders.termQuery("brand", "value2")).must(QueryBuilders.termQuery("rating", 3.0))
                ),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.AND, call.getKind());
        assertEquals(3, call.getOperands().size());
    }

    public void testNestedShouldQueryFlattening() throws ConversionException {
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
        assertEquals(3, call.getOperands().size());
    }

    public void testDoubleNegationElimination() throws ConversionException {
        // IS_NOT_TRUE(IS_NOT_TRUE(term)) → term
        RexNode result = translator.convert(
            QueryBuilders.boolQuery().mustNot(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("name", "value"))),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
    }

    public void testNestedMustNotDoubleNegationWithMultipleClauses() throws ConversionException {
        // Inner must_not emits IS_NOT_TRUE(term2); outer must_not unwraps → AND(term1, term2).
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("name", "keep"))
                .mustNot(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("brand", "wanted"))),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.AND, call.getKind());
        assertEquals(2, call.getOperands().size());
        assertEquals(SqlKind.EQUALS, ((RexCall) call.getOperands().get(0)).getKind());
        assertEquals(SqlKind.EQUALS, ((RexCall) call.getOperands().get(1)).getKind());
    }

    // --- MSM result clamping tests ---

    public void testCalculateRequiredMatchesClampedToZeroWhenNegativeResult() throws ConversionException {
        // "-10" with 3 clauses → 3+(-10)=-7 → clamp to 0
        int result = MinimumShouldMatchParser.calculateRequiredMatches("-10", 3, false);
        assertEquals("Negative MSM result must be clamped to 0", 0, result);
    }

    public void testCalculateRequiredMatchesClampedToTotalWhenExceedsTotal() throws ConversionException {
        // "5" with 3 clauses → 5 > 3 → raw value returned; convert() handles match-none.
        // Witnessed: BoolQueryBuilderTests.testMinShouldMatchBiggerThanNumberOfShouldClauses.
        int result = MinimumShouldMatchParser.calculateRequiredMatches("5", 3, false);
        assertEquals("MSM exceeding totalShould must be returned as-is (> totalShould signals match-none)", 5, result);
    }

    public void testCalculateRequiredMatchesNotClampedWhenWithinRange() throws ConversionException {
        int result = MinimumShouldMatchParser.calculateRequiredMatches("2", 3, false);
        assertEquals("MSM within valid range should not be clamped", 2, result);
    }

    public void testCalculateRequiredMatchesClampedToTotalProducesAnd() throws ConversionException {
        // MSM > shouldCount → legacy matches nothing → FALSE literal.
        // Witnessed: BoolQueryBuilderTests.testMinShouldMatchBiggerThanNumberOfShouldClauses.
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("name", "a"))
                .should(QueryBuilders.termQuery("name", "b"))
                .should(QueryBuilders.termQuery("name", "c"))
                .minimumShouldMatch("5"),
            ctx
        );

        assertTrue("MSM exceeding should-count must produce FALSE literal (match-none)", result instanceof RexLiteral);
        assertFalse("Must be boolean FALSE", RexLiteral.booleanValue(result));
    }

    public void testCalculateRequiredMatchesClampedToZeroMakesShouldOptional() throws ConversionException {
        // "-10" on 3 should → result=-7 → clamp to 0 → should optional
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("name", "required"))
                .should(QueryBuilders.termQuery("brand", "a"))
                .should(QueryBuilders.termQuery("brand", "b"))
                .should(QueryBuilders.termQuery("brand", "c"))
                .minimumShouldMatch("-10"),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.EQUALS, call.getKind());
    }

    public void testCalculateRequiredMatchesPercentageClampedAboveTotal() throws ConversionException {
        // "200%" with 3 clauses → floor(6) > 3 → raw value returned; convert() handles match-none.
        int result = MinimumShouldMatchParser.calculateRequiredMatches("200%", 3, false);
        assertEquals("Percentage exceeding 100% returns raw computed value (> totalShould signals match-none)", 6, result);
    }

    // --- Leaf-occurrence cap tests ---

    public void testLeafOccurrenceCapExceededThrowsConversionException() {
        // C(20,10)*10 far exceeds MAX_LEAF_OCCURRENCES=1024
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (int i = 0; i < 20; i++) {
            query.should(QueryBuilders.termQuery("name", "val" + i));
        }
        query.minimumShouldMatch("10");

        ConversionException ex = expectThrows(ConversionException.class, () -> translator.convert(query, ctx));
        assertTrue("Message must mention clause count (n)", ex.getMessage().contains("20"));
        assertTrue("Message must mention required count (k)", ex.getMessage().contains("10"));
        assertTrue("Message must mention exceeds limit", ex.getMessage().toLowerCase().contains("exceed"));
    }

    public void testVeryLargeLeafOccurrenceCapExceededThrowsConversionException() {
        // C(15,7)*7 = 45045 > 1024
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (int i = 0; i < 15; i++) {
            query.should(QueryBuilders.termQuery("name", "val" + i));
        }
        query.minimumShouldMatch("7");

        ConversionException ex = expectThrows(ConversionException.class, () -> translator.convert(query, ctx));
        assertTrue("Message must mention clause count (n)", ex.getMessage().contains("15"));
        assertTrue("Message must mention required count (k)", ex.getMessage().contains("7"));
    }

    public void testLeafOccurrenceCapBoundaryJustBelowConvertsSuccessfully() throws ConversionException {
        // C(32,2)*2 = 992 <= 1024
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (int i = 0; i < 32; i++) {
            query.should(QueryBuilders.termQuery("name", "val" + i));
        }
        query.minimumShouldMatch("2");

        RexNode result = translator.convert(query, ctx);
        assertTrue("Must produce OR", result instanceof RexCall);
        RexCall or = (RexCall) result;
        assertEquals(SqlKind.OR, or.getKind());
        assertEquals(496, or.getOperands().size());
    }

    public void testLeafOccurrenceCapBoundaryJustAboveThrows() {
        // C(33,2)*2 = 1056 > 1024
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (int i = 0; i < 33; i++) {
            query.should(QueryBuilders.termQuery("name", "val" + i));
        }
        query.minimumShouldMatch("2");

        ConversionException ex = expectThrows(ConversionException.class, () -> translator.convert(query, ctx));
        assertTrue("Message must mention clause count (n)", ex.getMessage().contains("33"));
        assertTrue("Message must mention required count (k)", ex.getMessage().contains("2"));
    }

    public void testLeafOccurrenceCapRejectsModerateCombinationsWithHighK() {
        // C(10,5)=252, product=252*5=1260 > 1024.
        // The incremental algorithm may bail early before computing full C(n,k).
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (int i = 0; i < 10; i++) {
            query.should(QueryBuilders.termQuery("name", "val" + i));
        }
        query.minimumShouldMatch("5");

        ConversionException ex = expectThrows(ConversionException.class, () -> translator.convert(query, ctx));
        assertTrue("Message must mention n=10", ex.getMessage().contains("10"));
        assertTrue("Message must mention k=5", ex.getMessage().contains("5"));
        assertTrue("Message must mention exceeds limit", ex.getMessage().toLowerCase().contains("exceed"));
    }

    public void testLeafOccurrenceCapAcceptsModerateCombinationsWithLowK() throws ConversionException {
        // C(10,4)*4 = 840 <= 1024
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (int i = 0; i < 10; i++) {
            query.should(QueryBuilders.termQuery("name", "val" + i));
        }
        query.minimumShouldMatch("4");

        RexNode result = translator.convert(query, ctx);
        assertTrue("Must produce OR", result instanceof RexCall);
        RexCall or = (RexCall) result;
        assertEquals(SqlKind.OR, or.getKind());
        assertEquals(210, or.getOperands().size());
    }

    public void testEnumeratedFormTruthTableEquivalence() throws ConversionException {
        // For n=4, verify every truth assignment for k in [2,3]:
        // expression is true iff at least k predicates are true.
        for (int required = 2; required <= 3; required++) {
            BoolQueryBuilder query = QueryBuilders.boolQuery();
            for (int i = 0; i < 4; i++) {
                query.should(QueryBuilders.termQuery("name", "val" + i));
            }
            query.minimumShouldMatch(String.valueOf(required));

            RexNode result = translator.convert(query, ctx);
            assertTrue("Must produce OR for k=" + required, result instanceof RexCall);
            RexCall or = (RexCall) result;
            assertEquals(SqlKind.OR, or.getKind());

            for (int mask = 0; mask < 16; mask++) {
                int trueCount = Integer.bitCount(mask);
                boolean expected = trueCount >= required;
                boolean actual = evaluateEnumeratedForm(or, mask, 4);
                assertEquals("k=" + required + " mask=" + Integer.toBinaryString(mask) + " trueCount=" + trueCount, expected, actual);
            }
        }
    }

    public void testEnumeratedFormNullConditionContributesZero() throws ConversionException {
        // 3 clauses, required=2 → OR of C(3,2)=3 ANDs, each AND has 2 EQUALS children.
        BoolQueryBuilder query = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("name", "a"))
            .should(QueryBuilders.termQuery("name", "b"))
            .should(QueryBuilders.termQuery("name", "c"))
            .minimumShouldMatch("2");

        RexNode result = translator.convert(query, ctx);
        assertTrue(result instanceof RexCall);
        RexCall or = (RexCall) result;
        assertEquals(SqlKind.OR, or.getKind());
        assertEquals(3, or.getOperands().size());

        for (RexNode operand : or.getOperands()) {
            assertTrue(operand instanceof RexCall);
            RexCall andCall = (RexCall) operand;
            assertEquals(SqlKind.AND, andCall.getKind());
            assertEquals(2, andCall.getOperands().size());
            for (RexNode child : andCall.getOperands()) {
                assertTrue(child instanceof RexCall);
                assertEquals(SqlKind.EQUALS, ((RexCall) child).getKind());
            }
        }
    }

    // --- MSM exceeding should-count: legacy matches nothing ---
    // Witnessed: BoolQueryBuilderTests.testMinShouldMatchBiggerThanNumberOfShouldClauses.

    public void testMsmExceedingShouldCountProducesMatchNone() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("name", "a"))
                .should(QueryBuilders.termQuery("name", "b"))
                .should(QueryBuilders.termQuery("name", "c"))
                .minimumShouldMatch("5"),
            ctx
        );

        assertTrue("MSM exceeding should-count must produce FALSE literal", result instanceof RexLiteral);
        assertFalse("Must be boolean FALSE (match-none)", RexLiteral.booleanValue(result));
    }

    public void testMsmExceedingShouldCountWithMustProducesMatchNone() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("name", "required"))
                .should(QueryBuilders.termQuery("brand", "a"))
                .should(QueryBuilders.termQuery("brand", "b"))
                .minimumShouldMatch("5"),
            ctx
        );

        assertTrue("MSM exceeding should-count must produce FALSE literal even with must", result instanceof RexLiteral);
        assertFalse("Must be boolean FALSE (match-none)", RexLiteral.booleanValue(result));
    }

    // --- Parameter audit: boost, _name, adjustPureNegative ---

    public void testNonDefaultBoostThrowsConversionException() {
        // Citation: AbstractQueryBuilder.toQuery wraps result in BoostQuery when boost != 1.0f.
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("name", "test")).boost(2.0f);

        ConversionException ex = expectThrows(ConversionException.class, () -> translator.convert(query, ctx));
        assertTrue("Message must mention boost", ex.getMessage().contains("boost"));
    }

    public void testQueryNameThrowsConversionException() {
        // Citation: AbstractQueryBuilder.toQuery registers named query for matched_queries response.
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("name", "test")).queryName("my_bool");

        ConversionException ex = expectThrows(ConversionException.class, () -> translator.convert(query, ctx));
        assertTrue("Message must mention _name", ex.getMessage().contains("_name"));
    }

    public void testAdjustPureNegativeFalsePureNegativeReturnsFalseLiteral() throws ConversionException {
        // Citation: BoolQueryBuilder.doToQuery:338, Queries.isNegativeQuery:113-119.
        RexNode result = translator.convert(
            QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("name", "test")).adjustPureNegative(false),
            ctx
        );

        assertTrue("Pure-negative with adjustPureNegative=false must produce FALSE literal", result instanceof RexLiteral);
        assertFalse("Must be boolean FALSE (match-none)", RexLiteral.booleanValue(result));
    }

    public void testAdjustPureNegativeFalseMustPlusMustNotAccepted() throws ConversionException {
        // adjustPureNegative=false is a no-op when must clauses exist.
        // Citation: Queries.isNegativeQuery:113-119.
        BoolQueryBuilder queryWithFlag = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("name", "keep"))
            .mustNot(QueryBuilders.termQuery("brand", "excluded"))
            .adjustPureNegative(false);
        BoolQueryBuilder queryDefault = QueryBuilders.boolQuery()
            .must(QueryBuilders.termQuery("name", "keep"))
            .mustNot(QueryBuilders.termQuery("brand", "excluded"));

        RexNode resultWithFlag = translator.convert(queryWithFlag, ctx);
        RexNode resultDefault = translator.convert(queryDefault, ctx);

        assertEquals(
            "must+must_not with adjustPureNegative=false must equal default-true result",
            resultDefault.toString(),
            resultWithFlag.toString()
        );
    }

    public void testAdjustPureNegativeFalseShouldOnlyAccepted() throws ConversionException {
        // should-only is not pure-negative, so adjustPureNegative=false is a no-op.
        BoolQueryBuilder queryWithFlag = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("name", "a"))
            .should(QueryBuilders.termQuery("name", "b"))
            .adjustPureNegative(false);
        BoolQueryBuilder queryDefault = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("name", "a"))
            .should(QueryBuilders.termQuery("name", "b"));

        RexNode resultWithFlag = translator.convert(queryWithFlag, ctx);
        RexNode resultDefault = translator.convert(queryDefault, ctx);

        assertEquals(
            "should-only with adjustPureNegative=false must equal default-true result",
            resultDefault.toString(),
            resultWithFlag.toString()
        );
    }

    public void testAdjustPureNegativeFalseFilterPlusMustNotAccepted() throws ConversionException {
        // filter+must_not is not pure-negative, so adjustPureNegative=false is a no-op.
        BoolQueryBuilder queryWithFlag = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("name", "active"))
            .mustNot(QueryBuilders.termQuery("brand", "excluded"))
            .adjustPureNegative(false);
        BoolQueryBuilder queryDefault = QueryBuilders.boolQuery()
            .filter(QueryBuilders.termQuery("name", "active"))
            .mustNot(QueryBuilders.termQuery("brand", "excluded"));

        RexNode resultWithFlag = translator.convert(queryWithFlag, ctx);
        RexNode resultDefault = translator.convert(queryDefault, ctx);

        assertEquals(
            "filter+must_not with adjustPureNegative=false must equal default-true result",
            resultDefault.toString(),
            resultWithFlag.toString()
        );
    }

    public void testAdjustPureNegativeDefaultTruePureNegativeProducesNot() throws ConversionException {
        // adjustPureNegative=true (default) → table scan is the implicit match-all → IS_NOT_TRUE.
        RexNode result = translator.convert(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("name", "test")), ctx);

        assertTrue("Pure-negative with default adjustPureNegative must produce IS_NOT_TRUE", result instanceof RexCall);
        assertEquals(SqlKind.IS_NOT_TRUE, ((RexCall) result).getKind());
    }

    public void testDefaultBoostAccepted() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("name", "test")).boost(1.0f), ctx);
        assertNotNull(result);
    }

    // --- Empty bool and pure-negative ---

    public void testEmptyBoolProducesTrueLiteral() throws ConversionException {
        // Citation: BoolQueryBuilder.doRewrite line ~279 and doToQuery line 333.
        RexNode result = translator.convert(QueryBuilders.boolQuery(), ctx);

        assertTrue("Empty bool must produce TRUE literal (match-all)", result instanceof RexLiteral);
        assertTrue("Must be boolean TRUE", RexLiteral.booleanValue(result));
    }

    public void testPureNegativeBoolProducesNot() throws ConversionException {
        // adjustPureNegative=true (default) → IS_NOT_TRUE conditions.
        // Citation: Queries.fixNegativeQueryIfNeeded lines 111-121.
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .mustNot(QueryBuilders.termQuery("name", "excluded1"))
                .mustNot(QueryBuilders.termQuery("brand", "excluded2")),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.AND, call.getKind());
        assertEquals(2, call.getOperands().size());
        for (RexNode operand : call.getOperands()) {
            assertTrue(operand instanceof RexCall);
            assertEquals(SqlKind.IS_NOT_TRUE, ((RexCall) operand).getKind());
        }
    }

    // --- Nested bool recursion with isFlat assertion ---

    public void testNestedBoolInShouldInMustIsFlat() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .must(
                    QueryBuilders.boolQuery()
                        .should(
                            QueryBuilders.boolQuery().must(QueryBuilders.termQuery("name", "a")).must(QueryBuilders.termQuery("brand", "b"))
                        )
                        .should(QueryBuilders.termQuery("rating", 3.0))
                ),
            ctx
        );

        assertNotNull(result);
        assertTrue("Nested bool composition must produce flat RexNode", RexUtil.isFlat(result));
    }

    // --- Parser hardening: NumberFormatException wrapping ---

    public void testParseIntegerInvalidThrowsConversionExceptionWithValue() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> MinimumShouldMatchParser.calculateRequiredMatches("abc", 3, false)
        );
        assertTrue("Message must contain the offending value", ex.getMessage().contains("abc"));
    }

    public void testParsePercentageInvalidThrowsConversionExceptionWithValue() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> MinimumShouldMatchParser.calculateRequiredMatches("xyz%", 3, false)
        );
        assertTrue("Message must contain the offending value", ex.getMessage().contains("xyz"));
    }

    public void testParseCombinationInvalidThresholdThrowsConversionException() {
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> MinimumShouldMatchParser.calculateRequiredMatches("abc<75%", 3, false)
        );
        assertTrue("Message must contain the offending value", ex.getMessage().contains("abc"));
    }

    public void testParseCombinationTrailingLessThanThrowsConversionException() {
        // "5<" is malformed — legacy throws ArrayIndexOutOfBoundsException.
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> MinimumShouldMatchParser.calculateRequiredMatches("5<", 6, false)
        );
        assertTrue("Message must mention the spec", ex.getMessage().contains("5<"));
    }

    // --- Should-gate local branch tests ---

    public void testShouldGateExactEqualProducesAnd() throws ConversionException {
        // MSM="3" with 3 should → requiredMatches==size → AND
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("name", "a"))
                .should(QueryBuilders.termQuery("name", "b"))
                .should(QueryBuilders.termQuery("name", "c"))
                .minimumShouldMatch("3"),
            ctx
        );

        assertTrue("Exact-equal gate must produce AND", result instanceof RexCall);
        assertEquals(SqlKind.AND, ((RexCall) result).getKind());
        assertEquals(3, ((RexCall) result).getOperands().size());
    }

    public void testShouldGateOverLargeProducesFalse() throws ConversionException {
        // MSM > shouldConditions.size() → unsatisfiable → FALSE
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("name", "a"))
                .should(QueryBuilders.termQuery("name", "b"))
                .minimumShouldMatch("5"),
            ctx
        );

        assertTrue("Over-large MSM must produce FALSE literal", result instanceof RexLiteral);
        assertFalse("Must be boolean FALSE (match-none)", RexLiteral.booleanValue(result));
    }

    // --- Data-type variety: bool composition preserves child literal typing ---

    public void testBoolMustWithNumericChildPreservesIntegerLiteral() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("price", 42)), ctx);

        assertTrue(result instanceof RexCall);
        RexCall eq = (RexCall) result;
        assertEquals(SqlKind.EQUALS, eq.getKind());
        assertEquals(1, ((RexInputRef) eq.getOperands().get(0)).getIndex());
        RexCall cast = (RexCall) eq.getOperands().get(1);
        RexLiteral literal = (RexLiteral) cast.getOperands().get(0);
        assertEquals(SqlTypeName.INTEGER, literal.getType().getSqlTypeName());
        assertEquals(Integer.valueOf(42), literal.getValueAs(Integer.class));
    }

    public void testBoolMustWithBooleanChildPreservesBooleanLiteral() throws ConversionException {
        RexNode result = translator.convert(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("is_active", true)), ctx);

        assertTrue(result instanceof RexCall);
        RexCall eq = (RexCall) result;
        assertEquals(SqlKind.EQUALS, eq.getKind());
        assertEquals(5, ((RexInputRef) eq.getOperands().get(0)).getIndex());
        RexCall cast = (RexCall) eq.getOperands().get(1);
        RexLiteral literal = (RexLiteral) cast.getOperands().get(0);
        assertEquals(SqlTypeName.BOOLEAN, literal.getType().getSqlTypeName());
        assertTrue("Boolean literal must be TRUE", literal.getValueAs(Boolean.class));
    }

    public void testBoolMustWithDateChildPropagatesRejection() {
        // Bool composition propagates a child translator's rejection rather than swallowing it.
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("created_date", 19738));

        ConversionException ex = expectThrows(ConversionException.class, () -> translator.convert(query, ctx));
        assertTrue("Message must identify date fields", ex.getMessage().toLowerCase().contains("date field"));
        assertTrue("Message must name the offending field", ex.getMessage().contains("created_date"));
    }

    public void testBoolMustMixedTypesPreservesAllLiteralsInOrder() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("name", "laptop"))
                .must(QueryBuilders.termQuery("price", 999))
                .must(QueryBuilders.termQuery("is_active", false)),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall and = (RexCall) result;
        assertEquals(SqlKind.AND, and.getKind());
        assertEquals(3, and.getOperands().size());

        RexCall eq0 = (RexCall) and.getOperands().get(0);
        assertEquals(SqlKind.EQUALS, eq0.getKind());
        assertEquals(0, ((RexInputRef) eq0.getOperands().get(0)).getIndex());
        RexLiteral lit0 = (RexLiteral) ((RexCall) eq0.getOperands().get(1)).getOperands().get(0);
        assertEquals("laptop", lit0.getValueAs(String.class));

        RexCall eq1 = (RexCall) and.getOperands().get(1);
        assertEquals(SqlKind.EQUALS, eq1.getKind());
        assertEquals(1, ((RexInputRef) eq1.getOperands().get(0)).getIndex());
        RexLiteral lit1 = (RexLiteral) ((RexCall) eq1.getOperands().get(1)).getOperands().get(0);
        assertEquals(Integer.valueOf(999), lit1.getValueAs(Integer.class));

        RexCall eq2 = (RexCall) and.getOperands().get(2);
        assertEquals(SqlKind.EQUALS, eq2.getKind());
        assertEquals(5, ((RexInputRef) eq2.getOperands().get(0)).getIndex());
        RexLiteral lit2 = (RexLiteral) ((RexCall) eq2.getOperands().get(1)).getOperands().get(0);
        assertFalse("Boolean literal must be FALSE", lit2.getValueAs(Boolean.class));
    }

    public void testBoolMinimumShouldMatchMixedTypesEnumeratedFormPreservesTypes() throws ConversionException {
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("name", "tablet"))
                .should(QueryBuilders.termQuery("price", 500))
                .should(QueryBuilders.termQuery("is_active", true))
                .minimumShouldMatch("2"),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall or = (RexCall) result;
        assertEquals(SqlKind.OR, or.getKind());
        assertEquals("Enumerated form must have C(3,2)=3 subsets", 3, or.getOperands().size());

        for (RexNode operand : or.getOperands()) {
            assertTrue(operand instanceof RexCall);
            RexCall andCall = (RexCall) operand;
            assertEquals(SqlKind.AND, andCall.getKind());
            assertEquals(2, andCall.getOperands().size());
        }

        // First subset: AND(name='tablet', price=500)
        RexCall and0 = (RexCall) or.getOperands().get(0);
        RexCall subEq0 = (RexCall) and0.getOperands().get(0);
        assertEquals(SqlKind.EQUALS, subEq0.getKind());
        assertEquals(0, ((RexInputRef) subEq0.getOperands().get(0)).getIndex());
        RexLiteral subLit0 = (RexLiteral) ((RexCall) subEq0.getOperands().get(1)).getOperands().get(0);
        assertEquals("tablet", subLit0.getValueAs(String.class));

        RexCall subEq1 = (RexCall) and0.getOperands().get(1);
        assertEquals(SqlKind.EQUALS, subEq1.getKind());
        assertEquals(1, ((RexInputRef) subEq1.getOperands().get(0)).getIndex());
        RexLiteral subLit1 = (RexLiteral) ((RexCall) subEq1.getOperands().get(1)).getOperands().get(0);
        assertEquals(Integer.valueOf(500), subLit1.getValueAs(Integer.class));

        // Last subset: AND(price=500, is_active=true)
        RexCall and2 = (RexCall) or.getOperands().get(2);
        RexCall subEq2 = (RexCall) and2.getOperands().get(0);
        assertEquals(SqlKind.EQUALS, subEq2.getKind());
        assertEquals(1, ((RexInputRef) subEq2.getOperands().get(0)).getIndex());

        RexCall subEq3 = (RexCall) and2.getOperands().get(1);
        assertEquals(SqlKind.EQUALS, subEq3.getKind());
        assertEquals(5, ((RexInputRef) subEq3.getOperands().get(0)).getIndex());
        RexLiteral subLit3 = (RexLiteral) ((RexCall) subEq3.getOperands().get(1)).getOperands().get(0);
        assertTrue("Boolean literal must be TRUE", subLit3.getValueAs(Boolean.class));
    }

    // --- Enumerated form flattening: nested bool children must not produce nested AND ---

    public void testEnumeratedFormFlattensNestedBoolChildren() throws ConversionException {
        // A should-child that is itself a bool(must:[term,term]) converts to an AND node.
        // In a k-subset AND, the result must be flat — no nested AND.
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("name", "a")).must(QueryBuilders.termQuery("brand", "b")))
                .should(QueryBuilders.termQuery("name", "c"))
                .should(QueryBuilders.termQuery("name", "d"))
                .minimumShouldMatch("2"),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall or = (RexCall) result;
        assertEquals(SqlKind.OR, or.getKind());
        assertEquals(3, or.getOperands().size());

        for (RexNode subset : or.getOperands()) {
            assertTrue("Each subset must be an AND or a leaf", subset instanceof RexCall);
            RexCall subsetCall = (RexCall) subset;
            if (subsetCall.getKind() == SqlKind.AND) {
                for (RexNode operand : subsetCall.getOperands()) {
                    if (operand instanceof RexCall) {
                        assertNotEquals(
                            "AND operand must not be a nested AND (flat invariant violated)",
                            SqlKind.AND,
                            ((RexCall) operand).getKind()
                        );
                    }
                }
            }
        }
        assertTrue("Enumerated form with nested bool children must be flat", RexUtil.isFlat(result));
    }

    // --- minimum_should_match deduplication ---

    public void testMinimumShouldMatchWithDuplicateChildrenReflectsDeduplication() throws ConversionException {
        // [A, A, B] with required=2: C(3,2)=3 subsets → composeDisjunction deduplicates.
        // Correct: logical truth table is unchanged when duplicates are present.
        RexNode result = translator.convert(
            QueryBuilders.boolQuery()
                .should(QueryBuilders.termQuery("name", "dup"))
                .should(QueryBuilders.termQuery("name", "dup"))
                .should(QueryBuilders.termQuery("name", "other"))
                .minimumShouldMatch("2"),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.OR, call.getKind());
        assertTrue("Deduplication must reduce operand count below C(3,2)=3", call.getOperands().size() < 3);
    }

    // --- must_not contract tests: IS_NOT_TRUE emission regardless of nullability ---

    public void testMustNotEmitsIsNotTrueForNonNullableBooleanChild() throws ConversionException {
        // Pins contract with DelegatedRelevanceCallHelper: non-nullable BOOLEAN child under
        // must_not must be wrapped in IS_NOT_TRUE. Calcite may later rewrite to NOT(x), but
        // the translator must emit IS_NOT_TRUE unconditionally.
        var rexBuilder = ctx.getRexBuilder();
        var typeFactory = rexBuilder.getTypeFactory();

        var nonNullableBoolType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        assertFalse("Precondition: type must be non-nullable", nonNullableBoolType.isNullable());
        RexNode syntheticChild = rexBuilder.makeCall(
            nonNullableBoolType,
            SqlStdOperatorTable.IS_NOT_NULL,
            List.of(rexBuilder.makeInputRef(typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.VARCHAR), true), 0))
        );

        QueryRegistry stubRegistry = new QueryRegistry();
        stubRegistry.register(new QueryTranslator() {
            @Override
            public Class<? extends QueryBuilder> getQueryType() {
                return org.opensearch.index.query.MatchAllQueryBuilder.class;
            }

            @Override
            public RexNode convert(QueryBuilder query, ConversionContext ctx) {
                return syntheticChild;
            }
        });
        BoolQueryTranslator stubTranslator = new BoolQueryTranslator(stubRegistry);

        BoolQueryBuilder query = QueryBuilders.boolQuery().mustNot(QueryBuilders.matchAllQuery());
        RexNode result = stubTranslator.convert(query, ctx);

        assertTrue("must_not must emit IS_NOT_TRUE", result instanceof RexCall);
        assertEquals(SqlKind.IS_NOT_TRUE, ((RexCall) result).getKind());

        RexNode inner = ((RexCall) result).getOperands().get(0);
        assertSame("Wrapped operand must be the injected non-nullable node", syntheticChild, inner);
        assertFalse("Operand type must be non-nullable BOOLEAN", inner.getType().isNullable());
        assertEquals(SqlTypeName.BOOLEAN, inner.getType().getSqlTypeName());
    }

    public void testMustNotEmitsIsNotTrueForNullableChild() throws ConversionException {
        // Pins: must_not wraps nullable child in IS_NOT_TRUE (not NOT) to preserve
        // Lucene's "include missing-field rows" semantics under SQL three-valued logic.
        RexNode result = translator.convert(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("brand", "excluded")), ctx);

        assertTrue("must_not on nullable child must emit IS_NOT_TRUE", result instanceof RexCall);
        RexCall call = (RexCall) result;
        assertEquals(SqlKind.IS_NOT_TRUE, call.getKind());
        RexNode inner = call.getOperands().get(0);
        assertTrue(inner instanceof RexCall);
        assertEquals(SqlKind.EQUALS, ((RexCall) inner).getKind());
        RexInputRef fieldRef = (RexInputRef) ((RexCall) inner).getOperands().get(0);
        assertEquals(2, fieldRef.getIndex());
        assertTrue("brand field must be nullable in test schema", fieldRef.getType().isNullable());
    }

    // --- Helper methods ---

    /**
     * Evaluates the enumerated form OR(AND(...), ...) against a truth mask.
     * Bit i of mask indicates predicate i is true.
     */
    private boolean evaluateEnumeratedForm(RexCall or, int mask, int n) {
        for (RexNode operand : or.getOperands()) {
            RexCall andCall = (RexCall) operand;
            boolean allTrue = true;
            for (RexNode child : andCall.getOperands()) {
                int idx = getPredicateIndex(child, n);
                if ((mask & (1 << idx)) == 0) {
                    allTrue = false;
                    break;
                }
            }
            if (allTrue) {
                return true;
            }
        }
        return false;
    }

    /** Extracts predicate index from literal value suffix "valN". */
    private int getPredicateIndex(RexNode node, int n) {
        RexCall eq = (RexCall) node;
        RexCall cast = (RexCall) eq.getOperands().get(1);
        RexLiteral literal = (RexLiteral) cast.getOperands().get(0);
        String val = literal.getValueAs(String.class);
        return Integer.parseInt(val.substring(3));
    }
}
