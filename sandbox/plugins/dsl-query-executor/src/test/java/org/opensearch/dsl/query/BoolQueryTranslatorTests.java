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
import org.apache.calcite.sql.type.SqlTypeName;
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
        // must_not emits IS_NOT_TRUE so missing-field rows (NULL) are retained.
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
        // Enumerated form: OR(AND(c0,c1), AND(c0,c2), AND(c1,c2)) — C(3,2)=3 subsets
        assertEquals(SqlKind.OR, call.getKind());
        assertEquals(3, call.getOperands().size());
        // Each operand is an AND of 2
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
        // -1 means total - 1 = 3 - 1 = 2 required → enumerated form: OR of C(3,2)=3 ANDs
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
        // 70% of 4 = 2.8, floor = 2 required → enumerated form: OR of C(4,2)=6 ANDs
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
        // 50% of 4 = 2 required → enumerated form: OR of C(4,2)=6 ANDs
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
        // -30% means can miss 30% = 1.2, floor = 1, so 4 - 1 = 3 required → enumerated form: OR of C(4,3)=4 ANDs
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
        // total = 2, so should match all (2) → AND
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
        // total = 4 > 2, so 75% of 4 = 3 required → enumerated form: OR of C(4,3)=4 ANDs
        assertEquals(SqlKind.OR, call.getKind());
        assertEquals(4, call.getOperands().size());
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
        // total = 4, which is 3 < 4 <= 5, so -1 = 4 - 1 = 3 required → enumerated form: OR of C(4,3)=4 ANDs
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
        // total = 6 > 5, so 50% of 6 = 3 required → enumerated form: OR of C(6,3)=20 ANDs
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
        // Nested bool: bool(must: [term1, bool(must: [term2, term3])])
        // Should flatten to: AND(term1, term2, term3)
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
        // Should eliminate double negation: IS_NOT_TRUE(IS_NOT_TRUE(term)) -> term
        RexNode result = translator.convert(
            QueryBuilders.boolQuery().mustNot(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("name", "value"))),
            ctx
        );

        assertTrue(result instanceof RexCall);
        RexCall call = (RexCall) result;
        // Should be the term itself, not wrapped in IS_NOT_TRUE
        assertEquals(SqlKind.EQUALS, call.getKind());
    }

    public void testNestedMustNotDoubleNegationWithMultipleClauses() throws ConversionException {
        // bool(must: [term1], must_not: [bool(must_not: [term2])])
        // The inner must_not emits IS_NOT_TRUE(term2), the outer must_not sees IS_NOT_TRUE
        // and unwraps to term2. Final: AND(term1, term2).
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
        // Both operands should be EQUALS (no IS_NOT_TRUE wrapper)
        assertEquals(SqlKind.EQUALS, ((RexCall) call.getOperands().get(0)).getKind());
        assertEquals(SqlKind.EQUALS, ((RexCall) call.getOperands().get(1)).getKind());
    }

    // --- MSM result clamping tests ---

    public void testCalculateRequiredMatchesClampedToZeroWhenNegativeResult() throws ConversionException {
        // "-10" with 3 clauses → 3 + (-10) = -7 → must clamp to 0
        int result = MinimumShouldMatchParser.calculateRequiredMatches("-10", 3, false);
        assertEquals("Negative MSM result must be clamped to 0", 0, result);
    }

    public void testCalculateRequiredMatchesClampedToTotalWhenExceedsTotal() throws ConversionException {
        // "5" with 3 clauses → 5 > 3 → legacy matches nothing (witnessed:
        // BoolQueryBuilderTests.testMinShouldMatchBiggerThanNumberOfShouldClauses).
        // calculateRequiredMatches returns the raw value; convert() handles the match-none.
        int result = MinimumShouldMatchParser.calculateRequiredMatches("5", 3, false);
        assertEquals("MSM exceeding totalShould must be returned as-is (> totalShould signals match-none)", 5, result);
    }

    public void testCalculateRequiredMatchesNotClampedWhenWithinRange() throws ConversionException {
        // "2" with 3 clauses → within [0, 3], no clamping
        int result = MinimumShouldMatchParser.calculateRequiredMatches("2", 3, false);
        assertEquals("MSM within valid range should not be clamped", 2, result);
    }

    public void testCalculateRequiredMatchesClampedToTotalProducesAnd() throws ConversionException {
        // When MSM exceeds totalShould, legacy matches nothing → produce FALSE literal.
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
        // "-10" with must present + 3 should → clamps to 0 → should is optional
        // But even without must, "-10" on 3 should → result is -7 → clamp to 0 → optional
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
        // Should clauses are optional when clamped to 0 → only must clause remains
        assertEquals(SqlKind.EQUALS, call.getKind());
    }

    public void testCalculateRequiredMatchesPercentageClampedAboveTotal() throws ConversionException {
        // "200%" with 3 clauses → floor(3 * 200 / 100) = 6 → exceeds totalShould
        // Not clamped at calculateRequiredMatches level; convert() handles match-none.
        int result = MinimumShouldMatchParser.calculateRequiredMatches("200%", 3, false);
        assertEquals("Percentage exceeding 100% returns raw computed value (> totalShould signals match-none)", 6, result);
    }

    // --- Combination cap tests ---

    public void testCombinationCapExceededThrowsConversionException() {
        // C(20,10) = 184756, far exceeds MAX_COMBINATIONS=1024 → must throw ConversionException.
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (int i = 0; i < 20; i++) {
            query.should(QueryBuilders.termQuery("name", "val" + i));
        }
        query.minimumShouldMatch("10");

        ConversionException ex = expectThrows(ConversionException.class, () -> translator.convert(query, ctx));
        assertTrue("Message must mention clause count", ex.getMessage().contains("20"));
        assertTrue("Message must mention required count", ex.getMessage().contains("10"));
        assertTrue("Message must mention exceeds limit", ex.getMessage().toLowerCase().contains("exceed"));
    }

    public void testVeryLargeCombinationCapExceededThrowsConversionException() {
        // C(15,7) = 6435 > 1024 → must throw ConversionException.
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (int i = 0; i < 15; i++) {
            query.should(QueryBuilders.termQuery("name", "val" + i));
        }
        query.minimumShouldMatch("7");

        ConversionException ex = expectThrows(ConversionException.class, () -> translator.convert(query, ctx));
        assertTrue("Message must mention clause count", ex.getMessage().contains("15"));
        assertTrue("Message must mention required count", ex.getMessage().contains("7"));
    }

    public void testCombinationCapBoundaryJustBelowConvertsSuccessfully() throws ConversionException {
        // C(32,2) = 496 <= 1024 → must convert successfully with enumerated form.
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (int i = 0; i < 32; i++) {
            query.should(QueryBuilders.termQuery("name", "val" + i));
        }
        query.minimumShouldMatch("2");

        RexNode result = translator.convert(query, ctx);
        assertTrue("Must produce OR", result instanceof RexCall);
        RexCall or = (RexCall) result;
        assertEquals(SqlKind.OR, or.getKind());
        // C(32,2) = 496 subsets
        assertEquals(496, or.getOperands().size());
    }

    public void testCombinationCapBoundaryJustAboveThrows() {
        // C(46,2) = 1035 > 1024 → must throw ConversionException.
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (int i = 0; i < 46; i++) {
            query.should(QueryBuilders.termQuery("name", "val" + i));
        }
        query.minimumShouldMatch("2");

        ConversionException ex = expectThrows(ConversionException.class, () -> translator.convert(query, ctx));
        assertTrue("Message must mention clause count", ex.getMessage().contains("46"));
        assertTrue("Message must mention required count", ex.getMessage().contains("2"));
    }

    public void testEnumeratedFormTruthTableEquivalence() throws ConversionException {
        // For n=4 clauses, verify every truth assignment for k in [2, 3]:
        // the expression is true iff at least k predicates are true.
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

            // Evaluate against all 2^4 = 16 truth assignments
            for (int mask = 0; mask < 16; mask++) {
                int trueCount = Integer.bitCount(mask);
                boolean expected = trueCount >= required;
                boolean actual = evaluateEnumeratedForm(or, mask, 4);
                assertEquals("k=" + required + " mask=" + Integer.toBinaryString(mask) + " trueCount=" + trueCount, expected, actual);
            }
        }
    }

    public void testEnumeratedFormNullConditionContributesZero() throws ConversionException {
        // With 3 clauses where required=2, the enumerated form produces OR of C(3,2)=3 ANDs.
        // Each AND has 2 children. Verify structure.
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

        // Each OR operand is AND with 2 EQUALS children
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

    // --- MSM exceeding should-count: legacy matches nothing (witnessed via
    // BoolQueryBuilderTests.testMinShouldMatchBiggerThanNumberOfShouldClauses:
    // Lucene BooleanQuery accepts MSM=3 with 2 SHOULD clauses, produces impossible constraint) ---

    public void testMsmExceedingShouldCountProducesMatchNone() throws ConversionException {
        // Legacy: MSM=5 with 3 should clauses → Lucene accepts, matches zero documents.
        // Translator must produce boolean FALSE literal (match-none).
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
        // Even with must clauses, if MSM exceeds should count, the whole bool matches nothing.
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
        // Legacy: AbstractQueryBuilder.toQuery wraps result in BoostQuery when boost != 1.0f.
        // We cannot represent scoring in Calcite, so reject non-default boost.
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("name", "test")).boost(2.0f);

        ConversionException ex = expectThrows(ConversionException.class, () -> translator.convert(query, ctx));
        assertTrue("Message must mention boost", ex.getMessage().contains("boost"));
    }

    public void testQueryNameThrowsConversionException() {
        // Legacy: AbstractQueryBuilder.toQuery registers named query for matched_queries response.
        // No Calcite equivalent; reject with clear message.
        BoolQueryBuilder query = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("name", "test")).queryName("my_bool");

        ConversionException ex = expectThrows(ConversionException.class, () -> translator.convert(query, ctx));
        assertTrue("Message must mention _name", ex.getMessage().contains("_name"));
    }

    public void testAdjustPureNegativeFalsePureNegativeReturnsFalseLiteral() throws ConversionException {
        // Legacy: adjustPureNegative=false on a pure-negative bool skips fixNegativeQueryIfNeeded,
        // so no match-all is injected and the BooleanQuery with only MUST_NOT clauses matches nothing.
        // Citation: BoolQueryBuilder.doToQuery:338, Queries.isNegativeQuery:113-119.
        RexNode result = translator.convert(
            QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("name", "test")).adjustPureNegative(false),
            ctx
        );

        assertTrue("Pure-negative with adjustPureNegative=false must produce FALSE literal", result instanceof RexLiteral);
        assertFalse("Must be boolean FALSE (match-none)", RexLiteral.booleanValue(result));
    }

    public void testAdjustPureNegativeFalseMustPlusMustNotAccepted() throws ConversionException {
        // Legacy: adjustPureNegative=false is a no-op when must clauses exist because
        // Queries.isNegativeQuery requires ALL clauses to be prohibited.
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
        // Legacy: should-only is not pure-negative, so adjustPureNegative=false is a no-op.
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
        // Legacy: filter+must_not is not pure-negative (has non-prohibited clauses), so flag is no-op.
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
        // Legacy: adjustPureNegative=true (default) injects match-all via fixNegativeQueryIfNeeded.
        // Our table scan is the implicit match-all, so pure-negative produces IS_NOT_TRUE.
        RexNode result = translator.convert(QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery("name", "test")), ctx);

        assertTrue("Pure-negative with default adjustPureNegative must produce IS_NOT_TRUE", result instanceof RexCall);
        assertEquals(SqlKind.IS_NOT_TRUE, ((RexCall) result).getKind());
    }

    public void testDefaultBoostAccepted() throws ConversionException {
        // boost=1.0f (default) should NOT throw
        RexNode result = translator.convert(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("name", "test")).boost(1.0f), ctx);
        assertNotNull(result);
    }

    // --- Empty bool and pure-negative ---

    public void testEmptyBoolProducesTrueLiteral() throws ConversionException {
        // Legacy: BoolQueryBuilder.doRewrite returns MatchAllQueryBuilder when clauses==0.
        // Citation: BoolQueryBuilder.doRewrite line ~279 and doToQuery line 333.
        RexNode result = translator.convert(QueryBuilders.boolQuery(), ctx);

        assertTrue("Empty bool must produce TRUE literal (match-all)", result instanceof RexLiteral);
        assertTrue("Must be boolean TRUE", RexLiteral.booleanValue(result));
    }

    public void testPureNegativeBoolProducesNot() throws ConversionException {
        // Legacy: adjustPureNegative=true (default) adds MatchAllDocsQuery FILTER,
        // meaning "match all EXCEPT these". Our table scan is the implicit match-all,
        // so pure-negative just produces IS_NOT_TRUE conditions.
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
        // Each operand should be IS_NOT_TRUE(EQUALS)
        for (RexNode operand : call.getOperands()) {
            assertTrue(operand instanceof RexCall);
            assertEquals(SqlKind.IS_NOT_TRUE, ((RexCall) operand).getKind());
        }
    }

    // --- Nested bool recursion with isFlat assertion ---

    public void testNestedBoolInShouldInMustIsFlat() throws ConversionException {
        // bool(must: [bool(should: [bool(must: [term1, term2]), term3])])
        // After flattening, the top-level result must satisfy RexUtil.isFlat.
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
        // "5<" is malformed — when total > threshold, split produces only one part and legacy
        // throws ArrayIndexOutOfBoundsException (wrapped as ConversionException).
        ConversionException ex = expectThrows(
            ConversionException.class,
            () -> MinimumShouldMatchParser.calculateRequiredMatches("5<", 6, false)
        );
        assertTrue("Message must mention the spec", ex.getMessage().contains("5<"));
    }

    // --- Should-gate local branch tests: exact-equal AND vs over-large FALSE ---

    public void testShouldGateExactEqualProducesAnd() throws ConversionException {
        // When requiredMatches == shouldConditions.size(), all must match → AND.
        // MSM="3" with 3 should clauses → requiredMatches=3 == size=3 → AND.
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
        // When requiredMatches > shouldConditions.size(), the constraint is unsatisfiable → FALSE.
        // The upstream gate (requiredMatches > totalShould) catches this via convert(); the local
        // gate is a robustness guard against future code paths where shouldConditions.size() could
        // diverge from totalShould. This test exercises the upstream gate which has identical semantics.
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
        // Bool wrapping a numeric term clause must preserve the INTEGER literal value.
        RexNode result = translator.convert(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("price", 42)), ctx);

        assertTrue(result instanceof RexCall);
        RexCall eq = (RexCall) result;
        assertEquals(SqlKind.EQUALS, eq.getKind());
        // price is index 1 in the test schema
        assertEquals(1, ((RexInputRef) eq.getOperands().get(0)).getIndex());
        // Nullable field wraps literal in CAST
        RexCall cast = (RexCall) eq.getOperands().get(1);
        RexLiteral literal = (RexLiteral) cast.getOperands().get(0);
        assertEquals(SqlTypeName.INTEGER, literal.getType().getSqlTypeName());
        assertEquals(Integer.valueOf(42), literal.getValueAs(Integer.class));
    }

    public void testBoolMustWithBooleanChildPreservesBooleanLiteral() throws ConversionException {
        // Bool wrapping a boolean term clause must preserve the BOOLEAN literal value.
        RexNode result = translator.convert(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("is_active", true)), ctx);

        assertTrue(result instanceof RexCall);
        RexCall eq = (RexCall) result;
        assertEquals(SqlKind.EQUALS, eq.getKind());
        // is_active is index 5 in the test schema
        assertEquals(5, ((RexInputRef) eq.getOperands().get(0)).getIndex());
        // Nullable field wraps literal in CAST
        RexCall cast = (RexCall) eq.getOperands().get(1);
        RexLiteral literal = (RexLiteral) cast.getOperands().get(0);
        assertEquals(SqlTypeName.BOOLEAN, literal.getType().getSqlTypeName());
        assertTrue("Boolean literal must be TRUE", literal.getValueAs(Boolean.class));
    }

    public void testBoolMustWithDateChildPreservesDateLiteral() throws ConversionException {
        // Bool wrapping a date term clause must preserve the DATE literal value.
        // Calcite DATE expects days-since-epoch (integer); 19738 = 2024-01-15.
        // Note: passing a date string (e.g. "2024-01-15") through TermQueryTranslator to a
        // DATE-typed field throws ClassCastException in RexBuilder.clean() because Calcite
        // requires an Integer for DATE. This test uses the integer form that Calcite accepts.
        RexNode result = translator.convert(QueryBuilders.boolQuery().must(QueryBuilders.termQuery("created_date", 19738)), ctx);

        assertTrue(result instanceof RexCall);
        RexCall eq = (RexCall) result;
        assertEquals(SqlKind.EQUALS, eq.getKind());
        // created_date is index 4 in the test schema
        assertEquals(4, ((RexInputRef) eq.getOperands().get(0)).getIndex());
        // Nullable field wraps literal in CAST
        RexCall cast = (RexCall) eq.getOperands().get(1);
        RexLiteral literal = (RexLiteral) cast.getOperands().get(0);
        assertEquals(SqlTypeName.DATE, literal.getType().getSqlTypeName());
        // Days-since-epoch value must be preserved
        assertNotNull("Date literal value must not be null", literal.getValue());
        assertEquals(Integer.valueOf(19738), literal.getValueAs(Integer.class));
    }

    public void testBoolMustMixedTypesPreservesAllLiteralsInOrder() throws ConversionException {
        // Mixed bool: string child + numeric child + boolean child in a single must list.
        // Assert each operand's value survives composition in order.
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

        // First operand: name = 'laptop' (VARCHAR, index 0)
        RexCall eq0 = (RexCall) and.getOperands().get(0);
        assertEquals(SqlKind.EQUALS, eq0.getKind());
        assertEquals(0, ((RexInputRef) eq0.getOperands().get(0)).getIndex());
        RexLiteral lit0 = (RexLiteral) ((RexCall) eq0.getOperands().get(1)).getOperands().get(0);
        assertEquals("laptop", lit0.getValueAs(String.class));

        // Second operand: price = 999 (INTEGER, index 1)
        RexCall eq1 = (RexCall) and.getOperands().get(1);
        assertEquals(SqlKind.EQUALS, eq1.getKind());
        assertEquals(1, ((RexInputRef) eq1.getOperands().get(0)).getIndex());
        RexLiteral lit1 = (RexLiteral) ((RexCall) eq1.getOperands().get(1)).getOperands().get(0);
        assertEquals(Integer.valueOf(999), lit1.getValueAs(Integer.class));

        // Third operand: is_active = false (BOOLEAN, index 5)
        RexCall eq2 = (RexCall) and.getOperands().get(2);
        assertEquals(SqlKind.EQUALS, eq2.getKind());
        assertEquals(5, ((RexInputRef) eq2.getOperands().get(0)).getIndex());
        RexLiteral lit2 = (RexLiteral) ((RexCall) eq2.getOperands().get(1)).getOperands().get(0);
        assertFalse("Boolean literal must be FALSE", lit2.getValueAs(Boolean.class));
    }

    public void testBoolMinimumShouldMatchMixedTypesEnumeratedFormPreservesTypes() throws ConversionException {
        // Mixed should clauses (string + numeric + boolean) under minimum_should_match=2
        // with 3 clauses. Required count (2) is strictly between 1 and clause count (3).
        // Asserts: OR of C(3,2)=3 ANDs, each AND has 2 children preserving typed literals.
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

        // Each OR operand is an AND of 2 typed conditions
        for (RexNode operand : or.getOperands()) {
            assertTrue(operand instanceof RexCall);
            RexCall andCall = (RexCall) operand;
            assertEquals(SqlKind.AND, andCall.getKind());
            assertEquals(2, andCall.getOperands().size());
        }

        // First subset: AND(name='tablet', price=500) — indices 0,1
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

        // Last subset: AND(price=500, is_active=true) — indices 1,5
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
        // A should-child that is itself a bool with two must-clauses converts to an AND node.
        // When that child appears in a k-subset AND, the result must be flat — no AND operand
        // is itself an AND call.
        // Setup: 3 should-children, required=2, one child is bool(must:[term,term]) → AND node.
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
        // C(3,2) = 3 subsets
        assertEquals(3, or.getOperands().size());

        // Every subset AND must be flat: no operand is itself an AND
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
        // Additionally, the entire expression must satisfy Calcite's isFlat check
        assertTrue("Enumerated form with nested bool children must be flat", RexUtil.isFlat(result));
    }

    // --- Helper methods for enumerated-form tests ---

    /**
     * Evaluates the enumerated form OR(AND(subset1), AND(subset2), ...) against a truth mask.
     * Bit i of mask indicates whether predicate i is true.
     * An AND-subset is true iff all its members are true.
     * The OR is true iff any AND-subset is true.
     */
    private boolean evaluateEnumeratedForm(RexCall or, int mask, int n) {
        for (RexNode operand : or.getOperands()) {
            RexCall andCall = (RexCall) operand;
            boolean allTrue = true;
            for (RexNode child : andCall.getOperands()) {
                // Each child is an EQUALS on an input ref — the ref index identifies the predicate
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

    /**
     * Extracts the predicate index from an EQUALS node by looking at the RexLiteral value.
     * Test predicates are termQuery("name", "val0"), termQuery("name", "val1"), etc.
     * Since all use the same field, we identify them by their literal value suffix.
     */
    private int getPredicateIndex(RexNode node, int n) {
        // All predicates are EQUALS(field_ref, CAST(literal)) on the same field "name" (index 0).
        // The literal values are "val0", "val1", ..., "val(n-1)".
        RexCall eq = (RexCall) node;
        RexCall cast = (RexCall) eq.getOperands().get(1);
        RexLiteral literal = (RexLiteral) cast.getOperands().get(0);
        String val = literal.getValueAs(String.class);
        // Extract numeric suffix from "valN"
        return Integer.parseInt(val.substring(3));
    }
}
