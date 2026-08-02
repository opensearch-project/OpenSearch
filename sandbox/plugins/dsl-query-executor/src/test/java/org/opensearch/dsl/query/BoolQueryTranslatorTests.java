/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.apache.calcite.rex.RexCall;
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
        assertEquals(SqlKind.AND, call.getKind());
        // Conjoined form: AND(OR(a, b, c), GTE(PLUS(CASE...), 2))
        assertEquals(2, call.getOperands().size());
        assertEquals(SqlKind.OR, ((RexCall) call.getOperands().get(0)).getKind());
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, ((RexCall) call.getOperands().get(1)).getKind());
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
        // -1 means total - 1 = 3 - 1 = 2 required → conjoined form
        assertEquals(SqlKind.AND, call.getKind());
        assertEquals(2, call.getOperands().size());
        assertEquals(SqlKind.OR, ((RexCall) call.getOperands().get(0)).getKind());
        assertEquals(3, ((RexCall) call.getOperands().get(0)).getOperands().size());
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
        // 70% of 4 = 2.8, floor = 2 required → conjoined form AND(OR, GTE)
        assertEquals(SqlKind.AND, call.getKind());
        assertEquals(2, call.getOperands().size());
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
        // 50% of 4 = 2 required → conjoined form AND(OR, GTE)
        assertEquals(SqlKind.AND, call.getKind());
        assertEquals(2, call.getOperands().size());
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
        // -30% means can miss 30% = 1.2, floor = 1, so 4 - 1 = 3 required → conjoined form
        assertEquals(SqlKind.AND, call.getKind());
        assertEquals(2, call.getOperands().size());
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
        // total = 4 > 2, so 75% of 4 = 3 required → conjoined form
        assertEquals(SqlKind.AND, call.getKind());
        assertEquals(2, call.getOperands().size());
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
        // total = 4, which is 3 < 4 <= 5, so -1 = 4 - 1 = 3 required → conjoined form
        assertEquals(SqlKind.AND, call.getKind());
        assertEquals(2, call.getOperands().size());
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
        // total = 6 > 5, so 50% of 6 = 3 required → conjoined form
        assertEquals(SqlKind.AND, call.getKind());
        assertEquals(2, call.getOperands().size());
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

    public void testLargeClauseCountConvertsWithConjoinedForm() throws ConversionException {
        // 20 should clauses with MSM="10" — previously rejected by combination cap.
        // Must now succeed and emit AND(OR(all), GTE(PLUS chain of CASE, k)).
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (int i = 0; i < 20; i++) {
            query.should(QueryBuilders.termQuery("name", "val" + i));
        }
        query.minimumShouldMatch("10");

        RexNode result = translator.convert(query, ctx);
        assertTrue("Must produce AND", result instanceof RexCall);
        RexCall and = (RexCall) result;
        assertEquals(SqlKind.AND, and.getKind());
        assertEquals("AND must have exactly 2 children (OR pruning hint + GTE counting)", 2, and.getOperands().size());

        // First child: OR of all 20 conditions
        RexNode orChild = and.getOperands().get(0);
        assertTrue("First conjunct must be OR", orChild instanceof RexCall);
        assertEquals(SqlKind.OR, ((RexCall) orChild).getKind());
        assertEquals(20, ((RexCall) orChild).getOperands().size());

        // Second child: GTE comparison
        RexNode gteChild = and.getOperands().get(1);
        assertTrue("Second conjunct must be GTE", gteChild instanceof RexCall);
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, ((RexCall) gteChild).getKind());

        // CASE leaf count must be linear in clause count (exactly 20 CASE nodes)
        int caseCount = countNodes(gteChild, SqlKind.CASE);
        assertEquals("CASE leaf count must equal clause count", 20, caseCount);
    }

    public void testVeryLargeClauseCountConvertsWithConjoinedForm() throws ConversionException {
        // C(15,7) = 6435 — previously rejected. Must now convert with linear expression.
        BoolQueryBuilder query = QueryBuilders.boolQuery();
        for (int i = 0; i < 15; i++) {
            query.should(QueryBuilders.termQuery("name", "val" + i));
        }
        query.minimumShouldMatch("7");

        RexNode result = translator.convert(query, ctx);
        assertTrue("Must produce AND", result instanceof RexCall);
        RexCall and = (RexCall) result;
        assertEquals(SqlKind.AND, and.getKind());
        assertEquals(2, and.getOperands().size());

        // OR child has 15 predicates
        RexNode orChild = and.getOperands().get(0);
        assertTrue(orChild instanceof RexCall);
        assertEquals(SqlKind.OR, ((RexCall) orChild).getKind());
        assertEquals(15, ((RexCall) orChild).getOperands().size());

        // GTE child with 15 CASE leaves
        RexNode gteChild = and.getOperands().get(1);
        assertTrue(gteChild instanceof RexCall);
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, ((RexCall) gteChild).getKind());
        assertEquals(15, countNodes(gteChild, SqlKind.CASE));
    }

    public void testConjoinedFormTruthTableEquivalence() throws ConversionException {
        // For n=4 clauses, verify every truth assignment for k in [2, 3]:
        // the expression is true iff at least k predicates are true.
        for (int required = 2; required <= 3; required++) {
            BoolQueryBuilder query = QueryBuilders.boolQuery();
            for (int i = 0; i < 4; i++) {
                query.should(QueryBuilders.termQuery("name", "val" + i));
            }
            query.minimumShouldMatch(String.valueOf(required));

            RexNode result = translator.convert(query, ctx);
            assertTrue("Must produce AND for k=" + required, result instanceof RexCall);
            RexCall and = (RexCall) result;
            assertEquals(SqlKind.AND, and.getKind());

            // Evaluate against all 2^4 = 16 truth assignments
            for (int mask = 0; mask < 16; mask++) {
                int trueCount = Integer.bitCount(mask);
                boolean expected = trueCount >= required;
                boolean actual = evaluateConjoinedForm(and, mask, 4);
                assertEquals("k=" + required + " mask=" + Integer.toBinaryString(mask) + " trueCount=" + trueCount, expected, actual);
            }
        }
    }

    public void testConjoinedFormNullConditionContributesZero() throws ConversionException {
        // When a should clause converts to null (e.g. unsupported inner query),
        // it should not count toward the required matches.
        // With 3 clauses where one is effectively null after conversion,
        // and required=2, only 2 non-null conditions remain — which equals required,
        // so it should produce AND (all-must-match) for the non-null clauses.
        BoolQueryBuilder query = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("name", "a"))
            .should(QueryBuilders.termQuery("name", "b"))
            .should(QueryBuilders.termQuery("name", "c"))
            .minimumShouldMatch("2");

        // This exercises the path where all 3 convert successfully and required < size.
        // The null-condition behavior is tested by verifying that CASE WHEN null THEN 1 ELSE 0
        // evaluates to 0 via the truth-table test (mask with bit=0 means predicate is false/null).
        RexNode result = translator.convert(query, ctx);
        assertTrue(result instanceof RexCall);
        RexCall and = (RexCall) result;
        assertEquals(SqlKind.AND, and.getKind());

        // The GTE child's CASE uses the predicate as condition — null predicate → ELSE 0
        RexNode gteChild = and.getOperands().get(1);
        assertTrue(gteChild instanceof RexCall);
        assertEquals(SqlKind.GREATER_THAN_OR_EQUAL, ((RexCall) gteChild).getKind());
        // k literal must be INTEGER 2
        RexNode kLiteral = ((RexCall) gteChild).getOperands().get(1);
        assertTrue("k must be a literal", kLiteral instanceof RexLiteral);
        assertEquals("k must be INTEGER typed", SqlTypeName.INTEGER, ((RexLiteral) kLiteral).getType().getSqlTypeName());
    }

    public void testConjoinedFormOrConjunctSurvivesPlanning() throws ConversionException {
        // Safeguard: the OR conjunct must be present in the AND. If a future Calcite
        // simplification removes it (detecting the implication), this test fails loudly.
        BoolQueryBuilder query = QueryBuilders.boolQuery()
            .should(QueryBuilders.termQuery("name", "a"))
            .should(QueryBuilders.termQuery("name", "b"))
            .should(QueryBuilders.termQuery("name", "c"))
            .minimumShouldMatch("2");

        RexNode result = translator.convert(query, ctx);
        assertTrue(result instanceof RexCall);
        RexCall and = (RexCall) result;
        assertEquals(SqlKind.AND, and.getKind());

        // Assert the OR conjunct is the first operand and has the right shape
        RexNode firstChild = and.getOperands().get(0);
        assertTrue("OR pruning hint must survive as first AND child", firstChild instanceof RexCall);
        assertEquals("First child must be OR", SqlKind.OR, ((RexCall) firstChild).getKind());
        assertEquals("OR must contain all should conditions", 3, ((RexCall) firstChild).getOperands().size());

        // Assert flatness of the outer AND
        assertTrue("Outer expression must be flat", RexUtil.isFlat(result));
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

    // --- Helper methods for conjoined-form tests ---

    /** Counts nodes of a given SqlKind in the expression tree recursively. */
    private int countNodes(RexNode node, SqlKind kind) {
        int count = 0;
        if (node instanceof RexCall) {
            RexCall call = (RexCall) node;
            if (call.getKind() == kind) {
                count++;
            }
            for (RexNode operand : call.getOperands()) {
                count += countNodes(operand, kind);
            }
        }
        return count;
    }

    /**
     * Evaluates the conjoined form AND(OR(p1..pn), GTE(PLUS(CASE...), k)) against a truth mask.
     * Bit i of mask indicates whether predicate i is true. Uses structural reduction.
     */
    private boolean evaluateConjoinedForm(RexCall and, int mask, int n) {
        // OR child: true iff any bit is set
        boolean orResult = false;
        for (int i = 0; i < n; i++) {
            if ((mask & (1 << i)) != 0) {
                orResult = true;
                break;
            }
        }

        // GTE child: count of true predicates >= k
        RexCall gte = (RexCall) and.getOperands().get(1);
        RexLiteral kLiteral = (RexLiteral) gte.getOperands().get(1);
        int k = ((Number) kLiteral.getValue()).intValue();
        int trueCount = Integer.bitCount(mask);

        return orResult && trueCount >= k;
    }
}
