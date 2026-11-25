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
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a {@link BoolQueryBuilder} to a Calcite {@link RexNode}.
 *
 * <p>Supports minimum_should_match of 0, 1, and values at or above the should-clause count
 * (optional, disjunction, and conjunction respectively). Intermediate values (1 less-than k
 * less-than n) are unsupported on this path and throw ConversionException.
 *
 * <p>Rejects non-default boost (AbstractQueryBuilder.toQuery wraps in BoostQuery),
 * non-null _name (AbstractQueryBuilder.toQuery registers for matched_queries), and
 * returns FALSE literal for pure-negative bools with adjust_pure_negative=false (legacy match-none).
 */
public class BoolQueryTranslator implements QueryTranslator {

    private final QueryRegistry queryRegistry;

    public BoolQueryTranslator(QueryRegistry queryRegistry) {
        this.queryRegistry = queryRegistry;
    }

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return BoolQueryBuilder.class;
    }

    /** @throws ConversionException if boost or _name is non-default, or if nested conversion fails */
    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;

        // Citation: AbstractQueryBuilder.toQuery lines 130-139 (boost wrapping + named query registration).
        if (boolQuery.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            throw new ConversionException("Bool query does not support non-default boost");
        }
        if (boolQuery.queryName() != null) {
            throw new ConversionException("Bool query does not support _name");
        }
        // Citation: BoolQueryBuilder.doToQuery:338 only calls fixNegativeQueryIfNeeded when
        // adjustPureNegative is true. Citation: Queries.isNegativeQuery:113-119 requires every
        // clause to be prohibited. Citation: Queries.fixNegativeQueryIfNeeded:121-130 injects
        // MatchAllDocsQuery as FILTER.
        boolean isPureNegative = boolQuery.must().isEmpty()
            && boolQuery.filter().isEmpty()
            && boolQuery.should().isEmpty()
            && !boolQuery.mustNot().isEmpty();
        if (isPureNegative && !boolQuery.adjustPureNegative()) {
            return ctx.getRexBuilder().makeLiteral(false);
        }

        List<RexNode> conditions = new ArrayList<>();

        for (QueryBuilder mustClause : boolQuery.must()) {
            conditions.add(queryRegistry.convert(mustClause, ctx));
        }

        for (QueryBuilder filterClause : boolQuery.filter()) {
            conditions.add(queryRegistry.convert(filterClause, ctx));
        }

        if (!boolQuery.should().isEmpty()) {
            RexNode shouldCondition = processShouldClauses(boolQuery, ctx);
            if (shouldCondition != null) {
                if (shouldCondition.isAlwaysFalse()) {
                    return ctx.getRexBuilder().makeLiteral(false);
                }
                conditions.add(shouldCondition);
            }
        }

        // WHY IS_NOT_TRUE: Under SQL three-valued logic NOT(NULL) evaluates to NULL (falsy in a
        // filter), excluding rows whose field is missing. Lucene must_not INCLUDES those rows.
        // IS_NOT_TRUE(condition) returns TRUE when condition is NULL, preserving that semantics.
        //
        // WHY this works for Lucene-delegated children: DelegatedRelevanceCallHelper declares a
        // non-nullable BOOLEAN return type, letting Calcite's ReduceExpressionsRule rewrite
        // IS_NOT_TRUE(call) to NOT(call), which the planner then recurses through.
        for (QueryBuilder mustNotClause : boolQuery.mustNot()) {
            RexNode condition = queryRegistry.convert(mustNotClause, ctx);
            if (condition instanceof RexCall && ((RexCall) condition).getOperator() == SqlStdOperatorTable.IS_NOT_TRUE) {
                // Double-negation: IS_NOT_TRUE(IS_NOT_TRUE(x)) is equivalent to x under filter
                // evaluation (a filter discards rows whose predicate is not TRUE).
                conditions.add(((RexCall) condition).getOperands().get(0));
            } else {
                conditions.add(ctx.getRexBuilder().makeCall(SqlStdOperatorTable.IS_NOT_TRUE, condition));
            }
        }

        if (conditions.isEmpty()) {
            // Citation: BoolQueryBuilder.doRewrite line ~279.
            return ctx.getRexBuilder().makeLiteral(true);
        }
        return RexUtil.composeConjunction(ctx.getRexBuilder(), conditions);
    }

    /** @return RexNode for should logic, FALSE if MSM is unsatisfiable, or null if optional */
    private RexNode processShouldClauses(BoolQueryBuilder boolQuery, ConversionContext ctx) throws ConversionException {
        List<QueryBuilder> shouldClauses = boolQuery.should();
        int totalShould = shouldClauses.size();

        boolean hasRequired = !boolQuery.must().isEmpty() || !boolQuery.filter().isEmpty();
        String minimumShouldMatch = boolQuery.minimumShouldMatch();

        int requiredMatches = MinimumShouldMatchParser.calculateRequiredMatches(minimumShouldMatch, totalShould, hasRequired);

        if (requiredMatches == 0) {
            return null;
        }

        // Legacy Lucene accepts MSM > shouldCount but matches nothing.
        // Witnessed: BoolQueryBuilderTests.testMinShouldMatchBiggerThanNumberOfShouldClauses.
        if (requiredMatches > totalShould) {
            return ctx.getRexBuilder().makeLiteral(false);
        }

        List<RexNode> shouldConditions = new ArrayList<>();
        for (QueryBuilder shouldClause : shouldClauses) {
            shouldConditions.add(queryRegistry.convert(shouldClause, ctx));
        }

        if (shouldConditions.isEmpty()) {
            return null;
        }

        if (requiredMatches > shouldConditions.size()) {
            return ctx.getRexBuilder().makeLiteral(false);
        }

        if (requiredMatches == shouldConditions.size()) {
            return RexUtil.composeConjunction(ctx.getRexBuilder(), shouldConditions);
        }

        if (requiredMatches == 1) {
            return RexUtil.composeDisjunction(ctx.getRexBuilder(), shouldConditions);
        }

        // Intermediate minimum_should_match (1 < k < n) has no flat AND/OR/NOT form; the
        // exception routes the request to the codec / non-Calcite execution path once that lands.
        throw new ConversionException(
            "Bool query does not support minimum_should_match between 1 and the number of should clauses"
                + " (resolved minimum_should_match = "
                + requiredMatches
                + ", should clauses = "
                + shouldConditions.size()
                + ")"
        );
    }
}
