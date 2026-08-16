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
 * <p>For minimum_should_match with 1 less-than k less-than n, emits an enumerated form:
 * OR over every k-sized subset of the should-children, each subset an AND. The planner
 * recurses only through AND/OR/NOT, so every child must sit in its own AND/OR-delimited
 * leaf for a Lucene-delegated child to resolve a backend in OpenSearchFilterRule.
 *
 * <p>Rejects non-default boost (AbstractQueryBuilder.toQuery wraps in BoostQuery),
 * non-null _name (AbstractQueryBuilder.toQuery registers for matched_queries), and
 * returns FALSE literal for pure-negative bools with adjust_pure_negative=false (legacy match-none).
 */
public class BoolQueryTranslator implements QueryTranslator {

    /**
     * Maximum total leaf occurrences in the enumerated minimum_should_match form.
     * Total = C(n,k) x k. When children are Lucene-delegated relevance calls, every leaf
     * occurrence becomes a clause in a Lucene BooleanQuery; default max_clause_count is 1024.
     * Exceeding this cap throws ConversionException so the request falls back to the non-Calcite
     * execution path. The cap is conservative for native-only children (term predicates never
     * become Lucene clauses but still count); distinguishing delegated from native children was
     * deliberately avoided to keep the translator backend-agnostic.
     */
    private static final int MAX_LEAF_OCCURRENCES = 1024;

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

        return createMinimumMatchCondition(shouldConditions, requiredMatches, ctx);
    }

    /**
     * Creates the enumerated form for minimum_should_match when 1 less-than required less-than n.
     *
     * @throws ConversionException if leaf-occurrence count C(n,k) x k exceeds MAX_LEAF_OCCURRENCES
     */
    private RexNode createMinimumMatchCondition(List<RexNode> conditions, int required, ConversionContext ctx) throws ConversionException {
        int n = conditions.size();
        int k = required;

        long combinations = computeCombinationsCapped(n, k);
        long leafOccurrences = combinations * k;
        if (leafOccurrences > MAX_LEAF_OCCURRENCES) {
            throw new ConversionException(
                "minimum_should_match leaf-occurrence count exceeds limit: C("
                    + n
                    + ", "
                    + k
                    + ") = "
                    + combinations
                    + ", leaf occurrences = "
                    + leafOccurrences
                    + " exceeds maximum "
                    + MAX_LEAF_OCCURRENCES
            );
        }

        var rexBuilder = ctx.getRexBuilder();

        List<RexNode> subsets = new ArrayList<>((int) combinations);
        int[] indices = new int[k];
        for (int i = 0; i < k; i++) {
            indices[i] = i;
        }

        while (true) {
            List<RexNode> andChildren = new ArrayList<>(k);
            for (int idx : indices) {
                andChildren.add(conditions.get(idx));
            }
            RexNode andNode = RexUtil.composeConjunction(rexBuilder, andChildren);
            subsets.add(andNode);

            if (!nextCombination(indices, n)) {
                break;
            }
        }

        // composeDisjunction deduplicates operands and absorbs FALSE literals — duplicate
        // should-clauses produce fewer than C(n,k) operands (identical subsets collapse).
        // Correct: the logical truth table is unchanged when duplicates are present.
        return RexUtil.composeDisjunction(rexBuilder, subsets);
    }

    /** Advances indices to the next k-subset in lexicographic order; returns false if exhausted. */
    private boolean nextCombination(int[] indices, int n) {
        int k = indices.length;
        int i = k - 1;
        while (i >= 0 && indices[i] == n - k + i) {
            i--;
        }
        if (i < 0) {
            return false;
        }
        indices[i]++;
        for (int j = i + 1; j < k; j++) {
            indices[j] = indices[j - 1] + 1;
        }
        return true;
    }

    /**
     * Computes C(n,k) with early bail when result x k exceeds MAX_LEAF_OCCURRENCES.
     *
     * <p>Uses binomial symmetry C(n,k) = C(n, n-k) so the incremental running value never
     * peaks at C(n, n/2) — without this, an early bail could reject a case whose final value
     * is under the cap.
     */
    private long computeCombinationsCapped(int n, int k) {
        int kOrig = k;
        if (k > n - k) {
            k = n - k;
        }
        long result = 1;
        for (int i = 0; i < k; i++) {
            result = result * (n - i) / (i + 1);
            // Overflow-safe: kOrig >= 2 on the enumerated path (1 < k < n).
            if (result > MAX_LEAF_OCCURRENCES / kOrig) {
                return result;
            }
        }
        return result;
    }
}
