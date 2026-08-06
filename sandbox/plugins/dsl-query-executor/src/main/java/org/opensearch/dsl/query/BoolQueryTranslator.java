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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.dsl.converter.ConversionContext;
import org.opensearch.dsl.converter.ConversionException;
import org.opensearch.index.query.AbstractQueryBuilder;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a {@link BoolQueryBuilder} to Calcite logical expressions (RexNode).
 *
 * <p>Handles must (AND), filter (AND), should (OR with minimum_should_match), and
 * must_not (IS_NOT_TRUE with double-negation elimination). Flattens nested AND/OR to satisfy
 * Calcite's RexUtil.isFlat requirement.
 *
 * <p>For minimum_should_match with 1 less-than k less-than n, emits the enumerated form:
 * OR over every k-sized subset of the should-children, where each subset is an AND of its
 * members. This keeps every child in its own AND/OR-delimited leaf so mixed native and
 * Lucene-delegated children both resolve a backend in OpenSearchFilterRule.
 *
 * <p>Rejects non-default boost (AbstractQueryBuilder.toQuery wraps in BoostQuery),
 * non-null _name (AbstractQueryBuilder.toQuery registers for matched_queries), and
 * returns FALSE literal for pure-negative bools with adjust_pure_negative=false (legacy match-none).
 */
public class BoolQueryTranslator implements QueryTranslator {

    /**
     * Maximum number of k-sized subsets allowed before throwing ConversionException.
     * Exceeding this cap signals that the request should fall back to the codec path
     * (PR #22597 catches ConversionException to trigger that fallback).
     * Note: total expression node count scales as C(n,k) × k × child-size, so a large nested
     * child can still produce a big expression even when the combination count is under the cap.
     */
    private static final int MAX_COMBINATIONS = 1024;

    private final QueryRegistry queryRegistry;

    /** Creates a new bool query translator with the given registry for recursive conversion. */
    public BoolQueryTranslator(QueryRegistry queryRegistry) {
        this.queryRegistry = queryRegistry;
    }

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return BoolQueryBuilder.class;
    }

    /**
     * Converts a bool query to a Calcite RexNode.
     *
     * @throws ConversionException if boost or _name is non-default, or if nested conversion fails
     */
    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;

        // Parameter audit: reject unsupported parameters matching ExistsQueryTranslator/TermsQueryTranslator style.
        // Citation: AbstractQueryBuilder.toQuery lines 130-139 (boost wrapping + named query registration).
        if (boolQuery.boost() != AbstractQueryBuilder.DEFAULT_BOOST) {
            throw new ConversionException("Bool query does not support non-default boost");
        }
        if (boolQuery.queryName() != null) {
            throw new ConversionException("Bool query does not support _name");
        }
        // WHY shape-gated: BoolQueryBuilder.doToQuery:338 only calls fixNegativeQueryIfNeeded when
        // adjustPureNegative is true. Queries.isNegativeQuery:113-119 requires every clause to be
        // prohibited, so the flag is a no-op unless the bool is pure-negative (must, filter, should
        // all empty). Queries.fixNegativeQueryIfNeeded:121-130 injects MatchAllDocsQuery as FILTER.
        boolean isPureNegative = boolQuery.must().isEmpty()
            && boolQuery.filter().isEmpty()
            && boolQuery.should().isEmpty()
            && !boolQuery.mustNot().isEmpty();
        if (isPureNegative && !boolQuery.adjustPureNegative()) {
            return ctx.getRexBuilder().makeLiteral(false);
        }

        List<RexNode> conditions = new ArrayList<>();

        // Must clauses (AND)
        for (QueryBuilder mustClause : boolQuery.must()) {
            conditions.add(queryRegistry.convert(mustClause, ctx));
        }

        // Filter clauses (AND) — identical to must at Calcite level (scoring irrelevant)
        for (QueryBuilder filterClause : boolQuery.filter()) {
            conditions.add(queryRegistry.convert(filterClause, ctx));
        }

        // Should clauses with minimum_should_match
        if (!boolQuery.should().isEmpty()) {
            RexNode shouldCondition = processShouldClauses(boolQuery, ctx);
            if (shouldCondition != null) {
                // Check if should processing determined match-none (FALSE literal)
                if (shouldCondition.isAlwaysFalse()) {
                    return ctx.getRexBuilder().makeLiteral(false);
                }
                conditions.add(shouldCondition);
            }
        }

        // Must_not clauses: IS_NOT_TRUE with double-negation elimination.
        // WHY IS_NOT_TRUE: Under SQL three-valued logic NOT(NULL) evaluates to NULL (falsy in a
        // filter), excluding rows whose field is missing. Lucene must_not INCLUDES those rows.
        // IS_NOT_TRUE(condition) returns TRUE when condition is NULL, preserving that semantics.
        for (QueryBuilder mustNotClause : boolQuery.mustNot()) {
            RexNode condition = queryRegistry.convert(mustNotClause, ctx);
            if (condition instanceof RexCall && ((RexCall) condition).getOperator() == SqlStdOperatorTable.IS_NOT_TRUE) {
                // Double-negation elimination: IS_NOT_TRUE(IS_NOT_TRUE(x)) → x.
                // Semantically sound: IS_NOT_TRUE(IS_NOT_TRUE(x)) is TRUE when x is TRUE, FALSE
                // when x is FALSE or NULL — identical to x under filter evaluation
                // (a filter discards rows whose predicate is not TRUE).
                conditions.add(((RexCall) condition).getOperands().get(0));
            } else {
                conditions.add(ctx.getRexBuilder().makeCall(SqlStdOperatorTable.IS_NOT_TRUE, condition));
            }
        }

        // Flatten nested ANDs to satisfy Calcite's RexUtil.isFlat requirement
        List<RexNode> flattenedConditions = flattenConditions(conditions, SqlStdOperatorTable.AND);

        if (flattenedConditions.isEmpty()) {
            // Empty bool → match-all. Citation: BoolQueryBuilder.doRewrite line ~279.
            return ctx.getRexBuilder().makeLiteral(true);
        } else if (flattenedConditions.size() == 1) {
            return flattenedConditions.get(0);
        } else {
            return ctx.getRexBuilder().makeCall(SqlStdOperatorTable.AND, flattenedConditions);
        }
    }

    /**
     * Flattens nested conditions with the same operator (AND or OR).
     * Example: AND(AND(a, b), c) becomes AND(a, b, c).
     */
    private List<RexNode> flattenConditions(List<RexNode> conditions, org.apache.calcite.sql.SqlOperator operator) {
        List<RexNode> flattened = new ArrayList<>();
        for (RexNode condition : conditions) {
            if (condition instanceof RexCall && ((RexCall) condition).getOperator() == operator) {
                flattened.addAll(((RexCall) condition).getOperands());
            } else {
                flattened.add(condition);
            }
        }
        return flattened;
    }

    /**
     * Processes should clauses with minimum_should_match logic.
     *
     * @return RexNode for should logic, FALSE literal if MSM is unsatisfiable, or null if optional
     */
    private RexNode processShouldClauses(BoolQueryBuilder boolQuery, ConversionContext ctx) throws ConversionException {
        List<QueryBuilder> shouldClauses = boolQuery.should();
        int totalShould = shouldClauses.size();

        boolean hasRequired = !boolQuery.must().isEmpty() || !boolQuery.filter().isEmpty();
        String minimumShouldMatch = boolQuery.minimumShouldMatch();

        int requiredMatches = MinimumShouldMatchParser.calculateRequiredMatches(minimumShouldMatch, totalShould, hasRequired);

        if (requiredMatches == 0) {
            return null; // Should clauses are optional
        }

        // WHY: Legacy Lucene accepts MSM > shouldCount but matches nothing (impossible constraint).
        // Witnessed: BoolQueryBuilderTests.testMinShouldMatchBiggerThanNumberOfShouldClauses sets
        // MSM=3 on 2 SHOULD clauses — Lucene accepts, query matches zero documents.
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

        // WHY: Unsatisfiable after conversion — requiredMatches exceeds converted clause count.
        // Legacy Lucene accepts an over-large minimum_should_match and matches zero documents.
        if (requiredMatches > shouldConditions.size()) {
            return ctx.getRexBuilder().makeLiteral(false);
        }

        if (requiredMatches == shouldConditions.size()) {
            // All should conditions must match — produce AND
            List<RexNode> flatAnd = flattenConditions(shouldConditions, SqlStdOperatorTable.AND);
            return flatAnd.size() == 1 ? flatAnd.get(0) : ctx.getRexBuilder().makeCall(SqlStdOperatorTable.AND, flatAnd);
        }

        if (requiredMatches == 1) {
            List<RexNode> flatOr = flattenConditions(shouldConditions, SqlStdOperatorTable.OR);
            return flatOr.size() == 1 ? flatOr.get(0) : ctx.getRexBuilder().makeCall(SqlStdOperatorTable.OR, flatOr);
        }

        return createMinimumMatchCondition(shouldConditions, requiredMatches, ctx);
    }

    /**
     * Creates the enumerated form for minimum_should_match when 1 less-than required less-than n.
     * Emits OR over every k-sized subset of the should-children, where each subset is AND of its members.
     *
     * @throws ConversionException if the combination count C(n,k) exceeds MAX_COMBINATIONS
     */
    private RexNode createMinimumMatchCondition(List<RexNode> conditions, int required, ConversionContext ctx) throws ConversionException {
        int n = conditions.size();
        int k = required;

        // Check combination cap before building
        long combinations = computeCombinationsCapped(n, k);
        if (combinations > MAX_COMBINATIONS) {
            throw new ConversionException(
                "minimum_should_match combination count exceeds limit: C(" + n + ", " + k + ") exceeds maximum " + MAX_COMBINATIONS
            );
        }

        var rexBuilder = ctx.getRexBuilder();

        // Enumerate all k-sized subsets in lexicographic order by index
        List<RexNode> subsets = new ArrayList<>((int) combinations);
        int[] indices = new int[k];
        for (int i = 0; i < k; i++) {
            indices[i] = i;
        }

        while (true) {
            // Build AND of the current subset, flattening any child that is itself an AND
            List<RexNode> andChildren = new ArrayList<>(k);
            for (int idx : indices) {
                andChildren.add(conditions.get(idx));
            }
            List<RexNode> flatAnd = flattenConditions(andChildren, SqlStdOperatorTable.AND);
            RexNode andNode = flatAnd.size() == 1 ? flatAnd.get(0) : rexBuilder.makeCall(SqlStdOperatorTable.AND, flatAnd);
            subsets.add(andNode);

            // Advance to next k-subset in lexicographic order
            if (!nextCombination(indices, n)) {
                break;
            }
        }

        // Flatten nested ORs to satisfy Calcite's RexUtil.isFlat requirement
        List<RexNode> flatOr = flattenConditions(subsets, SqlStdOperatorTable.OR);
        return flatOr.size() == 1 ? flatOr.get(0) : rexBuilder.makeCall(SqlStdOperatorTable.OR, flatOr);
    }

    /**
     * Advances the combination indices to the next k-subset in lexicographic order.
     *
     * @return true if advanced successfully, false if exhausted
     */
    private boolean nextCombination(int[] indices, int n) {
        int k = indices.length;
        // Find the rightmost index that can be incremented
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
     * Computes C(n,k) using an overflow-safe incremental algorithm that bails out early
     * if the running value exceeds MAX_COMBINATIONS. Never computes a full binomial that
     * could overflow long.
     *
     * @return the exact value if within range, or a value exceeding MAX_COMBINATIONS if the cap is exceeded
     */
    private long computeCombinationsCapped(int n, int k) {
        // Use symmetry: C(n,k) = C(n, n-k)
        if (k > n - k) {
            k = n - k;
        }
        long result = 1;
        for (int i = 0; i < k; i++) {
            result = result * (n - i) / (i + 1);
            if (result > MAX_COMBINATIONS) {
                return result; // Early bail — already exceeds cap
            }
        }
        return result;
    }
}
