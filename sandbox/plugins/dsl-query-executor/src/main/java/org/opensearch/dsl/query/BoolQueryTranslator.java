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
import org.apache.calcite.sql.type.SqlTypeName;
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
 * must_not (NOT with double-negation elimination). Flattens nested AND/OR to satisfy
 * Calcite's RexUtil.isFlat requirement.
 *
 * <p>For minimum_should_match with 1 less-than k less-than n, emits the conjoined form
 * AND(OR(p1..pn), GTE(left-deep PLUS chain of CASE(pi, 1, 0), k)) which provides
 * linear expression size and preserves page pruning via the OR conjunct.
 *
 * <p>Rejects non-default boost (AbstractQueryBuilder.toQuery wraps in BoostQuery),
 * non-null _name (AbstractQueryBuilder.toQuery registers for matched_queries), and
 * returns FALSE literal for pure-negative bools with adjust_pure_negative=false (legacy match-none).
 */
public class BoolQueryTranslator implements QueryTranslator {

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
            RexNode condition = queryRegistry.convert(mustClause, ctx);
            if (condition != null) {
                conditions.add(condition);
            }
        }

        // Filter clauses (AND) — identical to must at Calcite level (scoring irrelevant)
        for (QueryBuilder filterClause : boolQuery.filter()) {
            RexNode condition = queryRegistry.convert(filterClause, ctx);
            if (condition != null) {
                conditions.add(condition);
            }
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

        // Must_not clauses (NOT) with double-negation elimination
        for (QueryBuilder mustNotClause : boolQuery.mustNot()) {
            RexNode condition = queryRegistry.convert(mustNotClause, ctx);
            if (condition != null) {
                if (condition instanceof RexCall && ((RexCall) condition).getOperator() == SqlStdOperatorTable.NOT) {
                    conditions.add(((RexCall) condition).getOperands().get(0));
                } else {
                    conditions.add(ctx.getRexBuilder().makeCall(SqlStdOperatorTable.NOT, condition));
                }
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

        int requiredMatches = calculateRequiredMatches(minimumShouldMatch, totalShould, hasRequired);

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
            RexNode condition = queryRegistry.convert(shouldClause, ctx);
            if (condition != null) {
                shouldConditions.add(condition);
            }
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
     * Calculates the required number of should clauses that must match.
     *
     * <p>Unlike the previous implementation, this does NOT clamp the upper bound. Legacy
     * Queries.calculateMinShouldMatch only clamps the floor to 0; values exceeding
     * totalShould are passed through to Lucene which matches nothing. The caller
     * (processShouldClauses) handles the match-none case when result exceeds totalShould.
     *
     * @return number of should clauses that must match (clamped floor to 0 only)
     */
    int calculateRequiredMatches(String minimumShouldMatch, int totalShould, boolean hasRequired) throws ConversionException {
        if (minimumShouldMatch == null || minimumShouldMatch.isEmpty()) {
            return hasRequired ? 0 : 1;
        }

        int result;

        if (minimumShouldMatch.contains(" ")) {
            result = parseMultipleCombinations(minimumShouldMatch, totalShould);
        } else if (minimumShouldMatch.contains("<")) {
            result = parseCombination(minimumShouldMatch, totalShould);
        } else if (minimumShouldMatch.endsWith("%")) {
            result = parsePercentage(minimumShouldMatch, totalShould);
        } else {
            result = parseInteger(minimumShouldMatch, totalShould);
        }

        // Floor clamp only — matches legacy Queries.calculateMinShouldMatch line 207: "return result < 0 ? 0 : result"
        return Math.max(0, result);
    }

    /**
     * Parses an integer minimum_should_match value.
     * Non-negative values are returned as-is. Negative values are subtracted from total.
     */
    private int parseInteger(String value, int total) throws ConversionException {
        try {
            int num = Integer.parseInt(value);
            return num >= 0 ? num : total + num;
        } catch (NumberFormatException e) {
            throw new ConversionException("Invalid integer in minimum_should_match: \"" + value + "\"", e);
        }
    }

    /**
     * Parses a percentage minimum_should_match value.
     * Non-negative percentages are applied directly. Negative percentages represent allowed misses.
     */
    private int parsePercentage(String value, int total) throws ConversionException {
        String numStr = value.substring(0, value.length() - 1);
        try {
            double percent = Double.parseDouble(numStr);
            if (percent >= 0) {
                return (int) Math.floor(total * percent / 100.0);
            } else {
                int allowed = (int) Math.floor(total * (-percent) / 100.0);
                return total - allowed;
            }
        } catch (NumberFormatException e) {
            throw new ConversionException("Invalid percentage in minimum_should_match: \"" + value + "\"", e);
        }
    }

    /**
     * Parses a single combination minimum_should_match value (e.g., "2&lt;75%").
     * If total is less than or equal to threshold, all clauses must match.
     */
    private int parseCombination(String value, int total) throws ConversionException {
        String[] parts = value.split("<");
        if (parts.length < 2 || parts[1].isEmpty()) {
            throw new ConversionException("Malformed combination in minimum_should_match: \"" + value + "\"");
        }
        int threshold;
        try {
            threshold = Integer.parseInt(parts[0]);
        } catch (NumberFormatException e) {
            throw new ConversionException("Invalid threshold in minimum_should_match: \"" + parts[0] + "\"", e);
        }
        if (total <= threshold) {
            return total;
        }
        return parts[1].endsWith("%") ? parsePercentage(parts[1], total) : parseInteger(parts[1], total);
    }

    /**
     * Parses multiple combinations minimum_should_match value (e.g., "3&lt;-1 5&lt;50%").
     * Applies the appropriate rule based on which threshold range the total falls into.
     */
    private int parseMultipleCombinations(String value, int total) throws ConversionException {
        String[] combinations = value.trim().split("\\s+");
        int result = total;
        for (String combination : combinations) {
            String[] parts = combination.split("<");
            if (parts.length < 2 || parts[1].isEmpty()) {
                throw new ConversionException("Malformed combination in minimum_should_match: \"" + combination + "\"");
            }
            int threshold;
            try {
                threshold = Integer.parseInt(parts[0]);
            } catch (NumberFormatException e) {
                throw new ConversionException("Invalid threshold in minimum_should_match: \"" + parts[0] + "\"", e);
            }
            if (total <= threshold) {
                return result;
            }
            result = parts[1].endsWith("%") ? parsePercentage(parts[1], total) : parseInteger(parts[1], total);
        }
        return result;
    }

    /**
     * Creates the conjoined form for minimum_should_match when 1 less-than required less-than n.
     * Emits AND(OR(p1..pn), GTE(left-deep PLUS chain of CASE(pi, 1, 0), k)).
     *
     * <p>The OR conjunct is logically redundant (implied by GTE when k is at least 1) but exists
     * so the analytics backend page pruner sees column-vs-constant comparison leaves. AND
     * intersects per-child pruning bitmaps, so the opaque counting sibling contributes all-true
     * and does not disable the OR child's pruning.
     */
    private RexNode createMinimumMatchCondition(List<RexNode> conditions, int required, ConversionContext ctx) {
        var rexBuilder = ctx.getRexBuilder();
        var typeFactory = rexBuilder.getTypeFactory();
        var intType = typeFactory.createSqlType(SqlTypeName.INTEGER);

        RexNode one = rexBuilder.makeLiteral(1, intType);
        RexNode zero = rexBuilder.makeLiteral(0, intType);
        RexNode kLiteral = rexBuilder.makeLiteral(required, intType);

        // Build left-deep PLUS chain of CASE(pi, 1, 0)
        RexNode sum = rexBuilder.makeCall(SqlStdOperatorTable.CASE, conditions.get(0), one, zero);
        for (int i = 1; i < conditions.size(); i++) {
            RexNode caseExpr = rexBuilder.makeCall(SqlStdOperatorTable.CASE, conditions.get(i), one, zero);
            sum = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, sum, caseExpr);
        }

        // GTE comparison: sum >= k
        RexNode gteExpr = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, sum, kLiteral);

        // OR pruning hint — redundant but enables page pruning via column-vs-constant leaves.
        // WHY: AND intersects per-child pruning bitmaps in the analytics backend page pruner
        // (page_pruner.rs:704-711). The counting child becomes an opaque all-true vector, so
        // only the OR child's leaf predicates drive pruning. Without the OR, no pruning occurs.
        List<RexNode> flatOr = flattenConditions(conditions, SqlStdOperatorTable.OR);
        RexNode orConjunct = flatOr.size() == 1 ? flatOr.get(0) : rexBuilder.makeCall(SqlStdOperatorTable.OR, flatOr);

        // Outer AND must be flat (no nested AND children)
        List<RexNode> andChildren = new ArrayList<>(2);
        andChildren.add(orConjunct);
        andChildren.add(gteExpr);
        List<RexNode> flatAnd = flattenConditions(andChildren, SqlStdOperatorTable.AND);
        return rexBuilder.makeCall(SqlStdOperatorTable.AND, flatAnd);
    }
}
