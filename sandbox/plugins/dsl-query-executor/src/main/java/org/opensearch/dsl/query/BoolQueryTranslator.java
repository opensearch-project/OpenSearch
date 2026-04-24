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
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a {@link BoolQueryBuilder} to Calcite logical expressions (RexNode).
 * <p>
 * Supports all bool query clauses:
 * <ul>
 *   <li><b>must</b> - Required clauses combined with AND logic</li>
 *   <li><b>should</b> - Optional clauses combined with OR logic, controlled by minimum_should_match</li>
 *   <li><b>must_not</b> - Exclusion clauses wrapped with NOT logic</li>
 *   <li><b>filter</b> - Filtering clauses combined with AND logic (no scoring)</li>
 * </ul>
 * <p>
 * <b>minimum_should_match</b> parameter supports:
 * <ul>
 *   <li>Non-negative integer: exact number of clauses (e.g., "2")</li>
 *   <li>Negative integer: total minus this number (e.g., "-1" with 3 clauses = 2 required)</li>
 *   <li>Non-negative percentage: percentage of total, rounded down (e.g., "70%" with 4 clauses = 2 required)</li>
 *   <li>Negative percentage: can miss this percentage (e.g., "-30%" with 4 clauses = 3 required)</li>
 *   <li>Single combination: threshold-based (e.g., "2&lt;75%")</li>
 *   <li>Multiple combinations: multiple thresholds (e.g., "3&lt;-1 5&lt;50%")</li>
 * </ul>
 * <p>
 * Optimizations:
 * <ul>
 *   <li>Flattens nested AND/OR structures to satisfy Calcite's RexUtil.isFlat requirement</li>
 *   <li>Eliminates double negations: NOT(NOT(a)) → a</li>
 * </ul>
 */
public class BoolQueryTranslator implements QueryTranslator {

    private final QueryRegistry queryRegistry;

    /**
     * Creates a new bool query translator.
     *
     * @param queryRegistry the registry for recursively converting nested queries
     */
    public BoolQueryTranslator(QueryRegistry queryRegistry) {
        this.queryRegistry = queryRegistry;
    }

    @Override
    public Class<? extends QueryBuilder> getQueryType() {
        return BoolQueryBuilder.class;
    }

    /**
     * Converts a bool query to a Calcite RexNode.
     * <p>
     * Processes clauses in order: must, filter, should, must_not, then combines them with AND.
     * Empty bool queries return a boolean true literal.
     *
     * @param query the bool query to convert
     * @param ctx the conversion context with schema and RexBuilder
     * @return the resulting RexNode representing the bool query logic
     * @throws ConversionException if any nested query conversion fails
     */
    @Override
    public RexNode convert(QueryBuilder query, ConversionContext ctx) throws ConversionException {
        BoolQueryBuilder boolQuery = (BoolQueryBuilder) query;
        List<RexNode> conditions = new ArrayList<>();

        // Must clauses (AND)
        for (QueryBuilder mustClause : boolQuery.must()) {
            RexNode condition = queryRegistry.convert(mustClause, ctx);
            if (condition != null) {
                conditions.add(condition);
            }
        }

        // Filter clauses (AND)
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
                conditions.add(shouldCondition);
            }
        }

        // Must_not clauses (NOT)
        for (QueryBuilder mustNotClause : boolQuery.mustNot()) {
            RexNode condition = queryRegistry.convert(mustNotClause, ctx);
            if (condition != null) {
                // Optimize double negation: NOT(NOT(a)) -> a
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
            return ctx.getRexBuilder().makeLiteral(true);
        } else if (flattenedConditions.size() == 1) {
            return flattenedConditions.get(0);
        } else {
            return ctx.getRexBuilder().makeCall(SqlStdOperatorTable.AND, flattenedConditions);
        }
    }

    /**
     * Flattens nested conditions with the same operator to satisfy Calcite's RexUtil.isFlat requirement.
     * <p>
     * Examples:
     * <ul>
     *   <li>AND(AND(a, b), c) → AND(a, b, c)</li>
     *   <li>OR(OR(a, b), c) → OR(a, b, c)</li>
     * </ul>
     * Note: NOT operations are not flattened as NOT(NOT(a)) has different semantics.
     *
     * @param conditions the list of conditions to flatten
     * @param operator the operator to flatten (AND or OR)
     * @return flattened list of conditions
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
     * <p>
     * Default behavior:
     * <ul>
     *   <li>Without must/filter: at least 1 should clause must match</li>
     *   <li>With must/filter: should clauses are optional (unless minimum_should_match is set)</li>
     * </ul>
     *
     * @param boolQuery the bool query containing should clauses
     * @param ctx the conversion context
     * @return RexNode representing the should clause logic, or null if should clauses are optional
     * @throws ConversionException if nested query conversion fails
     */
    private RexNode processShouldClauses(BoolQueryBuilder boolQuery, ConversionContext ctx) throws ConversionException {
        List<QueryBuilder> shouldClauses = boolQuery.should();
        int totalShould = shouldClauses.size();

        // If there are must/filter clauses, should is optional unless minimum_should_match is set
        boolean hasRequired = !boolQuery.must().isEmpty() || !boolQuery.filter().isEmpty();
        String minimumShouldMatch = boolQuery.minimumShouldMatch();

        int requiredMatches = calculateRequiredMatches(minimumShouldMatch, totalShould, hasRequired);

        if (requiredMatches == 0) {
            return null; // Should clauses are optional
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

        if (requiredMatches == 1) {
            // Flatten nested ORs
            List<RexNode> flatOr = flattenConditions(shouldConditions, SqlStdOperatorTable.OR);
            return flatOr.size() == 1 ? flatOr.get(0) : ctx.getRexBuilder().makeCall(SqlStdOperatorTable.OR, flatOr);
        }

        return createMinimumMatchCondition(shouldConditions, requiredMatches, ctx);
    }

    /**
     * Calculates the required number of should clauses that must match based on minimum_should_match parameter.
     * <p>
     * Supports multiple formats:
     * <ul>
     *   <li>null/empty: returns 0 if must/filter present, otherwise 1</li>
     *   <li>"2": exactly 2 clauses must match</li>
     *   <li>"-1": total - 1 clauses must match</li>
     *   <li>"70%": 70% of clauses (rounded down) must match</li>
     *   <li>"-30%": can miss 30% of clauses</li>
     *   <li>"2&lt;75%": if total ≤ 2 match all, else 75%</li>
     *   <li>"3&lt;-1 5&lt;50%": multiple thresholds</li>
     * </ul>
     *
     * @param minimumShouldMatch the minimum_should_match parameter value
     * @param totalShould total number of should clauses
     * @param hasRequired true if must or filter clauses are present
     * @return number of should clauses that must match
     */
    int calculateRequiredMatches(String minimumShouldMatch, int totalShould, boolean hasRequired) {
        if (minimumShouldMatch == null || minimumShouldMatch.isEmpty()) {
            return hasRequired ? 0 : 1;
        }

        // Multiple combinations: "3<-1 5<50%"
        if (minimumShouldMatch.contains(" ")) {
            return parseMultipleCombinations(minimumShouldMatch, totalShould);
        }

        // Single combination: "2<75%"
        if (minimumShouldMatch.contains("<")) {
            return parseCombination(minimumShouldMatch, totalShould);
        }

        // Percentage: "70%" or "-30%"
        if (minimumShouldMatch.endsWith("%")) {
            return parsePercentage(minimumShouldMatch, totalShould);
        }

        // Integer: "2" or "-1"
        return parseInteger(minimumShouldMatch, totalShould);
    }

    /**
     * Parses an integer minimum_should_match value.
     * Non-negative values are returned as-is. Negative values are subtracted from total.
     *
     * @param value the integer string (e.g., "2" or "-1")
     * @param total total number of should clauses
     * @return required number of matches
     */
    private int parseInteger(String value, int total) {
        int num = Integer.parseInt(value);
        return num >= 0 ? num : Math.max(0, total + num);
    }

    /**
     * Parses a percentage minimum_should_match value.
     * Non-negative percentages are applied directly. Negative percentages represent allowed misses.
     *
     * @param value the percentage string (e.g., "70%" or "-30%")
     * @param total total number of should clauses
     * @return required number of matches (rounded down)
     */
    private int parsePercentage(String value, int total) {
        double percent = Double.parseDouble(value.substring(0, value.length() - 1));
        if (percent >= 0) {
            return (int) Math.floor(total * percent / 100.0);
        } else {
            int allowed = (int) Math.floor(total * (-percent) / 100.0);
            return Math.max(0, total - allowed);
        }
    }

    /**
     * Parses a single combination minimum_should_match value (e.g., "2&lt;75%").
     * If total ≤ threshold, all clauses must match. Otherwise, applies the specified value.
     *
     * @param value the combination string (e.g., "2&lt;75%")
     * @param total total number of should clauses
     * @return required number of matches
     */
    private int parseCombination(String value, int total) {
        String[] parts = value.split("<");
        int threshold = Integer.parseInt(parts[0]);
        if (total <= threshold) {
            return total;
        }
        return parts[1].endsWith("%") ? parsePercentage(parts[1], total) : parseInteger(parts[1], total);
    }

    /**
     * Parses multiple combinations minimum_should_match value (e.g., "3&lt;-1 5&lt;50%").
     * Applies the appropriate rule based on which threshold range the total falls into.
     *
     * @param value the combinations string (e.g., "3&lt;-1 5&lt;50%")
     * @param total total number of should clauses
     * @return required number of matches
     */
    private int parseMultipleCombinations(String value, int total) {
        String[] combinations = value.trim().split("\\s+");
        for (int i = 0; i < combinations.length; i++) {
            String combination = combinations[i];
            String[] parts = combination.split("<");
            int threshold = Integer.parseInt(parts[0]);

            if (total <= threshold) {
                return total;
            }

            // If this is the last combination or we're in the range for this combination
            if (i == combinations.length - 1) {
                return parts[1].endsWith("%") ? parsePercentage(parts[1], total) : parseInteger(parts[1], total);
            }

            // Check if we're in the range for the next threshold
            if (i + 1 < combinations.length) {
                String[] nextParts = combinations[i + 1].split("<");
                int nextThreshold = Integer.parseInt(nextParts[0]);
                if (total <= nextThreshold) {
                    return parts[1].endsWith("%") ? parsePercentage(parts[1], total) : parseInteger(parts[1], total);
                }
            }
        }
        return total;
    }

    /**
     * Creates a RexNode representing the minimum_should_match condition when required &gt; 1.
     * Generates all C(n,k) combinations where n = total conditions, k = required matches.
     * <p>
     * Example: 3 conditions with 2 required generates: (a AND b) OR (a AND c) OR (b AND c)
     * <p>
     * Performance note: Large clause counts with high required matches can generate many combinations.
     * For example, C(10,5) = 252, C(20,10) = 184,756.
     *
     * @param conditions the list of should clause conditions
     * @param required number of conditions that must match
     * @param ctx the conversion context
     * @return RexNode representing all valid combinations
     */
    private RexNode createMinimumMatchCondition(List<RexNode> conditions, int required, ConversionContext ctx) {
        List<RexNode> combinations = new ArrayList<>();
        generateCombinations(conditions, required, 0, new ArrayList<>(), combinations, ctx);
        return combinations.size() == 1 ? combinations.get(0) : ctx.getRexBuilder().makeCall(SqlStdOperatorTable.OR, combinations);
    }

    /**
     * Recursively generates all C(n,k) combinations of conditions using backtracking.
     * Each combination of size k is combined with AND, and all combinations are collected for OR.
     *
     * @param conditions all available conditions
     * @param required number of conditions needed per combination
     * @param start starting index for this recursion level
     * @param current current combination being built
     * @param result accumulator for all generated combinations
     * @param ctx the conversion context
     */
    private void generateCombinations(
        List<RexNode> conditions,
        int required,
        int start,
        List<RexNode> current,
        List<RexNode> result,
        ConversionContext ctx
    ) {
        if (current.size() == required) {
            result.add(
                current.size() == 1 ? current.get(0) : ctx.getRexBuilder().makeCall(SqlStdOperatorTable.AND, new ArrayList<>(current))
            );
            return;
        }

        for (int i = start; i <= conditions.size() - (required - current.size()); i++) {
            current.add(conditions.get(i));
            generateCombinations(conditions, required, i + 1, current, result, ctx);
            current.remove(current.size() - 1);
        }
    }
}
