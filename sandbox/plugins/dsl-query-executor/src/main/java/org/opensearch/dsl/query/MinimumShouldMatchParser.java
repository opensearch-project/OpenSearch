/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.opensearch.dsl.converter.ConversionException;

/**
 * Parses the minimum_should_match specification string into a required-match count.
 * Supports integer, percentage, single-combination, and multiple-combination forms.
 *
 * <p>Semantics match legacy {@code org.opensearch.common.lucene.search.Queries.calculateMinShouldMatch}.
 */
final class MinimumShouldMatchParser {

    private MinimumShouldMatchParser() {}

    /**
     * Calculates the required number of should clauses that must match.
     *
     * <p>Unlike legacy, this does NOT clamp the upper bound. Values exceeding
     * totalShould are passed through; the caller handles the match-none case.
     * Floor clamp to 0 matches legacy Queries.calculateMinShouldMatch line 207.
     *
     * @return number of should clauses that must match (clamped floor to 0 only)
     */
    static int calculateRequiredMatches(String minimumShouldMatch, int totalShould, boolean hasRequired) throws ConversionException {
        if (minimumShouldMatch == null || minimumShouldMatch.isEmpty()) {
            return hasRequired ? 0 : 1;
        }

        // Trim whitespace matching legacy Queries.calculateMinShouldMatch (line 177).
        minimumShouldMatch = minimumShouldMatch.trim();

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
    private static int parseInteger(String value, int total) throws ConversionException {
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
     *
     * <p>WHY Integer.parseInt: legacy Queries.calculateMinShouldMatch (line ~199) uses
     * {@code Integer.parseInt(spec)} for the percentage numeric part, rejecting non-integer
     * and non-finite values (NaN, Infinity, fractional). We match that behavior exactly.
     */
    private static int parsePercentage(String value, int total) throws ConversionException {
        String numStr = value.substring(0, value.length() - 1);
        try {
            int percent = Integer.parseInt(numStr);
            float calc = (total * percent) * (1 / 100f);
            return percent >= 0 ? (int) calc : total + (int) calc;
        } catch (NumberFormatException e) {
            throw new ConversionException("Invalid percentage in minimum_should_match: \"" + value + "\"", e);
        }
    }

    /**
     * Parses a single combination minimum_should_match value (e.g., "2&lt;75%").
     * If total is less than or equal to threshold, all clauses must match.
     */
    private static int parseCombination(String value, int total) throws ConversionException {
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
    private static int parseMultipleCombinations(String value, int total) throws ConversionException {
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
}
