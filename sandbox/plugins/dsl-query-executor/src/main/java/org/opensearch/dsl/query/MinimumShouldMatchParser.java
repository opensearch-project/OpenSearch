/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.query;

import org.opensearch.common.lucene.search.Queries;
import org.opensearch.dsl.converter.ConversionException;

/**
 * Delegates minimum_should_match arithmetic to legacy {@link Queries#calculateMinShouldMatch}.
 * Floor-clamps to 0 (matching legacy); no upper clamp is applied.
 */
final class MinimumShouldMatchParser {

    private MinimumShouldMatchParser() {}

    /**
     * Calculates the required number of should clauses that must match.
     *
     * @return number of should clauses required (floor-clamped to 0, no upper clamp)
     */
    static int calculateRequiredMatches(String minimumShouldMatch, int totalShould, boolean hasRequired) throws ConversionException {
        if (minimumShouldMatch == null) {
            return hasRequired ? 0 : 1;
        }

        try {
            return Queries.calculateMinShouldMatch(totalShould, minimumShouldMatch);
        } catch (NumberFormatException e) {
            throw new ConversionException("Invalid minimum_should_match spec: \"" + minimumShouldMatch + "\"", e);
        } catch (ArrayIndexOutOfBoundsException e) {
            // WHY ArrayIndexOutOfBoundsException: This is a real legacy failure mode, not defensive noise.
            // Queries.java uses lessThanPattern.split(s, 0) then accesses parts[1]; a trailing "<"
            // (e.g. "5<") produces a single-element array, causing ArrayIndexOutOfBoundsException.
            throw new ConversionException("Invalid minimum_should_match spec: \"" + minimumShouldMatch + "\"", e);
        }
    }
}
