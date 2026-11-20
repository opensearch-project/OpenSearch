/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import java.util.Map;

/**
 * Statistics about the shape of string values: how many there are, how long they are, and how evenly their characters are spread.
 * <p>
 * Characters are Unicode code points, so a character from outside the Basic Multilingual Plane counts once, although Java's
 * {@link String#length()} counts it as two. The lengths and the entropy are all measured over the code points of the values.
 *
 * @opensearch.internal
 */
public interface StringStats extends NumericMetricsAggregation.MultiValue {

    /**
     * @return The number of values that were aggregated. A document with several values counts once for each of them.
     */
    long getCount();

    /**
     * @return The length in characters of the shortest value, or {@code NaN} if there were no values.
     */
    double getMinLength();

    /**
     * @return The length in characters of the longest value, or {@code NaN} if there were no values.
     */
    double getMaxLength();

    /**
     * @return The average length in characters of the values, or {@code NaN} if there were no values.
     */
    double getAvgLength();

    /**
     * @return The Shannon entropy in bits of the characters of all values together, or {@code NaN} if there were no values. It is
     *         zero when the values have no characters, or only a single distinct one, and at most the base 2 logarithm of the number of
     *         distinct characters, which it reaches when they are all equally frequent.
     */
    double getEntropy();

    /**
     * @return The relative frequency of each character in the values, from the most to the least frequent, with characters that are
     *         equally frequent in ascending order. It is empty if the aggregation was not asked to show the distribution, or if there
     *         were no characters. The keys are strings of one character each.
     */
    Map<String, Double> getDistribution();
}
