/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

/**
 * Box plot statistics over a set of values: the minimum, maximum and quartiles, plus the lower and upper whiskers.
 * <p>
 * The whiskers mark the most extreme values that still lie within 1.5 times the interquartile range of the first and third quartile,
 * so that outliers beyond them can be told apart from the rest of the values. Like the quartiles, the whiskers are approximations
 * derived from a t-digest of the values, while the minimum and maximum are exact.
 *
 * @opensearch.internal
 */
public interface Boxplot extends NumericMetricsAggregation.MultiValue {

    /**
     * @return The minimum of all aggregated values.
     */
    double getMin();

    /**
     * @return The maximum of all aggregated values.
     */
    double getMax();

    /**
     * @return The first quartile, the 25th percentile, of all aggregated values.
     */
    double getQ1();

    /**
     * @return The second quartile, the median, of all aggregated values.
     */
    double getQ2();

    /**
     * @return The third quartile, the 75th percentile, of all aggregated values.
     */
    double getQ3();

    /**
     * @return The lower whisker: an approximation, derived from the t-digest, of the smallest value that is not an outlier below
     *         the first quartile. It may not be one of the aggregated values.
     */
    double getLower();

    /**
     * @return The upper whisker: an approximation, derived from the t-digest, of the largest value that is not an outlier above
     *         the third quartile. It may not be one of the aggregated values.
     */
    double getUpper();

    /**
     * @return The minimum of all aggregated values as a String.
     */
    String getMinAsString();

    /**
     * @return The maximum of all aggregated values as a String.
     */
    String getMaxAsString();

    /**
     * @return The first quartile as a String.
     */
    String getQ1AsString();

    /**
     * @return The second quartile, the median, as a String.
     */
    String getQ2AsString();

    /**
     * @return The third quartile as a String.
     */
    String getQ3AsString();

    /**
     * @return The lower whisker as a String.
     */
    String getLowerAsString();

    /**
     * @return The upper whisker as a String.
     */
    String getUpperAsString();
}
