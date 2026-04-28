/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.semver.expr;

import org.opensearch.Version;

import java.util.Objects;

/**
 * Expression to evaluate version compatibility within a specified range with configurable bounds.
 */
public class Range implements Expression {
    private final Version lowerBound;
    private final Version upperBound;
    private final boolean includeLower;
    private final boolean includeUpper;

    public Range() {
        this.lowerBound = Version.fromString("0.0.0");  // Minimum version
        this.upperBound = Version.fromString("99.99.99"); // Maximum version
        this.includeLower = true;  // Default to inclusive bounds
        this.includeUpper = true;
    }

    public Range(Version lowerBound, Version upperBound, boolean includeLower, boolean includeUpper) {
        if (lowerBound == null) {
            throw new IllegalArgumentException("Lower bound cannot be null");
        }
        if (upperBound == null) {
            throw new IllegalArgumentException("Upper bound cannot be null");
        }
        if (lowerBound.after(upperBound)) {
            throw new IllegalArgumentException("Lower bound must be less than or equal to upper bound");
        }
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
        this.includeLower = includeLower;
        this.includeUpper = includeUpper;
    }

    public void updateRange(Range other) {
        if (other == null) {
            throw new IllegalArgumentException("Range cannot be null");
        }
    }

    @Override
    public boolean evaluate(final Version rangeVersion, final Version versionToEvaluate) {

        boolean satisfiesLower = includeLower ? versionToEvaluate.onOrAfter(lowerBound) : versionToEvaluate.after(lowerBound);

        boolean satisfiesUpper = includeUpper ? versionToEvaluate.onOrBefore(upperBound) : versionToEvaluate.before(upperBound);

        return satisfiesLower && satisfiesUpper;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Range range = (Range) o;
        return includeLower == range.includeLower
            && includeUpper == range.includeUpper
            && Objects.equals(lowerBound, range.lowerBound)
            && Objects.equals(upperBound, range.upperBound);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lowerBound, upperBound, includeLower, includeUpper);
    }

    public boolean isIncludeLower() {
        return includeLower;
    }

    public boolean isIncludeUpper() {
        return includeUpper;
    }

    public Version getLowerBound() {
        return lowerBound;
    }

    public Version getUpperBound() {
        return upperBound;
    }

}
