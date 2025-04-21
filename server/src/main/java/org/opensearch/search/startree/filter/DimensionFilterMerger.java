/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree.filter;

import org.apache.lucene.util.BytesRef;

import java.util.ArrayList;
import java.util.List;

public class DimensionFilterMerger {

    /**
     * Gets intersection of two DimensionFilters
     * Returns null if intersection results in no possible matches.
     */
    public static DimensionFilter intersect(DimensionFilter filter1, DimensionFilter filter2) {
        if (filter1 == null || filter2 == null) {
            return null;
        }

        if (filter1.getDimensionName() == null || filter2.getDimensionName() == null) {
            throw new IllegalArgumentException("Cannot intersect filters with null dimension name");
        }

        // Verify filters are for same dimension
        if (!filter1.getDimensionName().equals(filter2.getDimensionName())) {
            throw new IllegalArgumentException(
                "Cannot intersect filters for different dimensions: " + filter1.getDimensionName() + " and " + filter2.getDimensionName()
            );
        }

        // Handle Range + Range combination
        if (filter1 instanceof RangeMatchDimFilter && filter2 instanceof RangeMatchDimFilter) {
            return intersectRangeFilters((RangeMatchDimFilter) filter1, (RangeMatchDimFilter) filter2);
        }

        // Handle ExactMatch + ExactMatch combination
        if (filter1 instanceof ExactMatchDimFilter && filter2 instanceof ExactMatchDimFilter) {
            return intersectExactMatchFilters((ExactMatchDimFilter) filter1, (ExactMatchDimFilter) filter2);
        }

        // Handle Range + ExactMatch combination
        if (filter1 instanceof RangeMatchDimFilter && filter2 instanceof ExactMatchDimFilter) {
            return intersectRangeWithExactMatch((RangeMatchDimFilter) filter1, (ExactMatchDimFilter) filter2);
        }

        // Handle ExactMatch + Range combination
        if (filter1 instanceof ExactMatchDimFilter && filter2 instanceof RangeMatchDimFilter) {
            return intersectRangeWithExactMatch((RangeMatchDimFilter) filter2, (ExactMatchDimFilter) filter1);
        }

        // throw exception for unsupported exception
        throw new IllegalArgumentException(
            "Unsupported filter combination: " + filter1.getClass().getSimpleName() + " and " + filter2.getClass().getSimpleName()
        );
    }

    /**
     * Intersects two range filters
     * Returns null if ranges don't overlap
     */
    private static DimensionFilter intersectRangeFilters(RangeMatchDimFilter range1, RangeMatchDimFilter range2) {
        Object low1 = range1.getLow();
        Object high1 = range1.getHigh();
        Object low2 = range2.getLow();
        Object high2 = range2.getHigh();

        // Find the more restrictive bounds
        Object newLow;
        boolean includeLow;
        if (low1 == null) {
            newLow = low2;
            includeLow = range2.isIncludeLow();
        } else if (low2 == null) {
            newLow = low1;
            includeLow = range1.isIncludeLow();
        } else {
            int comparison = compareValues(low1, low2);
            if (comparison > 0) {
                newLow = low1;
                includeLow = range1.isIncludeLow();
            } else if (comparison < 0) {
                newLow = low2;
                includeLow = range2.isIncludeLow();
            } else {
                newLow = low1;
                includeLow = range1.isIncludeLow() && range2.isIncludeLow();
            }
        }

        Object newHigh;
        boolean includeHigh;
        if (high1 == null) {
            newHigh = high2;
            includeHigh = range2.isIncludeHigh();
        } else if (high2 == null) {
            newHigh = high1;
            includeHigh = range1.isIncludeHigh();
        } else {
            int comparison = compareValues(high1, high2);
            if (comparison < 0) {
                newHigh = high1;
                includeHigh = range1.isIncludeHigh();
            } else if (comparison > 0) {
                newHigh = high2;
                includeHigh = range2.isIncludeHigh();
            } else {
                newHigh = high1;
                includeHigh = range1.isIncludeHigh() && range2.isIncludeHigh();
            }
        }

        // Check if range is valid
        if (newLow != null && newHigh != null) {
            int comparison = compareValues(newLow, newHigh);
            if (comparison > 0 || (comparison == 0 && (!includeLow || !includeHigh))) {
                return null; // No overlap
            }
        }

        return new RangeMatchDimFilter(range1.getDimensionName(), newLow, newHigh, includeLow, includeHigh);
    }

    /**
     * Intersects two exact match filters
     * Returns null if no common values
     */
    private static DimensionFilter intersectExactMatchFilters(ExactMatchDimFilter exact1, ExactMatchDimFilter exact2) {
        List<Object> values1 = exact1.getRawValues();
        List<Object> values2 = exact2.getRawValues();

        List<Object> intersection = new ArrayList<>();
        for (Object value : values1) {
            if (values2.contains(value)) {
                intersection.add(value);
            }
        }

        if (intersection.isEmpty()) {
            return null;
        }

        return new ExactMatchDimFilter(exact1.getDimensionName(), intersection);
    }

    /**
     * Intersects a range filter with an exact match filter.
     * Returns null if no values from exact match are within range.
     */
    private static DimensionFilter intersectRangeWithExactMatch(RangeMatchDimFilter range, ExactMatchDimFilter exact) {
        List<Object> validValues = new ArrayList<>();

        for (Object value : exact.getRawValues()) {
            if (isValueInRange(value, range)) {
                validValues.add(value);
            }
        }

        if (validValues.isEmpty()) {
            return null;
        }

        return new ExactMatchDimFilter(exact.getDimensionName(), validValues);
    }

    /**
     * Checks if a value falls within a range.
     */
    private static boolean isValueInRange(Object value, RangeMatchDimFilter range) {
        if (range.getLow() != null) {
            int comparison = compareValues(value, range.getLow());
            if (comparison < 0 || (comparison == 0 && !range.isIncludeLow())) {
                return false;
            }
        }

        if (range.getHigh() != null) {
            int comparison = compareValues(value, range.getHigh());
            if (comparison > 0 || (comparison == 0 && !range.isIncludeHigh())) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compares two values - Long for numeric fields and BytesRef for keywords field
     */
    private static int compareValues(Object v1, Object v2) {
        if (v1 instanceof Long && v2 instanceof Long) {
            return Long.compare((Long) v1, (Long) v2);
        }

        if (v1 instanceof BytesRef && v2 instanceof BytesRef) {
            return ((BytesRef) v1).compareTo((BytesRef) v2);
        }

        throw new IllegalArgumentException(
            "Can only compare Long or BytesRef values, got types: " + v1.getClass().getName() + " and " + v2.getClass().getName()
        );
    }
}
