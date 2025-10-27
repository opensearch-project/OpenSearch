/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree.filter;

import org.opensearch.search.startree.filter.provider.DimensionFilterMapper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Utility class for merging different types of {@link DimensionFilter}
 * Handles intersection operations between {@link ExactMatchDimFilter} and {@link RangeMatchDimFilter}
 */
public class DimensionFilterMergerUtils {

    /**
     * Gets intersection of two DimensionFilters
     * Returns null if intersection results in no possible matches.
     */
    public static DimensionFilter intersect(DimensionFilter filter1, DimensionFilter filter2, DimensionFilterMapper mapper) {
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
        if (filter1 instanceof RangeMatchDimFilter rangeFilter1 && filter2 instanceof RangeMatchDimFilter rangeFilter2) {
            return intersectRangeFilters(rangeFilter1, rangeFilter2, mapper);
        }

        // Handle ExactMatch + ExactMatch combination
        if (filter1 instanceof ExactMatchDimFilter exactFilter1 && filter2 instanceof ExactMatchDimFilter exactFilter2) {
            return intersectExactMatchFilters(exactFilter1, exactFilter2);
        }

        // Handle Range + ExactMatch combination
        if (filter1 instanceof RangeMatchDimFilter rangeFilter && filter2 instanceof ExactMatchDimFilter exactFilter) {
            return intersectRangeWithExactMatch(rangeFilter, exactFilter, mapper);
        }

        // Handle ExactMatch + Range combination
        if (filter1 instanceof ExactMatchDimFilter exactFilter && filter2 instanceof RangeMatchDimFilter rangeFilter) {
            return intersectRangeWithExactMatch(rangeFilter, exactFilter, mapper);
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
    private static DimensionFilter intersectRangeFilters(
        RangeMatchDimFilter range1,
        RangeMatchDimFilter range2,
        DimensionFilterMapper mapper
    ) {
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
            int comparison = mapper.compareValues(low1, low2);
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
            int comparison = mapper.compareValues(high1, high2);
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
            if (!mapper.isValidRange(newLow, newHigh, includeLow, includeHigh)) {
                return null; // No overlap
            }
        }

        String effectiveSubDimension = mapper.resolveUsingSubDimension()
            ? mapper.getSubDimensionFieldEffective(range1.getSubDimensionName(), range2.getSubDimensionName())
            : null;

        return new RangeMatchDimFilter(range1.getDimensionName(), newLow, newHigh, includeLow, includeHigh) {
            @Override
            public String getSubDimensionName() {
                return effectiveSubDimension;
            }
        };
    }

    /**
     * Intersects two exact match filters
     * Returns null if no common values
     */
    private static DimensionFilter intersectExactMatchFilters(ExactMatchDimFilter exact1, ExactMatchDimFilter exact2) {
        List<Object> values1 = exact1.getRawValues();
        Set<Object> values2Set = new HashSet<>(exact2.getRawValues());

        List<Object> intersection = new ArrayList<>();
        for (Object value : values1) {
            if (values2Set.contains(value)) {
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
    private static DimensionFilter intersectRangeWithExactMatch(
        RangeMatchDimFilter range,
        ExactMatchDimFilter exact,
        DimensionFilterMapper mapper
    ) {
        List<Object> validValues = new ArrayList<>();

        for (Object value : exact.getRawValues()) {
            if (isValueInRange(value, range, mapper)) {
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
    private static boolean isValueInRange(Object value, RangeMatchDimFilter range, DimensionFilterMapper mapper) {
        return mapper.isValueInRange(value, range.getLow(), range.getHigh(), range.isIncludeLow(), range.isIncludeHigh());
    }
}
