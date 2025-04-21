/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.startree;

import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.DimensionFilterMerger;
import org.opensearch.search.startree.filter.ExactMatchDimFilter;
import org.opensearch.search.startree.filter.RangeMatchDimFilter;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class DimensionFilterMergerTests extends OpenSearchTestCase {

    public void testRangeIntersection() {
        // Basic range intersection
        assertRangeIntersection(
            range("status", 200L, 500L, true, true),
            range("status", 300L, 400L, true, true),
            range("status", 300L, 400L, true, true)
        );

        // Boundary conditions
        assertRangeIntersection(
            range("status", 200L, 200L, true, true),
            range("status", 200L, 200L, true, true),
            range("status", 200L, 200L, true, true)
        );

        // Inclusive/Exclusive boundaries
        assertRangeIntersection(
            range("status", 200L, 300L, true, false),
            range("status", 200L, 300L, false, true),
            range("status", 200L, 300L, false, false)
        );

        // Non-overlapping ranges
        assertNoIntersection(range("status", 200L, 300L, true, true), range("status", 301L, 400L, true, true));

        // Exactly touching ranges (no overlap)
        assertNoIntersection(range("status", 200L, 300L, true, false), range("status", 300L, 400L, true, true));

        // Null bounds (unbounded ranges)
        assertRangeIntersection(
            range("status", null, 500L, true, true),
            range("status", 200L, null, true, true),
            range("status", 200L, 500L, true, true)
        );

        // Single point overlap
        assertRangeIntersection(
            range("status", 200L, 300L, true, true),
            range("status", 300L, 400L, true, true),
            range("status", 300L, 300L, true, true)
        );

        // Very large ranges
        assertRangeIntersection(
            range("status", Long.MIN_VALUE, Long.MAX_VALUE, true, true),
            range("status", 200L, 300L, true, true),
            range("status", 200L, 300L, true, true)
        );

        // Zero-width ranges
        assertNoIntersection(range("status", 200L, 200L, true, true), range("status", 200L, 200L, false, false));

        // incompatible types
        assertThrows(
            IllegalArgumentException.class,
            () -> DimensionFilterMerger.intersect(range("status", "200", "300", true, true), range("status", 200, 300, true, true))
        );
    }

    public void testExactMatchIntersection() {
        // Single value intersection
        assertExactMatchIntersection(
            exactMatch("status", List.of(200)),
            exactMatch("status", List.of(200)),
            exactMatch("status", List.of(200))
        );

        // Multiple values intersection
        assertExactMatchIntersection(
            exactMatch("status", Arrays.asList(200, 300, 400)),
            exactMatch("status", Arrays.asList(300, 400, 500)),
            exactMatch("status", Arrays.asList(300, 400))
        );

        // No intersection
        assertNoIntersection(exactMatch("status", List.of(200)), exactMatch("status", List.of(300)));

        // Empty list
        assertNoIntersection(exactMatch("status", Collections.emptyList()), exactMatch("status", List.of(200)));

        // Duplicate values
        assertExactMatchIntersection(
            exactMatch("status", Arrays.asList(200, 200, 300)),
            exactMatch("status", Arrays.asList(200, 300, 300)),
            exactMatch("status", Arrays.asList(200, 300))
        );

        // Special characters in string values
        assertExactMatchIntersection(
            exactMatch("method", Arrays.asList("GET", "GET*")),
            exactMatch("method", Arrays.asList("GET", "GET/")),
            exactMatch("method", List.of("GET"))
        );

        // Case sensitivity
        assertNoIntersection(exactMatch("method", Arrays.asList("GET", "Post")), exactMatch("method", Arrays.asList("get", "POST")));
    }

    public void testRangeExactMatchIntersection() {
        // Value in range
        assertRangeExactMatchIntersection(
            range("status", 200L, 300L, true, true),
            exactMatch("status", List.of(250L)),
            exactMatch("status", List.of(250L))
        );

        // Value at range boundaries
        assertRangeExactMatchIntersection(
            range("status", 200L, 300L, true, true),
            exactMatch("status", Arrays.asList(200L, 300L)),
            exactMatch("status", Arrays.asList(200L, 300L))
        );

        // Value at exclusive boundaries
        assertRangeExactMatchIntersection(
            range("status", 200L, 300L, false, false),
            exactMatch("status", Arrays.asList(201L, 299L)),
            exactMatch("status", Arrays.asList(201L, 299L))
        );

        // No values in range
        assertNoIntersection(range("status", 200L, 300L, true, true), exactMatch("status", Arrays.asList(199L, 301L)));

        // Multiple values, some in range
        assertRangeExactMatchIntersection(
            range("status", 200L, 300L, true, true),
            exactMatch("status", Arrays.asList(199L, 200L, 250L, 300L, 301L)),
            exactMatch("status", Arrays.asList(200L, 250L, 300L))
        );
    }

    public void testDifferentDimensions() {
        // Cannot intersect different dimensions
        assertThrows(
            IllegalArgumentException.class,
            () -> DimensionFilterMerger.intersect(range("status", 200, 300, true, true), range("port", 80, 443, true, true))
        );
    }

    // Helper methods
    private RangeMatchDimFilter range(String dimension, Object low, Object high, boolean includeLow, boolean includeHigh) {
        return new RangeMatchDimFilter(dimension, low, high, includeLow, includeHigh);
    }

    private ExactMatchDimFilter exactMatch(String dimension, List<Object> values) {
        return new ExactMatchDimFilter(dimension, values);
    }

    private void assertRangeIntersection(RangeMatchDimFilter filter1, RangeMatchDimFilter filter2, RangeMatchDimFilter expected) {
        DimensionFilter result = DimensionFilterMerger.intersect(filter1, filter2);
        assertTrue(result instanceof RangeMatchDimFilter);
        RangeMatchDimFilter rangeResult = (RangeMatchDimFilter) result;
        assertEquals(expected.getLow(), rangeResult.getLow());
        assertEquals(expected.getHigh(), rangeResult.getHigh());
        assertEquals(expected.isIncludeLow(), rangeResult.isIncludeLow());
        assertEquals(expected.isIncludeHigh(), rangeResult.isIncludeHigh());
    }

    private void assertExactMatchIntersection(ExactMatchDimFilter filter1, ExactMatchDimFilter filter2, ExactMatchDimFilter expected) {
        DimensionFilter result = DimensionFilterMerger.intersect(filter1, filter2);
        assertTrue(result instanceof ExactMatchDimFilter);
        ExactMatchDimFilter exactResult = (ExactMatchDimFilter) result;
        assertEquals(new HashSet<>(expected.getRawValues()), new HashSet<>(exactResult.getRawValues()));
    }

    private void assertRangeExactMatchIntersection(RangeMatchDimFilter range, ExactMatchDimFilter exact, ExactMatchDimFilter expected) {
        DimensionFilter result = DimensionFilterMerger.intersect(range, exact);
        assertTrue(result instanceof ExactMatchDimFilter);
        ExactMatchDimFilter exactResult = (ExactMatchDimFilter) result;
        assertEquals(new HashSet<>(expected.getRawValues()), new HashSet<>(exactResult.getRawValues()));
    }

    private void assertNoIntersection(DimensionFilter filter1, DimensionFilter filter2) {
        assertNull(DimensionFilterMerger.intersect(filter1, filter2));
    }
}
