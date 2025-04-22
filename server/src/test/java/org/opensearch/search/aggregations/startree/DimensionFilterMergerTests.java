/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.startree;

import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.DimensionFilterMerger;
import org.opensearch.search.startree.filter.ExactMatchDimFilter;
import org.opensearch.search.startree.filter.RangeMatchDimFilter;
import org.opensearch.search.startree.filter.provider.DimensionFilterMapper;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class DimensionFilterMergerTests extends OpenSearchTestCase {

    private DimensionFilterMapper numericMapper;
    private DimensionFilterMapper keywordMapper;

    @Before
    public void setup() {
        numericMapper = DimensionFilterMapper.Factory.fromMappedFieldType(
            new NumberFieldMapper.NumberFieldType("status", NumberFieldMapper.NumberType.LONG)
        );
        keywordMapper = DimensionFilterMapper.Factory.fromMappedFieldType(new KeywordFieldMapper.KeywordFieldType("method"));
    }

    public void testRangeIntersection() {
        // Basic range intersection
        assertRangeIntersection(
            range("status", 200L, 500L, true, true),
            range("status", 300L, 400L, true, true),
            range("status", 300L, 400L, true, true),
            numericMapper
        );

        // Boundary conditions
        assertRangeIntersection(
            range("status", 200L, 200L, true, true),
            range("status", 200L, 200L, true, true),
            range("status", 200L, 200L, true, true),
            numericMapper
        );

        // Inclusive/Exclusive boundaries
        assertRangeIntersection(
            range("status", 200L, 300L, true, false),
            range("status", 200L, 300L, false, true),
            range("status", 200L, 300L, false, false),
            numericMapper
        );

        // Non-overlapping ranges
        assertNoIntersection(range("status", 200L, 300L, true, true), range("status", 301L, 400L, true, true), numericMapper);

        // Exactly touching ranges (no overlap)
        assertNoIntersection(range("status", 200L, 300L, true, false), range("status", 300L, 400L, true, true), numericMapper);

        // Null bounds (unbounded ranges)
        assertRangeIntersection(
            range("status", null, 500L, true, true),
            range("status", 200L, null, true, true),
            range("status", 200L, 500L, true, true),
            numericMapper
        );

        // Single point overlap
        assertRangeIntersection(
            range("status", 200L, 300L, true, true),
            range("status", 300L, 400L, true, true),
            range("status", 300L, 300L, true, true),
            numericMapper
        );

        // Very large ranges
        assertRangeIntersection(
            range("status", Long.MIN_VALUE, Long.MAX_VALUE, true, true),
            range("status", 200L, 300L, true, true),
            range("status", 200L, 300L, true, true),
            numericMapper
        );

        // Zero-width ranges
        assertNoIntersection(range("status", 200L, 200L, true, true), range("status", 200L, 200L, false, false), numericMapper);

        // incompatible types
        assertThrows(
            IllegalArgumentException.class,
            () -> DimensionFilterMerger.intersect(
                range("status", "200", "300", true, true),
                range("status", 200, 300, true, true),
                numericMapper
            )
        );
    }

    public void testExactMatchIntersection() {
        // Single value intersection
        assertExactMatchIntersection(
            exactMatch("status", List.of(200)),
            exactMatch("status", List.of(200)),
            exactMatch("status", List.of(200)),
            numericMapper
        );

        // Multiple values intersection
        assertExactMatchIntersection(
            exactMatch("status", Arrays.asList(200, 300, 400)),
            exactMatch("status", Arrays.asList(300, 400, 500)),
            exactMatch("status", Arrays.asList(300, 400)),
            numericMapper
        );

        // No intersection
        assertNoIntersection(exactMatch("status", List.of(200)), exactMatch("status", List.of(300)), numericMapper);

        // Empty list
        assertNoIntersection(exactMatch("status", Collections.emptyList()), exactMatch("status", List.of(200)), numericMapper);

        // Duplicate values
        assertExactMatchIntersection(
            exactMatch("status", Arrays.asList(200, 200, 300)),
            exactMatch("status", Arrays.asList(200, 300, 300)),
            exactMatch("status", Arrays.asList(200, 300)),
            numericMapper
        );

        // Special characters in string values
        assertExactMatchIntersection(
            exactMatch("method", Arrays.asList("GET", "GET*")),
            exactMatch("method", Arrays.asList("GET", "GET/")),
            exactMatch("method", List.of("GET")),
            keywordMapper
        );

        // Case sensitivity
        assertNoIntersection(
            exactMatch("method", Arrays.asList("GET", "Post")),
            exactMatch("method", Arrays.asList("get", "POST")),
            keywordMapper
        );
    }

    public void testRangeExactMatchIntersection() {
        // Value in range
        assertRangeExactMatchIntersection(
            range("status", 200L, 300L, true, true),
            exactMatch("status", List.of(250L)),
            exactMatch("status", List.of(250L)),
            numericMapper
        );

        // Value at range boundaries
        assertRangeExactMatchIntersection(
            range("status", 200L, 300L, true, true),
            exactMatch("status", Arrays.asList(200L, 300L)),
            exactMatch("status", Arrays.asList(200L, 300L)),
            numericMapper
        );

        // Value at exclusive boundaries
        assertRangeExactMatchIntersection(
            range("status", 200L, 300L, false, false),
            exactMatch("status", Arrays.asList(201L, 299L)),
            exactMatch("status", Arrays.asList(201L, 299L)),
            numericMapper
        );

        // No values in range
        assertNoIntersection(range("status", 200L, 300L, true, true), exactMatch("status", Arrays.asList(199L, 301L)), numericMapper);

        // Multiple values, some in range
        assertRangeExactMatchIntersection(
            range("status", 200L, 300L, true, true),
            exactMatch("status", Arrays.asList(199L, 200L, 250L, 300L, 301L)),
            exactMatch("status", Arrays.asList(200L, 250L, 300L)),
            numericMapper
        );
    }

    public void testDifferentDimensions() {
        // Cannot intersect different dimensions
        assertThrows(
            IllegalArgumentException.class,
            () -> DimensionFilterMerger.intersect(range("status", 200, 300, true, true), range("port", 80, 443, true, true), numericMapper)
        );
    }

    // Helper methods
    private RangeMatchDimFilter range(String dimension, Object low, Object high, boolean includeLow, boolean includeHigh) {
        return new RangeMatchDimFilter(dimension, low, high, includeLow, includeHigh);
    }

    private ExactMatchDimFilter exactMatch(String dimension, List<Object> values) {
        return new ExactMatchDimFilter(dimension, values);
    }

    private void assertRangeIntersection(
        RangeMatchDimFilter filter1,
        RangeMatchDimFilter filter2,
        RangeMatchDimFilter expected,
        DimensionFilterMapper mapper
    ) {
        DimensionFilter result = DimensionFilterMerger.intersect(filter1, filter2, mapper);
        assertTrue(result instanceof RangeMatchDimFilter);
        RangeMatchDimFilter rangeResult = (RangeMatchDimFilter) result;
        assertEquals(expected.getLow(), rangeResult.getLow());
        assertEquals(expected.getHigh(), rangeResult.getHigh());
        assertEquals(expected.isIncludeLow(), rangeResult.isIncludeLow());
        assertEquals(expected.isIncludeHigh(), rangeResult.isIncludeHigh());
    }

    private void assertExactMatchIntersection(
        ExactMatchDimFilter filter1,
        ExactMatchDimFilter filter2,
        ExactMatchDimFilter expected,
        DimensionFilterMapper mapper
    ) {
        DimensionFilter result = DimensionFilterMerger.intersect(filter1, filter2, mapper);
        assertTrue(result instanceof ExactMatchDimFilter);
        ExactMatchDimFilter exactResult = (ExactMatchDimFilter) result;
        assertEquals(new HashSet<>(expected.getRawValues()), new HashSet<>(exactResult.getRawValues()));
    }

    private void assertRangeExactMatchIntersection(
        RangeMatchDimFilter range,
        ExactMatchDimFilter exact,
        ExactMatchDimFilter expected,
        DimensionFilterMapper mapper
    ) {
        DimensionFilter result = DimensionFilterMerger.intersect(range, exact, mapper);
        assertTrue(result instanceof ExactMatchDimFilter);
        ExactMatchDimFilter exactResult = (ExactMatchDimFilter) result;
        assertEquals(new HashSet<>(expected.getRawValues()), new HashSet<>(exactResult.getRawValues()));
    }

    private void assertNoIntersection(DimensionFilter filter1, DimensionFilter filter2, DimensionFilterMapper mapper) {
        assertNull(DimensionFilterMerger.intersect(filter1, filter2, mapper));
    }
}
