/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.startree;

import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.StarTreeNodeCollector;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.DimensionFilterMergerUtils;
import org.opensearch.search.startree.filter.ExactMatchDimFilter;
import org.opensearch.search.startree.filter.RangeMatchDimFilter;
import org.opensearch.search.startree.filter.provider.DimensionFilterMapper;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.mockito.Mockito.mock;

public class DimensionFilterMergerUtilsTests extends OpenSearchTestCase {

    private DimensionFilterMapper numericMapper;
    private DimensionFilterMapper keywordMapper;
    private final SearchContext searchContext = mock(SearchContext.class);

    @Before
    public void setup() {
        numericMapper = DimensionFilterMapper.Factory.fromMappedFieldType(
            new NumberFieldMapper.NumberFieldType("status", NumberFieldMapper.NumberType.LONG),
            searchContext
        );
        keywordMapper = DimensionFilterMapper.Factory.fromMappedFieldType(new KeywordFieldMapper.KeywordFieldType("method"), searchContext);
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
        assertRangeIntersection(
            range("status", 200L, null, true, true),
            range("status", null, 500L, true, true),
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
            () -> DimensionFilterMergerUtils.intersect(
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
            () -> DimensionFilterMergerUtils.intersect(
                range("status", 200, 300, true, true),
                range("port", 80, 443, true, true),
                numericMapper
            )
        );
    }

    public void testUnsignedLongRangeIntersection() {
        // Setup unsigned long mapper
        DimensionFilterMapper unsignedLongMapper = DimensionFilterMapper.Factory.fromMappedFieldType(
            new NumberFieldMapper.NumberFieldType("unsigned_field", NumberFieldMapper.NumberType.UNSIGNED_LONG),
            searchContext
        );

        // Test case 1: Regular positive values
        assertRangeIntersection(
            range("unsigned_field", 100L, 500L, true, true),
            range("unsigned_field", 200L, 300L, true, true),
            range("unsigned_field", 200L, 300L, true, true),
            unsignedLongMapper
        );

        // Test case 2: High values (near max unsigned long)
        assertRangeIntersection(
            range("unsigned_field", -10L, -1L, true, true),  // -1L is max unsigned long (2^64 - 1)
            range("unsigned_field", -5L, -2L, true, true),
            range("unsigned_field", -5L, -2L, true, true),
            unsignedLongMapper
        );

        // Test case 3: Crossing the unsigned boundary
        assertRangeIntersection(
            range("unsigned_field", 2L, -2L, true, true),    // -2L is near max unsigned long
            range("unsigned_field", 1L, -1L, true, true),    // -1L is max unsigned long
            range("unsigned_field", 2L, -2L, true, true),
            unsignedLongMapper
        );

        // Test case 4: Non-overlapping ranges in unsigned space
        assertNoIntersection(
            range("unsigned_field", -10L, -5L, true, true),  // High unsigned values
            range("unsigned_field", 1L, 10L, true, true),    // Low unsigned values
            unsignedLongMapper
        );

        // Test case 5: Single point intersection at max unsigned value
        assertRangeIntersection(
            range("unsigned_field", -2L, -1L, true, true),   // -1L is max unsigned long
            range("unsigned_field", 0L, -1L, true, true),
            range("unsigned_field", -2L, -1L, true, true),
            unsignedLongMapper
        );

        // Test case 6: Full range
        assertRangeIntersection(
            range("unsigned_field", 0L, -1L, true, true),    // 0 to max unsigned
            range("unsigned_field", 100L, 200L, true, true),
            range("unsigned_field", 100L, 200L, true, true),
            unsignedLongMapper
        );
    }

    public void testIntersectValidation() {
        // Test null filters
        assertNull(
            "Should return null for null first filter",
            DimensionFilterMergerUtils.intersect(null, exactMatch("status", List.of(200L)), numericMapper)
        );
        assertNull(
            "Should return null for null second filter",
            DimensionFilterMergerUtils.intersect(exactMatch("status", List.of(200L)), null, numericMapper)
        );
        assertNull("Should return null for both null filters", DimensionFilterMergerUtils.intersect(null, null, numericMapper));

        // Test null dimension names
        DimensionFilter nullDimFilter1 = new ExactMatchDimFilter(null, List.of(200L));
        DimensionFilter nullDimFilter2 = new ExactMatchDimFilter(null, List.of(300L));
        IllegalArgumentException e1 = assertThrows(
            IllegalArgumentException.class,
            () -> DimensionFilterMergerUtils.intersect(nullDimFilter1, exactMatch("status", List.of(200L)), numericMapper)
        );
        assertEquals("Cannot intersect filters with null dimension name", e1.getMessage());

        IllegalArgumentException e2 = assertThrows(
            IllegalArgumentException.class,
            () -> DimensionFilterMergerUtils.intersect(exactMatch("status", List.of(200L)), nullDimFilter2, numericMapper)
        );
        assertEquals("Cannot intersect filters with null dimension name", e2.getMessage());

        IllegalArgumentException e3 = assertThrows(
            IllegalArgumentException.class,
            () -> DimensionFilterMergerUtils.intersect(nullDimFilter1, nullDimFilter2, numericMapper)
        );
        assertEquals("Cannot intersect filters with null dimension name", e3.getMessage());

        // Test different dimensions
        IllegalArgumentException e4 = assertThrows(
            IllegalArgumentException.class,
            () -> DimensionFilterMergerUtils.intersect(
                exactMatch("status", List.of(200L)),
                exactMatch("method", List.of("GET")),
                numericMapper
            )
        );
        assertEquals("Cannot intersect filters for different dimensions: status and method", e4.getMessage());
    }

    public void testUnsupportedFilterCombination() {
        // Create a custom filter type for testing
        class CustomDimensionFilter implements DimensionFilter {
            @Override
            public String getDimensionName() {
                return "status";
            }

            @Override
            public void initialiseForSegment(StarTreeValues starTreeValues, SearchContext searchContext) {}

            @Override
            public void matchStarTreeNodes(StarTreeNode parentNode, StarTreeValues starTreeValues, StarTreeNodeCollector collector) {}

            @Override
            public boolean matchDimValue(long ordinal, StarTreeValues starTreeValues) {
                return false;
            }
        }

        DimensionFilter customFilter = new CustomDimensionFilter();

        // Test unsupported combination with ExactMatchDimFilter
        IllegalArgumentException e1 = assertThrows(
            IllegalArgumentException.class,
            () -> DimensionFilterMergerUtils.intersect(customFilter, exactMatch("status", List.of(200L)), numericMapper)
        );
        assertEquals("Unsupported filter combination: CustomDimensionFilter and ExactMatchDimFilter", e1.getMessage());

        // Test unsupported combination with RangeMatchDimFilter
        IllegalArgumentException e2 = assertThrows(
            IllegalArgumentException.class,
            () -> DimensionFilterMergerUtils.intersect(range("status", 200L, 300L, true, true), customFilter, numericMapper)
        );
        assertEquals("Unsupported filter combination: RangeMatchDimFilter and CustomDimensionFilter", e2.getMessage());
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
        DimensionFilter result = DimensionFilterMergerUtils.intersect(filter1, filter2, mapper);
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
        DimensionFilter result = DimensionFilterMergerUtils.intersect(filter1, filter2, mapper);
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
        DimensionFilter result = DimensionFilterMergerUtils.intersect(range, exact, mapper);
        assertTrue(result instanceof ExactMatchDimFilter);
        ExactMatchDimFilter exactResult = (ExactMatchDimFilter) result;
        assertEquals(new HashSet<>(expected.getRawValues()), new HashSet<>(exactResult.getRawValues()));
    }

    private void assertNoIntersection(DimensionFilter filter1, DimensionFilter filter2, DimensionFilterMapper mapper) {
        assertNull(DimensionFilterMergerUtils.intersect(filter1, filter2, mapper));
    }
}
