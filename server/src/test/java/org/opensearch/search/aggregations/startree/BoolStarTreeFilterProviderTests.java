/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.startree;

import org.apache.lucene.util.BytesRef;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.OrdinalDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.StarTreeMapper;
import org.opensearch.index.mapper.WildcardFieldMapper;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.ExactMatchDimFilter;
import org.opensearch.search.startree.filter.RangeMatchDimFilter;
import org.opensearch.search.startree.filter.StarTreeFilter;
import org.opensearch.search.startree.filter.provider.StarTreeFilterProvider;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BoolStarTreeFilterProviderTests extends OpenSearchTestCase {
    private SearchContext searchContext;
    private MapperService mapperService;
    private CompositeDataCubeFieldType compositeFieldType;

    @Before
    public void setup() {
        // Setup common test dependencies
        searchContext = mock(SearchContext.class);
        mapperService = mock(MapperService.class);
        when(searchContext.mapperService()).thenReturn(mapperService);

        // Setup field types
        KeywordFieldMapper.KeywordFieldType methodType = new KeywordFieldMapper.KeywordFieldType("method");
        NumberFieldMapper.NumberFieldType statusType = new NumberFieldMapper.NumberFieldType(
            "status",
            NumberFieldMapper.NumberType.INTEGER
        );
        NumberFieldMapper.NumberFieldType portType = new NumberFieldMapper.NumberFieldType("port", NumberFieldMapper.NumberType.INTEGER);
        KeywordFieldMapper.KeywordFieldType zoneType = new KeywordFieldMapper.KeywordFieldType("zone");
        NumberFieldMapper.NumberFieldType responseTimeType = new NumberFieldMapper.NumberFieldType(
            "response_time",
            NumberFieldMapper.NumberType.INTEGER
        );
        when(mapperService.fieldType("method")).thenReturn(methodType);
        when(mapperService.fieldType("status")).thenReturn(statusType);
        when(mapperService.fieldType("port")).thenReturn(portType);
        when(mapperService.fieldType("zone")).thenReturn(zoneType);
        when(mapperService.fieldType("response_time")).thenReturn(responseTimeType);

        // Create composite field type with dimensions
        compositeFieldType = new StarTreeMapper.StarTreeFieldType(
            "star_tree",
            new StarTreeField(
                "star_tree",
                List.of(
                    new OrdinalDimension("method"),
                    new OrdinalDimension("status"),
                    new OrdinalDimension("port"),
                    new OrdinalDimension("zone"),
                    new OrdinalDimension("response_time")
                ),
                List.of(new Metric("size", List.of(MetricStat.SUM))),
                new StarTreeFieldConfiguration(
                    randomIntBetween(1, 10_000),
                    Collections.emptySet(),
                    StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP
                )
            )
        );
    }

    public void testSimpleMustWithMultipleDimensions() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"))
            .must(new TermQueryBuilder("status", 200));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have two dimensions", 2, filter.getDimensions().size());
        assertTrue("Should contain method dimension", filter.getDimensions().contains("method"));
        assertTrue("Should contain status dimension", filter.getDimensions().contains("status"));

        List<DimensionFilter> methodFilters = filter.getFiltersForDimension("method");
        assertEquals("Should have one filter for method", 1, methodFilters.size());
        assertTrue("Should be ExactMatchDimFilter", methodFilters.getFirst() instanceof ExactMatchDimFilter);
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "GET");

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertEquals("Should have one filter for status", 1, statusFilters.size());
        assertTrue("Should be ExactMatchDimFilter", statusFilters.getFirst() instanceof ExactMatchDimFilter);
        assertExactMatchValue((ExactMatchDimFilter) statusFilters.getFirst(), 200L);
    }

    public void testNestedMustClauses() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"))
            .must(new BoolQueryBuilder().must(new TermQueryBuilder("status", 200)).must(new TermQueryBuilder("port", 443)));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have three dimensions", 3, filter.getDimensions().size());

        // Verify method filter
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension("method");
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "GET");

        // Verify status filter
        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertExactMatchValue((ExactMatchDimFilter) statusFilters.getFirst(), 200L);

        // Verify port filter
        List<DimensionFilter> portFilters = filter.getFiltersForDimension("port");
        assertExactMatchValue((ExactMatchDimFilter) portFilters.getFirst(), 443L);
    }

    public void testMustWithDifferentQueryTypes() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"))
            .must(new TermsQueryBuilder("status", Arrays.asList(200, 201)))
            .must(new RangeQueryBuilder("port").gte(80).lte(443));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);

        // Verify method filter
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension("method");
        assertTrue("Method should be ExactMatchDimFilter", methodFilters.getFirst() instanceof ExactMatchDimFilter);
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "GET");

        // Verify status filter
        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertTrue("Status should be ExactMatchDimFilter", statusFilters.getFirst() instanceof ExactMatchDimFilter);
        Set<Object> expectedStatusValues = Set.of(200L, 201L);
        Set<Object> actualStatusValues = new HashSet<>(((ExactMatchDimFilter) statusFilters.getFirst()).getRawValues());
        assertEquals("Status should have expected values", expectedStatusValues, actualStatusValues);

        // Verify port filter
        List<DimensionFilter> portFilters = filter.getFiltersForDimension("port");
        assertTrue("Port should be RangeMatchDimFilter", portFilters.getFirst() instanceof RangeMatchDimFilter);
        RangeMatchDimFilter portRange = (RangeMatchDimFilter) portFilters.getFirst();
        assertEquals("Port lower bound should be 80", 80L, portRange.getLow());
        assertEquals("Port upper bound should be 443", 443L, portRange.getHigh());
        assertTrue("Port bounds should be inclusive", portRange.isIncludeLow() && portRange.isIncludeHigh());
    }

    public void testMustWithSameDimension() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("status", 200))
            .must(new TermQueryBuilder("status", 404));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        // this should return null as same dimension in MUST is logically impossible
        assertNull("Filter should be null for same dimension in MUST", filter);
    }

    public void testEmptyMustClause() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for empty bool query", filter);
    }

    public void testShouldWithSameDimension() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().should(new TermQueryBuilder("status", 200))
            .should(new TermQueryBuilder("status", 404));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());
        assertTrue("Should contain status dimension", filter.getDimensions().contains("status"));

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertEquals("Should have two filters for status", 2, statusFilters.size());
        assertTrue("Both should be ExactMatchDimFilter", statusFilters.stream().allMatch(f -> f instanceof ExactMatchDimFilter));

        Set<Object> expectedValues = Set.of(200L, 404L);
        Set<Object> actualValues = new HashSet<>();
        for (DimensionFilter dimensionFilter : statusFilters) {
            actualValues.addAll(((ExactMatchDimFilter) dimensionFilter).getRawValues());
        }
        assertEquals("Should contain expected status values", expectedValues, actualValues);
    }

    public void testShouldWithSameDimensionRange() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().should(new RangeQueryBuilder("status").gte(200).lte(300))
            .should(new RangeQueryBuilder("status").gte(400).lte(500));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertEquals("Should have two filters for status", 2, statusFilters.size());
        assertTrue("Both should be RangeMatchDimFilter", statusFilters.stream().allMatch(f -> f instanceof RangeMatchDimFilter));

        // Verify first range
        RangeMatchDimFilter firstRange = (RangeMatchDimFilter) statusFilters.getFirst();
        assertEquals("First range lower bound should be 200", 200L, firstRange.getLow());
        assertEquals("First range upper bound should be 300", 300L, firstRange.getHigh());
        assertTrue("First range lower bound should be inclusive", firstRange.isIncludeLow());
        assertTrue("First range upper bound should be inclusive", firstRange.isIncludeHigh());

        // Verify second range
        RangeMatchDimFilter secondRange = (RangeMatchDimFilter) statusFilters.get(1);
        assertEquals("Second range lower bound should be 400", 400L, secondRange.getLow());
        assertEquals("Second range upper bound should be 500", 500L, secondRange.getHigh());
        assertTrue("Second range lower bound should be inclusive", secondRange.isIncludeLow());
        assertTrue("Second range upper bound should be inclusive", secondRange.isIncludeHigh());
    }

    public void testShouldWithSameDimensionMixed() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().should(new TermQueryBuilder("status", 200))
            .should(new RangeQueryBuilder("status").gte(400).lte(500));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertEquals("Should have two filters for status", 2, statusFilters.size());

        // Find and verify exact match filter
        Optional<ExactMatchDimFilter> exactFilter = statusFilters.stream()
            .filter(f -> f instanceof ExactMatchDimFilter)
            .map(f -> (ExactMatchDimFilter) f)
            .findFirst();
        assertTrue("Should have exact match filter", exactFilter.isPresent());
        assertEquals("Exact match should be 200", 200L, exactFilter.get().getRawValues().get(0));

        // Find and verify range filter
        Optional<RangeMatchDimFilter> rangeFilter = statusFilters.stream()
            .filter(f -> f instanceof RangeMatchDimFilter)
            .map(f -> (RangeMatchDimFilter) f)
            .findFirst();
        assertTrue("Should have range filter", rangeFilter.isPresent());
        assertEquals("Range lower bound should be 400", 400L, rangeFilter.get().getLow());
        assertEquals("Range upper bound should be 500", 500L, rangeFilter.get().getHigh());
        assertTrue("Range lower bound should be inclusive", rangeFilter.get().isIncludeLow());
        assertTrue("Range upper bound should be inclusive", rangeFilter.get().isIncludeHigh());
    }

    public void testShouldWithDifferentDimensions() throws IOException {
        // SHOULD with different dimensions (should be rejected)
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().should(new TermQueryBuilder("status", 200))
            .should(new TermQueryBuilder("method", "GET"));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for SHOULD across different dimensions", filter);
    }

    public void testNestedShouldSameDimension() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().should(new TermQueryBuilder("status", 200))
            .should(new BoolQueryBuilder().should(new TermQueryBuilder("status", 404)).should(new TermQueryBuilder("status", 500)));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertEquals("Should have three filters for status", 3, statusFilters.size());
        assertTrue("All should be ExactMatchDimFilter", statusFilters.stream().allMatch(f -> f instanceof ExactMatchDimFilter));

        Set<Object> expectedValues = Set.of(200L, 404L, 500L);
        Set<Object> actualValues = new HashSet<>();
        for (DimensionFilter dimensionFilter : statusFilters) {
            actualValues.addAll(((ExactMatchDimFilter) dimensionFilter).getRawValues());
        }
        assertEquals("Should contain all expected status values", expectedValues, actualValues);
    }

    public void testEmptyShouldClause() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().should(new BoolQueryBuilder());

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for empty SHOULD clause", filter);
    }

    public void testMustContainingShouldSameDimension() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new RangeQueryBuilder("status").gte(200).lt(500))
            .must(new BoolQueryBuilder().should(new TermQueryBuilder("status", 404)).should(new TermQueryBuilder("status", 403)));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertEquals("Should have two filters after intersection", 2, statusFilters.size());

        Set<Object> expectedValues = Set.of(403L, 404L);
        Set<Object> actualValues = new HashSet<>();
        for (DimensionFilter dimFilter : statusFilters) {
            assertTrue("Should be ExactMatchDimFilter", dimFilter instanceof ExactMatchDimFilter);
            ExactMatchDimFilter exactFilter = (ExactMatchDimFilter) dimFilter;
            actualValues.addAll(exactFilter.getRawValues());
        }
        assertEquals("Should contain expected status values", expectedValues, actualValues);
    }

    public void testMustContainingShouldDifferentDimension() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"))
            .must(new BoolQueryBuilder().should(new TermQueryBuilder("status", 200)).should(new TermQueryBuilder("status", 404)));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have two dimensions", 2, filter.getDimensions().size());

        // Verify method filter
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension("method");
        assertEquals("Method should have one filter", 1, methodFilters.size());
        assertTrue("Should be ExactMatchDimFilter", methodFilters.getFirst() instanceof ExactMatchDimFilter);
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "GET");

        // Verify status filters
        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertEquals("Status should have two filters", 2, statusFilters.size());
        Set<Object> expectedStatusValues = Set.of(200L, 404L);
        Set<Object> actualStatusValues = new HashSet<>();
        for (DimensionFilter dimFilter : statusFilters) {
            assertTrue("Should be ExactMatchDimFilter", dimFilter instanceof ExactMatchDimFilter);
            ExactMatchDimFilter exactFilter = (ExactMatchDimFilter) dimFilter;
            actualStatusValues.addAll(exactFilter.getRawValues());
        }
        assertEquals("Should contain expected status values", expectedStatusValues, actualStatusValues);
    }

    public void testMultipleLevelsMustNesting() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"))
            .must(
                new BoolQueryBuilder().must(new RangeQueryBuilder("status").gte(200).lte(300))
                    .must(new BoolQueryBuilder().must(new TermQueryBuilder("port", 443)))
            );

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);

        // Verify method filter
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension("method");
        assertEquals("Method should have one filter", 1, methodFilters.size());
        assertTrue("Should be ExactMatchDimFilter", methodFilters.getFirst() instanceof ExactMatchDimFilter);
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "GET");

        // Verify status filter
        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertEquals("Status should have one filter", 1, statusFilters.size());
        assertTrue("Should be RangeMatchDimFilter", statusFilters.getFirst() instanceof RangeMatchDimFilter);
        RangeMatchDimFilter rangeFilter = (RangeMatchDimFilter) statusFilters.getFirst();
        assertEquals("Lower bound should be 200", 200L, rangeFilter.getLow());
        assertEquals("Upper bound should be 300", 300L, rangeFilter.getHigh());
        assertTrue("Lower bound should be inclusive", rangeFilter.isIncludeLow());
        assertTrue("Upper bound should be inclusive", rangeFilter.isIncludeHigh());

        // Verify port filter
        List<DimensionFilter> portFilters = filter.getFiltersForDimension("port");
        assertEquals("Port should have one filter", 1, portFilters.size());
        assertTrue("Should be ExactMatchDimFilter", portFilters.getFirst() instanceof ExactMatchDimFilter);
        assertExactMatchValue((ExactMatchDimFilter) portFilters.getFirst(), 443L);
    }

    public void testShouldInsideShouldSameDimension() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().should(new TermQueryBuilder("status", 200))
            .should(new BoolQueryBuilder().should(new TermQueryBuilder("status", 404)).should(new TermQueryBuilder("status", 500)));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertEquals("Should have three filters", 3, statusFilters.size());

        Set<Object> expectedValues = Set.of(200L, 404L, 500L);
        Set<Object> actualValues = new HashSet<>();
        for (DimensionFilter dimFilter : statusFilters) {
            assertTrue("Should be ExactMatchDimFilter", dimFilter instanceof ExactMatchDimFilter);
            ExactMatchDimFilter exactFilter = (ExactMatchDimFilter) dimFilter;
            actualValues.addAll(exactFilter.getRawValues());
        }
        assertEquals("Should contain all expected values", expectedValues, actualValues);
    }

    public void testMustInsideShouldRejected() throws IOException {
        // MUST inside SHOULD (should be rejected)
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().should(
            new BoolQueryBuilder().must(new TermQueryBuilder("status", 200)).must(new TermQueryBuilder("method", "GET"))
        );

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for MUST inside SHOULD", filter);
    }

    public void testComplexNestedStructure() throws IOException {
        // Complex nested structure with both MUST and SHOULD
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"))
            .must(
                new BoolQueryBuilder().must(new RangeQueryBuilder("port").gte(80).lte(443))
                    .must(new BoolQueryBuilder().should(new TermQueryBuilder("status", 200)).should(new TermQueryBuilder("status", 404)))
            );

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have three dimensions", 3, filter.getDimensions().size());
        assertTrue("Should contain all dimensions", filter.getDimensions().containsAll(Set.of("method", "port", "status")));
    }

    public void testMaximumNestingDepth() throws IOException {
        // Build a deeply nested bool query
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"));

        BoolQueryBuilder current = boolQuery;
        for (int i = 0; i < 10; i++) { // Test with 10 levels of nesting
            BoolQueryBuilder nested = new BoolQueryBuilder().must(new TermQueryBuilder("status", 200 + i));
            current.must(nested);
            current = nested;
        }

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should not be null", filter);
    }

    public void testAllClauseTypesCombined() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"))
            .must(
                new BoolQueryBuilder().must(new RangeQueryBuilder("port").gte(80).lte(443))
                    .must(new BoolQueryBuilder().should(new TermQueryBuilder("status", 200)).should(new TermQueryBuilder("status", 201)))
            )
            .must(new TermsQueryBuilder("method", Arrays.asList("GET", "POST"))); // This should intersect with first method term

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have three dimensions", 3, filter.getDimensions().size());

        // Verify method filter (should be intersection of term and terms)
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension("method");
        assertEquals("Should have one filter for method after intersection", 1, methodFilters.size());
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "GET");

        // Verify port filter
        List<DimensionFilter> portFilters = filter.getFiltersForDimension("port");
        assertTrue("Port should be RangeMatchDimFilter", portFilters.getFirst() instanceof RangeMatchDimFilter);
        RangeMatchDimFilter portRange = (RangeMatchDimFilter) portFilters.getFirst();
        assertEquals("Port lower bound should be 80", 80L, portRange.getLow());
        assertEquals("Port upper bound should be 443", 443L, portRange.getHigh());

        // Verify status filters
        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertEquals("Should have two filters for status", 2, statusFilters.size());
        Set<Object> expectedStatusValues = Set.of(200L, 201L);
        Set<Object> actualStatusValues = new HashSet<>();
        for (DimensionFilter statusFilter : statusFilters) {
            actualStatusValues.addAll(((ExactMatchDimFilter) statusFilter).getRawValues());
        }
        assertEquals("Status should have expected values", expectedStatusValues, actualStatusValues);
    }

    public void testEmptyNestedBools() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new BoolQueryBuilder())
            .must(new BoolQueryBuilder().must(new BoolQueryBuilder()));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for empty nested bool queries", filter);
    }

    public void testSingleClauseBoolQueries() throws IOException {
        // Test single MUST clause
        BoolQueryBuilder mustOnly = new BoolQueryBuilder().must(new TermQueryBuilder("status", 200));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(mustOnly);
        StarTreeFilter filter = provider.getFilter(searchContext, mustOnly, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());
        assertExactMatchValue((ExactMatchDimFilter) filter.getFiltersForDimension("status").get(0), 200L);

        // Test single SHOULD clause
        BoolQueryBuilder shouldOnly = new BoolQueryBuilder().should(new TermQueryBuilder("status", 200));

        filter = provider.getFilter(searchContext, shouldOnly, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());
        assertExactMatchValue((ExactMatchDimFilter) filter.getFiltersForDimension("status").get(0), 200L);
    }

    public void testDuplicateDimensionsAcrossNesting() throws IOException {
        // Test duplicate dimensions that should be merged/intersected
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new RangeQueryBuilder("status").gte(200).lte(500))
            .must(new BoolQueryBuilder().must(new RangeQueryBuilder("status").gte(300).lte(400)));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertEquals("Should have one filter after intersection", 1, statusFilters.size());
        RangeMatchDimFilter rangeFilter = (RangeMatchDimFilter) statusFilters.getFirst();
        assertEquals("Lower bound should be 300", 300L, rangeFilter.getLow());
        assertEquals("Upper bound should be 400", 400L, rangeFilter.getHigh());
        assertTrue("Lower bound should be exclusive", rangeFilter.isIncludeLow());
        assertTrue("Upper bound should be exclusive", rangeFilter.isIncludeHigh());
    }

    public void testKeywordFieldTypeHandling() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermsQueryBuilder("method", Arrays.asList("GET", "POST")))
            .must(new TermQueryBuilder("status", 200))
            .must(new BoolQueryBuilder().should(new TermQueryBuilder("port", 80)).should(new TermQueryBuilder("port", 9200)));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);

        // Verify method filter (keyword term query)
        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertExactMatchValue((ExactMatchDimFilter) statusFilters.getFirst(), 200L);

        // Verify method filter (keyword terms query)
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension("method");
        ExactMatchDimFilter methodFilter = (ExactMatchDimFilter) methodFilters.getFirst();
        Set<Object> expectedMethod = new HashSet<>();
        expectedMethod.add(new BytesRef("GET"));
        expectedMethod.add(new BytesRef("POST"));
        assertEquals(expectedMethod, new HashSet<>(methodFilter.getRawValues()));

        // Verify port filter (keyword SHOULD terms)
        List<DimensionFilter> portFilters = filter.getFiltersForDimension("port");
        assertEquals(2, portFilters.size());
        Set<Object> expectedPorts = new HashSet<>();
        expectedPorts.add(80L);
        expectedPorts.add(9200L);
        Set<Object> actualZones = new HashSet<>();
        for (DimensionFilter portFilter : portFilters) {
            actualZones.addAll(((ExactMatchDimFilter) portFilter).getRawValues());
        }
        assertEquals(expectedPorts, actualZones);
    }

    public void testInvalidDimensionNames() throws IOException {
        // Test dimension that doesn't exist in mapping
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("non_existent_field", "value"))
            .must(new TermQueryBuilder("method", "GET"));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for non-existent dimension", filter);

        // Test dimension that exists in mapping but not in star tree dimensions
        NumberFieldMapper.NumberFieldType nonStarTreeField = new NumberFieldMapper.NumberFieldType(
            "non_star_tree_field",
            NumberFieldMapper.NumberType.INTEGER
        );
        when(mapperService.fieldType("non_star_tree_field")).thenReturn(nonStarTreeField);

        boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("non_star_tree_field", 100))
            .must(new TermQueryBuilder("method", "GET"));

        filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);
        assertNull("Filter should be null for non-star-tree dimension", filter);
    }

    public void testUnsupportedQueryTypes() throws IOException {
        // Test unsupported query type in MUST
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new WildcardQueryBuilder("method", "GET*"))
            .must(new TermQueryBuilder("status", 200));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for unsupported query type", filter);

        // Test unsupported query type in SHOULD
        boolQuery = new BoolQueryBuilder().should(new WildcardQueryBuilder("status", "2*")).should(new TermQueryBuilder("status", 404));

        filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);
        assertNull("Filter should be null for unsupported query type in SHOULD", filter);
    }

    public void testInvalidFieldTypes() throws IOException {
        // Test with unsupported field type
        WildcardFieldMapper.WildcardFieldType wildcardType = new WildcardFieldMapper.WildcardFieldType("wildcard_field");
        when(mapperService.fieldType("wildcard_field")).thenReturn(wildcardType);

        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("wildcard_field", "value"))
            .must(new TermQueryBuilder("method", "GET"));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for unsupported field type", filter);
    }

    public void testInvalidShouldClauses() throws IOException {
        // Test SHOULD clauses with different dimensions
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().should(new TermQueryBuilder("status", 200))
            .should(new TermQueryBuilder("method", "GET"));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for SHOULD with different dimensions", filter);

        // Test nested MUST inside SHOULD
        boolQuery = new BoolQueryBuilder().should(
            new BoolQueryBuilder().must(new TermQueryBuilder("status", 200)).must(new TermQueryBuilder("method", "GET"))
        );

        filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);
        assertNull("Filter should be null for MUST inside SHOULD", filter);
    }

    public void testInvalidMustClauses() throws IOException {
        // Test MUST clauses with same dimension
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("status", 200))
            .must(new TermQueryBuilder("status", 404));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for multiple MUST on same dimension", filter);

        // Test incompatible range intersections
        boolQuery = new BoolQueryBuilder().must(new RangeQueryBuilder("status").gte(200).lt(300))
            .must(new RangeQueryBuilder("status").gte(400).lt(500));

        filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);
        assertNull("Filter should be null for non-overlapping ranges", filter);
    }

    public void testMalformedQueries() throws IOException {
        // Test empty bool query
        BoolQueryBuilder boolQuery = new BoolQueryBuilder();
        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for empty bool query", filter);

        // Test deeply nested empty bool queries
        boolQuery = new BoolQueryBuilder().must(new BoolQueryBuilder().must(new BoolQueryBuilder().must(new BoolQueryBuilder())));

        filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);
        assertNull("Filter should be null for nested empty bool queries", filter);
    }

    public void testComplexMustWithNestedShould() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"))
            .must(new RangeQueryBuilder("port").gte(80).lte(443))
            .must(
                new BoolQueryBuilder().should(new TermQueryBuilder("status", 200)).should(new RangeQueryBuilder("status").gte(500).lt(600))
            )  // Success or 5xx errors
            .must(
                new BoolQueryBuilder().must(
                    new BoolQueryBuilder().should(new TermQueryBuilder("zone", "us-east")).should(new TermQueryBuilder("zone", "us-west"))
                )
            );

        // Add field type for zone
        KeywordFieldMapper.KeywordFieldType zoneType = new KeywordFieldMapper.KeywordFieldType("zone");
        when(mapperService.fieldType("zone")).thenReturn(zoneType);

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have four dimensions", 4, filter.getDimensions().size());

        // Verify method filter
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension("method");
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "GET");

        // Verify port range
        List<DimensionFilter> portFilters = filter.getFiltersForDimension("port");
        RangeMatchDimFilter portRange = (RangeMatchDimFilter) portFilters.getFirst();
        assertEquals(80L, portRange.getLow());
        assertEquals(443L, portRange.getHigh());
        assertTrue(portRange.isIncludeLow() && portRange.isIncludeHigh());

        // Verify status filters (term OR range)
        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertEquals(2, statusFilters.size());
        for (DimensionFilter statusFilter : statusFilters) {
            if (statusFilter instanceof ExactMatchDimFilter) {
                assertEquals(200L, ((ExactMatchDimFilter) statusFilter).getRawValues().getFirst());
            } else {
                RangeMatchDimFilter statusRange = (RangeMatchDimFilter) statusFilter;
                assertEquals(500L, statusRange.getLow());
                assertEquals(599L, statusRange.getHigh());
                assertTrue(statusRange.isIncludeLow());
                assertTrue(statusRange.isIncludeHigh());
            }
        }

        // Verify zone filters
        List<DimensionFilter> zoneFilters = filter.getFiltersForDimension("zone");
        assertEquals(2, zoneFilters.size());
        Set<Object> expectedZones = new HashSet<>();
        expectedZones.add(new BytesRef("us-east"));
        expectedZones.add(new BytesRef("us-west"));
        Set<Object> actualZones = new HashSet<>();
        for (DimensionFilter zoneFilter : zoneFilters) {
            actualZones.addAll(((ExactMatchDimFilter) zoneFilter).getRawValues());
        }
        assertEquals(expectedZones, actualZones);
    }

    public void testRangeAndTermCombinations() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new RangeQueryBuilder("status").gte(200).lt(300))  // 2xx status codes
            .must(new BoolQueryBuilder().should(new TermQueryBuilder("status", 201)).should(new TermQueryBuilder("status", 204)))  // Specific
                                                                                                                                   // success
                                                                                                                                   // codes
            .must(new TermQueryBuilder("method", "POST"));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);

        // Verify method filter
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension("method");
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "POST");

        // Verify status filters (intersection of range and terms)
        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertEquals(2, statusFilters.size());
        Set<Object> expectedStatus = Set.of(201L, 204L);
        Set<Object> actualStatus = new HashSet<>();
        for (DimensionFilter statusFilter : statusFilters) {
            assertTrue(statusFilter instanceof ExactMatchDimFilter);
            actualStatus.addAll(((ExactMatchDimFilter) statusFilter).getRawValues());
        }
        assertEquals(expectedStatus, actualStatus);
    }

    public void testDeepNestedShouldClauses() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"))
            .must(
                new BoolQueryBuilder().should(
                    new BoolQueryBuilder().should(new TermQueryBuilder("response_time", 100))
                        .should(new TermQueryBuilder("response_time", 200))
                )
                    .should(
                        new BoolQueryBuilder().should(new TermQueryBuilder("response_time", 300))
                            .should(new TermQueryBuilder("response_time", 400))
                    )
            );

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);

        // Verify method filter
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension("method");
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.get(0), "GET");

        // Verify response_time filters (all SHOULD conditions)
        List<DimensionFilter> responseTimeFilters = filter.getFiltersForDimension("response_time");
        assertEquals(4, responseTimeFilters.size());
        Set<Object> expectedTimes = Set.of(100L, 200L, 300L, 400L);
        Set<Object> actualTimes = new HashSet<>();
        for (DimensionFilter timeFilter : responseTimeFilters) {
            assertTrue(timeFilter instanceof ExactMatchDimFilter);
            actualTimes.addAll(((ExactMatchDimFilter) timeFilter).getRawValues());
        }
        assertEquals(expectedTimes, actualTimes);
    }

    public void testLargeNumberOfClauses() throws IOException {
        // Create a bool query with large number of SHOULD clauses
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"));

        // Add 100 SHOULD clauses for status
        BoolQueryBuilder statusShould = new BoolQueryBuilder();
        for (int i = 200; i < 300; i++) {
            statusShould.should(new TermQueryBuilder("status", i));
        }
        boolQuery.must(statusShould);

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);

        // Verify filters
        List<DimensionFilter> methodFilters = filter.getFiltersForDimension("method");
        assertExactMatchValue((ExactMatchDimFilter) methodFilters.getFirst(), "GET");

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertEquals(100, statusFilters.size());
    }

    // Helper methods for assertions
    private void assertExactMatchValue(ExactMatchDimFilter filter, String expectedValue) {
        assertEquals(new BytesRef(expectedValue), filter.getRawValues().getFirst());
    }

    private void assertExactMatchValue(ExactMatchDimFilter filter, Long expectedValue) {
        assertEquals(expectedValue, filter.getRawValues().getFirst());
    }

}
