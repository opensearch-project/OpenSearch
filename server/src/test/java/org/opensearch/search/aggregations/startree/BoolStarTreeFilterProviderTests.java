/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.startree;

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
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
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
import java.util.List;
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
        when(mapperService.fieldType("method")).thenReturn(methodType);
        when(mapperService.fieldType("status")).thenReturn(statusType);
        when(mapperService.fieldType("port")).thenReturn(portType);

        // Create composite field type with dimensions
        compositeFieldType = new StarTreeMapper.StarTreeFieldType(
            "star_tree",
            new StarTreeField(
                "star_tree",
                List.of(new OrdinalDimension("method"), new OrdinalDimension("status"), new OrdinalDimension("port")),
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

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertEquals("Should have one filter for status", 1, statusFilters.size());
        assertTrue("Should be ExactMatchDimFilter", statusFilters.getFirst() instanceof ExactMatchDimFilter);
    }

    public void testNestedMustClauses() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"))
            .must(new BoolQueryBuilder().must(new TermQueryBuilder("status", 200)).must(new TermQueryBuilder("port", 443)));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have three dimensions", 3, filter.getDimensions().size());
        assertTrue("Should contain all dimensions", filter.getDimensions().containsAll(Set.of("method", "status", "port")));
    }

    public void testMustWithDifferentQueryTypes() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"))
            .must(new TermsQueryBuilder("status", Arrays.asList(200, 201)))
            .must(new RangeQueryBuilder("port").gte(80).lte(443));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have three dimensions", 3, filter.getDimensions().size());

        assertTrue(
            "Status should have ExactMatchDimFilter",
            filter.getFiltersForDimension("status").getFirst() instanceof ExactMatchDimFilter
        );
        assertTrue("Port should have RangeMatchDimFilter", filter.getFiltersForDimension("port").getFirst() instanceof RangeMatchDimFilter);
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
        // SHOULD with same dimension (Term queries)
        BoolQueryBuilder boolQuery = new BoolQueryBuilder()
            .should(new TermQueryBuilder("status", 200))
            .should(new TermQueryBuilder("status", 404));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());
        assertTrue("Should contain status dimension", filter.getDimensions().contains("status"));

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertEquals("Should have two filters for status", 2, statusFilters.size());
        assertTrue("Both should be ExactMatchDimFilter",
            statusFilters.stream().allMatch(f -> f instanceof ExactMatchDimFilter));
    }

    public void testShouldWithSameDimensionRange() throws IOException {
        // SHOULD with same dimension (Range queries)
        BoolQueryBuilder boolQuery = new BoolQueryBuilder()
            .should(new RangeQueryBuilder("status").gte(200).lt(300))
            .should(new RangeQueryBuilder("status").gte(400).lt(500));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertEquals("Should have two filters for status", 2, statusFilters.size());
        assertTrue("Both should be RangeMatchDimFilter",
            statusFilters.stream().allMatch(f -> f instanceof RangeMatchDimFilter));
    }

    public void testShouldWithSameDimensionMixed() throws IOException {
        // SHOULD with same dimension (Mixed Term and Range)
        BoolQueryBuilder boolQuery = new BoolQueryBuilder()
            .should(new TermQueryBuilder("status", 200))
            .should(new RangeQueryBuilder("status").gte(400).lt(500));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertEquals("Should have two filters for status", 2, statusFilters.size());
        assertTrue("Should have both ExactMatch and RangeMatch filters",
            statusFilters.stream().anyMatch(f -> f instanceof ExactMatchDimFilter) &&
                statusFilters.stream().anyMatch(f -> f instanceof RangeMatchDimFilter));
    }

    public void testShouldWithDifferentDimensions() throws IOException {
        // SHOULD with different dimensions (should be rejected)
        BoolQueryBuilder boolQuery = new BoolQueryBuilder()
            .should(new TermQueryBuilder("status", 200))
            .should(new TermQueryBuilder("method", "GET"));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for SHOULD across different dimensions", filter);
    }

    public void testNestedShouldSameDimension() throws IOException {
        // Nested SHOULD with same dimension
        BoolQueryBuilder boolQuery = new BoolQueryBuilder()
            .should(new TermQueryBuilder("status", 200))
            .should(new BoolQueryBuilder()
                .should(new TermQueryBuilder("status", 404))
                .should(new TermQueryBuilder("status", 500)));

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNotNull("Filter should not be null", filter);
        assertEquals("Should have one dimension", 1, filter.getDimensions().size());

        List<DimensionFilter> statusFilters = filter.getFiltersForDimension("status");
        assertEquals("Should have three filters for status", 3, statusFilters.size());
        assertTrue("All should be ExactMatchDimFilter",
            statusFilters.stream().allMatch(f -> f instanceof ExactMatchDimFilter));
    }

    public void testEmptyShouldClause() throws IOException {
        BoolQueryBuilder boolQuery = new BoolQueryBuilder()
            .should(new BoolQueryBuilder());

        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(boolQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, boolQuery, compositeFieldType);

        assertNull("Filter should be null for empty SHOULD clause", filter);
    }

}
