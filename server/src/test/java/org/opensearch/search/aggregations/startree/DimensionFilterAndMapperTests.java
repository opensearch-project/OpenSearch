/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.startree;

import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.OrdinalDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedSetStarTreeValuesIterator;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.NumberFieldMapper;
import org.opensearch.index.mapper.StarTreeMapper;
import org.opensearch.index.mapper.WildcardFieldMapper;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.DimensionFilter.MatchType;
import org.opensearch.search.startree.filter.MatchNoneFilter;
import org.opensearch.search.startree.filter.StarTreeFilter;
import org.opensearch.search.startree.filter.provider.DimensionFilterMapper;
import org.opensearch.search.startree.filter.provider.StarTreeFilterProvider;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DimensionFilterAndMapperTests extends OpenSearchTestCase {

    public void testKeywordOrdinalMapping() throws IOException {
        DimensionFilterMapper dimensionFilterMapper = DimensionFilterMapper.Factory.fromMappedFieldType(
            new KeywordFieldMapper.KeywordFieldType("keyword")
        );
        StarTreeValues starTreeValues = mock(StarTreeValues.class);
        SortedSetStarTreeValuesIterator sortedSetStarTreeValuesIterator = mock(SortedSetStarTreeValuesIterator.class);
        TermsEnum termsEnum = mock(TermsEnum.class);
        when(sortedSetStarTreeValuesIterator.termsEnum()).thenReturn(termsEnum);
        when(starTreeValues.getDimensionValuesIterator("field")).thenReturn(sortedSetStarTreeValuesIterator);
        Optional<Long> matchingOrdinal;

        // Case Exact Match and found
        BytesRef bytesRef = new BytesRef(new byte[] { 17, 29 });
        when(sortedSetStarTreeValuesIterator.lookupTerm(bytesRef)).thenReturn(1L);
        matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, MatchType.EXACT);
        assertTrue(matchingOrdinal.isPresent());
        assertEquals(1, (long) matchingOrdinal.get());

        // Case Exact Match and not found
        when(sortedSetStarTreeValuesIterator.lookupTerm(bytesRef)).thenReturn(-10L);
        matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, MatchType.EXACT);
        assertFalse(matchingOrdinal.isPresent());

        // Case GTE -> FOUND and NOT_FOUND
        for (TermsEnum.SeekStatus seekStatus : new TermsEnum.SeekStatus[] { TermsEnum.SeekStatus.FOUND, TermsEnum.SeekStatus.NOT_FOUND }) {
            when(termsEnum.seekCeil(bytesRef)).thenReturn(seekStatus);
            when(termsEnum.ord()).thenReturn(10L);
            matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, MatchType.GTE);
            assertTrue(matchingOrdinal.isPresent());
            assertEquals(10L, (long) matchingOrdinal.get());
        }

        // Seek Status END is same for GTE, GT
        for (MatchType matchType : new MatchType[] { MatchType.GT, MatchType.GTE }) {
            when(termsEnum.seekCeil(bytesRef)).thenReturn(TermsEnum.SeekStatus.END);
            when(termsEnum.ord()).thenReturn(10L);
            matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, matchType);
            assertFalse(matchingOrdinal.isPresent());
        }

        // Case GT -> FOUND and matched
        when(termsEnum.seekCeil(bytesRef)).thenReturn(TermsEnum.SeekStatus.FOUND);
        when(sortedSetStarTreeValuesIterator.getValueCount()).thenReturn(2L);
        when(termsEnum.ord()).thenReturn(0L);
        matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, MatchType.GT);
        assertTrue(matchingOrdinal.isPresent());
        assertEquals(1L, (long) matchingOrdinal.get());
        // Case GT -> FOUND and unmatched
        when(termsEnum.ord()).thenReturn(3L);
        matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, MatchType.GT);
        assertFalse(matchingOrdinal.isPresent());

        // Case GT -> NOT_FOUND
        when(termsEnum.seekCeil(bytesRef)).thenReturn(TermsEnum.SeekStatus.NOT_FOUND);
        when(termsEnum.ord()).thenReturn(10L);
        matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, MatchType.GT);
        assertTrue(matchingOrdinal.isPresent());
        assertEquals(10L, (long) matchingOrdinal.get());

        // Seek Status END is same for LTE, LT
        for (MatchType matchType : new MatchType[] { MatchType.LT, MatchType.LTE }) {
            when(termsEnum.seekCeil(bytesRef)).thenReturn(TermsEnum.SeekStatus.END);
            when(termsEnum.ord()).thenReturn(10L);
            matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, matchType);
            assertTrue(matchingOrdinal.isPresent());
            assertEquals(10L, (long) matchingOrdinal.get());
        }

        // Seek Status NOT_FOUND is same for LTE, LT
        for (MatchType matchType : new MatchType[] { MatchType.LT, MatchType.LTE }) {
            when(termsEnum.seekCeil(bytesRef)).thenReturn(TermsEnum.SeekStatus.NOT_FOUND);
            when(sortedSetStarTreeValuesIterator.getValueCount()).thenReturn(2L);
            when(termsEnum.ord()).thenReturn(1L);
            matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, matchType);
            assertTrue(matchingOrdinal.isPresent());
            assertEquals(0L, (long) matchingOrdinal.get());
            // Case unmatched
            when(termsEnum.ord()).thenReturn(0L);
            matchingOrdinal = dimensionFilterMapper.getMatchingOrdinal("field", bytesRef, starTreeValues, matchType);
            assertFalse(matchingOrdinal.isPresent());
        }
    }

    public void testStarTreeFilterProviders() throws IOException {
        CompositeDataCubeFieldType compositeDataCubeFieldType = new StarTreeMapper.StarTreeFieldType(
            "star_tree",
            new StarTreeField(
                "star_tree",
                List.of(new OrdinalDimension("keyword")),
                List.of(new Metric("field", List.of(MetricStat.MAX))),
                new StarTreeFieldConfiguration(
                    randomIntBetween(1, 10_000),
                    Collections.emptySet(),
                    StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP
                )
            )
        );
        MapperService mapperService = mock(MapperService.class);
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.mapperService()).thenReturn(mapperService);

        // Null returned when mapper doesn't exist
        assertNull(DimensionFilterMapper.Factory.fromMappedFieldType(new WildcardFieldMapper.WildcardFieldType("field")));

        // Null returned for no mapped field type
        assertNull(DimensionFilterMapper.Factory.fromMappedFieldType(null));

        // Provider for null Query builder
        assertEquals(StarTreeFilterProvider.MATCH_ALL_PROVIDER, StarTreeFilterProvider.SingletonFactory.getProvider(null));

        QueryBuilder[] queryBuilders = new QueryBuilder[] {
            new TermQueryBuilder("field", "value"),
            new TermsQueryBuilder("field", List.of("value")),
            new RangeQueryBuilder("field") };

        for (QueryBuilder queryBuilder : queryBuilders) {
            // Dimension Not Found
            StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(queryBuilder);
            assertNull(provider.getFilter(searchContext, queryBuilder, compositeDataCubeFieldType));
        }

        queryBuilders = new QueryBuilder[] {
            new TermQueryBuilder("keyword", "value"),
            new TermsQueryBuilder("keyword", List.of("value")),
            new RangeQueryBuilder("keyword") };

        for (QueryBuilder queryBuilder : queryBuilders) {
            // Mapped field type not supported
            StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(queryBuilder);
            when(mapperService.fieldType("keyword")).thenReturn(new WildcardFieldMapper.WildcardFieldType("keyword"));
            assertNull(provider.getFilter(searchContext, queryBuilder, compositeDataCubeFieldType));

            // Unsupported Mapped Type
            when(mapperService.fieldType("keyword")).thenReturn(null);
            assertNull(provider.getFilter(searchContext, queryBuilder, compositeDataCubeFieldType));
        }

        // Testing MatchNoneFilter
        DimensionFilter dimensionFilter = new MatchNoneFilter("random");
        dimensionFilter.initialiseForSegment(null, null);
        ArrayBasedCollector collector = new ArrayBasedCollector();
        assertFalse(dimensionFilter.matchDimValue(1, null));
        dimensionFilter.matchStarTreeNodes(null, null, collector);
        assertEquals(0, collector.collectedNodeCount());
    }

    public void testBoolStarTreeFilterProvider_MustClause() throws IOException {
        CompositeDataCubeFieldType compositeDataCubeFieldType = new StarTreeMapper.StarTreeFieldType(
            "star_tree",
            new StarTreeField(
                "star_tree",
                List.of(
                    new OrdinalDimension("keyword"),
                    new OrdinalDimension("status"),
                    new OrdinalDimension("method")
                ),
                List.of(new Metric("field", List.of(MetricStat.MAX))),
                new StarTreeFieldConfiguration(
                    randomIntBetween(1, 10_000),
                    Collections.emptySet(),
                    StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP
                )
            )
        );
        MapperService mapperService = mock(MapperService.class);
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.mapperService()).thenReturn(mapperService);

        // Test empty bool query
        BoolQueryBuilder emptyBoolQuery = new BoolQueryBuilder();
        StarTreeFilterProvider boolProvider = StarTreeFilterProvider.SingletonFactory.getProvider(emptyBoolQuery);
        assertNull(boolProvider.getFilter(searchContext, emptyBoolQuery, compositeDataCubeFieldType));

        // Test bool query with single must clause
        KeywordFieldMapper.KeywordFieldType keywordFieldType = new KeywordFieldMapper.KeywordFieldType("keyword");
        when(mapperService.fieldType("keyword")).thenReturn(keywordFieldType);

        BoolQueryBuilder singleClauseBoolQuery = new BoolQueryBuilder()
            .must(new TermQueryBuilder("keyword", "value"));
        StarTreeFilter singleClauseFilter = boolProvider.getFilter(searchContext, singleClauseBoolQuery, compositeDataCubeFieldType);
        assertNotNull(singleClauseFilter);
        assertFalse(singleClauseFilter.getFiltersForDimension("keyword").isEmpty());

        // Test bool query with multiple must clauses
        NumberFieldMapper.NumberFieldType statusFieldType = new NumberFieldMapper.NumberFieldType(
            "status",
            NumberFieldMapper.NumberType.INTEGER
        );
        when(mapperService.fieldType("status")).thenReturn(statusFieldType);

        BoolQueryBuilder multiClauseBoolQuery = new BoolQueryBuilder()
            .must(new TermQueryBuilder("keyword", "value"))
            .must(new RangeQueryBuilder("status").gte(200).lt(500));
        StarTreeFilter multiClauseFilter = boolProvider.getFilter(searchContext, multiClauseBoolQuery, compositeDataCubeFieldType);
        assertNotNull(multiClauseFilter);
        assertFalse(multiClauseFilter.getFiltersForDimension("keyword").isEmpty());
        assertFalse(multiClauseFilter.getFiltersForDimension("status").isEmpty());

        // Test bool query with unsupported dimension
        when(mapperService.fieldType("unsupported")).thenReturn(new WildcardFieldMapper.WildcardFieldType("unsupported"));
        BoolQueryBuilder unsupportedBoolQuery = new BoolQueryBuilder()
            .must(new TermQueryBuilder("unsupported", "value"));
        assertNull(boolProvider.getFilter(searchContext, unsupportedBoolQuery, compositeDataCubeFieldType));

        // Test bool query with mixed valid and invalid clauses
        BoolQueryBuilder mixedBoolQuery = new BoolQueryBuilder()
            .must(new TermQueryBuilder("keyword", "value"))
            .must(new TermQueryBuilder("unsupported", "value"));
        assertNull(boolProvider.getFilter(searchContext, mixedBoolQuery, compositeDataCubeFieldType));

        // Test bool query with terms query
        BoolQueryBuilder termsBoolQuery = new BoolQueryBuilder()
            .must(new TermsQueryBuilder("keyword", List.of("value1", "value2")));
        StarTreeFilter termsFilter = boolProvider.getFilter(searchContext, termsBoolQuery, compositeDataCubeFieldType);
        assertNotNull(termsFilter);
        assertFalse(termsFilter.getFiltersForDimension("keyword").isEmpty());
    }

    public void testBoolStarTreeFilterProvider_ShouldClause() throws IOException {
        CompositeDataCubeFieldType compositeDataCubeFieldType = new StarTreeMapper.StarTreeFieldType(
            "star_tree",
            new StarTreeField(
                "star_tree",
                List.of(
                    new OrdinalDimension("status"),
                    new OrdinalDimension("method")
                ),
                List.of(new Metric("field", List.of(MetricStat.MAX))),
                new StarTreeFieldConfiguration(
                    randomIntBetween(1, 10_000),
                    Collections.emptySet(),
                    StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP
                )
            )
        );
        MapperService mapperService = mock(MapperService.class);
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.mapperService()).thenReturn(mapperService);

        // Setup field types
        NumberFieldMapper.NumberFieldType statusFieldType = new NumberFieldMapper.NumberFieldType(
            "status",
            NumberFieldMapper.NumberType.INTEGER
        );
        KeywordFieldMapper.KeywordFieldType methodFieldType = new KeywordFieldMapper.KeywordFieldType("method");
        when(mapperService.fieldType("status")).thenReturn(statusFieldType);
        when(mapperService.fieldType("method")).thenReturn(methodFieldType);

        StarTreeFilterProvider boolProvider = StarTreeFilterProvider.SingletonFactory.getProvider(new BoolQueryBuilder());

        // Test valid SHOULD clause (same dimension)
        BoolQueryBuilder validShouldQuery = new BoolQueryBuilder()
            .should(new TermQueryBuilder("status", 200))
            .should(new TermQueryBuilder("status", 404));
        StarTreeFilter validShouldFilter = boolProvider.getFilter(searchContext, validShouldQuery, compositeDataCubeFieldType);
        assertNotNull("Should support OR within same dimension", validShouldFilter);
        assertFalse(validShouldFilter.getFiltersForDimension("status").isEmpty());

        // Test valid SHOULD clause with range
        BoolQueryBuilder validRangeShouldQuery = new BoolQueryBuilder()
            .should(new RangeQueryBuilder("status").gte(200).lt(300))
            .should(new RangeQueryBuilder("status").gte(400).lt(500));
        StarTreeFilter validRangeShouldFilter = boolProvider.getFilter(
            searchContext,
            validRangeShouldQuery,
            compositeDataCubeFieldType
        );
        assertNotNull("Should support OR of ranges within same dimension", validRangeShouldFilter);
        assertFalse(validRangeShouldFilter.getFiltersForDimension("status").isEmpty());

        // Test invalid SHOULD clause (different dimensions)
        BoolQueryBuilder invalidShouldQuery = new BoolQueryBuilder()
            .should(new TermQueryBuilder("status", 200))
            .should(new TermQueryBuilder("method", "GET"));
        StarTreeFilter invalidShouldFilter = boolProvider.getFilter(searchContext, invalidShouldQuery, compositeDataCubeFieldType);
        assertNull("Should not support OR across different dimensions", invalidShouldFilter);

        // Test mixed MUST and SHOULD (same dimension)
        BoolQueryBuilder mixedValidQuery = new BoolQueryBuilder()
            .must(new TermQueryBuilder("method", "GET"))
            .should(new TermQueryBuilder("status", 200))
            .should(new TermQueryBuilder("status", 404));
        StarTreeFilter mixedValidFilter = boolProvider.getFilter(searchContext, mixedValidQuery, compositeDataCubeFieldType);
        assertNotNull("Should support mixed MUST and SHOULD when SHOULD clauses are on same dimension", mixedValidFilter);
        assertFalse(mixedValidFilter.getFiltersForDimension("method").isEmpty());
        assertFalse(mixedValidFilter.getFiltersForDimension("status").isEmpty());

        // Test mixed MUST and SHOULD (different dimensions)
        BoolQueryBuilder mixedInvalidQuery = new BoolQueryBuilder()
            .must(new TermQueryBuilder("method", "GET"))
            .should(new TermQueryBuilder("status", 200))
            .should(new TermQueryBuilder("method", "POST"));
        StarTreeFilter mixedInvalidFilter = boolProvider.getFilter(searchContext, mixedInvalidQuery, compositeDataCubeFieldType);
        assertNull("Should not support mixed MUST and SHOULD when SHOULD clauses are on different dimensions", mixedInvalidFilter);
    }

}
