/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.startree;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.index.compositeindex.datacube.Metric;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.OrdinalDimension;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeField;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeFieldConfiguration;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedSetStarTreeValuesIterator;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.mapper.IpFieldMapper;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
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
import org.opensearch.search.startree.StarTreeNodeCollector;
import org.opensearch.search.startree.filter.DimensionFilter;
import org.opensearch.search.startree.filter.DimensionFilter.MatchType;
import org.opensearch.search.startree.filter.ExactMatchDimFilter;
import org.opensearch.search.startree.filter.MatchAllFilter;
import org.opensearch.search.startree.filter.MatchNoneFilter;
import org.opensearch.search.startree.filter.RangeMatchDimFilter;
import org.opensearch.search.startree.filter.StarTreeFilter;
import org.opensearch.search.startree.filter.provider.DimensionFilterMapper;
import org.opensearch.search.startree.filter.provider.StarTreeFilterProvider;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class DimensionFilterAndMapperTests extends OpenSearchTestCase {

    public void testIpMapping() throws Exception {
        MappedFieldType mappedFieldType = new IpFieldMapper.IpFieldType("ip_field");
        BytesRef ipAsBytes = new BytesRef(InetAddressPoint.encode(InetAddress.getByName("192.168.1.1")));
        testOrdinalMapping(mappedFieldType, ipAsBytes);
    }

    public void testRawValuesIpParsing() throws UnknownHostException {
        SearchContext searchContext = mock(SearchContext.class);
        MappedFieldType mappedFieldType = new IpFieldMapper.IpFieldType("ip_field");
        DimensionFilterMapper dimensionFilterMapper = DimensionFilterMapper.Factory.fromMappedFieldType(mappedFieldType, searchContext);

        assertThrows(IllegalArgumentException.class, () -> dimensionFilterMapper.getExactMatchFilter(mappedFieldType, List.of(1.0f)));
        assertThrows(
            IllegalArgumentException.class,
            () -> dimensionFilterMapper.getExactMatchFilter(mappedFieldType, List.of("not.a.valid.ip"))
        );
        DimensionFilter df = dimensionFilterMapper.getExactMatchFilter(mappedFieldType, List.of(InetAddress.getByName("192.168.1.1")));
        assertEquals("ip_field", df.getMatchingDimension());
    }

    public void testKeywordOrdinalMapping() throws IOException {
        MappedFieldType mappedFieldType = new KeywordFieldMapper.KeywordFieldType("keyword");
        BytesRef bytesRef = new BytesRef(new byte[] { 17, 29 });
        testOrdinalMapping(mappedFieldType, bytesRef);
    }

    private void testOrdinalMapping(final MappedFieldType mappedFieldType, final BytesRef bytesRef) throws IOException {
        SearchContext searchContext = mock(SearchContext.class);
        DimensionFilterMapper dimensionFilterMapper = DimensionFilterMapper.Factory.fromMappedFieldType(mappedFieldType, searchContext);
        StarTreeValues starTreeValues = mock(StarTreeValues.class);
        SortedSetStarTreeValuesIterator sortedSetStarTreeValuesIterator = mock(SortedSetStarTreeValuesIterator.class);
        TermsEnum termsEnum = mock(TermsEnum.class);
        when(sortedSetStarTreeValuesIterator.termsEnum()).thenReturn(termsEnum);
        when(starTreeValues.getDimensionValuesIterator("field")).thenReturn(sortedSetStarTreeValuesIterator);
        Optional<Long> matchingOrdinal;

        // Case Exact Match and found
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

    /**
     * Concurrent search slices share a request's dimension filters while they traverse different segments, so a
     * filter's per-segment state must live in the instance {@link DimensionFilter#forSegment} returns.
     */
    public void testForSegmentKeepsSegmentStateOutOfSharedFilter() throws IOException {
        StarTreeField starTreeField = keywordStarTreeField();
        SearchContext searchContext = keywordSearchContext();
        BytesRef value = new BytesRef("value");
        // The same value has ordinal 1 in one segment and 5 in the other
        StarTreeValues segment1 = keywordSegment(starTreeField, value, 1L);
        StarTreeValues segment2 = keywordSegment(starTreeField, value, 5L);

        ExactMatchDimFilter exact = new ExactMatchDimFilter("keyword", List.of(value));
        DimensionFilter exactForSegment1 = exact.forSegment(segment1, searchContext);
        // Another slice binds the same request-level filter to its segment before the first slice traverses its own
        DimensionFilter exactForSegment2 = exact.forSegment(segment2, searchContext);
        assertTrue(exactForSegment1.matchDimValue(1L, segment1));
        assertFalse(exactForSegment1.matchDimValue(5L, segment1));
        assertTrue(exactForSegment2.matchDimValue(5L, segment2));
        assertFalse(exactForSegment2.matchDimValue(1L, segment2));

        // Date range filters resolve on a sub-dimension, which the segment filter must keep
        RangeMatchDimFilter range = new RangeMatchDimFilter("keyword", value, value, true, true, "keyword_sub");
        DimensionFilter rangeForSegment1 = range.forSegment(segment1, searchContext);
        DimensionFilter rangeForSegment2 = range.forSegment(segment2, searchContext);
        assertTrue(rangeForSegment1.matchDimValue(1L, segment1));
        assertFalse(rangeForSegment1.matchDimValue(5L, segment1));
        assertTrue(rangeForSegment2.matchDimValue(5L, segment2));
        assertFalse(rangeForSegment2.matchDimValue(1L, segment2));
        assertEquals("keyword_sub", rangeForSegment1.getMatchingDimension());

        // The shared, request-level filters hold no segment state and cannot be matched directly
        expectThrows(IllegalStateException.class, () -> exact.matchDimValue(1L, segment1));
        expectThrows(IllegalStateException.class, () -> range.matchDimValue(1L, segment1));
    }

    /** {@link StarTreeFilter#forSegment}, which the traversal uses, binds every dimension and leaves the request's filter as it was. */
    public void testStarTreeFilterForSegmentBindsEveryDimension() throws IOException {
        StarTreeField starTreeField = keywordStarTreeField();
        SearchContext searchContext = keywordSearchContext();
        BytesRef value = new BytesRef("value");
        StarTreeValues segment = keywordSegment(starTreeField, value, 3L);
        ExactMatchDimFilter exact = new ExactMatchDimFilter("keyword", List.of(value));
        MatchAllFilter matchAll = new MatchAllFilter("other");
        StarTreeFilter shared = new StarTreeFilter(Map.of("keyword", List.of(exact), "other", List.of(matchAll)));

        StarTreeFilter bound = shared.forSegment(segment, searchContext);

        assertEquals(shared.getDimensions(), bound.getDimensions());
        DimensionFilter boundExact = bound.getFiltersForDimension("keyword").getFirst();
        assertNotSame(exact, boundExact);
        assertTrue(boundExact.matchDimValue(3L, segment));
        // A stateless filter is shared as it is
        assertSame(matchAll, bound.getFiltersForDimension("other").getFirst());
        // The request-level filter is unchanged and still unbound
        assertSame(exact, shared.getFiltersForDimension("keyword").getFirst());
        expectThrows(IllegalStateException.class, () -> exact.matchDimValue(3L, segment));
    }

    /** A keyword range whose lower bound is above every term of a segment matches nothing in that segment. */
    public void testRangeAboveEveryTermSkipsTheSegment() throws IOException {
        StarTreeField starTreeField = keywordStarTreeField();
        SearchContext searchContext = keywordSearchContext();
        BytesRef low = new BytesRef("zzz");
        StarTreeValues segment = keywordSegment(starTreeField, low, -1L, TermsEnum.SeekStatus.END);

        DimensionFilter bound = new RangeMatchDimFilter("keyword", low, null, true, true).forSegment(segment, searchContext);

        StarTreeNode parent = mock(StarTreeNode.class);
        bound.matchStarTreeNodes(parent, segment, mock(StarTreeNodeCollector.class));
        verifyNoInteractions(parent);
        assertFalse(bound.matchDimValue(0L, segment));
    }

    private static StarTreeField keywordStarTreeField() {
        return new StarTreeField(
            "star_tree",
            List.of(new OrdinalDimension("keyword")),
            List.of(new Metric("field", List.of(MetricStat.MAX))),
            new StarTreeFieldConfiguration(1, Collections.emptySet(), StarTreeFieldConfiguration.StarTreeBuildMode.ON_HEAP)
        );
    }

    private static SearchContext keywordSearchContext() {
        MapperService mapperService = mock(MapperService.class);
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.mapperService()).thenReturn(mapperService);
        when(mapperService.fieldType("keyword")).thenReturn(new KeywordFieldMapper.KeywordFieldType("keyword"));
        return searchContext;
    }

    private static StarTreeValues keywordSegment(StarTreeField starTreeField, BytesRef value, long ordinal) throws IOException {
        return keywordSegment(starTreeField, value, ordinal, TermsEnum.SeekStatus.FOUND);
    }

    /** A segment where looking {@code value} up gives {@code ordinal}, and seeking it gives {@code seekStatus}. */
    private static StarTreeValues keywordSegment(StarTreeField starTreeField, BytesRef value, long ordinal, TermsEnum.SeekStatus seekStatus)
        throws IOException {
        StarTreeValues starTreeValues = mock(StarTreeValues.class);
        SortedSetStarTreeValuesIterator iterator = mock(SortedSetStarTreeValuesIterator.class);
        TermsEnum termsEnum = mock(TermsEnum.class);
        when(starTreeValues.getStarTreeField()).thenReturn(starTreeField);
        when(starTreeValues.getDimensionValuesIterator("keyword")).thenReturn(iterator);
        when(iterator.lookupTerm(value)).thenReturn(ordinal);
        when(iterator.termsEnum()).thenReturn(termsEnum);
        when(termsEnum.seekCeil(value)).thenReturn(seekStatus);
        when(termsEnum.ord()).thenReturn(ordinal);
        return starTreeValues;
    }

    public void testStarTreeFilterProviders() throws IOException {
        CompositeDataCubeFieldType compositeDataCubeFieldType = new StarTreeMapper.StarTreeFieldType(
            "star_tree",
            new StarTreeField(
                "star_tree",
                List.of(new OrdinalDimension("keyword"), new OrdinalDimension("status"), new OrdinalDimension("method")),
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
        assertNull(DimensionFilterMapper.Factory.fromMappedFieldType(new WildcardFieldMapper.WildcardFieldType("field"), searchContext));

        // Null returned for no mapped field type
        assertNull(DimensionFilterMapper.Factory.fromMappedFieldType(null, searchContext));

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
        DimensionFilter dimensionFilter = new MatchNoneFilter();
        assertSame(dimensionFilter, dimensionFilter.forSegment(null, null));
        ArrayBasedCollector collector = new ArrayBasedCollector();
        assertFalse(dimensionFilter.matchDimValue(1, null));
        dimensionFilter.matchStarTreeNodes(null, null, collector);
        assertEquals(0, collector.collectedNodeCount());

        // Setup common field types
        KeywordFieldMapper.KeywordFieldType methodType = new KeywordFieldMapper.KeywordFieldType("method");
        NumberFieldMapper.NumberFieldType statusType = new NumberFieldMapper.NumberFieldType(
            "status",
            NumberFieldMapper.NumberType.INTEGER
        );
        when(mapperService.fieldType("method")).thenReturn(methodType);
        when(mapperService.fieldType("status")).thenReturn(statusType);

        // Test simple MUST clause
        BoolQueryBuilder simpleMustQuery = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"))
            .must(new TermQueryBuilder("status", 200));
        StarTreeFilterProvider provider = StarTreeFilterProvider.SingletonFactory.getProvider(simpleMustQuery);
        StarTreeFilter filter = provider.getFilter(searchContext, simpleMustQuery, compositeDataCubeFieldType);
        assertNotNull(filter);
        assertEquals(2, filter.getDimensions().size());
        assertTrue(filter.getDimensions().contains("method"));
        assertTrue(filter.getDimensions().contains("status"));

        // Test MUST with nested SHOULD on different dimension
        BoolQueryBuilder mustWithShould = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"))
            .must(new BoolQueryBuilder().should(new TermQueryBuilder("status", 200)).should(new TermQueryBuilder("status", 404)));
        filter = provider.getFilter(searchContext, mustWithShould, compositeDataCubeFieldType);
        assertNotNull(filter);
        assertEquals(2, filter.getDimensions().size());
        assertEquals(1, filter.getFiltersForDimension("method").size());
        assertEquals(2, filter.getFiltersForDimension("status").size());

        // Test invalid SHOULD across dimensions
        BoolQueryBuilder invalidShould = new BoolQueryBuilder().should(new TermQueryBuilder("method", "GET"))
            .should(new TermQueryBuilder("status", 200));
        assertNull(provider.getFilter(searchContext, invalidShould, compositeDataCubeFieldType));

        // Test MUST with invalid nested SHOULD
        BoolQueryBuilder invalidNestedShould = new BoolQueryBuilder().must(new TermQueryBuilder("method", "GET"))
            .must(new BoolQueryBuilder().should(new TermQueryBuilder("method", "POST")).should(new TermQueryBuilder("status", 200)));
        assertNull(provider.getFilter(searchContext, invalidNestedShould, compositeDataCubeFieldType));

        // Test MUST with SHOULD on same dimension
        BoolQueryBuilder mustWithSameDimShould = new BoolQueryBuilder().must(new TermQueryBuilder("status", 200))
            .must(new BoolQueryBuilder().should(new TermQueryBuilder("status", 404)).should(new TermQueryBuilder("status", 500)));
        assertNull(provider.getFilter(searchContext, mustWithSameDimShould, compositeDataCubeFieldType));
    }
}
