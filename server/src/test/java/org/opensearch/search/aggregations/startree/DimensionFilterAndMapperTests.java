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
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedSetStarTreeValuesIterator;
import org.opensearch.index.mapper.KeywordFieldMapper;
import org.opensearch.search.startree.filter.DimensionFilter.MatchType;
import org.opensearch.search.startree.filter.ExactMatchDimFilter;
import org.opensearch.search.startree.filter.RangeMatchDimFilter;
import org.opensearch.search.startree.filter.StarTreeFilter;
import org.opensearch.search.startree.filter.provider.DimensionFilterMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    public void testFilterComparison() {

        // Range Filter Same
        RangeMatchDimFilter rangeFilter1 = new RangeMatchDimFilter("field", 12, 14, true, false);
        RangeMatchDimFilter rangeFilter2 = new RangeMatchDimFilter("field", 12, 14, true, false);
        assertEquals(rangeFilter1, rangeFilter2);

        // Range Filter Different
        rangeFilter2 = new RangeMatchDimFilter("field", 12, 14, true, true);
        assertNotEquals(rangeFilter1, rangeFilter2);
        rangeFilter2 = new RangeMatchDimFilter("field", 12, 15, true, false);
        assertNotEquals(rangeFilter1, rangeFilter2);
        rangeFilter2 = new RangeMatchDimFilter("fields", 12, 14, true, false);
        assertNotEquals(rangeFilter1, rangeFilter2);
        rangeFilter2 = new RangeMatchDimFilter("field", 12, 14, false, false);
        assertNotEquals(rangeFilter1, rangeFilter2);
        rangeFilter2 = new RangeMatchDimFilter("field", 11, 14, true, false);
        assertNotEquals(rangeFilter1, rangeFilter2);

        // ExactMatch Filter Same
        ExactMatchDimFilter exactFilter1 = new ExactMatchDimFilter("field", List.of("hello", "world"));
        ExactMatchDimFilter exactFilter2 = new ExactMatchDimFilter("field", List.of("world", "hello"));
        assertEquals(exactFilter1, exactFilter2);

        // ExactMatch Filter Different
        exactFilter2 = new ExactMatchDimFilter("field2", List.of("hello", "world"));
        assertNotEquals(exactFilter1, exactFilter2);
        exactFilter2 = new ExactMatchDimFilter("field", List.of("hello", "worlds"));
        assertNotEquals(exactFilter1, exactFilter2);

        // Same StarTreeFilter
        StarTreeFilter starTreeFilter1 = new StarTreeFilter(Map.of("field", Set.of(rangeFilter1)));
        StarTreeFilter starTreeFilter2 = new StarTreeFilter(Map.of("field", Set.of(rangeFilter1)));
        assertEquals(starTreeFilter1, starTreeFilter2);
        rangeFilter2 = new RangeMatchDimFilter("field", 12, 14, true, false);
        starTreeFilter2 = new StarTreeFilter(Map.of("field", Set.of(rangeFilter2)));
        assertEquals(starTreeFilter1, starTreeFilter2);

        // Different StarTreeFilter
        starTreeFilter2 = new StarTreeFilter(Map.of("field1", Set.of(rangeFilter2)));
        assertNotEquals(starTreeFilter1, starTreeFilter2);
        rangeFilter2 = new RangeMatchDimFilter("field", 12, 15, true, false);
        starTreeFilter2 = new StarTreeFilter(Map.of("field", Set.of(rangeFilter2)));
        assertNotEquals(starTreeFilter1, starTreeFilter2);

    }

}
