/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree.filter;

import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.StarTreeNodeCollector;
import org.opensearch.search.startree.filter.provider.DimensionFilterMapper;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class RangeMatchDimFilter implements DimensionFilter {

    private final String dimensionName;

    private final Object low;
    private final Object high;
    private final boolean includeLow;
    private final boolean includeHigh;

    private Long lowOrdinal;
    private Long highOrdinal;

    public RangeMatchDimFilter(String dimensionName, Object low, Object high, boolean includeLow, boolean includeHigh) {
        this.dimensionName = dimensionName;
        this.low = low;
        this.high = high;
        this.includeLow = includeLow;
        this.includeHigh = includeHigh;
    }

    @Override
    public void initialiseForSegment(StarTreeValues starTreeValues, SearchContext searchContext) {
        DimensionFilterMapper dimensionFilterMapper = DimensionFilterMapper.Factory.fromMappedFieldType(
            searchContext.mapperService().fieldType(dimensionName)
        );
        lowOrdinal = 0L; // Unspecified from so start from beginning
        Optional<Long> lowOrdinalFound = Optional.of(0L), highOrdinalFound = Optional.of(Long.MAX_VALUE);
        if (low != null) {
            MatchType lowMatchType = includeLow ? MatchType.GTE : MatchType.GT;
            lowOrdinalFound = dimensionFilterMapper.getMatchingOrdinal(dimensionName, low, starTreeValues, lowMatchType);
            lowOrdinalFound.ifPresent(ord -> lowOrdinal = ord); // If we found the ord or something greater than it, continue
        }
        highOrdinal = Long.MAX_VALUE;
        if (high != null) {
            MatchType highMatchType = includeHigh ? MatchType.LTE : MatchType.LT;
            highOrdinalFound = dimensionFilterMapper.getMatchingOrdinal(dimensionName, high, starTreeValues, highMatchType);
            highOrdinalFound.ifPresent(ord -> highOrdinal = ord);
        }
        // Both low and high were not found, so not point in searching.
        if (lowOrdinalFound.isEmpty() && highOrdinalFound.isEmpty()) {
            lowOrdinal = highOrdinal = Long.MAX_VALUE;
        }
    }

    @Override
    public void matchStarTreeNodes(StarTreeNode parentNode, StarTreeValues starTreeValues, StarTreeNodeCollector collector)
        throws IOException {
        if (parentNode != null) {
            parentNode.collectChildrenInRange(lowOrdinal, highOrdinal, collector);
        }
    }

    @Override
    public boolean matchDimValue(long ordinal, StarTreeValues starTreeValues) {
        // FIXME : Needs to be based on Match Type.
        return lowOrdinal <= ordinal && ordinal <= highOrdinal;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof RangeMatchDimFilter)) return false;
        RangeMatchDimFilter that = (RangeMatchDimFilter) o;
        return includeLow == that.includeLow
            && includeHigh == that.includeHigh
            && Objects.equals(dimensionName, that.dimensionName)
            && Objects.equals(low, that.low)
            && Objects.equals(high, that.high);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dimensionName, low, high, includeLow, includeHigh);
    }
}
