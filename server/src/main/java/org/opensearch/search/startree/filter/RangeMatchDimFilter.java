/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree.filter;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.StarTreeNodeCollector;
import org.opensearch.search.startree.filter.provider.DimensionFilterMapper;

import java.io.IOException;
import java.util.Optional;

/**
 * Performs range match based on the params of @{@link org.opensearch.index.query.RangeQueryBuilder}
 * Also, contains logic to skip performing range search if it's sure that it won't be found in Star Tree.
 */
@ExperimentalApi
public class RangeMatchDimFilter implements DimensionFilter {

    private final String dimensionName;

    private final Object low;
    private final Object high;
    private final boolean includeLow;
    private final boolean includeHigh;

    private Long lowOrdinal;
    private Long highOrdinal;

    private boolean skipRangeCollection = false;

    public RangeMatchDimFilter(String dimensionName, Object low, Object high, boolean includeLow, boolean includeHigh) {
        this.dimensionName = dimensionName;
        this.low = low;
        this.high = high;
        this.includeLow = includeLow;
        this.includeHigh = includeHigh;
    }

    @Override
    public void initialiseForSegment(StarTreeValues starTreeValues, SearchContext searchContext) {
        skipRangeCollection = false;
        DimensionFilterMapper dimensionFilterMapper = DimensionFilterMapper.Factory.fromMappedFieldType(
            searchContext.mapperService().fieldType(dimensionName)
        );
        lowOrdinal = 0L;
        if (low != null) {
            MatchType lowMatchType = includeLow ? MatchType.GTE : MatchType.GT;
            Optional<Long> lowOrdinalFound = dimensionFilterMapper.getMatchingOrdinal(dimensionName, low, starTreeValues, lowMatchType);
            if (lowOrdinalFound.isPresent()) {
                lowOrdinal = lowOrdinalFound.get();
            } else {
                // This is only valid for Non-numeric fields.
                // High can't be found since nothing >= low exists.
                lowOrdinal = highOrdinal = Long.MAX_VALUE;
                skipRangeCollection = true;
                return;
            }
        }
        highOrdinal = Long.MAX_VALUE;
        if (high != null) {
            MatchType highMatchType = includeHigh ? MatchType.LTE : MatchType.LT;
            Optional<Long> highOrdinalFound = dimensionFilterMapper.getMatchingOrdinal(dimensionName, high, starTreeValues, highMatchType);
            highOrdinalFound.ifPresent(ord -> highOrdinal = ord);
        }
    }

    @Override
    public void matchStarTreeNodes(StarTreeNode parentNode, StarTreeValues starTreeValues, StarTreeNodeCollector collector)
        throws IOException {
        if (parentNode != null && !skipRangeCollection) {
            parentNode.collectChildrenInRange(lowOrdinal, highOrdinal, collector);
        }
    }

    @Override
    public boolean matchDimValue(long ordinal, StarTreeValues starTreeValues) {
        return lowOrdinal <= ordinal && ordinal <= highOrdinal;
    }

}
