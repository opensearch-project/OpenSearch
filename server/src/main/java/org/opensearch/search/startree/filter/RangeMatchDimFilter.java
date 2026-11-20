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
public final class RangeMatchDimFilter implements DimensionFilter {

    private final String dimensionName;
    private final Object low;
    private final Object high;
    private final boolean includeLow;
    private final boolean includeHigh;
    private final String subDimensionName;

    // Segment specific, set only on the instances forSegment returns (lowOrdinal is null until then)
    private final Long lowOrdinal;
    private final Long highOrdinal;
    private final boolean skipRangeCollection;
    private final DimensionFilterMapper dimensionFilterMapper;

    public RangeMatchDimFilter(String dimensionName, Object low, Object high, boolean includeLow, boolean includeHigh) {
        this(dimensionName, low, high, includeLow, includeHigh, null);
    }

    public RangeMatchDimFilter(
        String dimensionName,
        Object low,
        Object high,
        boolean includeLow,
        boolean includeHigh,
        String subDimensionName
    ) {
        this.dimensionName = dimensionName;
        this.low = low;
        this.high = high;
        this.includeLow = includeLow;
        this.includeHigh = includeHigh;
        this.subDimensionName = subDimensionName;
        this.lowOrdinal = null;
        this.highOrdinal = null;
        this.skipRangeCollection = false;
        this.dimensionFilterMapper = null;
    }

    /** The request-level filter {@code unbound}, bound to one segment's ordinals. */
    private RangeMatchDimFilter(
        RangeMatchDimFilter unbound,
        long lowOrdinal,
        long highOrdinal,
        boolean skipRangeCollection,
        DimensionFilterMapper dimensionFilterMapper
    ) {
        this.dimensionName = unbound.dimensionName;
        this.low = unbound.low;
        this.high = unbound.high;
        this.includeLow = unbound.includeLow;
        this.includeHigh = unbound.includeHigh;
        this.subDimensionName = unbound.subDimensionName;
        this.lowOrdinal = lowOrdinal;
        this.highOrdinal = highOrdinal;
        this.skipRangeCollection = skipRangeCollection;
        this.dimensionFilterMapper = dimensionFilterMapper;
    }

    @Override
    public DimensionFilter forSegment(StarTreeValues starTreeValues, SearchContext searchContext) {
        DimensionFilterMapper mapper = DimensionFilterMapper.Factory.fromMappedFieldType(
            searchContext.mapperService().fieldType(dimensionName),
            searchContext
        );

        long segmentLow = 0L;
        if (low != null) {
            MatchType lowMatchType = includeLow ? MatchType.GTE : MatchType.GT;
            Optional<Long> lowOrdinalFound = mapper.getMatchingOrdinal(dimensionName, low, starTreeValues, lowMatchType);
            if (lowOrdinalFound.isEmpty()) {
                // This is only valid for Non-numeric fields.
                // High can't be found since nothing >= low exists.
                return new RangeMatchDimFilter(this, Long.MAX_VALUE, Long.MAX_VALUE, true, mapper);
            }
            segmentLow = lowOrdinalFound.get();
        }
        long segmentHigh = Long.MAX_VALUE;
        if (high != null) {
            MatchType highMatchType = includeHigh ? MatchType.LTE : MatchType.LT;
            Optional<Long> highOrdinalFound = mapper.getMatchingOrdinal(dimensionName, high, starTreeValues, highMatchType);
            if (highOrdinalFound.isPresent()) {
                segmentHigh = highOrdinalFound.get();
            }
        }
        return new RangeMatchDimFilter(this, segmentLow, segmentHigh, false, mapper);
    }

    @Override
    public void matchStarTreeNodes(StarTreeNode parentNode, StarTreeValues starTreeValues, StarTreeNodeCollector collector)
        throws IOException {
        requireBound();
        if (parentNode != null && !skipRangeCollection) {
            parentNode.collectChildrenInRange(lowOrdinal, highOrdinal, collector, dimensionFilterMapper);
        }
    }

    @Override
    public boolean matchDimValue(long ordinal, StarTreeValues starTreeValues) {
        requireBound();
        return dimensionFilterMapper.comparator().compare(lowOrdinal, ordinal) <= 0
            && dimensionFilterMapper.comparator().compare(ordinal, highOrdinal) <= 0;
    }

    private void requireBound() {
        if (lowOrdinal == null) {
            throw new IllegalStateException("filter on [" + dimensionName + "] is not bound to a segment; use forSegment");
        }
    }

    @Override
    public String getDimensionName() {
        return dimensionName;
    }

    @Override
    public String getSubDimensionName() {
        return subDimensionName;
    }

    public Object getLow() {
        return low;
    }

    public Object getHigh() {
        return high;
    }

    public boolean isIncludeLow() {
        return includeLow;
    }

    public boolean isIncludeHigh() {
        return includeHigh;
    }

}
