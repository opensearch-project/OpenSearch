/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;

import java.io.IOException;

import static org.opensearch.search.startree.FieldToDimensionOrdinalMapper.MatchType;
import static org.opensearch.search.startree.FieldToDimensionOrdinalMapper.SingletonFactory.getFieldToDimensionOrdinalMapper;

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
    public void initialiseForSegment(StarTreeValues starTreeValues) throws IOException {
        Dimension matchedDim = StarTreeQueryHelper.getMatchingDimensionOrError(
            dimensionName,
            starTreeValues.getStarTreeField().getDimensionsOrder()
        );
        FieldToDimensionOrdinalMapper fieldToDimensionOrdinalMapper = getFieldToDimensionOrdinalMapper(matchedDim.getDocValuesType());
        if (low != null) {
            MatchType lowMatchType = includeLow ? MatchType.GTE : MatchType.GT;
            lowOrdinal = fieldToDimensionOrdinalMapper.getMatchingOrdinal(dimensionName, low, starTreeValues, lowMatchType);
        } else {
            lowOrdinal = 0L;
        }
        if (high != null) {
            MatchType highMatchType = includeHigh ? MatchType.LTE : MatchType.LT;
            highOrdinal = fieldToDimensionOrdinalMapper.getMatchingOrdinal(dimensionName, high, starTreeValues, highMatchType);
        } else {
            highOrdinal = Long.MAX_VALUE;
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
        return lowOrdinal <= ordinal && ordinal <= highOrdinal;
    }
}
