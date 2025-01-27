/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree.filter;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.StarTreeNodeCollector;
import org.opensearch.search.startree.StarTreeQueryHelper;
import org.opensearch.search.startree.filter.provider.DimensionFilterMapper;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.TreeSet;

/**
 * Handles Term and Terms query like search in StarTree Dimension filtering.
 */
@ExperimentalApi
public class ExactMatchDimFilter implements DimensionFilter {

    private final String dimensionName;

    private final List<Object> rawValues;

    // Order is essential for successive binary search
    private TreeSet<Long> convertedOrdinals;

    public ExactMatchDimFilter(String dimensionName, List<Object> valuesToMatch) {
        this.dimensionName = dimensionName;
        this.rawValues = valuesToMatch;
    }

    @Override
    public void initialiseForSegment(StarTreeValues starTreeValues, SearchContext searchContext) {
        convertedOrdinals = new TreeSet<>();
        Dimension matchedDim = StarTreeQueryHelper.getMatchingDimensionOrError(
            dimensionName,
            starTreeValues.getStarTreeField().getDimensionsOrder()
        );
        DimensionFilterMapper dimensionFilterMapper = DimensionFilterMapper.Factory.fromMappedFieldType(
            searchContext.mapperService().fieldType(dimensionName)
        );
        for (Object rawValue : rawValues) {
            Optional<Long> ordinal = dimensionFilterMapper.getMatchingOrdinal(
                matchedDim.getField(),
                rawValue,
                starTreeValues,
                MatchType.EXACT
            );
            // Numeric type returning negative ordinal ( same as their value ) is valid
            // Whereas Keyword type returning -ve ordinal indicates it doesn't exist in Star Tree Dimension values.
            ordinal.ifPresent(aLong -> convertedOrdinals.add(aLong));
        }
    }

    @Override
    public void matchStarTreeNodes(StarTreeNode parentNode, StarTreeValues starTreeValues, StarTreeNodeCollector collector)
        throws IOException {
        if (parentNode != null) {
            StarTreeNode lastMatchedNode = null;
            for (long ordinal : convertedOrdinals) {
                lastMatchedNode = parentNode.getChildForDimensionValue(ordinal, lastMatchedNode);
                if (lastMatchedNode != null) {
                    collector.collectStarTreeNode(lastMatchedNode);
                }
            }
        }
    }

    @Override
    public boolean matchDimValue(long ordinal, StarTreeValues starTreeValues) {
        return convertedOrdinals.contains(ordinal);
    }
}
