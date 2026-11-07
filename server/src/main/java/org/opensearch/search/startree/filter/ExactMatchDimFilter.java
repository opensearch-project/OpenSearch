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
public final class ExactMatchDimFilter implements DimensionFilter {

    private final String dimensionName;

    private final List<Object> rawValues;

    // Segment specific, set only on the instances forSegment returns. Order is essential for successive binary search.
    private final TreeSet<Long> convertedOrdinals;

    private final DimensionFilterMapper dimensionFilterMapper;

    public ExactMatchDimFilter(String dimensionName, List<Object> valuesToMatch) {
        this(dimensionName, valuesToMatch, null, null);
    }

    private ExactMatchDimFilter(
        String dimensionName,
        List<Object> valuesToMatch,
        TreeSet<Long> convertedOrdinals,
        DimensionFilterMapper dimensionFilterMapper
    ) {
        this.dimensionName = dimensionName;
        this.rawValues = valuesToMatch;
        this.convertedOrdinals = convertedOrdinals;
        this.dimensionFilterMapper = dimensionFilterMapper;
    }

    @Override
    public DimensionFilter forSegment(StarTreeValues starTreeValues, SearchContext searchContext) {
        TreeSet<Long> ordinals = new TreeSet<>();
        Dimension matchedDim = StarTreeQueryHelper.getMatchingDimensionOrThrow(
            dimensionName,
            starTreeValues.getStarTreeField().getDimensionsOrder()
        );
        DimensionFilterMapper mapper = DimensionFilterMapper.Factory.fromMappedFieldType(
            searchContext.mapperService().fieldType(dimensionName),
            searchContext
        );

        for (Object rawValue : rawValues) {
            Optional<Long> ordinal = mapper.getMatchingOrdinal(matchedDim.getField(), rawValue, starTreeValues, MatchType.EXACT);
            // Numeric type returning negative ordinal ( same as their value ) is valid
            // Whereas Keyword type returning -ve ordinal indicates it doesn't exist in Star Tree Dimension values.
            ordinal.ifPresent(ordinals::add);
        }
        return new ExactMatchDimFilter(dimensionName, rawValues, ordinals, mapper);
    }

    @Override
    public void matchStarTreeNodes(StarTreeNode parentNode, StarTreeValues starTreeValues, StarTreeNodeCollector collector)
        throws IOException {
        TreeSet<Long> ordinals = boundOrdinals();
        if (parentNode != null) {
            StarTreeNode lastMatchedNode = null;
            for (long ordinal : ordinals) {
                lastMatchedNode = parentNode.getChildForDimensionValue(ordinal, lastMatchedNode, dimensionFilterMapper);
                if (lastMatchedNode != null) {
                    collector.collectStarTreeNode(lastMatchedNode);
                }
            }
        }
    }

    @Override
    public boolean matchDimValue(long ordinal, StarTreeValues starTreeValues) {
        return boundOrdinals().contains(ordinal);
    }

    private TreeSet<Long> boundOrdinals() {
        if (convertedOrdinals == null) {
            throw new IllegalStateException("filter on [" + dimensionName + "] is not bound to a segment; use forSegment");
        }
        return convertedOrdinals;
    }

    public List<Object> getRawValues() {
        return rawValues;
    }

    @Override
    public String getDimensionName() {
        return dimensionName;
    }

}
