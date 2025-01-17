/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;

import java.io.IOException;
import java.util.List;
import java.util.TreeSet;

import static org.opensearch.search.startree.FieldToDimensionOrdinalMapper.SingletonFactory.getFieldToDimensionOrdinalMapper;

@ExperimentalApi
public class ExactMatchDimFilter implements DimensionFilter {

    private final String dimensionName;

    private final List<Object> rawValues;

    private TreeSet<Long> convertedOrdinals;

    public ExactMatchDimFilter(String dimensionName, List<Object> valuesToMatch) {
        this.dimensionName = dimensionName;
        this.rawValues = valuesToMatch;
    }

    @Override
    public void initialiseForSegment(StarTreeValues starTreeValues) {
        convertedOrdinals = new TreeSet<>();
        Dimension matchedDim = StarTreeQueryHelper.getMatchingDimensionOrError(dimensionName, starTreeValues);
        FieldToDimensionOrdinalMapper fieldToDimensionOrdinalMapper = getFieldToDimensionOrdinalMapper(matchedDim.getDocValuesType());
        for (Object rawValue : rawValues) {
            long ordinal = fieldToDimensionOrdinalMapper.getMatchingOrdinal(
                matchedDim.getField(),
                rawValue,
                starTreeValues,
                FieldToDimensionOrdinalMapper.MatchType.EXACT
            );
            if (ordinal >= 0) {
                convertedOrdinals.add(ordinal);
            }
        }
    }

    @Override
    public void matchStarTreeNodes(StarTreeNode parentNode, StarTreeValues starTreeValues, StarTreeNodeCollector collector)
        throws IOException {
        if (parentNode != null) {
            // TODO : [Optimisation] Implement storing the last searched StarTreeNode nodeId for successive binary search.
            StarTreeNode lastMatchedNode = null;
            for (long ordinal : convertedOrdinals) {
                lastMatchedNode = parentNode.getChildForDimensionValue(ordinal, lastMatchedNode);
                if (lastMatchedNode != null) {
                    collector.collectStarNode(lastMatchedNode);
                }
            }
        }
    }

    @Override
    public boolean matchDimValue(long ordinal, StarTreeValues starTreeValues) {
        return convertedOrdinals.contains(ordinal);
    }
}
