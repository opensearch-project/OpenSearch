/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.opensearch.search.startree.FieldToDimensionOrdinalMapper.SingletonFactory.getFieldToDimensionOrdinalMapper;


@ExperimentalApi
public class ExactMatchDimFilter implements DimensionFilter {

    private final String dimensionName;

    private final List<Object> rawValues;

    private List<Long> convertedOrdinals;

    public ExactMatchDimFilter(String dimensionName, List<Object> valuesToMatch) {
        this.dimensionName = dimensionName;
        this.rawValues = valuesToMatch;
    }

    @Override
    public void initialiseForSegment(LeafReaderContext leafReaderContext, StarTreeValues starTreeValues) {
        convertedOrdinals = new ArrayList<>();
        List<Dimension> matchingDimensions = starTreeValues.getStarTreeField()
            .getDimensionsOrder()
            .stream()
            .filter(x -> x.getField().equals(dimensionName))
            .collect(Collectors.toList());
        if (matchingDimensions.size() != 1) {
            throw new IllegalStateException("Expected exactly one dimension but found " + matchingDimensions);
        }
        Dimension matchedDim = matchingDimensions.get(0);
        FieldToDimensionOrdinalMapper fieldToDimensionOrdinalMapper = getFieldToDimensionOrdinalMapper(matchedDim.getDocValuesType());
        for (Object rawValue : rawValues) {
            convertedOrdinals.add(fieldToDimensionOrdinalMapper.getOrdinal(matchedDim.getField(), rawValue, starTreeValues));
        }
    }

    @Override
    public void matchStarTreeNodes(StarTreeNode parentNode, StarTreeValues starTreeValues, StarTreeNodeCollector collector)
        throws IOException {
        if (parentNode.getChildStarNode() != null) {
            Dimension dimension = starTreeValues.getStarTreeField()
                .getDimensionsOrder()
                .get(parentNode.getChildStarNode().getDimensionId());
            if (dimension.getField().equals(dimensionName)) {
                FieldToDimensionOrdinalMapper queryToDimensionOrdinalMapper = getFieldToDimensionOrdinalMapper(dimension.getDocValuesType());
                // TODO : [Optimisation] Implement storing the last searched StarTreeNode nodeId for successive binary search.
                for (Object value : rawValues) {
                    collector.collectStarNode(
                        parentNode.getChildForDimensionValue(
                            queryToDimensionOrdinalMapper.getOrdinal(dimension.getField(), value, starTreeValues)
                        )
                    );
                }
            }
        }
    }

    @Override
    public boolean matchDimValue(long ordinal, StarTreeValues starTreeValues) {
        return convertedOrdinals.contains(ordinal);
    }
}
