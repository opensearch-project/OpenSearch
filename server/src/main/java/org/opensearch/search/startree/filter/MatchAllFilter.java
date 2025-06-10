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
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNodeType;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.StarTreeNodeCollector;

import java.io.IOException;
import java.util.Iterator;

/**
 * Matches all StarTreeNodes
 */
@ExperimentalApi
public class MatchAllFilter implements DimensionFilter {

    public final String dimensionName;
    public final String subDimensionName;

    public MatchAllFilter(String dimensionName) {
        this.dimensionName = dimensionName;
        this.subDimensionName = null;
    }

    public MatchAllFilter(String dimensionName, String subDimensionName) {
        this.dimensionName = dimensionName;
        this.subDimensionName = subDimensionName;
    }

    @Override
    public void initialiseForSegment(StarTreeValues starTreeValues, SearchContext searchContext) throws IOException {}

    @Override
    public void matchStarTreeNodes(StarTreeNode parentNode, StarTreeValues starTreeValues, StarTreeNodeCollector collector)
        throws IOException {
        if (parentNode != null) {
            for (Iterator<? extends StarTreeNode> it = parentNode.getChildrenIterator(); it.hasNext();) {
                StarTreeNode starTreeNode = it.next();
                if (starTreeNode.getStarTreeNodeType() == StarTreeNodeType.DEFAULT.getValue()
                    || starTreeNode.getStarTreeNodeType() == StarTreeNodeType.NULL.getValue()) {
                    collector.collectStarTreeNode(starTreeNode);
                }
            }
        }
    }

    @Override
    public boolean matchDimValue(long ordinal, StarTreeValues starTreeValues) {
        return true;
    }

    @Override
    public String getDimensionName() {
        return dimensionName;
    }

    public String getSubDimensionName() {
        return subDimensionName;
    }
}
