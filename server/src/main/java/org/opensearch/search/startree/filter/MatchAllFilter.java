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
import org.opensearch.index.compositeindex.datacube.startree.node.StarTreeNodeType;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.StarTreeNodeCollector;

import java.io.IOException;
import java.util.Iterator;

/**
 * A {@link DimensionFilter} implementation that matches all dimension values.
 */
public class MatchAllFilter implements DimensionFilter {
    @Override
    public void initialiseForSegment(StarTreeValues starTreeValues, SearchContext searchContext) throws IOException {

    }

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
}
