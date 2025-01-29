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

/**
 * Filter which matches no StarTreeNodes.
 */
@ExperimentalApi
public class MatchNoneFilter implements DimensionFilter {
    @Override
    public void initialiseForSegment(StarTreeValues starTreeValues, SearchContext searchContext) {
        // Nothing to do as we won't match anything.
    }

    @Override
    public void matchStarTreeNodes(StarTreeNode parentNode, StarTreeValues starTreeValues, StarTreeNodeCollector collector) {
        // Don't match any star tree node.
    }

    @Override
    public boolean matchDimValue(long ordinal, StarTreeValues starTreeValues) {
        return false;
    }
}
