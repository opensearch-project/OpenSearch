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

import java.io.IOException;

@ExperimentalApi
public interface DimensionFilter {

    public void initialiseForSegment(StarTreeValues starTreeValues, SearchContext searchContext) throws IOException;

    public void matchStarTreeNodes(StarTreeNode parentNode, StarTreeValues starTreeValues, StarTreeNodeCollector collector)
        throws IOException;

    public boolean matchDimValue(long ordinal, StarTreeValues starTreeValues);

    DimensionFilter MATCH_ALL_FILTER = new DimensionFilter() {
        @Override
        public void initialiseForSegment(StarTreeValues starTreeValues, SearchContext searchContext) {}

        @Override
        public void matchStarTreeNodes(StarTreeNode parentNode, StarTreeValues starTreeValues, StarTreeNodeCollector collector)
            throws IOException {
            parentNode.getChildrenIterator().forEachRemaining(collector::collectStarNode);
        }

        @Override
        public boolean matchDimValue(long ordinal, StarTreeValues starTreeValues) {
            return true;
        }
    };

    DimensionFilter MATCH_NONE = new DimensionFilter() {
        @Override
        public void initialiseForSegment(StarTreeValues starTreeValues, SearchContext searchContext) {}

        @Override
        public void matchStarTreeNodes(StarTreeNode parentNode, StarTreeValues starTreeValues, StarTreeNodeCollector collector)
            throws IOException {}

        @Override
        public boolean matchDimValue(long ordinal, StarTreeValues starTreeValues) {
            return false;
        }
    };

}
