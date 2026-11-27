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

/**
 * Contains the logic to filter over a dimension either in StarTree Index or it's Dimension DocValues
 */
@ExperimentalApi
public interface DimensionFilter {
    /**
     * Returns this filter bound to one segment: user values converted to the segment's ordinals, and any other
     * state that depends on the segment. A request's filters are shared by all of its concurrent search slices,
     * which traverse different segments at the same time, so the returned filter must not share mutable state
     * with this one. Filters without per-segment state return {@code this}.
     * @param starTreeValues : Segment specific star tree root node and other metadata
     * @param searchContext : Search context
     * @return the filter to match nodes and values of this segment with
     * @throws IOException :
     */
    DimensionFilter forSegment(StarTreeValues starTreeValues, SearchContext searchContext) throws IOException;

    /**
     * Called when matching a dimension values in the star tree index.
     * @param parentNode : StarTreeNode below which the dimension to be filtered is present.
     * @param starTreeValues : Segment specific star tree root node and other metadata
     * @param collector : Collector which collates the matched StarTreeNode's
     * @throws IOException :
     */
    void matchStarTreeNodes(StarTreeNode parentNode, StarTreeValues starTreeValues, StarTreeNodeCollector collector) throws IOException;

    /**
     * Called when a dimension is not found in star tree index and needs to matched by iterating its docValues
     * @param ordinal : Value to Match
     * @param starTreeValues : Segment specific star tree root node and other metadata
     * @return : true if matches, else false.
     */
    boolean matchDimValue(long ordinal, StarTreeValues starTreeValues);

    String getDimensionName();

    default String getSubDimensionName() {
        return null;
    }

    default String getMatchingDimension() {
        return getSubDimensionName() == null ? getDimensionName() : getSubDimensionName();
    }

    /**
     * Represents how to match a value when comparing during StarTreeTraversal
     */
    @ExperimentalApi
    enum MatchType {
        GT,
        LT,
        GTE,
        LTE,
        EXACT
    }
}
