/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.search.startree.filter.DimensionFilter;

import java.io.IOException;
import java.util.List;

/**
 * This interface is used to pre-compute the star tree bucket collector for each segment/leaf.
 * It is utilized by parent aggregation to retrieve a StarTreeBucketCollector which can be used to
 * pre-compute the associated aggregation along with its parent pre-computation using star-tree
 *
 * @opensearch.internal
 */
public interface StarTreePreComputeCollector {
    /**
     * Get the star tree bucket collector for the specified segment/leaf
     */
    StarTreeBucketCollector getStarTreeBucketCollector(
        LeafReaderContext ctx,
        CompositeIndexFieldInfo starTree,
        StarTreeBucketCollector parentCollector
    ) throws IOException;

    /**
     * Returns the list of dimensions filters involved in this aggregation, which are required for
     * merging dimension filters during StarTree precomputation. This is specifically needed
     * for nested bucket aggregations to ensure that the correct dimensions are considered when
     * constructing or merging filters during StarTree traversal.
     * For metric aggregations, there is no need to specify dimensions since they operate
     * purely on values within the buckets formed by parent bucket aggregations.
     *
     * @return List of dimension field names involved in the aggregation.
     */
    default List<DimensionFilter> getDimensionFilters() {
        return null;
    }
}
