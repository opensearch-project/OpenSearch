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

import java.io.IOException;

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
}
