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

public interface StarTreePreComputeCollector {
    StarTreeBucketCollector getStarTreeBucketCollector(LeafReaderContext ctx, CompositeIndexFieldInfo starTree) throws IOException;
}
