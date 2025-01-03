/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;

import java.io.IOException;

public interface StarTreeCollector {
    // public void collectStarEntry(int starTreeEntryBit, long bucket) throws IOException;

    public void preCompute(LeafReaderContext ctx, CompositeIndexFieldInfo starTree, LongKeyedBucketOrds bucketOrds) throws IOException;
}
