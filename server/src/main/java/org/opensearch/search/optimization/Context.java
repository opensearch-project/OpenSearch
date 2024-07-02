/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.optimization;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.optimization.ranges.AggregatorBridge;

import java.io.IOException;
import java.util.function.BiConsumer;

public abstract class Context {
    protected final AggregatorBridge aggregatorBridge;

    public Context(AggregatorBridge aggregatorBridge) {
        this.aggregatorBridge = aggregatorBridge;
    }

    abstract public boolean canOptimize(final Object parent, final int subAggLength, SearchContext context);

    abstract public void prepare() throws IOException;
    abstract public void prepareFromSegment(LeafReaderContext leaf) throws IOException;

    abstract public boolean tryOptimize(final LeafReaderContext leafCtx, final BiConsumer<Long, Long> incrementDocCount) throws IOException;
}
