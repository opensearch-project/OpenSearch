/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.optimization.ranges;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * To do the optimization, we need access to some data from Aggregator
 * <p>
 * To provide access, implement this interface as an inner class of the aggregator,
 * Any business logic other than providing data can be put into a base abstract class
 *
 * @opensearch.internal
 */
public abstract class AggregatorDataProvider {

    protected OptimizationContext optimizationContext;

    /**
     * Check whether we can optimize the aggregator
     * If not, don't call the other methods
     *
     * @return result will be saved in optimization context
     */
    protected abstract boolean canOptimize();

    void setOptimizationContext(OptimizationContext optimizationContext) {
        this.optimizationContext = optimizationContext;
    }

    protected abstract void buildRanges(SearchContext ctx) throws IOException;

    protected abstract void buildRanges(LeafReaderContext leaf, SearchContext ctx) throws IOException;

    protected abstract void tryFastFilterAggregation(PointValues values, BiConsumer<Long, Long> incrementDocCount) throws IOException;

    protected abstract Function<Object, Long> bucketOrdProducer();
}
