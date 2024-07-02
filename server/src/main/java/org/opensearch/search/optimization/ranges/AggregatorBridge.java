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
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.optimization.ranges.OptimizationContext.Ranges;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * This class holds aggregator-specific logic and provides access to some data from Aggregator
 * <p>
 * To provide the access to data, you can implement this interface as an inner class of the aggregator.
 * Any business logic other than providing data access should stay in the base class of this package.
 *
 * @opensearch.internal
 */
public abstract class AggregatorBridge {

    OptimizationContext optimizationContext;
    MappedFieldType fieldType;

    void setOptimizationContext(OptimizationContext optimizationContext) {
        this.optimizationContext = optimizationContext;
    }

    /**
     * Check whether we can optimize the aggregator
     * If not, don't call the other methods
     *
     * @return result will be saved in optimization context
     */
    protected abstract boolean canOptimize();

    protected abstract void prepare() throws IOException;

    abstract void prepareFromSegment(LeafReaderContext leaf) throws IOException;

    abstract void tryOptimize(PointValues values, BiConsumer<Long, Long> incrementDocCount, Ranges ranges) throws IOException;

    protected abstract Function<Object, Long> bucketOrdProducer();

    protected boolean segmentMatchAll(LeafReaderContext leaf) throws IOException {
        return false;
    }
}
