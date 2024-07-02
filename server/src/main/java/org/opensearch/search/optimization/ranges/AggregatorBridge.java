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

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * This class holds aggregator-specific optimization logic and
 * provides optimization necessary access to the data from Aggregator
 * <p>
 * To provide the access to data, instantiate this class inside the aggregator
 * and send in data through the implemented methods
 * <p>
 * The optimization business logic other than providing data should stay in this package.
 *
 * @opensearch.internal
 */
public abstract class AggregatorBridge {

    OptimizationContext optimizationContext;
    MappedFieldType fieldType;

    void setOptimizationContext(OptimizationContext context) {
        this.optimizationContext = context;
    }

    /**
     * Check whether we can optimize the aggregator
     * If not, don't call the other methods
     *
     * @return result will be saved in optimization context
     */
    public abstract boolean canOptimize();

    public abstract void prepare() throws IOException;

    public abstract void prepareFromSegment(LeafReaderContext leaf) throws IOException;

    public abstract void tryOptimize(PointValues values, BiConsumer<Long, Long> incrementDocCount) throws IOException;

    protected abstract Function<Object, Long> bucketOrdProducer();

    protected boolean segmentMatchAll(LeafReaderContext leaf) throws IOException {
        return false;
    }
}
