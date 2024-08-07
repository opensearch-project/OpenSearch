/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.filterrewrite;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.opensearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.function.BiConsumer;

/**
 * This interface provides a bridge between an aggregator and the optimization context, allowing
 * the aggregator to provide data and optimize the aggregation process.
 *
 * <p>The main purpose of this interface is to encapsulate the aggregator-specific optimization
 * logic and provide access to the data in Aggregator that is required for optimization, while keeping the optimization
 * business logic separate from the aggregator implementation.
 *
 * <p>To use this interface to optimize an aggregator, you should subclass this interface in this package
 * and put any specific optimization business logic in it. Then implement this subclass in the aggregator
 * to provide data that is needed for doing the optimization
 *
 * @opensearch.internal
 */
public abstract class AggregatorBridge {

    /**
     * The optimization context associated with this aggregator bridge.
     */
    FilterRewriteOptimizationContext filterRewriteOptimizationContext;

    /**
     * The field type associated with this aggregator bridge.
     */
    MappedFieldType fieldType;

    void setOptimizationContext(FilterRewriteOptimizationContext context) {
        this.filterRewriteOptimizationContext = context;
    }

    /**
     * Checks whether the aggregator can be optimized.
     *
     * @return {@code true} if the aggregator can be optimized, {@code false} otherwise.
     * The result will be saved in the optimization context.
     */
    protected abstract boolean canOptimize();

    /**
     * Prepares the optimization at shard level after checking aggregator is optimizable.
     * For example, figure out what are the ranges from the aggregation to do the optimization later
     */
    protected abstract void prepare() throws IOException;

    /**
     * Prepares the optimization for a specific segment and ignore whatever built at shard level
     *
     * @param leaf the leaf reader context for the segment
     */
    protected abstract void prepareFromSegment(LeafReaderContext leaf) throws IOException;

    /**
     * Attempts to build aggregation results for a segment
     *
     * @param values              the point values (index structure for numeric values) for a segment
     * @param incrementDocCount   a consumer to increment the document count for a range bucket. The First parameter is document count, the second is the key of the bucket
     */
    protected abstract void tryOptimize(PointValues values, BiConsumer<Long, Long> incrementDocCount) throws IOException;
}
