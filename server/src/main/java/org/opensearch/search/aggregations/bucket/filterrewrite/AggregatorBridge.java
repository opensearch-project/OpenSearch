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
import org.opensearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.opensearch.search.aggregations.bucket.filterrewrite.PointTreeTraversal.multiRangesTraverse;

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
     * The field type associated with this aggregator bridge.
     */
    MappedFieldType fieldType;

    Consumer<PackedValueRanges> setRanges;

    void setRangesConsumer(Consumer<PackedValueRanges> setRanges) {
        this.setRanges = setRanges;
    }

    /**
     * Checks whether the aggregator can be optimized.
     * <p>
     * This method is supposed to be implemented in a specific aggregator to take in fields from there
     *
     * @return {@code true} if the aggregator can be optimized, {@code false} otherwise.
     * The result will be saved in the optimization context.
     */
    protected abstract boolean canOptimize();

    /**
     * Prepares the optimization at shard level after checking aggregator is optimizable.
     * <p>
     * For example, figure out what are the ranges from the aggregation to do the optimization later
     * <p>
     * This method is supposed to be implemented in a specific aggregator to take in fields from there
     */
    protected abstract void prepare() throws IOException;

    /**
     * Prepares the optimization for a specific segment when the segment is functionally matching all docs
     *
     * @param leaf the leaf reader context for the segment
     */
    abstract PackedValueRanges tryBuildRangesFromSegment(LeafReaderContext leaf) throws IOException;

    /**
     * @return max range to stop collecting at.
     * Utilized by aggs which stop early.
     */
    protected int rangeMax() {
        return Integer.MAX_VALUE;
    }

    /**
     * Translate an index of the packed value range array to an agg bucket ordinal.
     */
    protected long getOrd(int rangeIdx) {
        return rangeIdx;
    }

    /**
     * Attempts to build aggregation results for a segment.
     * With no sub agg count docs and avoid iterating docIds.
     * If a sub agg is present we must iterate through and collect docIds to support it.
     *
     * @param values              the point values (index structure for numeric values) for a segment
     * @param incrementDocCount   a consumer to increment the document count for a range bucket. The First parameter is document count, the second is the key of the bucket
     */
    public final FilterRewriteOptimizationContext.DebugInfo tryOptimize(
        PointValues values,
        BiConsumer<Long, Long> incrementDocCount,
        PackedValueRanges ranges,
        final LeafBucketCollector sub
    ) throws IOException {
        PointTreeTraversal.RangeAwareIntersectVisitor treeVisitor;

        if (sub != null) {
            treeVisitor = new PointTreeTraversal.DocCollectRangeAwareIntersectVisitor(
                values.getPointTree(),
                ranges,
                rangeMax(),
                (activeIndex, docID) -> {
                    long ord = this.getOrd(activeIndex);
                    try {
                        incrementDocCount.accept(ord, (long) 1);
                        sub.collect(docID, ord);
                    } catch (IOException ioe) {
                        throw new RuntimeException(ioe);
                    }
                }
            );
        } else {
            treeVisitor = new PointTreeTraversal.DocCountRangeAwareIntersectVisitor(
                values.getPointTree(),
                ranges,
                rangeMax(),
                (activeIndex, docCount) -> {
                    long ord = this.getOrd(activeIndex);
                    incrementDocCount.accept(ord, (long) docCount);
                }
            );
        }

        return multiRangesTraverse(treeVisitor);
    }
}
