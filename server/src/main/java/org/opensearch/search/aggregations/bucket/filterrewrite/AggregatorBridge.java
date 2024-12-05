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
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

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

    Consumer<Ranges> setRanges;

    void setRangesConsumer(Consumer<Ranges> setRanges) {
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
    abstract Ranges tryBuildRangesFromSegment(LeafReaderContext leaf) throws IOException;

    /**
     * Attempts to build aggregation results for a segment
     *
     * @param values            the point values (index structure for numeric values) for a segment
     * @param incrementDocCount a consumer to increment the document count for a range bucket. The First parameter is document count, the second is the key of the bucket
     * @param ranges
     */
    abstract FilterRewriteOptimizationContext.DebugInfo tryOptimize(
        PointValues values,
        BiConsumer<Long, Long> incrementDocCount,
        Ranges ranges
    ) throws IOException;

    /**
     * Checks whether the top level query matches all documents on the segment
     *
     * <p>This method creates a weight from the search context's query and checks whether the weight's
     * document count matches the total number of documents in the leaf reader context.
     *
     * @param ctx      the search context
     * @param leafCtx  the leaf reader context for the segment
     * @return {@code true} if the segment matches all documents, {@code false} otherwise
     */
    public static boolean segmentMatchAll(SearchContext ctx, LeafReaderContext leafCtx) throws IOException {
        Weight weight = ctx.query().rewrite(ctx.searcher()).createWeight(ctx.searcher(), ScoreMode.COMPLETE_NO_SCORES, 1f);
        return weight != null && weight.count(leafCtx) == leafCtx.reader().numDocs();
    }
}
