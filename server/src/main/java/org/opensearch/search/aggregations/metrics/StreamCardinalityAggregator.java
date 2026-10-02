/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * A streaming aggregator that computes approximate counts of unique values.
 * Uses {@link CardinalityAggregator.DeferredOrdinalsCollector} to defer expensive
 * ordinal-to-value hashing until after the parent terms aggregation selects top-N buckets.
 *
 * @opensearch.internal
 */
public class StreamCardinalityAggregator extends CardinalityAggregator {

    private Collector streamCollector;

    public StreamCardinalityAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        int precision,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        CardinalityAggregatorFactory.ExecutionMode executionMode,
        CardinalityUpperBound bucketCardinality
    ) throws IOException {
        super(name, valuesSourceConfig, precision, context, parent, metadata, executionMode, bucketCardinality);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        // Clean up previous collector if it exists
        cleanupCollector();

        // Handle null values source
        if (valuesSource == null) {
            emptyCollectorsUsed++;
            streamCollector = new EmptyCollector();
            deferredCollector = null;
            return streamCollector;
        }

        // Only support ordinal value sources for streaming
        if (!(valuesSource instanceof ValuesSource.Bytes.WithOrdinals)) {
            throw new IllegalStateException("StreamCardinalityAggregator only supports ordinal value sources");
        }

        // Handle ordinal value sources - use DeferredOrdinalsCollector with global ordinals
        if (deferredCollector == null) {
            deferredCollector = new DeferredOrdinalsCollector(
                counts,
                (ValuesSource.Bytes.WithOrdinals) valuesSource,
                context.bigArrays(),
                context.searcher()
            );
        }
        streamCollector = deferredCollector.leafCollector(ctx);
        return streamCollector;
    }

    @Override
    public double metric(long owningBucketOrd) {
        // Use ordinal-based cardinality for ranking (before materialization)
        if (deferredCollector != null) {
            return deferredCollector.ordinalCardinality(owningBucketOrd);
        }
        return super.metric(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) throws IOException {
        return super.buildAggregation(owningBucketOrdinal);
    }

    @Override
    public void doReset() {
        super.doReset();
        cleanupCollector();
        // Recreate HLL for the next batch
        if (counts != null) {
            counts.close();
            counts = valuesSource == null ? null : new HyperLogLogPlusPlus(precision, context.bigArrays(), 1);
        }
    }

    @Override
    protected void doPostCollection() throws IOException {
        // No-op for deferred collector — keep it alive for materialization in buildAggregation.
        // For non-deferred (empty) collector, do the standard cleanup.
        if (deferredCollector == null && streamCollector != null) {
            try {
                streamCollector.postCollect();
            } finally {
                streamCollector.close();
                streamCollector = null;
            }
        }
    }

    @Override
    protected void doClose() {
        super.doClose();
        cleanupCollector();
    }

    private void cleanupCollector() {
        if (streamCollector != null) {
            streamCollector.close();
            streamCollector = null;
        }
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
    }
}
