/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * A streaming aggregator that computes approximate counts of unique values.
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
        CardinalityAggregatorFactory.ExecutionMode executionMode
    ) throws IOException {
        super(name, valuesSourceConfig, precision, context, parent, metadata, executionMode);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        // Clean up previous collector if it exists
        if (streamCollector != null) {
            try {
                streamCollector.postCollect();
            } finally {
                streamCollector.close();
                streamCollector = null;
            }
        }

        // Handle null values source
        if (valuesSource == null) {
            emptyCollectorsUsed++;
            streamCollector = new EmptyCollector();
            return streamCollector;
        }

        // Only support ordinal value sources for streaming
        if (!(valuesSource instanceof ValuesSource.Bytes.WithOrdinals)) {
            throw new IllegalStateException("StreamCardinalityAggregator only supports ordinal value sources");
        }

        // Handle ordinal value sources - always use OrdinalsCollector
        final SortedSetDocValues ordinalValues = ((ValuesSource.Bytes.WithOrdinals) valuesSource).ordinalsValues(ctx);
        final long maxOrd = ordinalValues.getValueCount();
        if (maxOrd == 0) {
            emptyCollectorsUsed++;
            streamCollector = new EmptyCollector();
        } else {
            ordinalsCollectorsUsed++;
            streamCollector = new OrdinalsCollector(counts, ordinalValues, context.bigArrays());
        }
        return streamCollector;
    }

    @Override
    public void doReset() {
        super.doReset();
        // Clean up the stream collector for the next batch
        if (streamCollector != null) {
            streamCollector.close();
            streamCollector = null;
        }
        // Close and recreate the HyperLogLog counts for the next batch
        // HyperLogLog doesn't have a public reset method, so we need to recreate it
        if (counts != null) {
            counts.close();
            counts = valuesSource == null ? null : new HyperLogLogPlusPlus(precision, context.bigArrays(), 1);
        }
    }

    @Override
    protected void doPostCollection() throws IOException {
        if (streamCollector != null) {
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
