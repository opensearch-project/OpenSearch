/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.index.fielddata.plain.HllFieldData;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * An aggregator that computes cardinality from pre-aggregated HLL++ sketch fields.
 * This aggregator merges HLL++ sketches stored in documents to produce a combined cardinality estimate.
 *
 * @opensearch.internal
 */
public class HllCardinalityAggregator extends NumericMetricsAggregator.SingleValue {

    private static final Logger logger = LogManager.getLogger(HllCardinalityAggregator.class);

    private final HllFieldData fieldData;
    private final int precision;
    private HyperLogLogPlusPlus counts;

    HllCardinalityAggregator(
        String name,
        HllFieldData fieldData,
        int precision,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.fieldData = fieldData;
        this.precision = precision;
        this.counts = null; // Lazy initialization
    }

    @Override
    public org.apache.lucene.search.ScoreMode scoreMode() {
        return org.apache.lucene.search.ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        final HllFieldData.HllLeafFieldData leafData = fieldData.load(ctx);

        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                AbstractHyperLogLogPlusPlus sketch = null;
                try {
                    sketch = leafData.getSketch(doc);
                    if (sketch != null) {
                        // Lazy initialize counts on first sketch
                        // Initialize with enough capacity for the current bucket
                        if (counts == null) {
                            counts = new HyperLogLogPlusPlus(precision, context.bigArrays(), bucket + 1);
                        }

                        // Grow if needed to accommodate this bucket
                        if (bucket >= counts.maxOrd()) {
                            HyperLogLogPlusPlus newCounts = new HyperLogLogPlusPlus(precision, context.bigArrays(), bucket + 1);
                            for (long i = 0; i < counts.maxOrd(); i++) {
                                if (counts.cardinality(i) > 0) {
                                    newCounts.merge(i, counts, i);
                                }
                            }
                            counts.close();
                            counts = newCounts;
                        }

                        // Merge the stored sketch into our aggregation sketch
                        counts.merge(bucket, sketch, 0);
                    }
                } catch (IllegalArgumentException e) {
                    // Log precision mismatch or other merge errors
                    logger.warn(
                        "Failed to merge HLL++ sketch for field [{}] in document {}: {}",
                        fieldData.getFieldName(),
                        doc,
                        e.getMessage()
                    );
                    throw e;
                } finally {
                    if (sketch != null) {
                        sketch.close();
                    }
                }
            }
        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        return counts == null ? 0 : counts.cardinality(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (counts == null || owningBucketOrdinal >= counts.maxOrd() || counts.cardinality(owningBucketOrdinal) == 0) {
            return buildEmptyAggregation();
        }
        // Build a copy because the returned Aggregation needs to remain usable after
        // this Aggregator (and its HLL++ counters) is released
        AbstractHyperLogLogPlusPlus copy = counts.clone(owningBucketOrdinal, BigArrays.NON_RECYCLING_INSTANCE);
        return new InternalCardinality(name, copy, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalCardinality(name, null, metadata());
    }

    @Override
    protected void doClose() {
        Releasables.close(counts);
    }
}
