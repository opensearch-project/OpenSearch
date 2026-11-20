/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.opensearch.common.Nullable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.ObjectArray;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Aggregates all the values of the matching documents into a boxplot, using a t-digest per bucket.
 *
 * @opensearch.internal
 */
public class BoxplotAggregator extends NumericMetricsAggregator.MultiValue {

    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat format;
    private final double compression;

    private ObjectArray<TDigestState> valueSketches;

    BoxplotAggregator(
        String name,
        @Nullable ValuesSource valuesSource,
        DocValueFormat format,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        double compression
    ) throws IOException {
        super(name, context, parent, metadata);

        this.valuesSource = (ValuesSource.Numeric) valuesSource;
        this.format = Objects.requireNonNull(format);
        this.compression = compression;
        this.valueSketches = context.bigArrays().newObjectArray(1);
    }

    private boolean hasDataForBucket(long bucketOrd) {
        return bucketOrd < valueSketches.size() && valueSketches.get(bucketOrd) != null;
    }

    @Override
    public boolean hasMetric(String name) {
        try {
            InternalBoxplot.Metrics.resolve(name);
            return true;
        } catch (IllegalArgumentException iae) {
            return false;
        }
    }

    @Override
    public double metric(String name, long owningBucketOrd) {
        final InternalBoxplot.Metrics metric = InternalBoxplot.Metrics.resolve(name);
        if (hasDataForBucket(owningBucketOrd) == false) {
            return Double.NaN;
        }
        return InternalBoxplot.computeStats(valueSketches.get(owningBucketOrd))[metric.ordinal()];
    }

    @Override
    public ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        } else {
            return ScoreMode.COMPLETE_NO_SCORES;
        }
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        final BigArrays bigArrays = context.bigArrays();
        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (values.advanceExact(doc) == false) {
                    return;
                }

                valueSketches = bigArrays.grow(valueSketches, bucket + 1);

                TDigestState valueSketch = valueSketches.get(bucket);
                if (valueSketch == null) {
                    valueSketch = new TDigestState(compression);
                    valueSketches.set(bucket, valueSketch);
                }

                final int valueCount = values.docValueCount();
                for (int i = 0; i < valueCount; i++) {
                    // every value has weight one, which keeps the minimum and the maximum of the sketch exact
                    valueSketch.add(values.nextValue());
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) throws IOException {
        if (hasDataForBucket(bucket)) {
            return new InternalBoxplot(name, metadata(), format, valueSketches.get(bucket));
        } else {
            return buildEmptyAggregation();
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalBoxplot(name, metadata(), format, new TDigestState(compression));
    }

    @Override
    public void doClose() {
        Releasables.close(valueSketches);
    }
}
