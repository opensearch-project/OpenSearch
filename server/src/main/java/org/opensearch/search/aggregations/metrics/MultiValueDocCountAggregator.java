/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ScoreMode;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.LongArray;
import org.opensearch.index.fielddata.MultiGeoPointValues;
import org.opensearch.index.fielddata.SortedBinaryDocValues;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

/**
 * Aggregation Builder for multivalue_doc_count agg
 *
 * @opensearch.internal
 */
public class MultiValueDocCountAggregator extends NumericMetricsAggregator.SingleValue {

    final ValuesSource valuesSource;

    LongArray counts;

    public MultiValueDocCountAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        SearchContext aggregationContext,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, aggregationContext, parent, metadata);
        this.valuesSource = valuesSourceConfig.hasValues() ? valuesSourceConfig.getValuesSource() : null;
        if (valuesSource != null) {
            counts = context.bigArrays().newLongArray(1, true);
        }
    }

    @Override
    protected boolean tryPrecomputeAggregationForLeaf(LeafReaderContext ctx) throws IOException {
        if (valuesSource == null) {
            return false;
        }

        if (valuesSource instanceof ValuesSource.Numeric) {
            SortedNumericDocValues values = ((ValuesSource.Numeric) valuesSource).longValues(ctx);
            if (DocValues.unwrapSingleton(values) != null) {
                return true;
            }
        }

        else if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals) {
            SortedSetDocValues values = ((ValuesSource.Bytes.WithOrdinals) valuesSource).ordinalsValues(ctx);
            if (DocValues.unwrapSingleton(values) != null) {
                return true;
            }
        }

        // Skip the StarTree optimization.
        return false;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();

        if (valuesSource instanceof ValuesSource.Numeric) {
            final SortedNumericDocValues values = ((ValuesSource.Numeric) valuesSource).longValues(ctx);
            return new LeafBucketCollectorBase(sub, values) {

                @Override
                public void collect(int doc, long bucket) throws IOException {
                    counts = bigArrays.grow(counts, bucket + 1);
                    if (values.advanceExact(doc) && values.docValueCount() > 1) {
                        counts.increment(bucket, 1);
                    }
                }
            };
        }
        if (valuesSource instanceof ValuesSource.Bytes.GeoPoint) {
            MultiGeoPointValues values = ((ValuesSource.GeoPoint) valuesSource).geoPointValues(ctx);
            return new LeafBucketCollectorBase(sub, null) {

                @Override
                public void collect(int doc, long bucket) throws IOException {
                    counts = bigArrays.grow(counts, bucket + 1);
                    if (values.advanceExact(doc) && values.docValueCount() > 1) {
                        counts.increment(bucket, 1);
                    }
                }
            };
        }
        // The following is default collector. Including the keyword FieldType
        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                counts = bigArrays.grow(counts, bucket + 1);
                int i = values.docValueCount();
                if (values.advanceExact(doc) && values.docValueCount() > 1) {
                    counts.increment(bucket, 1);
                }
            }
        };
    }

    @Override
    public double metric(long owningBucketOrd) {
        return (valuesSource == null || owningBucketOrd >= counts.size()) ? 0 : counts.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= counts.size()) {
            return buildEmptyAggregation();
        }
        return new InternalValueCount(name, counts.get(bucket), metadata());
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource != null && valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalValueCount(name, 0L, metadata());
    }

    @Override
    public void doClose() {
        if (counts != null) {
            Releasables.close(counts);
        }
    }
}
