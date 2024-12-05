/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.DoubleArray;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeQueryHelper;
import org.opensearch.index.fielddata.NumericDoubleValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.MultiValueMode;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeQueryHelper.getSupportedStarTree;

/**
 * Aggregate all docs into a min value
 *
 * @opensearch.internal
 */
class MinAggregator extends NumericMetricsAggregator.SingleValue {
    private static final int MAX_BKD_LOOKUPS = 1024;

    final ValuesSource.Numeric valuesSource;
    final DocValueFormat format;

    final String pointField;
    final Function<byte[], Number> pointConverter;

    DoubleArray mins;

    MinAggregator(String name, ValuesSourceConfig config, SearchContext context, Aggregator parent, Map<String, Object> metadata)
        throws IOException {
        super(name, context, parent, metadata);
        // TODO: Stop using nulls here
        this.valuesSource = config.hasValues() ? (ValuesSource.Numeric) config.getValuesSource() : null;
        if (valuesSource != null) {
            mins = context.bigArrays().newDoubleArray(1, false);
            mins.fill(0, mins.size(), Double.POSITIVE_INFINITY);
        }
        this.format = config.format();
        this.pointConverter = pointReaderIfAvailable(config);
        if (pointConverter != null) {
            pointField = config.fieldContext().field();
        } else {
            pointField = null;
        }
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource != null && valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            if (parent == null) {
                return LeafBucketCollector.NO_OP_COLLECTOR;
            } else {
                // we have no parent and the values source is empty so we can skip collecting hits.
                throw new CollectionTerminatedException();
            }
        }
        if (pointConverter != null) {
            Number segMin = findLeafMinValue(ctx.reader(), pointField, pointConverter);
            if (segMin != null) {
                /*
                 * There is no parent aggregator (see {@link MinAggregator#getPointReaderOrNull}
                 * so the ordinal for the bucket is always 0.
                 */
                double min = mins.get(0);
                min = Math.min(min, segMin.doubleValue());
                mins.set(0, min);
                // the minimum value has been extracted, we don't need to collect hits on this segment.
                throw new CollectionTerminatedException();
            }
        }

        CompositeIndexFieldInfo supportedStarTree = getSupportedStarTree(this.context);
        if (supportedStarTree != null) {
            return getStarTreeCollector(ctx, sub, supportedStarTree);
        }
        return getDefaultLeafCollector(ctx, sub);
    }

    private LeafBucketCollector getDefaultLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        final BigArrays bigArrays = context.bigArrays();
        final SortedNumericDoubleValues allValues = valuesSource.doubleValues(ctx);
        final NumericDoubleValues values = MultiValueMode.MIN.select(allValues);
        return new LeafBucketCollectorBase(sub, allValues) {

            @Override
            public void collect(int doc, long bucket) throws IOException {
                if (bucket >= mins.size()) {
                    long from = mins.size();
                    mins = bigArrays.grow(mins, bucket + 1);
                    mins.fill(from, mins.size(), Double.POSITIVE_INFINITY);
                }
                if (values.advanceExact(doc)) {
                    final double value = values.doubleValue();
                    double min = mins.get(bucket);
                    min = Math.min(min, value);
                    mins.set(bucket, min);
                }
            }
        };
    }

    public LeafBucketCollector getStarTreeCollector(LeafReaderContext ctx, LeafBucketCollector sub, CompositeIndexFieldInfo starTree)
        throws IOException {
        AtomicReference<Double> min = new AtomicReference<>(mins.get(0));
        return StarTreeQueryHelper.getStarTreeLeafCollector(
            context,
            valuesSource,
            ctx,
            sub,
            starTree,
            MetricStat.MIN.getTypeName(),
            value -> {
                min.set(Math.min(min.get(), (NumericUtils.sortableLongToDouble(value))));
            },
            () -> mins.set(0, min.get())
        );
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (valuesSource == null || owningBucketOrd >= mins.size()) {
            return Double.POSITIVE_INFINITY;
        }
        return mins.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= mins.size()) {
            return buildEmptyAggregation();
        }
        return new InternalMin(name, mins.get(bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalMin(name, Double.POSITIVE_INFINITY, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(mins);
    }

    /**
     * Returns the minimum value indexed in the <code>fieldName</code> field or <code>null</code>
     * if the value cannot be inferred from the indexed {@link PointValues}.
     */
    static Number findLeafMinValue(LeafReader reader, String fieldName, Function<byte[], Number> converter) throws IOException {
        final PointValues pointValues = reader.getPointValues(fieldName);
        if (pointValues == null) {
            return null;
        }
        final Bits liveDocs = reader.getLiveDocs();
        if (liveDocs == null) {
            return converter.apply(pointValues.getMinPackedValue());
        }
        final Number[] result = new Number[1];
        try {
            pointValues.intersect(new PointValues.IntersectVisitor() {
                private short lookupCounter = 0;

                @Override
                public void visit(int docID) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public void visit(int docID, byte[] packedValue) {
                    if (liveDocs.get(docID)) {
                        result[0] = converter.apply(packedValue);
                        // this is the first leaf with a live doc so the value is the minimum for this segment.
                        throw new CollectionTerminatedException();
                    }
                    if (++lookupCounter > MAX_BKD_LOOKUPS) {
                        throw new CollectionTerminatedException();
                    }
                }

                @Override
                public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
            });
        } catch (CollectionTerminatedException e) {}
        return result[0];
    }
}
