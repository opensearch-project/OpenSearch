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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.DoubleArray;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.StarTreeBucketCollector;
import org.opensearch.search.aggregations.StarTreePreComputeCollector;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.startree.StarTreeQueryHelper;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.search.startree.StarTreeQueryHelper.getSupportedStarTree;

/**
 * Aggregate all docs into a single sum value
 *
 * @opensearch.internal
 */
public class SumAggregator extends NumericMetricsAggregator.SingleValue implements StarTreePreComputeCollector {

    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat format;

    private DoubleArray sums;
    private DoubleArray compensations;

    SumAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        // TODO: stop expecting nulls here
        this.valuesSource = valuesSourceConfig.hasValues() ? (ValuesSource.Numeric) valuesSourceConfig.getValuesSource() : null;
        this.format = valuesSourceConfig.format();
        if (valuesSource != null) {
            sums = context.bigArrays().newDoubleArray(1, true);
            compensations = context.bigArrays().newDoubleArray(1, true);
        }
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource != null && valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    protected boolean tryPrecomputeAggregationForLeaf(LeafReaderContext ctx) throws IOException {
        if (valuesSource == null) {
            return false;
        }
        CompositeIndexFieldInfo supportedStarTree = getSupportedStarTree(this.context.getQueryShardContext());
        if (supportedStarTree != null) {
            if (parent != null && subAggregators.length == 0) {
                // If this a child aggregator, then the parent will trigger star-tree pre-computation.
                // Returning NO_OP_COLLECTOR explicitly because the getLeafCollector() are invoked starting from innermost aggregators
                return true;
            }
            precomputeLeafUsingStarTree(ctx, supportedStarTree);
            return true;
        }
        return false;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {

        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final BigArrays bigArrays = context.bigArrays();
        final SortedNumericDoubleValues values = valuesSource.doubleValues(ctx);
        final CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                sums = bigArrays.grow(sums, bucket + 1);
                compensations = bigArrays.grow(compensations, bucket + 1);

                if (values.advanceExact(doc)) {
                    final int valuesCount = values.docValueCount();
                    // Compute the sum of double values with Kahan summation algorithm which is more
                    // accurate than naive summation.
                    double sum = sums.get(bucket);
                    double compensation = compensations.get(bucket);
                    kahanSummation.reset(sum, compensation);

                    for (int i = 0; i < valuesCount; i++) {
                        double value = values.nextValue();
                        kahanSummation.add(value);
                    }

                    compensations.set(bucket, kahanSummation.delta());
                    sums.set(bucket, kahanSummation.value());
                }
            }
        };
    }

    private void precomputeLeafUsingStarTree(LeafReaderContext ctx, CompositeIndexFieldInfo starTree) throws IOException {
        final CompensatedSum kahanSummation = new CompensatedSum(sums.get(0), compensations.get(0));

        StarTreeQueryHelper.precomputeLeafUsingStarTree(
            context,
            valuesSource,
            ctx,
            starTree,
            MetricStat.SUM.getTypeName(),
            value -> kahanSummation.add(NumericUtils.sortableLongToDouble(value)),
            () -> {
                sums.set(0, kahanSummation.value());
                compensations.set(0, kahanSummation.delta());
            }
        );
    }

    /**
     * The parent aggregator invokes this method to get a StarTreeBucketCollector,
     * which exposes collectStarTreeEntry() to be evaluated on filtered star tree entries
     */
    public StarTreeBucketCollector getStarTreeBucketCollector(
        LeafReaderContext ctx,
        CompositeIndexFieldInfo starTree,
        StarTreeBucketCollector parentCollector
    ) throws IOException {
        final CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        return StarTreeQueryHelper.getStarTreeBucketMetricCollector(
            starTree,
            MetricStat.SUM.getTypeName(),
            valuesSource,
            parentCollector,
            (bucket) -> {
                sums = context.bigArrays().grow(sums, bucket + 1);
                compensations = context.bigArrays().grow(compensations, bucket + 1);
            },
            (bucket, metricValue) -> {
                kahanSummation.reset(sums.get(bucket), compensations.get(bucket));
                kahanSummation.add(NumericUtils.sortableLongToDouble(metricValue));
                sums.set(bucket, kahanSummation.value());
                compensations.set(bucket, kahanSummation.delta());
            }
        );
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (valuesSource == null || owningBucketOrd >= sums.size()) {
            return 0.0;
        }
        return sums.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= sums.size()) {
            return buildEmptyAggregation();
        }
        return new InternalSum(name, sums.get(bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalSum(name, 0.0, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(sums, compensations);
    }
}
