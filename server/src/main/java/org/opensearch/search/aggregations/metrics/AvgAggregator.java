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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.DoubleArray;
import org.opensearch.common.util.LongArray;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.StarTreeUtils;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
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

import static org.opensearch.search.startree.StarTreeQueryHelper.getStarTreeFilteredValues;
import static org.opensearch.search.startree.StarTreeQueryHelper.getSupportedStarTree;

/**
 * Aggregate all docs into an average
 *
 * @opensearch.internal
 */
class AvgAggregator extends NumericMetricsAggregator.SingleValue implements StarTreePreComputeCollector {

    final ValuesSource.Numeric valuesSource;

    LongArray counts;
    DoubleArray sums;
    DoubleArray compensations;
    DocValueFormat format;

    AvgAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        // TODO Stop expecting nulls here
        this.valuesSource = valuesSourceConfig.hasValues() ? (ValuesSource.Numeric) valuesSourceConfig.getValuesSource() : null;
        this.format = valuesSourceConfig.format();
        if (valuesSource != null) {
            final BigArrays bigArrays = context.bigArrays();
            counts = bigArrays.newLongArray(1, true);
            sums = bigArrays.newDoubleArray(1, true);
            compensations = bigArrays.newDoubleArray(1, true);
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
                counts = bigArrays.grow(counts, bucket + 1);
                sums = bigArrays.grow(sums, bucket + 1);
                compensations = bigArrays.grow(compensations, bucket + 1);

                if (values.advanceExact(doc)) {
                    final int valueCount = values.docValueCount();
                    counts.increment(bucket, valueCount);
                    // Compute the sum of double values with Kahan summation algorithm which is more
                    // accurate than naive summation.
                    double sum = sums.get(bucket);
                    double compensation = compensations.get(bucket);

                    kahanSummation.reset(sum, compensation);

                    for (int i = 0; i < valueCount; i++) {
                        double value = values.nextValue();
                        kahanSummation.add(value);
                    }

                    sums.set(bucket, kahanSummation.value());
                    compensations.set(bucket, kahanSummation.delta());
                }
            }
        };
    }

    private void precomputeLeafUsingStarTree(LeafReaderContext ctx, CompositeIndexFieldInfo starTree) throws IOException {
        StarTreeValues starTreeValues = StarTreeQueryHelper.getStarTreeValues(ctx, starTree);
        assert starTreeValues != null;

        String fieldName = ((ValuesSource.Numeric.FieldData) valuesSource).getIndexFieldName();
        String sumMetricName = StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues(
            starTree.getField(),
            fieldName,
            MetricStat.SUM.getTypeName()
        );
        String countMetricName = StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues(
            starTree.getField(),
            fieldName,
            MetricStat.VALUE_COUNT.getTypeName()
        );

        final CompensatedSum kahanSummation = new CompensatedSum(sums.get(0), compensations.get(0));
        SortedNumericStarTreeValuesIterator sumValuesIterator = (SortedNumericStarTreeValuesIterator) starTreeValues
            .getMetricValuesIterator(sumMetricName);
        SortedNumericStarTreeValuesIterator countValueIterator = (SortedNumericStarTreeValuesIterator) starTreeValues
            .getMetricValuesIterator(countMetricName);
        FixedBitSet matchedDocIds = getStarTreeFilteredValues(context, ctx, starTreeValues);
        assert matchedDocIds != null;

        int numBits = matchedDocIds.length();  // Get the length of the FixedBitSet
        if (numBits > 0) {
            // Iterate over the FixedBitSet
            for (int bit = matchedDocIds.nextSetBit(0); bit != DocIdSetIterator.NO_MORE_DOCS; bit = bit + 1 < numBits
                ? matchedDocIds.nextSetBit(bit + 1)
                : DocIdSetIterator.NO_MORE_DOCS) {
                // Advance to the bit (entryId) in the valuesIterator
                if ((sumValuesIterator.advanceExact(bit) && countValueIterator.advanceExact(bit)) == false) {
                    continue;  // Skip if no more entries
                }

                // Iterate over the values for the current entryId
                for (int i = 0; i < sumValuesIterator.entryValueCount(); i++) {
                    kahanSummation.add(NumericUtils.sortableLongToDouble(sumValuesIterator.nextValue()));
                    counts.increment(0, countValueIterator.nextValue()); // Apply the consumer operation (e.g., max, sum)
                }
            }
        }

        sums.set(0, kahanSummation.value());
        compensations.set(0, kahanSummation.delta());
    }

    @Override
    public double metric(long owningBucketOrd) {
        if (valuesSource == null || owningBucketOrd >= sums.size()) {
            return Double.NaN;
        }
        return sums.get(owningBucketOrd) / counts.get(owningBucketOrd);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= sums.size()) {
            return buildEmptyAggregation();
        }
        return new InternalAvg(name, sums.get(bucket), counts.get(bucket), format, metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalAvg(name, 0.0, 0L, format, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(counts, sums, compensations);
    }

    public StarTreeBucketCollector getStarTreeBucketCollector(
        LeafReaderContext ctx,
        CompositeIndexFieldInfo starTree,
        StarTreeBucketCollector parentCollector
    ) throws IOException {
        assert parentCollector != null;
        return new StarTreeBucketCollector(parentCollector) {
            String sumMetricName = StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues(
                starTree.getField(),
                ((ValuesSource.Numeric.FieldData) valuesSource).getIndexFieldName(),
                MetricStat.SUM.getTypeName()
            );
            String valueCountMetricName = StarTreeUtils.fullyQualifiedFieldNameForStarTreeMetricsDocValues(
                starTree.getField(),
                ((ValuesSource.Numeric.FieldData) valuesSource).getIndexFieldName(),
                MetricStat.VALUE_COUNT.getTypeName()
            );
            SortedNumericStarTreeValuesIterator sumMetricValuesIterator = (SortedNumericStarTreeValuesIterator) starTreeValues
                .getMetricValuesIterator(sumMetricName);
            SortedNumericStarTreeValuesIterator valueCountMetricValuesIterator = (SortedNumericStarTreeValuesIterator) starTreeValues
                .getMetricValuesIterator(valueCountMetricName);

            final CompensatedSum kahanSummation = new CompensatedSum(0, 0);

            @Override
            public void collectStarTreeEntry(int starTreeEntryBit, long bucket) throws IOException {
                counts = context.bigArrays().grow(counts, bucket + 1);
                sums = context.bigArrays().grow(sums, bucket + 1);
                compensations = context.bigArrays().grow(compensations, bucket + 1);
                // Advance the valuesIterator to the current bit
                if (!sumMetricValuesIterator.advanceExact(starTreeEntryBit)
                    || !valueCountMetricValuesIterator.advanceExact(starTreeEntryBit)) {
                    return; // Skip if no entries for this document
                }
                kahanSummation.reset(sums.get(bucket), compensations.get(bucket));
                kahanSummation.add(NumericUtils.sortableLongToDouble(sumMetricValuesIterator.nextValue()));

                sums.set(bucket, kahanSummation.value());
                compensations.set(bucket, kahanSummation.delta());
                counts.increment(bucket, valueCountMetricValuesIterator.nextValue());
            }
        };
    }
}
