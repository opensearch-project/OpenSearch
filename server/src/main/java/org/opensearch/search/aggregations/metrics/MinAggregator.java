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

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.NumericUtils;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.DoubleArray;
import org.opensearch.common.util.LongArray;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.index.compositeindex.datacube.MetricStat;
import org.opensearch.index.fielddata.NumericDoubleValues;
import org.opensearch.index.fielddata.SortedNumericDoubleValues;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.MultiValueMode;
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
import org.opensearch.search.streaming.Streamable;
import org.opensearch.search.streaming.StreamingCostMetrics;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.opensearch.search.startree.StarTreeQueryHelper.getSupportedStarTree;

/**
 * Aggregate all docs into a min value
 *
 * @opensearch.internal
 */
class MinAggregator extends NumericMetricsAggregator.SingleValue implements StarTreePreComputeCollector, Streamable {
    private static final int MAX_BKD_LOOKUPS = 1024;

    final ValuesSource.Numeric valuesSource;
    final DocValueFormat format;

    final String pointField;
    final Function<byte[], Number> pointConverter;
    final String fieldName;
    final boolean fieldIsFloat;

    DoubleArray mins;
    LongArray skipUpTo;

    private int defaultCollectorsUsed = 0;
    private int skipListCollectorsUsed = 0;

    MinAggregator(String name, ValuesSourceConfig config, SearchContext context, Aggregator parent, Map<String, Object> metadata)
        throws IOException {
        super(name, context, parent, metadata);
        skipUpTo = null;
        // TODO: Stop using nulls here
        this.valuesSource = config.hasValues() ? (ValuesSource.Numeric) config.getValuesSource() : null;
        if (valuesSource != null) {
            mins = context.bigArrays().newDoubleArray(1, false);
            mins.fill(0, mins.size(), Double.POSITIVE_INFINITY);
            fieldName = valuesSource.getIndexFieldName();
            fieldIsFloat = valuesSource.isFloatingPoint();
        } else {
            fieldName = null;
            fieldIsFloat = false;
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
    protected boolean tryPrecomputeAggregationForLeaf(LeafReaderContext ctx) throws IOException {
        if (valuesSource == null) {
            return false;
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
                return true;
            }
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
            if (parent == null) {
                return LeafBucketCollector.NO_OP_COLLECTOR;
            } else {
                // we have no parent and the values source is empty so we can skip collecting hits.
                throw new CollectionTerminatedException();
            }
        }

        final BigArrays bigArrays = context.bigArrays();
        final SortedNumericDoubleValues allValues = valuesSource.doubleValues(ctx);
        final NumericDoubleValues values = MultiValueMode.MIN.select(allValues);

        // Try to use skiplist optimization if available
        DocValuesSkipper skipper = null;
        if (this.fieldName != null) {
            skipper = ctx.reader().getDocValuesSkipper(this.fieldName);
        }

        // Use skiplist collector if conditions are met
        if (skipper != null) {
            skipListCollectorsUsed++;
            this.skipUpTo = bigArrays.newLongArray(1, false);
            this.skipUpTo.fill(0, this.skipUpTo.size(), -1);
            return new MinSkiplistLeafCollector(values, skipper, mins, fieldIsFloat, MinAggregator.this, sub);
        }

        // Fall back to standard collector selection logic
        defaultCollectorsUsed++;
        return new LeafBucketCollectorBase(sub, allValues) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                growMins(bucket);
                if (values.advanceExact(doc)) {
                    final double value = values.doubleValue();
                    double min = mins.get(bucket);
                    min = Math.min(min, value);
                    mins.set(bucket, min);
                }
            }

            @Override
            public void collect(DocIdStream stream, long bucket) throws IOException {
                growMins(bucket);
                final double[] min = { mins.get(bucket) };
                stream.forEach((doc) -> {
                    if (values.advanceExact(doc)) {
                        min[0] = Math.min(min[0], values.doubleValue());
                    }
                });
                mins.set(bucket, min[0]);
            }

            @Override
            public void collectRange(int min, int max) throws IOException {
                growMins(0);
                double minimum = mins.get(0);
                for (int doc = min; doc < max; doc++) {
                    if (values.advanceExact(doc)) {
                        minimum = Math.min(minimum, values.doubleValue());
                    }
                }
                mins.set(0, minimum);
            }


        };
    }

    private DoubleArray growMins(long bucket) {
        if (bucket >= mins.size()) {
            long from = mins.size();
            mins = context.bigArrays().grow(mins, bucket + 1);
            mins.fill(from, mins.size(), Double.POSITIVE_INFINITY);
        }
        return mins;
    }


    private LongArray growSkipUpTo(long bucket) {
        if (bucket >= skipUpTo.size()) {
            long from = skipUpTo.size();
            skipUpTo = context.bigArrays().grow(skipUpTo, bucket + 1);
            skipUpTo.fill(from, skipUpTo.size(), -1);
        }
        return skipUpTo;
    }

    /**
     * Specialized leaf collector that uses DocValuesSkipper to efficiently skip document ranges
     * that cannot improve the current minimum value.
     *
     * This collector leverages skip list metadata to avoid processing documents when the skip range's
     * minimum value is greater than or equal to the current tracked minimum for a bucket.
     */
    private static class MinSkiplistLeafCollector extends LeafBucketCollectorBase {
        private final NumericDoubleValues values;
        private final DocValuesSkipper skipper;
        private final MinAggregator minAgg;
        private DoubleArray mins;
        private final boolean isFloat;
        private LongArray skipUpTo;
        private final LeafBucketCollector sub;
        private final boolean isSubNoOp;

        /**
         * Constructs a new MinSkiplistLeafCollector.
         *
         * @param values        the numeric doc values for the field
         * @param skipper       the doc values skipper for skip list optimization
         * @param mins          the array storing minimum values per bucket
         * @param isFloat
         * @param minAggregator
         * @param sub           the sub-aggregator collector
         */
        MinSkiplistLeafCollector(
            NumericDoubleValues values,
            DocValuesSkipper skipper,
            DoubleArray mins,
            boolean isFloat,
            MinAggregator minAggregator,
            LeafBucketCollector sub
        ) {
            super(sub, null);
            this.values = values;
            this.isFloat = isFloat;
            this.skipper = skipper;
            this.mins = mins;
            this.minAgg = minAggregator;
            this.sub = sub;
            this.isSubNoOp = sub == LeafBucketCollector.NO_OP_COLLECTOR;

        }



        /**
         * Advances the skipper to the appropriate position and determines the skip range
         * for a specific bucket. The result is stored in the skipUpTo array:
         * - Positive value: Can skip up to and including this doc ID
         * - Negative value: Cannot skip, must process documents individually
         *
         * @param doc the current document ID
         * @param owningBucketOrd the bucket ordinal for which to evaluate skip range
         * @throws IOException if an I/O error occurs
         */
        private void advanceSkipper(int doc, long owningBucketOrd) throws IOException {
            if (doc > skipper.maxDocID(0)) {
                skipper.advance(doc);
            }

            // Initialize to "do not skip" state
            long upToInclusive = -1;

            if (skipper.minDocID(0) > doc) {
                // Corner case: doc is between skip intervals
                // Set to (minDocID - 1) but keep negative to indicate no skipping
                upToInclusive = -(skipper.minDocID(0) - 1) - 1;
                mins = minAgg.growMins(owningBucketOrd);
                skipUpTo = minAgg.growSkipUpTo(owningBucketOrd);
                skipUpTo.set(owningBucketOrd, upToInclusive);
                return;
            }

            upToInclusive = skipper.maxDocID(0);

            // Ensure arrays are large enough
            mins = minAgg.growMins(owningBucketOrd);
            skipUpTo = minAgg.growSkipUpTo(owningBucketOrd);
            double currentMin = mins.get(owningBucketOrd);

            // Check progressively larger skip levels
            boolean canSkip = false;
            for (int level = 0; level < skipper.numLevels(); ++level) {
                // Convert skipper's minValue (stored as sortable long) to double
                long sortableLong = skipper.minValue(level);
                double skipperMin = isFloat ? NumericUtils.sortableLongToDouble(sortableLong) : sortableLong;

                if (skipperMin >= currentMin) {
                    // All values in this range are >= current min, can skip
                    upToInclusive = skipper.maxDocID(level);
                    canSkip = true;
                } else {
                    // This range might contain better minimums
                    break;
                }
            }

            // Store result: negative if cannot skip, positive if can skip
            skipUpTo.set(owningBucketOrd, canSkip ? upToInclusive : -upToInclusive - 1);
        }

        @Override
        public void collect(int doc, long owningBucketOrd) throws IOException {
            // Get skipUpTo value for this bucket
            mins = minAgg.growMins(owningBucketOrd);
            skipUpTo = minAgg.growSkipUpTo(owningBucketOrd);
            long skipUpToValue = skipUpTo.get(owningBucketOrd);

            // Extract the upToInclusive boundary (handle negative encoding)
            long upToInclusive = skipUpToValue >= 0 ? skipUpToValue : -skipUpToValue - 1;

            // If doc > upToInclusive, we need to advance the skipper
            // TODO: on 1st iter, upToInclusive will be 0 so won't advance
            if (doc >= upToInclusive) {
                advanceSkipper(doc, owningBucketOrd);
                skipUpToValue = skipUpTo.get(owningBucketOrd);
            }

            // If skipUpTo >= 0, we can skip this document
            if (skipUpToValue >= 0) {
                return;
            }

            // Otherwise, process the document
            if (values.advanceExact(doc)) {
                double value = values.doubleValue();
                double min = mins.get(owningBucketOrd);
                if (value < min) {
                    mins.set(owningBucketOrd, value);
                    System.out.println("(Old, New)min: \t"  + min + "\t" + value);
                }
            }

            // Delegate to sub-aggregator for non-skipped documents
            if (!isSubNoOp) {
                sub.collect(doc, owningBucketOrd);
            }
        }
    }

    private void precomputeLeafUsingStarTree(LeafReaderContext ctx, CompositeIndexFieldInfo starTree) throws IOException {
        AtomicReference<Double> min = new AtomicReference<>(mins.get(0));
        StarTreeQueryHelper.precomputeLeafUsingStarTree(context, valuesSource, ctx, starTree, MetricStat.MIN.getTypeName(), value -> {
            min.set(Math.min(min.get(), (NumericUtils.sortableLongToDouble(value))));
        }, () -> mins.set(0, min.get()));
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

    public Map<String, Object> collectDebugInfo() {
        return Map.of("defaultCollectorsUsed", defaultCollectorsUsed,
            "skipListCollectorsUsed", skipListCollectorsUsed);
    }

    @Override
    public void doClose() {
        Releasables.close(mins);
        Releasables.close(skipUpTo);
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

    /**
     * The parent aggregator invokes this method to get a StarTreeBucketCollector,
     * which exposes collectStarTreeEntry() to be evaluated on filtered star tree entries
     */
    public StarTreeBucketCollector getStarTreeBucketCollector(
        LeafReaderContext ctx,
        CompositeIndexFieldInfo starTree,
        StarTreeBucketCollector parentCollector
    ) throws IOException {
        return StarTreeQueryHelper.getStarTreeBucketMetricCollector(
            starTree,
            MetricStat.MIN.getTypeName(),
            valuesSource,
            parentCollector,
            (bucket) -> {
                long from = mins.size();
                mins = context.bigArrays().grow(mins, bucket + 1);
                mins.fill(from, mins.size(), Double.POSITIVE_INFINITY);
            },
            (bucket, metricValue) -> mins.set(bucket, Math.min(mins.get(bucket), NumericUtils.sortableLongToDouble(metricValue)))
        );
    }

    @Override
    public void doReset() {
        mins.fill(0, mins.size(), Double.POSITIVE_INFINITY);
        if (skipUpTo != null) {
            skipUpTo.fill(0, skipUpTo.size(), -1);
        }
    }

    @Override
    public StreamingCostMetrics getStreamingCostMetrics() {
        return new StreamingCostMetrics(true, 1, 1, 1, 1);
    }
}
