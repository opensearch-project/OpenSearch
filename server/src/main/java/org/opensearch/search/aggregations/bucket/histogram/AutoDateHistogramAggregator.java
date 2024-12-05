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

package org.opensearch.search.aggregations.bucket.histogram;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.util.CollectionUtil;
import org.opensearch.common.Rounding;
import org.opensearch.common.Rounding.Prepared;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.IntArray;
import org.opensearch.common.util.LongArray;
import org.opensearch.core.common.util.ByteArray;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.DeferableBucketAggregator;
import org.opensearch.search.aggregations.bucket.DeferringBucketCollector;
import org.opensearch.search.aggregations.bucket.MergingBucketsDeferringCollector;
import org.opensearch.search.aggregations.bucket.filterrewrite.DateHistogramAggregatorBridge;
import org.opensearch.search.aggregations.bucket.filterrewrite.FilterRewriteOptimizationContext;
import org.opensearch.search.aggregations.bucket.histogram.AutoDateHistogramAggregationBuilder.RoundingInfo;
import org.opensearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.aggregations.support.ValuesSourceConfig;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongToIntFunction;

import static org.opensearch.search.aggregations.bucket.filterrewrite.DateHistogramAggregatorBridge.segmentMatchAll;

/**
 * An aggregator for date values that attempts to return a specific number of
 * buckets, reconfiguring how it rounds dates to buckets on the fly as new
 * data arrives.
 * <p>
 * This class is abstract because there is a simple implementation for when the
 * aggregator only collects from a single bucket and a more complex
 * implementation when it doesn't. This ain't great from a test coverage
 * standpoint but the simpler implementation is between 7% and 15% faster
 * when you can use it. This is an important aggregation and we need that
 * performance.
 *
 * @opensearch.internal
 */
abstract class AutoDateHistogramAggregator extends DeferableBucketAggregator {
    static AutoDateHistogramAggregator build(
        String name,
        AggregatorFactories factories,
        int targetBuckets,
        RoundingInfo[] roundingInfos,
        Function<Rounding, Rounding.Prepared> roundingPreparer,
        ValuesSourceConfig valuesSourceConfig,
        SearchContext aggregationContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return cardinality == CardinalityUpperBound.ONE
            ? new FromSingle(
                name,
                factories,
                targetBuckets,
                roundingInfos,
                roundingPreparer,
                valuesSourceConfig,
                aggregationContext,
                parent,
                metadata
            )
            : new FromMany(
                name,
                factories,
                targetBuckets,
                roundingInfos,
                roundingPreparer,
                valuesSourceConfig,
                aggregationContext,
                parent,
                metadata
            );
    }

    private final ValuesSource.Numeric valuesSource;
    private final DocValueFormat formatter;
    private final Function<Rounding, Rounding.Prepared> roundingPreparer;
    /**
     * A reference to the collector so we can
     * {@link MergingBucketsDeferringCollector#mergeBuckets(long[])}.
     */
    private MergingBucketsDeferringCollector deferringCollector;

    protected final RoundingInfo[] roundingInfos;
    protected final int targetBuckets;
    protected int roundingIdx;
    protected Rounding.Prepared preparedRounding;

    private final FilterRewriteOptimizationContext filterRewriteOptimizationContext;

    private AutoDateHistogramAggregator(
        String name,
        AggregatorFactories factories,
        int targetBuckets,
        RoundingInfo[] roundingInfos,
        Function<Rounding, Rounding.Prepared> roundingPreparer,
        ValuesSourceConfig valuesSourceConfig,
        SearchContext aggregationContext,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {

        super(name, factories, aggregationContext, parent, metadata);
        this.targetBuckets = targetBuckets;
        // TODO: Remove null usage here, by using a different aggregator for create
        this.valuesSource = valuesSourceConfig.hasValues() ? (ValuesSource.Numeric) valuesSourceConfig.getValuesSource() : null;
        this.formatter = valuesSourceConfig.format();
        this.roundingInfos = roundingInfos;
        this.roundingPreparer = roundingPreparer;
        this.preparedRounding = prepareRounding(0);

        DateHistogramAggregatorBridge bridge = new DateHistogramAggregatorBridge() {
            @Override
            protected boolean canOptimize() {
                return canOptimize(valuesSourceConfig);
            }

            @Override
            protected void prepare() throws IOException {
                buildRanges(context);
            }

            @Override
            protected Rounding getRounding(final long low, final long high) {
                // max - min / targetBuckets = bestDuration
                // find the right innerInterval this bestDuration belongs to
                // since we cannot exceed targetBuckets, bestDuration should go up,
                // so the right innerInterval should be an upper bound
                long bestDuration = (high - low) / targetBuckets;
                // reset so this function is idempotent
                roundingIdx = 0;
                while (roundingIdx < roundingInfos.length - 1) {
                    final RoundingInfo curRoundingInfo = roundingInfos[roundingIdx];
                    final int temp = curRoundingInfo.innerIntervals[curRoundingInfo.innerIntervals.length - 1];
                    // If the interval duration is covered by the maximum inner interval,
                    // we can start with this outer interval for creating the buckets
                    if (bestDuration <= temp * curRoundingInfo.roughEstimateDurationMillis) {
                        break;
                    }
                    roundingIdx++;
                }

                preparedRounding = prepareRounding(roundingIdx);
                return roundingInfos[roundingIdx].rounding;
            }

            @Override
            protected Prepared getRoundingPrepared() {
                return preparedRounding;
            }

            @Override
            protected Function<Long, Long> bucketOrdProducer() {
                return (key) -> getBucketOrds().add(0, preparedRounding.round((long) key));
            }
        };
        filterRewriteOptimizationContext = new FilterRewriteOptimizationContext(bridge, parent, subAggregators.length, context);
    }

    protected abstract LongKeyedBucketOrds getBucketOrds();

    @Override
    public final ScoreMode scoreMode() {
        if (valuesSource != null && valuesSource.needsScores()) {
            return ScoreMode.COMPLETE;
        }
        return super.scoreMode();
    }

    @Override
    protected final boolean shouldDefer(Aggregator aggregator) {
        return true;
    }

    @Override
    public final DeferringBucketCollector getDeferringCollector() {
        deferringCollector = new MergingBucketsDeferringCollector(context, descendsFromGlobalAggregator(parent()));
        return deferringCollector;
    }

    protected abstract LeafBucketCollector getLeafCollector(SortedNumericDocValues values, LeafBucketCollector sub) throws IOException;

    @Override
    public final LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        boolean optimized = filterRewriteOptimizationContext.tryOptimize(ctx, this::incrementBucketDocCount, segmentMatchAll(context, ctx));
        if (optimized) throw new CollectionTerminatedException();

        final SortedNumericDocValues values = valuesSource.longValues(ctx);
        final LeafBucketCollector iteratingCollector = getLeafCollector(values, sub);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                iteratingCollector.collect(doc, owningBucketOrd);
            }
        };
    }

    protected final InternalAggregation[] buildAggregations(
        LongKeyedBucketOrds bucketOrds,
        LongToIntFunction roundingIndexFor,
        long[] owningBucketOrds
    ) throws IOException {
        return buildAggregationsForVariableBuckets(
            owningBucketOrds,
            bucketOrds,
            (bucketValue, docCount, subAggregationResults) -> new InternalAutoDateHistogram.Bucket(
                bucketValue,
                docCount,
                formatter,
                subAggregationResults
            ),
            (owningBucketOrd, buckets) -> {
                // the contract of the histogram aggregation is that shards must return
                // buckets ordered by key in ascending order
                CollectionUtil.introSort(buckets, BucketOrder.key(true).comparator());

                // value source will be null for unmapped fields
                InternalAutoDateHistogram.BucketInfo emptyBucketInfo = new InternalAutoDateHistogram.BucketInfo(
                    roundingInfos,
                    roundingIndexFor.applyAsInt(owningBucketOrd),
                    buildEmptySubAggregations()
                );

                return new InternalAutoDateHistogram(name, buckets, targetBuckets, emptyBucketInfo, formatter, metadata(), 1);
            }
        );
    }

    @Override
    public final InternalAggregation buildEmptyAggregation() {
        InternalAutoDateHistogram.BucketInfo emptyBucketInfo = new InternalAutoDateHistogram.BucketInfo(
            roundingInfos,
            0,
            buildEmptySubAggregations()
        );
        return new InternalAutoDateHistogram(name, Collections.emptyList(), targetBuckets, emptyBucketInfo, formatter, metadata(), 1);
    }

    protected final Rounding.Prepared prepareRounding(int index) {
        return roundingPreparer.apply(roundingInfos[index].rounding);
    }

    protected final void merge(long[] mergeMap, long newNumBuckets) {
        mergeBuckets(mergeMap, newNumBuckets);
        if (deferringCollector != null) {
            deferringCollector.mergeBuckets(mergeMap);
        }
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        filterRewriteOptimizationContext.populateDebugInfo(add);
    }

    /**
     * Initially it uses the most fine grained rounding configuration possible
     * but as more data arrives it rebuckets the data until it "fits" in the
     * aggregation rounding. Similar to {@link FromMany} this checks both the
     * bucket count and range of the aggregation, but unlike
     * {@linkplain FromMany} it keeps an accurate count of the buckets and it
     * doesn't delay rebucketing.
     * <p>
     * Rebucketing is roughly {@code O(number_of_hits_collected_so_far)} but we
     * rebucket roughly {@code O(log number_of_hits_collected_so_far)} because
     * the "shape" of the roundings is <strong>roughly</strong>
     * logarithmically increasing.
     *
     * @opensearch.internal
     */
    private static class FromSingle extends AutoDateHistogramAggregator {
        /**
         * Map from value to bucket ordinals.
         * <p>
         * It is important that this is the exact subtype of
         * {@link LongKeyedBucketOrds} so that the JVM can make a monomorphic
         * call to {@link LongKeyedBucketOrds#add(long, long)} in the tight
         * inner loop of {@link LeafBucketCollector#collect(int, long)}. You'd
         * think that it wouldn't matter, but its seriously 7%-15% performance
         * difference for the aggregation. Yikes.
         */
        private LongKeyedBucketOrds.FromSingle bucketOrds;
        private long min = Long.MAX_VALUE;
        private long max = Long.MIN_VALUE;

        FromSingle(
            String name,
            AggregatorFactories factories,
            int targetBuckets,
            RoundingInfo[] roundingInfos,
            Function<Rounding, Prepared> roundingPreparer,
            ValuesSourceConfig valuesSourceConfig,
            SearchContext aggregationContext,
            Aggregator parent,
            Map<String, Object> metadata
        ) throws IOException {
            super(
                name,
                factories,
                targetBuckets,
                roundingInfos,
                roundingPreparer,
                valuesSourceConfig,
                aggregationContext,
                parent,
                metadata
            );

            bucketOrds = new LongKeyedBucketOrds.FromSingle(context.bigArrays());
        }

        @Override
        protected LongKeyedBucketOrds getBucketOrds() {
            return bucketOrds;
        }

        @Override
        protected LeafBucketCollector getLeafCollector(SortedNumericDocValues values, LeafBucketCollector sub) throws IOException {
            return new LeafBucketCollectorBase(sub, values) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    assert owningBucketOrd == 0;
                    if (false == values.advanceExact(doc)) {
                        return;
                    }
                    int valuesCount = values.docValueCount();

                    long previousRounded = Long.MIN_VALUE;
                    for (int i = 0; i < valuesCount; ++i) {
                        long value = values.nextValue();
                        long rounded = preparedRounding.round(value);
                        assert rounded >= previousRounded;
                        if (rounded == previousRounded) {
                            continue;
                        }
                        collectValue(doc, rounded);
                        previousRounded = rounded;
                    }
                }

                private void collectValue(int doc, long rounded) throws IOException {
                    long bucketOrd = bucketOrds.add(0, rounded);
                    if (bucketOrd < 0) { // already seen
                        bucketOrd = -1 - bucketOrd;
                        collectExistingBucket(sub, doc, bucketOrd);
                        return;
                    }
                    collectBucket(sub, doc, bucketOrd);
                    increaseRoundingIfNeeded(rounded);
                }

                private void increaseRoundingIfNeeded(long rounded) {
                    if (roundingIdx >= roundingInfos.length - 1) {
                        return;
                    }
                    min = Math.min(min, rounded);
                    max = Math.max(max, rounded);
                    if (bucketOrds.size() <= targetBuckets * roundingInfos[roundingIdx].getMaximumInnerInterval()
                        && max - min <= targetBuckets * roundingInfos[roundingIdx].getMaximumRoughEstimateDurationMillis()) {
                        return;
                    }
                    do {
                        try (LongKeyedBucketOrds oldOrds = bucketOrds) {
                            preparedRounding = prepareRounding(++roundingIdx);
                            long[] mergeMap = new long[Math.toIntExact(oldOrds.size())];
                            bucketOrds = new LongKeyedBucketOrds.FromSingle(context.bigArrays());
                            LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = oldOrds.ordsEnum(0);
                            while (ordsEnum.next()) {
                                long oldKey = ordsEnum.value();
                                long newKey = preparedRounding.round(oldKey);
                                long newBucketOrd = bucketOrds.add(0, newKey);
                                mergeMap[(int) ordsEnum.ord()] = newBucketOrd >= 0 ? newBucketOrd : -1 - newBucketOrd;
                            }
                            merge(mergeMap, bucketOrds.size());
                        }
                    } while (roundingIdx < roundingInfos.length - 1
                        && (bucketOrds.size() > targetBuckets * roundingInfos[roundingIdx].getMaximumInnerInterval()
                            || max - min > targetBuckets * roundingInfos[roundingIdx].getMaximumRoughEstimateDurationMillis()));
                }
            };
        }

        @Override
        public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
            return buildAggregations(bucketOrds, l -> roundingIdx, owningBucketOrds);
        }

        @Override
        public void collectDebugInfo(BiConsumer<String, Object> add) {
            super.collectDebugInfo(add);
            add.accept("surviving_buckets", bucketOrds.size());
        }

        @Override
        protected void doClose() {
            Releasables.close(bucketOrds);
        }
    }

    /**
     * Initially it uses the most fine grained rounding configuration possible but
     * as more data arrives it uses two heuristics to shift to coarser and coarser
     * rounding. The first heuristic is the number of buckets, specifically,
     * when there are more buckets than can "fit" in the current rounding it shifts
     * to the next rounding. Instead of redoing the rounding, it estimates the
     * number of buckets that will "survive" at the new rounding and uses
     * <strong>that</strong> as the initial value for the bucket count that it
     * increments in order to trigger another promotion to another coarser
     * rounding. This works fairly well at containing the number of buckets, but
     * the estimate of the number of buckets will be wrong if the buckets are
     * quite a spread out compared to the rounding.
     * <p>
     * The second heuristic it uses to trigger promotion to a coarser rounding is
     * the distance between the min and max bucket. When that distance is greater
     * than what the current rounding supports it promotes. This heuristic
     * isn't good at limiting the number of buckets but is great when the buckets
     * are spread out compared to the rounding. So it should complement the first
     * heuristic.
     * <p>
     * When promoting a rounding we keep the old buckets around because it is
     * expensive to call {@link MergingBucketsDeferringCollector#mergeBuckets}.
     * In particular it is {@code O(number_of_hits_collected_so_far)}. So if we
     * called it frequently we'd end up in {@code O(n^2)} territory. Bad news for
     * aggregations! Instead, we keep a "budget" of buckets that we're ok
     * "wasting". When we promote the rounding and our estimate of the number of
     * "dead" buckets that have data but have yet to be merged into the buckets
     * that are valid for the current rounding exceeds the budget then we rebucket
     * the entire aggregation and double the budget.
     * <p>
     * Once we're done collecting and we know exactly which buckets we'll be
     * returning we <strong>finally</strong> perform a "real", "perfect bucketing",
     * rounding all of the keys for {@code owningBucketOrd} that we're going to
     * collect and picking the rounding based on a real, accurate count and the
     * min and max.
     *
     * @opensearch.internal
     */
    private static class FromMany extends AutoDateHistogramAggregator {
        /**
         * An array of prepared roundings in the same order as
         * {@link #roundingInfos}. The 0th entry is prepared initially,
         * and other entries are null until first needed.
         */
        private final Rounding.Prepared[] preparedRoundings;
        /**
         * Map from value to bucket ordinals.
         * <p>
         * It is important that this is the exact subtype of
         * {@link LongKeyedBucketOrds} so that the JVM can make a monomorphic
         * call to {@link LongKeyedBucketOrds#add(long, long)} in the tight
         * inner loop of {@link LeafBucketCollector#collect(int, long)}.
         */
        private LongKeyedBucketOrds.FromMany bucketOrds;
        /**
         * The index of the rounding that each {@code owningBucketOrd} is
         * currently using.
         * <p>
         * During collection we use overestimates for how much buckets are save
         * by bumping to the next rounding index. So we end up bumping less
         * aggressively than a "perfect" algorithm. That is fine because we
         * correct the error when we merge the buckets together all the way
         * up in {@link InternalAutoDateHistogram#reduceBucket}. In particular,
         * on final reduce we bump the rounding until it we appropriately
         * cover the date range across all of the results returned by all of
         * the {@link AutoDateHistogramAggregator}s.
         */
        private ByteArray roundingIndices;
        /**
         * The minimum key per {@code owningBucketOrd}.
         */
        private LongArray mins;
        /**
         * The max key per {@code owningBucketOrd}.
         */
        private LongArray maxes;

        /**
         * An underestimate of the number of buckets that are "live" in the
         * current rounding for each {@code owningBucketOrdinal}.
         */
        private IntArray liveBucketCountUnderestimate;
        /**
         * An over estimate of the number of wasted buckets. When this gets
         * too high we {@link #rebucket} which sets it to 0.
         */
        private long wastedBucketsOverestimate = 0;
        /**
         * The next {@link #wastedBucketsOverestimate} that will trigger a
         * {@link #rebucket() rebucketing}.
         */
        private long nextRebucketAt = 1000; // TODO this could almost certainly start higher when asMultiBucketAggregator is gone
        /**
         * The number of times the aggregator had to {@link #rebucket()} the
         * results. We keep this just to report to the profiler.
         */
        private int rebucketCount = 0;

        FromMany(
            String name,
            AggregatorFactories factories,
            int targetBuckets,
            RoundingInfo[] roundingInfos,
            Function<Rounding, Rounding.Prepared> roundingPreparer,
            ValuesSourceConfig valuesSourceConfig,
            SearchContext aggregationContext,
            Aggregator parent,
            Map<String, Object> metadata
        ) throws IOException {

            super(
                name,
                factories,
                targetBuckets,
                roundingInfos,
                roundingPreparer,
                valuesSourceConfig,
                aggregationContext,
                parent,
                metadata
            );
            assert roundingInfos.length < 127 : "Rounding must fit in a signed byte";
            roundingIndices = context.bigArrays().newByteArray(1, true);
            mins = context.bigArrays().newLongArray(1, false);
            mins.set(0, Long.MAX_VALUE);
            maxes = context.bigArrays().newLongArray(1, false);
            maxes.set(0, Long.MIN_VALUE);
            preparedRoundings = new Rounding.Prepared[roundingInfos.length];
            // Prepare the first rounding because we know we'll need it.
            preparedRoundings[0] = roundingPreparer.apply(roundingInfos[0].rounding);
            bucketOrds = new LongKeyedBucketOrds.FromMany(context.bigArrays());
            liveBucketCountUnderestimate = context.bigArrays().newIntArray(1, true);
        }

        @Override
        protected LongKeyedBucketOrds getBucketOrds() {
            return bucketOrds;
        }

        @Override
        protected LeafBucketCollector getLeafCollector(SortedNumericDocValues values, LeafBucketCollector sub) throws IOException {
            return new LeafBucketCollectorBase(sub, values) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    if (false == values.advanceExact(doc)) {
                        return;
                    }
                    int valuesCount = values.docValueCount();

                    long previousRounded = Long.MIN_VALUE;
                    int roundingIdx = roundingIndexFor(owningBucketOrd);
                    for (int i = 0; i < valuesCount; ++i) {
                        long value = values.nextValue();
                        long rounded = preparedRoundings[roundingIdx].round(value);
                        assert rounded >= previousRounded;
                        if (rounded == previousRounded) {
                            continue;
                        }
                        roundingIdx = collectValue(owningBucketOrd, roundingIdx, doc, rounded);
                        previousRounded = rounded;
                    }
                }

                private int collectValue(long owningBucketOrd, int roundingIdx, int doc, long rounded) throws IOException {
                    long bucketOrd = bucketOrds.add(owningBucketOrd, rounded);
                    if (bucketOrd < 0) { // already seen
                        bucketOrd = -1 - bucketOrd;
                        collectExistingBucket(sub, doc, bucketOrd);
                        return roundingIdx;
                    }
                    collectBucket(sub, doc, bucketOrd);
                    liveBucketCountUnderestimate = context.bigArrays().grow(liveBucketCountUnderestimate, owningBucketOrd + 1);
                    int estimatedBucketCount = liveBucketCountUnderestimate.increment(owningBucketOrd, 1);
                    return increaseRoundingIfNeeded(owningBucketOrd, estimatedBucketCount, rounded, roundingIdx);
                }

                /**
                 * Increase the rounding of {@code owningBucketOrd} using
                 * estimated, bucket counts, {@link FromMany#rebucket()} rebucketing} the all
                 * buckets if the estimated number of wasted buckets is too high.
                 */
                private int increaseRoundingIfNeeded(long owningBucketOrd, int oldEstimatedBucketCount, long newKey, int oldRounding) {
                    if (oldRounding >= roundingInfos.length - 1) {
                        return oldRounding;
                    }
                    if (mins.size() < owningBucketOrd + 1) {
                        long oldSize = mins.size();
                        mins = context.bigArrays().grow(mins, owningBucketOrd + 1);
                        mins.fill(oldSize, mins.size(), Long.MAX_VALUE);
                    }
                    if (maxes.size() < owningBucketOrd + 1) {
                        long oldSize = maxes.size();
                        maxes = context.bigArrays().grow(maxes, owningBucketOrd + 1);
                        maxes.fill(oldSize, maxes.size(), Long.MIN_VALUE);
                    }

                    long min = Math.min(mins.get(owningBucketOrd), newKey);
                    mins.set(owningBucketOrd, min);
                    long max = Math.max(maxes.get(owningBucketOrd), newKey);
                    maxes.set(owningBucketOrd, max);
                    if (oldEstimatedBucketCount <= targetBuckets * roundingInfos[oldRounding].getMaximumInnerInterval()
                        && max - min <= targetBuckets * roundingInfos[oldRounding].getMaximumRoughEstimateDurationMillis()) {
                        return oldRounding;
                    }
                    long oldRoughDuration = roundingInfos[oldRounding].roughEstimateDurationMillis;
                    int newRounding = oldRounding;
                    int newEstimatedBucketCount;
                    do {
                        newRounding++;
                        double ratio = (double) oldRoughDuration / (double) roundingInfos[newRounding].getRoughEstimateDurationMillis();
                        newEstimatedBucketCount = (int) Math.ceil(oldEstimatedBucketCount * ratio);
                    } while (newRounding < roundingInfos.length - 1
                        && (newEstimatedBucketCount > targetBuckets * roundingInfos[newRounding].getMaximumInnerInterval()
                            || max - min > targetBuckets * roundingInfos[newRounding].getMaximumRoughEstimateDurationMillis()));
                    setRounding(owningBucketOrd, newRounding);
                    mins.set(owningBucketOrd, preparedRoundings[newRounding].round(mins.get(owningBucketOrd)));
                    maxes.set(owningBucketOrd, preparedRoundings[newRounding].round(maxes.get(owningBucketOrd)));
                    wastedBucketsOverestimate += oldEstimatedBucketCount - newEstimatedBucketCount;
                    if (wastedBucketsOverestimate > nextRebucketAt) {
                        rebucket();
                        // Bump the threshold for the next rebucketing
                        wastedBucketsOverestimate = 0;
                        nextRebucketAt *= 2;
                    } else {
                        liveBucketCountUnderestimate.set(owningBucketOrd, newEstimatedBucketCount);
                    }
                    return newRounding;
                }
            };
        }

        private void rebucket() {
            rebucketCount++;
            try (LongKeyedBucketOrds oldOrds = bucketOrds) {
                long[] mergeMap = new long[Math.toIntExact(oldOrds.size())];
                bucketOrds = new LongKeyedBucketOrds.FromMany(context.bigArrays());
                for (long owningBucketOrd = 0; owningBucketOrd <= oldOrds.maxOwningBucketOrd(); owningBucketOrd++) {
                    LongKeyedBucketOrds.BucketOrdsEnum ordsEnum = oldOrds.ordsEnum(owningBucketOrd);
                    Rounding.Prepared preparedRounding = preparedRoundings[roundingIndexFor(owningBucketOrd)];
                    while (ordsEnum.next()) {
                        long oldKey = ordsEnum.value();
                        long newKey = preparedRounding.round(oldKey);
                        long newBucketOrd = bucketOrds.add(owningBucketOrd, newKey);
                        mergeMap[(int) ordsEnum.ord()] = newBucketOrd >= 0 ? newBucketOrd : -1 - newBucketOrd;
                    }
                    liveBucketCountUnderestimate = context.bigArrays().grow(liveBucketCountUnderestimate, owningBucketOrd + 1);
                    liveBucketCountUnderestimate.set(owningBucketOrd, Math.toIntExact(bucketOrds.bucketsInOrd(owningBucketOrd)));
                }
                merge(mergeMap, bucketOrds.size());
            }
        }

        @Override
        public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
            /*
             * Rebucket before building the aggregation to build as small as result
             * as possible.
             *
             * TODO it'd be faster if we could apply the merging on the fly as we
             * replay the hits and build the buckets. How much faster is not clear,
             * but it does have the advantage of only touching the buckets that we
             * want to collect.
             */
            rebucket();
            return buildAggregations(bucketOrds, this::roundingIndexFor, owningBucketOrds);
        }

        @Override
        public void collectDebugInfo(BiConsumer<String, Object> add) {
            super.collectDebugInfo(add);
            add.accept("surviving_buckets", bucketOrds.size());
            add.accept("wasted_buckets_overestimate", wastedBucketsOverestimate);
            add.accept("next_rebucket_at", nextRebucketAt);
            add.accept("rebucket_count", rebucketCount);
        }

        private void setRounding(long owningBucketOrd, int newRounding) {
            roundingIndices = context.bigArrays().grow(roundingIndices, owningBucketOrd + 1);
            roundingIndices.set(owningBucketOrd, (byte) newRounding);
            if (preparedRoundings[newRounding] == null) {
                preparedRoundings[newRounding] = prepareRounding(newRounding);
            }
        }

        private int roundingIndexFor(long owningBucketOrd) {
            return owningBucketOrd < roundingIndices.size() ? roundingIndices.get(owningBucketOrd) : 0;
        }

        @Override
        public void doClose() {
            Releasables.close(bucketOrds, roundingIndices, mins, maxes, liveBucketCountUnderestimate);
        }
    }

}
