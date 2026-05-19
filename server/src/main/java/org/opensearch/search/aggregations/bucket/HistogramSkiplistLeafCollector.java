/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket;

import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.DocIdStream;
import org.apache.lucene.search.Scorable;
import org.opensearch.common.Rounding;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorBase;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.bucket.histogram.LongBounds;
import org.opensearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;

import java.io.IOException;
import java.util.function.LongFunction;
import java.util.function.Supplier;

/**
 * Histogram collection logic using skip list.
 *
 * Currently, it can only handle one owningBucketOrd at a time.
 *
 * @opensearch.internal
 */
public class HistogramSkiplistLeafCollector extends LeafBucketCollector {

    private final NumericDocValues values;
    private final DocValuesSkipper skipper;
    private final LeafBucketCollector sub;
    private final boolean isSubNoOp;
    private final BucketsAggregator aggregator;

    /**
     * Supplier function to get the current preparedRounding from the parent aggregator.
     * This allows detection of rounding changes in AutoDateHistogramAggregator.
     */
    private final LongFunction<Rounding.Prepared> preparedRoundingSupplier;
    private final Supplier<LongKeyedBucketOrds> bucketOrdsSupplier;
    private final IncreaseRoundingIfNeeded increaseRoundingIfNeeded;

    /**
     * Max doc ID (inclusive) up to which all docs values may map to the same
     * bucket.
     */
    private int upToInclusive = -1;

    /**
     * Whether all docs up to {@link #upToInclusive} values map to the same bucket.
     */
    private boolean upToSameBucket;

    /**
     * Index in bucketOrds for docs up to {@link #upToInclusive}.
     */
    private long upToBucketIndex;

    /**
     * Tracks the last preparedRounding reference to detect rounding changes.
     * Used for cache invalidation when AutoDateHistogramAggregator changes rounding.
     */
    private Rounding.Prepared lastPreparedRounding;

    public HistogramSkiplistLeafCollector(
        NumericDocValues values,
        DocValuesSkipper skipper,
        Rounding.Prepared preparedRounding,
        LongKeyedBucketOrds bucketOrds,
        LeafBucketCollector sub,
        BucketsAggregator aggregator
    ) {
        this(values, skipper, (owningBucketOrd) -> preparedRounding, () -> bucketOrds, sub, aggregator, (owningBucketOrd, rounded) -> {});
    }

    /**
     * Constructor that accepts a supplier for dynamic rounding (used by AutoDateHistogramAggregator).
     */
    public HistogramSkiplistLeafCollector(
        NumericDocValues values,
        DocValuesSkipper skipper,
        LongFunction<Rounding.Prepared> preparedRoundingSupplier,
        Supplier<LongKeyedBucketOrds> bucketOrdsSupplier,
        LeafBucketCollector sub,
        BucketsAggregator aggregator,
        IncreaseRoundingIfNeeded increaseRoundingIfNeeded
    ) {
        this.values = values;
        this.skipper = skipper;
        this.preparedRoundingSupplier = preparedRoundingSupplier;
        this.bucketOrdsSupplier = bucketOrdsSupplier;
        this.sub = sub;
        this.isSubNoOp = (sub == NO_OP_COLLECTOR);
        this.aggregator = aggregator;
        this.increaseRoundingIfNeeded = increaseRoundingIfNeeded;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        if (sub != null) {
            sub.setScorer(scorer);
        }
    }

    private void advanceSkipper(int doc, long owningBucketOrd) throws IOException {
        if (doc > skipper.maxDocID(0)) {
            skipper.advance(doc);
        }
        upToSameBucket = false;

        if (skipper.minDocID(0) > doc) {
            // Corner case which happens if `doc` doesn't have a value and is between two
            // intervals of
            // the doc-value skip index.
            upToInclusive = skipper.minDocID(0) - 1;
            return;
        }

        upToInclusive = skipper.maxDocID(0);

        // Get current rounding from supplier
        Rounding.Prepared currentRounding = preparedRoundingSupplier.apply(owningBucketOrd);

        // Now find the highest level where all docs map to the same bucket.
        for (int level = 0; level < skipper.numLevels(); ++level) {
            int totalDocsAtLevel = skipper.maxDocID(level) - skipper.minDocID(level) + 1;
            long minBucket = currentRounding.round(skipper.minValue(level));
            long maxBucket = currentRounding.round(skipper.maxValue(level));

            if (skipper.docCount(level) == totalDocsAtLevel && minBucket == maxBucket) {
                // All docs at this level have a value, and all values map to the same bucket.
                upToInclusive = skipper.maxDocID(level);
                upToSameBucket = true;
                upToBucketIndex = bucketOrdsSupplier.get().add(owningBucketOrd, maxBucket);
                if (upToBucketIndex < 0) {
                    upToBucketIndex = -1 - upToBucketIndex;
                }
            } else {
                break;
            }
        }
    }

    @Override
    public void collect(int doc, long owningBucketOrd) throws IOException {
        Rounding.Prepared currentRounding = preparedRoundingSupplier.apply(owningBucketOrd);

        // Check if rounding changed (using reference equality)
        // AutoDateHistogramAggregator creates a new Rounding.Prepared instance when rounding changes
        if (currentRounding != lastPreparedRounding) {
            upToInclusive = -1;  // Invalidate
            upToSameBucket = false;
            lastPreparedRounding = currentRounding;
        }

        if (doc > upToInclusive) {
            advanceSkipper(doc, owningBucketOrd);
        }

        if (upToSameBucket) {
            aggregator.incrementBucketDocCount(upToBucketIndex, 1L);
            sub.collect(doc, upToBucketIndex);
        } else if (values.advanceExact(doc)) {
            final long value = values.longValue();
            long rounded = currentRounding.round(value);
            long bucketIndex = bucketOrdsSupplier.get().add(owningBucketOrd, rounded);
            if (bucketIndex < 0) {
                bucketIndex = -1 - bucketIndex;
                aggregator.collectExistingBucket(sub, doc, bucketIndex);
            } else {
                aggregator.collectBucket(sub, doc, bucketIndex);
                increaseRoundingIfNeeded.accept(owningBucketOrd, rounded);
            }
        }
    }

    @Override
    public void collect(DocIdStream stream) throws IOException {
        // This will only be called if its the top agg
        collect(stream, 0);
    }

    @Override
    public void collect(DocIdStream stream, long owningBucketOrd) throws IOException {
        for (;;) {
            int upToExclusive = upToInclusive + 1;
            if (upToExclusive < 0) { // overflow
                upToExclusive = Integer.MAX_VALUE;
            }

            if (upToSameBucket) {
                if (isSubNoOp) {
                    // stream.count maybe faster when we don't need to handle sub-aggs
                    long count = stream.count(upToExclusive);
                    aggregator.incrementBucketDocCount(upToBucketIndex, count);
                } else {
                    final int[] count = { 0 };
                    stream.forEach(upToExclusive, doc -> {
                        sub.collect(doc, upToBucketIndex);
                        count[0]++;
                    });
                    aggregator.incrementBucketDocCount(upToBucketIndex, count[0]);
                }
            } else {
                stream.forEach(upToExclusive, doc -> collect(doc, owningBucketOrd));
            }

            if (stream.mayHaveRemaining()) {
                advanceSkipper(upToExclusive, owningBucketOrd);
            } else {
                break;
            }
        }
    }

    /**
     * Call back for auto date histogram
     *
     * @opensearch.internal
     */
    public interface IncreaseRoundingIfNeeded {
        void accept(long owningBucket, long rounded);
    }

    /**
     * Skiplist is based as top level agg (null parent) or parent that will execute in sorted order
     *
     */
    public static boolean canUseSkiplist(LongBounds hardBounds, Aggregator parent, DocValuesSkipper skipper, NumericDocValues singleton) {
        if (skipper == null || singleton == null) return false;
        // TODO: add hard bounds support
        if (hardBounds != null) return false;

        if (parent == null) return true;

        if (parent instanceof AggregatorBase base) {
            return base.getLeafCollectorMode() == AggregatorBase.LeafCollectionMode.FILTER_REWRITE;
        }
        return false;
    }
}
