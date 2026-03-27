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
    public void collect(int[] docs, int count, long owningBucketOrd) throws IOException {
        int i = 0;
        while (i < count) {
            if (docs[i] > upToInclusive) {
                advanceSkipper(docs[i], owningBucketOrd);
            }

            if (upToSameBucket) {
                // Find how many consecutive docs in this batch fall within the same skiplist interval
                int j = i;
                while (j < count && docs[j] <= upToInclusive) {
                    j++;
                }
                int batchCount = j - i;
                if (isSubNoOp) {
                    aggregator.incrementBucketDocCount(upToBucketIndex, batchCount);
                } else {
                    // Pass the sub-array slice to sub-aggregator's collect(int[], int, long)
                    // We need to copy since the sub-agg expects docs starting at index 0
                    if (i == 0 && batchCount == count) {
                        sub.collect(docs, batchCount, upToBucketIndex);
                    } else {
                        int[] subDocs = new int[batchCount];
                        System.arraycopy(docs, i, subDocs, 0, batchCount);
                        sub.collect(subDocs, batchCount, upToBucketIndex);
                    }
                    aggregator.incrementBucketDocCount(upToBucketIndex, batchCount);
                }
                i = j;
            } else {
                // Per-doc fallback for docs that don't all map to the same bucket
                collect(docs[i], owningBucketOrd);
                i++;
            }
        }
    }

    @Override
    public void collectRange(int min, int max, long owningBucketOrd) throws IOException {
        while (min < max) {
            if (min > upToInclusive) {
                advanceSkipper(min, owningBucketOrd);
            }

            int upToExclusive = upToInclusive + 1;
            if (upToExclusive < 0) { // overflow
                upToExclusive = Integer.MAX_VALUE;
            }
            // Clamp to the range we're collecting
            int end = Math.min(upToExclusive, max);

            if (upToSameBucket) {
                if (isSubNoOp) {
                    // All docs in [min, end) map to the same bucket — just count them
                    aggregator.incrementBucketDocCount(upToBucketIndex, end - min);
                } else {
                    // Delegate to sub-aggregator's collectRange with the resolved bucket
                    sub.collectRange(min, end, upToBucketIndex);
                    aggregator.incrementBucketDocCount(upToBucketIndex, end - min);
                }
            } else {
                // Fall back to per-doc collection for docs that don't all map to the same bucket
                for (int doc = min; doc < end; doc++) {
                    collect(doc, owningBucketOrd);
                }
            }

            min = end;
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
