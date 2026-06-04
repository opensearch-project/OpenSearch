/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.opensearch.search.aggregations.bucket.LocalBucketCountThresholds;
import org.opensearch.search.aggregations.bucket.terms.LongKeyedBucketOrds.BucketOrdsEnum;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.function.Supplier;

import static org.opensearch.search.aggregations.InternalOrder.isKeyOrder;

/**
 * Strategy for selecting top buckets from aggregation results.
 *
 */
enum BucketSelectionStrategy {
    PRIORITY_QUEUE {
        @Override
        public <B extends InternalMultiBucketAggregation.InternalBucket> SelectionResult<B> selectTopBuckets(SelectionInput<B> input)
            throws IOException {
            PriorityQueue<B> ordered = input.buildPriorityQueue.buildPriorityQueue(input.size);
            B spare = null;
            long otherDocCount = 0;

            while (input.ordsEnum.next()) {
                long docCount = input.bucketDocCountFunction.bucketDocCount(input.ordsEnum.ord());
                otherDocCount += docCount;
                if (docCount < input.localBucketCountThresholds.getMinDocCount()) {
                    continue;
                }
                if (spare == null) {
                    spare = input.emptyBucketBuilder.get();
                }
                input.bucketUpdateFunction.updateBucket(spare, input.ordsEnum, docCount);
                spare = ordered.insertWithOverflow(spare);
            }

            B[] topBuckets = input.bucketArrayBuilder.buildBuckets(ordered.size());
            if (isKeyOrder(input.order)) {
                for (int b = ordered.size() - 1; b >= 0; --b) {
                    topBuckets[b] = ordered.pop();
                    otherDocCount -= topBuckets[b].getDocCount();
                }
            } else {
                Iterator<B> itr = ordered.iterator();
                for (int b = ordered.size() - 1; b >= 0; --b) {
                    topBuckets[b] = itr.next();
                    otherDocCount -= topBuckets[b].getDocCount();
                }
            }

            return new SelectionResult<>(topBuckets, otherDocCount, "priority_queue");
        }
    },

    QUICK_SELECT_OR_SELECT_ALL {
        @Override
        public <B extends InternalMultiBucketAggregation.InternalBucket> SelectionResult<B> selectTopBuckets(SelectionInput<B> input)
            throws IOException {
            B[] bucketsForOrd = input.bucketArrayBuilder.buildBuckets((int) input.bucketsInOrd);
            int validBucketCount = 0;
            long otherDocCount = 0;

            // Collect all valid buckets
            while (input.ordsEnum.next()) {
                long docCount = input.bucketDocCountFunction.bucketDocCount(input.ordsEnum.ord());
                otherDocCount += docCount;
                if (docCount < input.localBucketCountThresholds.getMinDocCount()) {
                    continue;
                }

                B spare = input.emptyBucketBuilder.get();
                input.bucketUpdateFunction.updateBucket(spare, input.ordsEnum, docCount);
                bucketsForOrd[validBucketCount++] = spare;
            }

            B[] topBuckets;
            String actualStrategy;
            if (validBucketCount > input.size) {
                ArrayUtil.select(
                    bucketsForOrd,
                    0,
                    validBucketCount,
                    input.size,
                    (b1, b2) -> input.partiallyBuiltBucketComparator.compare((InternalTerms.Bucket<?>) b1, (InternalTerms.Bucket<?>) b2)
                );
                topBuckets = Arrays.copyOf(bucketsForOrd, input.size);
                for (int b = 0; b < input.size; b++) {
                    otherDocCount -= topBuckets[b].getDocCount();
                }
                actualStrategy = "quick_select";
            } else {
                // Return all buckets (no selection needed)
                topBuckets = Arrays.copyOf(bucketsForOrd, validBucketCount);
                otherDocCount = 0L;
                actualStrategy = "select_all";
            }

            return new SelectionResult<>(topBuckets, otherDocCount, actualStrategy);
        }
    };

    public static BucketSelectionStrategy determine(
        int size,
        long bucketsInOrd,
        BucketOrder order,
        Comparator<InternalTerms.Bucket<?>> partiallyBuiltBucketComparator,
        int factor
    ) {
        /*
        We select the strategy based on the following condition with configurable threshold factor:
            case 1: size is less than 20% of bucketsInOrd: PRIORITY_QUEUE
            case 2: size is greater than 20% of bucketsInOrd: QUICK_SELECT
            case 3: size == bucketsInOrd : return all buckets
        case 2 and 3 are encapsulated in QUICK_SELECT_OR_SELECT_ALL method.

        Along with the above conditions, we also go with the original PRIORITY_QUEUE based approach
        if isKeyOrder or its significant term aggregation.

        if factor is 0, always use PRIORITY_QUEUE strategy (since 0 < bucketsInOrd is always true).
        */
        if (((long) size * factor < bucketsInOrd) || isKeyOrder(order) || partiallyBuiltBucketComparator == null) {
            return PRIORITY_QUEUE;
        } else {
            return QUICK_SELECT_OR_SELECT_ALL;
        }
    }

    public abstract <B extends InternalMultiBucketAggregation.InternalBucket> SelectionResult<B> selectTopBuckets(SelectionInput<B> input)
        throws IOException;

    /**
     * Represents the inputs for strategy execution to select buckets
     */
    public static class SelectionInput<B extends InternalMultiBucketAggregation.InternalBucket> {
        public final int size;
        public final long bucketsInOrd;
        public final BucketOrdsEnum ordsEnum;
        public final Supplier<B> emptyBucketBuilder;
        public final LocalBucketCountThresholds localBucketCountThresholds;
        public final int ordIdx;
        public final BucketOrder order;
        public final PriorityQueueBuilder<B> buildPriorityQueue;
        public final BucketArrayBuilder<B> bucketArrayBuilder;
        public final BucketUpdateFunction<B> bucketUpdateFunction;
        public final BucketDocCountFunction bucketDocCountFunction;
        public final Comparator<InternalTerms.Bucket<?>> partiallyBuiltBucketComparator;

        public SelectionInput(
            int size,
            long bucketsInOrd,
            BucketOrdsEnum ordsEnum,
            Supplier<B> emptyBucketBuilder,
            LocalBucketCountThresholds localBucketCountThresholds,
            int ordIdx,
            BucketOrder order,
            PriorityQueueBuilder<B> buildPriorityQueue,
            BucketArrayBuilder<B> bucketArrayBuilder,
            BucketUpdateFunction<B> bucketUpdateFunction,
            BucketDocCountFunction bucketDocCountFunction,
            Comparator<InternalTerms.Bucket<?>> partiallyBuiltBucketComparator
        ) {
            this.size = size;
            this.bucketsInOrd = bucketsInOrd;
            this.ordsEnum = ordsEnum;
            this.emptyBucketBuilder = emptyBucketBuilder;
            this.localBucketCountThresholds = localBucketCountThresholds;
            this.ordIdx = ordIdx;
            this.order = order;
            this.buildPriorityQueue = buildPriorityQueue;
            this.bucketArrayBuilder = bucketArrayBuilder;
            this.bucketUpdateFunction = bucketUpdateFunction;
            this.bucketDocCountFunction = bucketDocCountFunction;
            this.partiallyBuiltBucketComparator = partiallyBuiltBucketComparator;
        }
    }

    /**
     * Represents the results strategy execution to select buckets
     */
    public static class SelectionResult<B extends InternalMultiBucketAggregation.InternalBucket> {
        public final B[] topBuckets;
        public final long otherDocCount;
        public final String actualStrategyUsed;

        public SelectionResult(B[] topBuckets, long otherDocCount, String actualStrategyUsed) {
            this.topBuckets = topBuckets;
            this.otherDocCount = otherDocCount;
            this.actualStrategyUsed = actualStrategyUsed;
        }
    }

    /**
     * Interface for bucketDocCount method
     */
    @FunctionalInterface
    public interface BucketDocCountFunction {
        long bucketDocCount(long ord) throws IOException;
    }

    /**
     * Interface for updateBucket method
     */
    @FunctionalInterface
    public interface BucketUpdateFunction<B> {
        void updateBucket(B spare, BucketOrdsEnum ordsEnum, long docCount) throws IOException;
    }

    /**
     * Interface for buildBuckets method
     */
    @FunctionalInterface
    public interface BucketArrayBuilder<B> {
        B[] buildBuckets(int size);
    }

    /**
     * Interface for buildPriorityQueue method
     */
    @FunctionalInterface
    public interface PriorityQueueBuilder<B> {
        PriorityQueue<B> buildPriorityQueue(int size);
    }
}
