/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntroSelector;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.IntArray;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.LocalBucketCountThresholds;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.opensearch.search.aggregations.InternalOrder.isKeyOrder;

/**
 * Stream search terms aggregation
 */
public class StreamStringTermsAggregator extends AbstractStringTermsAggregator {
    private static final Logger logger = LogManager.getLogger(StreamStringTermsAggregator.class);
    private SortedSetDocValues sortedDocValuesPerBatch;
    private long valueCount;
    private final ValuesSource.Bytes.WithOrdinals valuesSource;
    protected int segmentsWithSingleValuedOrds = 0;
    protected int segmentsWithMultiValuedOrds = 0;
    protected final ResultStrategy<?, ?> resultStrategy;
    private boolean leafCollectorCreated = false;
    private final int segmentTopN;

    private Aggregator.BucketComparator ordinalComparator;
    private StringTerms.Bucket tempBucket1;
    private StringTerms.Bucket tempBucket2;

    public StreamStringTermsAggregator(
        String name,
        AggregatorFactories factories,
        Function<StreamStringTermsAggregator, ResultStrategy<?, ?>> resultStrategy,
        ValuesSource.Bytes.WithOrdinals valuesSource,
        BucketOrder order,
        DocValueFormat format,
        BucketCountThresholds bucketCountThresholds,
        SearchContext context,
        Aggregator parent,
        SubAggCollectionMode collectionMode,
        boolean showTermDocCountError,
        int segmentTopN,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, order, format, bucketCountThresholds, collectionMode, showTermDocCountError, metadata);
        this.valuesSource = valuesSource;
        this.resultStrategy = resultStrategy.apply(this);
        this.segmentTopN = segmentTopN;
    }

    @Override
    public void doReset() {
        super.doReset();
        valueCount = 0;
        sortedDocValuesPerBatch = null;
        this.leafCollectorCreated = false;
        this.ordinalComparator = null;
        this.tempBucket1 = null;
        this.tempBucket2 = null;
    }

    private void ensureOrdinalComparator() {
        if (ordinalComparator == null) {
            if (isKeyOrder(order)) {
                // For key-based ordering, compare ordinals directly (alphabetical order)
                // Reverse comparison for descending order
                boolean ascending = InternalOrder.isKeyAsc(order);
                ordinalComparator = (leftOrd, rightOrd) -> {
                    return ascending ? Long.compare(leftOrd, rightOrd) : Long.compare(rightOrd, leftOrd);
                };
            } else if (partiallyBuiltBucketComparator != null) {
                // For sub-aggregation ordering, use bucket comparator
                tempBucket1 = new StringTerms.Bucket(null, 0, null, false, 0, format) {
                    @Override
                    public int compareKey(StringTerms.Bucket other) {
                        return Long.compare(this.bucketOrd, other.bucketOrd);
                    }
                };
                tempBucket2 = new StringTerms.Bucket(null, 0, null, false, 0, format) {
                    @Override
                    public int compareKey(StringTerms.Bucket other) {
                        return Long.compare(this.bucketOrd, other.bucketOrd);
                    }
                };
                ordinalComparator = (leftOrd, rightOrd) -> {
                    tempBucket1.bucketOrd = leftOrd;
                    tempBucket1.docCount = bucketDocCount(leftOrd);
                    tempBucket2.bucketOrd = rightOrd;
                    tempBucket2.docCount = bucketDocCount(rightOrd);
                    return partiallyBuiltBucketComparator.compare(tempBucket1, tempBucket2);
                };
            }
        }
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return resultStrategy.buildAggregationsBatch(owningBucketOrds);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return resultStrategy.buildEmptyResult();
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        if (this.leafCollectorCreated) {
            throw new IllegalStateException(
                "Calling " + StreamStringTermsAggregator.class.getSimpleName() + " for the second segment: " + ctx
            );
        } else {
            this.leafCollectorCreated = true;
        }
        this.sortedDocValuesPerBatch = valuesSource.ordinalsValues(ctx);
        this.valueCount = sortedDocValuesPerBatch.getValueCount();
        if (docCounts == null) {
            this.docCounts = context.bigArrays().newLongArray(valueCount, true);
        } else {
            // TODO: check performance of grow vs creating a new one
            this.docCounts = context.bigArrays().grow(docCounts, valueCount);
        }

        SortedDocValues singleValues = DocValues.unwrapSingleton(sortedDocValuesPerBatch);
        if (singleValues != null) {
            segmentsWithSingleValuedOrds++;
            /*
             * Optimize when there isn't a filter because that is very
             * common and marginally faster.
             */
            return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, sortedDocValuesPerBatch) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    if (false == singleValues.advanceExact(doc)) {
                        return;
                    }
                    int ordinal = singleValues.ordValue();
                    collectExistingBucket(sub, doc, ordinal);
                }
            });

        }
        segmentsWithMultiValuedOrds++;
        /*
         * Optimize when there isn't a filter because that is very
         * common and marginally faster.
         */
        return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, sortedDocValuesPerBatch) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (false == sortedDocValuesPerBatch.advanceExact(doc)) {
                    return;
                }
                int count = sortedDocValuesPerBatch.docValueCount();
                long ordinal;
                while ((count-- > 0) && (ordinal = sortedDocValuesPerBatch.nextOrd()) != SortedSetDocValues.NO_MORE_DOCS) {
                    collectExistingBucket(sub, doc, ordinal);
                }
            }
        });
    }

    /**
     * Strategy for building results.
     */
    public abstract class ResultStrategy<R extends InternalAggregation, B extends InternalMultiBucketAggregation.InternalBucket>
        implements
            Releasable {

        protected IntArray reusableIndices;

        protected ResultStrategy() {}

        void prepareIndicesArray(long valueCount) {
            if (reusableIndices == null) {
                reusableIndices = context.bigArrays().newIntArray(valueCount, false);
            } else {
                reusableIndices = context.bigArrays().grow(reusableIndices, valueCount);
            }
        }

        InternalAggregation[] buildAggregationsBatch(long[] owningBucketOrds) throws IOException {
            LocalBucketCountThresholds localBucketCountThresholds = context.asLocalBucketCountThresholds(bucketCountThresholds);
            if (valueCount == 0) {
                InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
                for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                    results[ordIdx] = buildNoValuesResult(owningBucketOrds[ordIdx]);
                }
                return results;
            }

            // for each owning bucket, there will be list of bucket ord of this aggregation
            B[][] topBucketsPerOwningOrd = buildTopBucketsPerOrd(owningBucketOrds.length);
            long[] otherDocCount = new long[owningBucketOrds.length];

            for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {

                // processing each owning bucket
                checkCancelled();
                logger.debug("Cardinality post collection for ordIdx {}: {}", ordIdx, valueCount);
                // using bucketCountThresholds since we don't do reduce across slice
                // and send results per segment to coordinator
                SelectionResult<B> selectionResult = selectTopBuckets(segmentTopN, bucketCountThresholds);

                topBucketsPerOwningOrd[ordIdx] = buildBuckets(selectionResult.buckets.size());
                for (int i = 0; i < topBucketsPerOwningOrd[ordIdx].length; i++) {
                    topBucketsPerOwningOrd[ordIdx][i] = selectionResult.buckets.get(i);
                }
                otherDocCount[ordIdx] = selectionResult.otherDocCount;
            }

            buildSubAggs(topBucketsPerOwningOrd);

            InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
            for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                results[ordIdx] = buildResult(owningBucketOrds[ordIdx], otherDocCount[ordIdx], topBucketsPerOwningOrd[ordIdx]);
            }
            return results;
        }

        private static class SelectionResult<B> {
            final List<B> buckets;
            final long otherDocCount;

            SelectionResult(List<B> buckets, long otherDocCount) {
                this.buckets = buckets;
                this.otherDocCount = otherDocCount;
            }
        }

        private SelectionResult<B> selectTopBuckets(int segmentSize, BucketCountThresholds thresholds) throws IOException {
            prepareIndicesArray(valueCount);

            int cnt = 0;
            long totalDocCount = 0;
            for (int i = 0; i < valueCount; i++) {
                long docCount = bucketDocCount(i);
                totalDocCount += docCount;
                if (docCount >= thresholds.getMinDocCount()) {
                    reusableIndices.set(cnt++, i);
                }
            }

            segmentSize = Math.min(segmentSize, cnt);

            if (cnt <= segmentSize) {
                List<B> result = new ArrayList<>();
                long selectedDocCount = 0;
                for (int i = 0; i < cnt; i++) {
                    long docCount = bucketDocCount(reusableIndices.get(i));
                    result.add(buildFinalBucket(reusableIndices.get(i), docCount));
                    selectedDocCount += docCount;
                }
                return new SelectionResult<>(result, totalDocCount - selectedDocCount);
            }

            IntroSelector selector = new IntroSelector() {
                int pivotOrdinal;

                @Override
                protected void swap(int i, int j) {
                    int temp = reusableIndices.get(i);
                    reusableIndices.set(i, reusableIndices.get(j));
                    reusableIndices.set(j, temp);
                }

                @Override
                protected void setPivot(int i) {
                    pivotOrdinal = reusableIndices.get(i);
                }

                @Override
                protected int comparePivot(int j) {
                    long leftOrd = reusableIndices.get(j);
                    long rightOrd = pivotOrdinal;
                    if (ordinalComparator != null) {
                        return -ordinalComparator.compare(leftOrd, rightOrd);
                    }
                    // Fallback to doc count for _count ordering
                    long leftDocCount = bucketDocCount(leftOrd);
                    long rightDocCount = bucketDocCount(rightOrd);
                    return Long.compare(leftDocCount, rightDocCount);
                }
            };

            ensureOrdinalComparator();
            selector.select(0, cnt, segmentSize);

            int[] selected = new int[segmentSize];
            for (int i = 0; i < segmentSize; i++) {
                selected[i] = reusableIndices.get(i);
            }

            reusableIndices.fill(0, valueCount, 0);
            for (int i = 0; i < segmentSize; i++) {
                reusableIndices.set(selected[i], 1);
            }

            List<B> result = new ArrayList<>(segmentSize);
            long selectedDocCount = 0;
            for (int ordinal = 0; ordinal < valueCount; ordinal++) {
                if (reusableIndices.get(ordinal) == 1) {
                    long docCount = bucketDocCount(ordinal);
                    result.add(buildFinalBucket(ordinal, docCount));
                    selectedDocCount += docCount;
                }
            }

            return new SelectionResult<>(result, totalDocCount - selectedDocCount);
        }

        @Override
        public void close() {
            Releasables.close(reusableIndices);
            reusableIndices = null;
        }

        abstract String describe();

        /**
         * Wrap the "standard" numeric terms collector to collect any more
         * information that this result type may need.
         */
        abstract LeafBucketCollector wrapCollector(LeafBucketCollector primary);

        /**
         * Build an array to hold the "top" buckets for each ordinal.
         */
        abstract B[][] buildTopBucketsPerOrd(int size);

        /**
         * Build an array of buckets for a particular ordinal to collect the
         * results. The populated list is passed to {@link #buildResult}.
         */
        abstract B[] buildBuckets(int size);

        /**
         * Build the sub-aggregations into the buckets. This will usually
         * delegate to {@link #buildSubAggsForAllBuckets}.
         */
        abstract void buildSubAggs(B[][] topBucketsPreOrd) throws IOException;

        /**
         * Turn the buckets into an aggregation result.
         */
        abstract R buildResult(long owningBucketOrd, long otherDocCount, B[] topBuckets);

        /**
         * Build an "empty" result. Only called if there isn't any data on this
         * shard.
         */
        abstract R buildEmptyResult();

        /**
         * Build an "empty" result for a particular bucket ordinal. Called when
         * there aren't any values for the field on this shard.
         */
        abstract R buildNoValuesResult(long owningBucketOrdinal);

        /**
         * Build a final bucket directly with the provided data, skipping temporary bucket creation.
         */
        abstract B buildFinalBucket(long ordinal, long docCount) throws IOException;
    }

    /**
     * StandardTermsResults for string terms
     *
     * @opensearch.internal
     */
    public class StandardTermsResults extends ResultStrategy<StringTerms, StringTerms.Bucket> {
        @Override
        String describe() {
            return "streaming_terms";
        }

        @Override
        LeafBucketCollector wrapCollector(LeafBucketCollector primary) {
            return primary;
        }

        @Override
        StringTerms.Bucket[][] buildTopBucketsPerOrd(int size) {
            return new StringTerms.Bucket[size][];
        }

        @Override
        StringTerms.Bucket[] buildBuckets(int size) {
            return new StringTerms.Bucket[size];
        }

        @Override
        void buildSubAggs(StringTerms.Bucket[][] topBucketsPerOrd) throws IOException {
            buildSubAggsForAllBuckets(topBucketsPerOrd, b -> b.bucketOrd, (b, aggs) -> b.aggregations = aggs);
        }

        @Override
        StringTerms buildResult(long owningBucketOrd, long otherDocCount, StringTerms.Bucket[] topBuckets) {
            final BucketOrder reduceOrder;
            if (isKeyOrder(order) == false) {
                reduceOrder = InternalOrder.key(true);
                Arrays.sort(topBuckets, reduceOrder.comparator());
            } else {
                reduceOrder = order;
            }
            return new StringTerms(
                name,
                reduceOrder,
                order,
                metadata(),
                format,
                bucketCountThresholds.getShardSize(),
                showTermDocCountError,
                otherDocCount,
                Arrays.asList(topBuckets),
                0,
                bucketCountThresholds
            );
        }

        @Override
        StringTerms buildEmptyResult() {
            return buildEmptyTermsAggregation();
        }

        @Override
        StringTerms buildNoValuesResult(long owningBucketOrdinal) {
            return buildEmptyResult();
        }

        @Override
        StringTerms.Bucket buildFinalBucket(long ordinal, long docCount) throws IOException {
            // Recreate DocValues as needed for concurrent segment search
            BytesRef term = BytesRef.deepCopyOf(sortedDocValuesPerBatch.lookupOrd(ordinal));

            StringTerms.Bucket result = new StringTerms.Bucket(term, docCount, null, showTermDocCountError, 0, format);
            result.bucketOrd = ordinal;
            result.setDocCountError(0);
            return result;
        }
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("result_strategy", resultStrategy.describe());
        add.accept("segments_with_single_valued_ords", segmentsWithSingleValuedOrds);
        add.accept("segments_with_multi_valued_ords", segmentsWithMultiValuedOrds);
    }

    @Override
    public void doClose() {
        Releasables.close(resultStrategy);
    }
}
