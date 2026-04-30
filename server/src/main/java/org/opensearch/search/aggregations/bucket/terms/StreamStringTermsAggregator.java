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
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
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

    // Keyed on (owningBucketOrd, segmentOrdinal) so each parent bucket owns a distinct
    // bucket ordinal for the same term. Without this, a streaming terms agg running as
    // a sub of another terms agg would collide every parent's doc counts into the same
    // flat array, producing identical inner bucket lists for every parent bucket.
    private final LongKeyedBucketOrds bucketOrds;

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
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, order, format, bucketCountThresholds, collectionMode, showTermDocCountError, metadata);
        this.valuesSource = valuesSource;
        this.resultStrategy = resultStrategy.apply(this);
        this.segmentTopN = segmentTopN;
        this.bucketOrds = LongKeyedBucketOrds.build(context.bigArrays(), cardinality);
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
                // For key-based ordering the callers hand us segment ordinals, and those
                // ordinals already reflect alphabetical order in the docvalues, so compare
                // them directly.
                boolean ascending = InternalOrder.isKeyAsc(order);
                ordinalComparator = (leftOrd, rightOrd) -> {
                    return ascending ? Long.compare(leftOrd, rightOrd) : Long.compare(rightOrd, leftOrd);
                };
            } else if (partiallyBuiltBucketComparator != null) {
                // For sub-aggregation ordering, compare via the per-bucket comparator. The
                // ordinals handed in here are composite bucket ords (already owning-bucket-
                // scoped), so bucketDocCount reads the right value.
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

        SortedDocValues singleValues = DocValues.unwrapSingleton(sortedDocValuesPerBatch);
        if (singleValues != null) {
            segmentsWithSingleValuedOrds++;
            return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, sortedDocValuesPerBatch) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    if (false == singleValues.advanceExact(doc)) {
                        return;
                    }
                    int ordinal = singleValues.ordValue();
                    collectInto(sub, doc, owningBucketOrd, ordinal);
                }
            });
        }
        segmentsWithMultiValuedOrds++;
        return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, sortedDocValuesPerBatch) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (false == sortedDocValuesPerBatch.advanceExact(doc)) {
                    return;
                }
                int count = sortedDocValuesPerBatch.docValueCount();
                long ordinal;
                while ((count-- > 0) && (ordinal = sortedDocValuesPerBatch.nextOrd()) != SortedSetDocValues.NO_MORE_DOCS) {
                    collectInto(sub, doc, owningBucketOrd, ordinal);
                }
            }
        });
    }

    private void collectInto(LeafBucketCollector sub, int doc, long owningBucketOrd, long segmentOrdinal) throws IOException {
        long bucketOrd = bucketOrds.add(owningBucketOrd, segmentOrdinal);
        if (bucketOrd < 0) {
            bucketOrd = -1 - bucketOrd;
            collectExistingBucket(sub, doc, bucketOrd);
        } else {
            collectBucket(sub, doc, bucketOrd);
        }
    }

    /**
     * Strategy for building results.
     */
    public abstract class ResultStrategy<R extends InternalAggregation, B extends InternalMultiBucketAggregation.InternalBucket>
        implements
            Releasable {

        protected ResultStrategy() {}

        InternalAggregation[] buildAggregationsBatch(long[] owningBucketOrds) throws IOException {
            LocalBucketCountThresholds localBucketCountThresholds = context.asLocalBucketCountThresholds(bucketCountThresholds);
            if (valueCount == 0) {
                InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
                for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                    results[ordIdx] = buildNoValuesResult(owningBucketOrds[ordIdx]);
                }
                return results;
            }

            B[][] topBucketsPerOwningOrd = buildTopBucketsPerOrd(owningBucketOrds.length);
            long[] otherDocCount = new long[owningBucketOrds.length];

            for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                checkCancelled();
                logger.debug("Cardinality post collection for ordIdx {}: {}", ordIdx, valueCount);
                SelectionResult<B> selectionResult = selectTopBuckets(owningBucketOrds[ordIdx], segmentTopN, bucketCountThresholds);

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

        /**
         * Collect candidate bucket ords for {@code owningBucketOrd}, pick the top
         * {@code segmentSize} according to the active ordering, and emit final buckets.
         * Each candidate entry carries both the bucket ordinal (into docCounts) and the
         * underlying segment ordinal (for docvalues term lookup).
         */
        private SelectionResult<B> selectTopBuckets(long owningBucketOrd, int segmentSize, BucketCountThresholds thresholds)
            throws IOException {
            long bucketsForOwner = bucketOrds.bucketsInOrd(owningBucketOrd);
            if (bucketsForOwner == 0) {
                return new SelectionResult<>(new ArrayList<>(), 0L);
            }

            // Pair up each candidate as (bucketOrd, segmentOrdinal). We keep them in two
            // parallel arrays sized bucketsForOwner.
            long[] candidateBucketOrds = new long[Math.toIntExact(bucketsForOwner)];
            long[] candidateSegmentOrds = new long[candidateBucketOrds.length];

            int cnt = 0;
            long totalDocCount = 0;
            LongKeyedBucketOrds.BucketOrdsEnum enumerator = bucketOrds.ordsEnum(owningBucketOrd);
            while (enumerator.next()) {
                long bucketOrd = enumerator.ord();
                long segmentOrd = enumerator.value();
                long docCount = bucketDocCount(bucketOrd);
                totalDocCount += docCount;
                if (docCount >= thresholds.getMinDocCount()) {
                    candidateBucketOrds[cnt] = bucketOrd;
                    candidateSegmentOrds[cnt] = segmentOrd;
                    cnt++;
                }
            }

            int effectiveSegmentSize = Math.min(segmentSize, cnt);

            if (cnt <= effectiveSegmentSize) {
                List<B> result = new ArrayList<>(cnt);
                long selectedDocCount = 0;
                for (int i = 0; i < cnt; i++) {
                    long bucketOrd = candidateBucketOrds[i];
                    long segmentOrd = candidateSegmentOrds[i];
                    long docCount = bucketDocCount(bucketOrd);
                    result.add(buildFinalBucket(bucketOrd, segmentOrd, docCount));
                    selectedDocCount += docCount;
                }
                return new SelectionResult<>(result, totalDocCount - selectedDocCount);
            }

            ensureOrdinalComparator();
            final long[] bucketOrdsRef = candidateBucketOrds;
            final long[] segmentOrdsRef = candidateSegmentOrds;
            IntroSelector selector = new IntroSelector() {
                long pivotBucketOrd;
                long pivotSegmentOrd;

                @Override
                protected void swap(int i, int j) {
                    long tmp = bucketOrdsRef[i];
                    bucketOrdsRef[i] = bucketOrdsRef[j];
                    bucketOrdsRef[j] = tmp;
                    tmp = segmentOrdsRef[i];
                    segmentOrdsRef[i] = segmentOrdsRef[j];
                    segmentOrdsRef[j] = tmp;
                }

                @Override
                protected void setPivot(int i) {
                    pivotBucketOrd = bucketOrdsRef[i];
                    pivotSegmentOrd = segmentOrdsRef[i];
                }

                @Override
                protected int comparePivot(int j) {
                    long left, right;
                    if (isKeyOrder(order)) {
                        // Compare segment ordinals (alphabetical proxy)
                        left = segmentOrdsRef[j];
                        right = pivotSegmentOrd;
                    } else {
                        // Compare bucket ordinals (so bucketDocCount reads per-owner counts)
                        left = bucketOrdsRef[j];
                        right = pivotBucketOrd;
                    }
                    if (ordinalComparator != null) {
                        return -ordinalComparator.compare(left, right);
                    }
                    long leftDocCount = bucketDocCount(bucketOrdsRef[j]);
                    long rightDocCount = bucketDocCount(pivotBucketOrd);
                    return Long.compare(leftDocCount, rightDocCount);
                }
            };

            selector.select(0, cnt, effectiveSegmentSize);

            // Match the historical emit order (ascending by segment ordinal, i.e.
            // alphabetical) so downstream reduce ordering assumptions hold.
            Integer[] indices = new Integer[effectiveSegmentSize];
            for (int i = 0; i < effectiveSegmentSize; i++) {
                indices[i] = i;
            }
            Arrays.sort(indices, (a, b) -> Long.compare(candidateSegmentOrds[a], candidateSegmentOrds[b]));

            List<B> result = new ArrayList<>(effectiveSegmentSize);
            long selectedDocCount = 0;
            for (int idx : indices) {
                long bucketOrd = candidateBucketOrds[idx];
                long segmentOrd = candidateSegmentOrds[idx];
                long docCount = bucketDocCount(bucketOrd);
                result.add(buildFinalBucket(bucketOrd, segmentOrd, docCount));
                selectedDocCount += docCount;
            }
            return new SelectionResult<>(result, totalDocCount - selectedDocCount);
        }

        @Override
        public void close() {}

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
         * Build a final bucket. {@code bucketOrd} is the composite bucket ordinal
         * (used for sub-aggregation wiring and doc count lookups); {@code segmentOrd}
         * is the underlying segment-local ordinal used to materialize the term bytes.
         */
        abstract B buildFinalBucket(long bucketOrd, long segmentOrd, long docCount) throws IOException;
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
        StringTerms.Bucket buildFinalBucket(long bucketOrd, long segmentOrd, long docCount) throws IOException {
            // Recreate DocValues as needed for concurrent segment search.
            BytesRef term = BytesRef.deepCopyOf(sortedDocValuesPerBatch.lookupOrd(segmentOrd));

            StringTerms.Bucket result = new StringTerms.Bucket(term, docCount, null, showTermDocCountError, 0, format);
            result.bucketOrd = bucketOrd;
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
        Releasables.close(resultStrategy, bucketOrds);
    }
}
