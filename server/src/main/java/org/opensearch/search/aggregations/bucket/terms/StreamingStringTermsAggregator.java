/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.common.lease.Releasable;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.aggregations.bucket.LocalBucketCountThresholds;
import org.opensearch.search.aggregations.bucket.terms.heuristic.SignificanceHeuristic;
import org.opensearch.search.aggregations.support.ValuesSource;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class StreamingStringTermsAggregator extends GlobalOrdinalsStringTermsAggregator {
    private SortedSetDocValues sortedDocValuesPerBatch;
    private long valueCount;

    public StreamingStringTermsAggregator(
        String name,
        AggregatorFactories factories,
        Function<StreamingStringTermsAggregator, ResultStrategy<?, ?, ?>> resultStrategy,
        ValuesSource.Bytes.WithOrdinals valuesSource,
        BucketOrder order,
        DocValueFormat format,
        BucketCountThresholds bucketCountThresholds,
        IncludeExclude.OrdinalsFilter includeExclude,
        SearchContext context,
        Aggregator parent,
        boolean remapGlobalOrds,
        SubAggCollectionMode collectionMode,
        boolean showTermDocCountError,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        super(
            name,
            factories,
            (GlobalOrdinalsStringTermsAggregator agg) -> resultStrategy.apply((StreamingStringTermsAggregator) agg),
            valuesSource,
            order,
            format,
            bucketCountThresholds,
            includeExclude,
            context,
            parent,
            remapGlobalOrds,
            collectionMode,
            showTermDocCountError,
            cardinality,
            metadata
        );
    }

    @Override
    public void doReset() {
        docCounts.fill(0, docCounts.size(), 0);
        valueCount = 0;
        sortedDocValuesPerBatch = null;
    }

    @Override
    public InternalAggregation[] buildAggregations(long[] owningBucketOrds) throws IOException {
        return ((StreamingStringTermsAggregator.ResultStrategy<?, ?, ?>) resultStrategy).buildAggregationsBatch(owningBucketOrds);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        this.sortedDocValuesPerBatch = valuesSource.ordinalsValues(ctx);
        this.valueCount = sortedDocValuesPerBatch.getValueCount(); // for streaming case, the value count is reset to per batch
        // cardinality
        if (docCounts == null) {
            this.docCounts = context.bigArrays().newLongArray(valueCount, true);
        } else {
            this.docCounts = context.bigArrays().grow(docCounts, valueCount);
        }

        SortedDocValues singleValues = DocValues.unwrapSingleton(sortedDocValuesPerBatch);
        if (singleValues != null) {
            segmentsWithSingleValuedOrds++;
            if (acceptedGlobalOrdinals == ALWAYS_TRUE) {
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
                        int batchOrd = singleValues.ordValue();
                        collectionStrategy.collectGlobalOrd(owningBucketOrd, doc, batchOrd, sub);
                    }
                });
            }
            return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, sortedDocValuesPerBatch) {
                @Override
                public void collect(int doc, long owningBucketOrd) throws IOException {
                    if (false == singleValues.advanceExact(doc)) {
                        return;
                    }
                    int batchOrd = singleValues.ordValue();
                    if (false == acceptedGlobalOrdinals.test(batchOrd)) {
                        return;
                    }
                    collectionStrategy.collectGlobalOrd(owningBucketOrd, doc, batchOrd, sub);
                }
            });
        }
        segmentsWithMultiValuedOrds++;
        if (acceptedGlobalOrdinals == ALWAYS_TRUE) {
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
                    long globalOrd;
                    while ((count-- > 0) && (globalOrd = sortedDocValuesPerBatch.nextOrd()) != SortedSetDocValues.NO_MORE_DOCS) {
                        collectionStrategy.collectGlobalOrd(owningBucketOrd, doc, globalOrd, sub);
                    }
                }
            });
        }
        return resultStrategy.wrapCollector(new LeafBucketCollectorBase(sub, sortedDocValuesPerBatch) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (false == sortedDocValuesPerBatch.advanceExact(doc)) {
                    return;
                }
                int count = sortedDocValuesPerBatch.docValueCount();
                long batchOrd;
                while ((count-- > 0) && (batchOrd = sortedDocValuesPerBatch.nextOrd()) != SortedSetDocValues.NO_MORE_DOCS) {
                    if (false == acceptedGlobalOrdinals.test(batchOrd)) {
                        continue;
                    }
                    collectionStrategy.collectGlobalOrd(owningBucketOrd, doc, batchOrd, sub);
                }
            }
        });
    }

    abstract class ResultStrategy<
        R extends InternalAggregation,
        B extends InternalMultiBucketAggregation.InternalBucket,
        TB extends InternalMultiBucketAggregation.InternalBucket> extends GlobalOrdinalsStringTermsAggregator.ResultStrategy<R, B, TB>
        implements
            Releasable {

        private InternalAggregation[] buildAggregationsBatch(long[] owningBucketOrds) throws IOException {
            LocalBucketCountThresholds localBucketCountThresholds = context.asLocalBucketCountThresholds(bucketCountThresholds);
            if (valueCount == 0) { // no context in this reader
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
                // final int size;
                // if (localBucketCountThresholds.getMinDocCount() == 0) {
                // // if minDocCount == 0 then we can end up with more buckets then maxBucketOrd() returns
                // size = (int) Math.min(valueCount, localBucketCountThresholds.getRequiredSize());
                // } else {
                // size = (int) Math.min(maxBucketOrd(), localBucketCountThresholds.getRequiredSize());
                // }

                // for streaming agg, we don't need priority queue, just a container for all the temp bucket
                // seems other count is also not needed, because we are not reducing any buckets

                // PriorityQueue<TB> ordered = buildPriorityQueue(size);
                List<B> bucketsPerOwningOrd = new ArrayList<>();
                // final int finalOrdIdx = ordIdx;

                int finalOrdIdx = ordIdx;
                collectionStrategy.forEach(owningBucketOrds[ordIdx], (globalOrd, bucketOrd, docCount) -> {
                    if (docCount >= localBucketCountThresholds.getMinDocCount()) {
                        B finalBucket = buildFinalBucket(globalOrd, bucketOrd, docCount, owningBucketOrds[finalOrdIdx]);
                        bucketsPerOwningOrd.add(finalBucket);
                    }
                });

                // Get the top buckets
                // ordered contains the top buckets for the owning bucket
                topBucketsPerOwningOrd[ordIdx] = buildBuckets(bucketsPerOwningOrd.size());

                for (int i = 0; i < topBucketsPerOwningOrd[ordIdx].length; i++) {
                    topBucketsPerOwningOrd[ordIdx][i] = bucketsPerOwningOrd.get(i);
                }
            }

            buildSubAggs(topBucketsPerOwningOrd);

            InternalAggregation[] results = new InternalAggregation[owningBucketOrds.length];
            for (int ordIdx = 0; ordIdx < owningBucketOrds.length; ordIdx++) {
                results[ordIdx] = buildResult(owningBucketOrds[ordIdx], otherDocCount[ordIdx], topBucketsPerOwningOrd[ordIdx]);
            }
            return results;
        }

        /**
         * Build a final bucket directly with the provided data, skipping temporary bucket creation.
         */
        abstract B buildFinalBucket(long globalOrd, long bucketOrd, long docCount, long owningBucketOrd) throws IOException;
    }

    class StandardTermsResults extends ResultStrategy<StringTerms, StringTerms.Bucket, OrdBucket> {
        // Delegate to the parent's StandardTermsResults for most functionality
        private final GlobalOrdinalsStringTermsAggregator.StandardTermsResults delegate;

        StandardTermsResults() {
            this.delegate = ((GlobalOrdinalsStringTermsAggregator) StreamingStringTermsAggregator.this).new StandardTermsResults();
        }

        @Override
        String describe() {
            return "streaming_terms";
        }

        @Override
        LeafBucketCollector wrapCollector(LeafBucketCollector primary) {
            return delegate.wrapCollector(primary);
        }

        @Override
        StringTerms.Bucket[][] buildTopBucketsPerOrd(int size) {
            return delegate.buildTopBucketsPerOrd(size);
        }

        @Override
        StringTerms.Bucket[] buildBuckets(int size) {
            return delegate.buildBuckets(size);
        }

        @Override
        OrdBucket buildEmptyTemporaryBucket() {
            return delegate.buildEmptyTemporaryBucket();
        }

        @Override
        BucketUpdater<OrdBucket> bucketUpdater(long owningBucketOrd) throws IOException {
            return delegate.bucketUpdater(owningBucketOrd);
        }

        @Override
        PriorityQueue<OrdBucket> buildPriorityQueue(int size) {
            return delegate.buildPriorityQueue(size);
        }

        @Override
        StringTerms.Bucket convertTempBucketToRealBucket(OrdBucket temp) throws IOException {
            return delegate.convertTempBucketToRealBucket(temp);
        }

        @Override
        void buildSubAggs(StringTerms.Bucket[][] topBucketsPerOrd) throws IOException {
            delegate.buildSubAggs(topBucketsPerOrd);
        }

        @Override
        StringTerms buildResult(long owningBucketOrd, long otherDocCount, StringTerms.Bucket[] topBuckets) {
            return delegate.buildResult(owningBucketOrd, otherDocCount, topBuckets);
        }

        @Override
        StringTerms buildEmptyResult() {
            return delegate.buildEmptyResult();
        }

        @Override
        StringTerms buildNoValuesResult(long owningBucketOrdinal) {
            return delegate.buildNoValuesResult(owningBucketOrdinal);
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        StringTerms.Bucket buildFinalBucket(long globalOrd, long bucketOrd, long docCount, long owningBucketOrd) throws IOException {
            // Recreate DocValues as needed for concurrent segment search
            SortedSetDocValues values = getDocValues();
            BytesRef term = BytesRef.deepCopyOf(values.lookupOrd(globalOrd));

            StringTerms.Bucket result = new StringTerms.Bucket(term, docCount, null, showTermDocCountError, 0, format);
            result.bucketOrd = bucketOrd;
            result.docCountError = 0;
            return result;
        }

    }

    class SignificantTermsResults extends ResultStrategy<
        SignificantStringTerms,
        SignificantStringTerms.Bucket,
        SignificantStringTerms.Bucket> {
        // Delegate to the parent's SignificantTermsResults for most functionality
        private final GlobalOrdinalsStringTermsAggregator.SignificantTermsResults delegate;

        SignificantTermsResults(
            SignificanceLookup significanceLookup,
            SignificanceHeuristic significanceHeuristic,
            CardinalityUpperBound cardinality
        ) {
            this.delegate = ((GlobalOrdinalsStringTermsAggregator) StreamingStringTermsAggregator.this).new SignificantTermsResults(
                significanceLookup, significanceHeuristic, cardinality
            );
        }

        @Override
        String describe() {
            return "streaming_significant_terms";
        }

        @Override
        LeafBucketCollector wrapCollector(LeafBucketCollector primary) {
            return delegate.wrapCollector(primary);
        }

        @Override
        SignificantStringTerms.Bucket[][] buildTopBucketsPerOrd(int size) {
            return delegate.buildTopBucketsPerOrd(size);
        }

        @Override
        SignificantStringTerms.Bucket[] buildBuckets(int size) {
            return delegate.buildBuckets(size);
        }

        @Override
        SignificantStringTerms.Bucket buildEmptyTemporaryBucket() {
            return delegate.buildEmptyTemporaryBucket();
        }

        @Override
        BucketUpdater<SignificantStringTerms.Bucket> bucketUpdater(long owningBucketOrd) throws IOException {
            return delegate.bucketUpdater(owningBucketOrd);
        }

        @Override
        PriorityQueue<SignificantStringTerms.Bucket> buildPriorityQueue(int size) {
            return delegate.buildPriorityQueue(size);
        }

        @Override
        SignificantStringTerms.Bucket convertTempBucketToRealBucket(SignificantStringTerms.Bucket temp) throws IOException {
            return delegate.convertTempBucketToRealBucket(temp);
        }

        @Override
        void buildSubAggs(SignificantStringTerms.Bucket[][] topBucketsPerOrd) throws IOException {
            delegate.buildSubAggs(topBucketsPerOrd);
        }

        @Override
        SignificantStringTerms buildResult(long owningBucketOrd, long otherDocCount, SignificantStringTerms.Bucket[] topBuckets) {
            return delegate.buildResult(owningBucketOrd, otherDocCount, topBuckets);
        }

        @Override
        SignificantStringTerms buildEmptyResult() {
            return delegate.buildEmptyResult();
        }

        @Override
        SignificantStringTerms buildNoValuesResult(long owningBucketOrdinal) {
            return delegate.buildNoValuesResult(owningBucketOrdinal);
        }

        @Override
        public void close() {
            delegate.close();
        }

        @Override
        SignificantStringTerms.Bucket buildFinalBucket(long globalOrd, long bucketOrd, long docCount, long owningBucketOrd)
            throws IOException {
            long subsetSize = delegate.subsetSize(owningBucketOrd);
            SortedSetDocValues values = getDocValues();
            BytesRef term = BytesRef.deepCopyOf(values.lookupOrd(globalOrd));

            SignificantStringTerms.Bucket bucket = new SignificantStringTerms.Bucket(term, 0, 0, 0, 0, null, format, 0);
            bucket.bucketOrd = bucketOrd;
            bucket.subsetDf = docCount;
            bucket.subsetSize = subsetSize;
            bucket.supersetDf = delegate.backgroundFrequencies.freq(term);
            bucket.supersetSize = delegate.supersetSize;
            /*
             * During shard-local down-selection we use subset/superset stats
             * that are for this shard only. Back at the central reducer these
             * properties will be updated with global stats.
             */
            bucket.updateScore(delegate.significanceHeuristic);
            return bucket;
        }

    }

    @Override
    SortedSetDocValues getDocValues() {
        return sortedDocValuesPerBatch;
    }
}
