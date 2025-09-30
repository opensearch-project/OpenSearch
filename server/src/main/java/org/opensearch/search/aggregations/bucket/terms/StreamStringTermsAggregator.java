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
import org.opensearch.common.lease.Releasable;
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
    private SortedSetDocValues sortedDocValuesPerBatch;
    private long valueCount;
    private final ValuesSource.Bytes.WithOrdinals valuesSource;
    protected int segmentsWithSingleValuedOrds = 0;
    protected int segmentsWithMultiValuedOrds = 0;
    protected final ResultStrategy<?, ?> resultStrategy;

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
        Map<String, Object> metadata
    ) throws IOException {
        super(name, factories, context, parent, order, format, bucketCountThresholds, collectionMode, showTermDocCountError, metadata);
        this.valuesSource = valuesSource;
        this.resultStrategy = resultStrategy.apply(this);
    }

    @Override
    public void doReset() {
        super.doReset();
        valueCount = 0;
        sortedDocValuesPerBatch = null;
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
        this.sortedDocValuesPerBatch = valuesSource.ordinalsValues(ctx);
        this.valueCount = sortedDocValuesPerBatch.getValueCount(); // for streaming case, the value count is reset to per batch
        // cardinality
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

        // build aggregation batch for stream search
        InternalAggregation[] buildAggregationsBatch(long[] owningBucketOrds) throws IOException {
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
                List<B> bucketsPerOwningOrd = new ArrayList<>();
                for (long ordinal = 0; ordinal < valueCount; ordinal++) {
                    long docCount = bucketDocCount(ordinal);
                    if (bucketCountThresholds.getMinDocCount() == 0 || docCount > 0) {
                        if (docCount >= localBucketCountThresholds.getMinDocCount()) {
                            B finalBucket = buildFinalBucket(ordinal, docCount);
                            bucketsPerOwningOrd.add(finalBucket);
                        }
                    }
                }

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
         * Short description of the collection mechanism added to the profile
         * output to help with debugging.
         */
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

        @Override
        public void close() {}
    }

    @Override
    public void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("result_strategy", resultStrategy.describe());
        add.accept("segments_with_single_valued_ords", segmentsWithSingleValuedOrds);
        add.accept("segments_with_multi_valued_ords", segmentsWithMultiValuedOrds);
    }
}
