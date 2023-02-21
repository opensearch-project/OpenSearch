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

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.PriorityQueue;
import org.opensearch.core.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationExecutionException;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.opensearch.search.aggregations.InternalOrder;
import org.opensearch.search.aggregations.KeyComparable;
import org.opensearch.search.aggregations.bucket.IteratorAndCurrent;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static org.opensearch.search.aggregations.InternalOrder.isKeyAsc;
import static org.opensearch.search.aggregations.InternalOrder.isKeyOrder;

/**
 * Implementation of terms
 *
 * @opensearch.internal
 */
public abstract class InternalTerms<A extends InternalTerms<A, B>, B extends InternalTerms.AbstractInternalBucket> extends
    InternalMultiBucketAggregation<A, B>
    implements
        Terms {

    protected static final ParseField DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME = new ParseField("doc_count_error_upper_bound");
    protected static final ParseField SUM_OF_OTHER_DOC_COUNTS = new ParseField("sum_other_doc_count");

    /**
     * Base internal multi bucket
     *
     * @opensearch.internal
     */
    public abstract static class AbstractInternalBucket extends InternalMultiBucketAggregation.InternalBucket implements Terms.Bucket {
        abstract void setDocCountError(long docCountError);

        abstract void setDocCountError(Function<Long, Long> updater);

        abstract boolean showDocCountError();
    }

    /**
     * Base bucket class
     *
     * @opensearch.internal
     */
    public abstract static class Bucket<B extends Bucket<B>> extends AbstractInternalBucket implements KeyComparable<B> {
        /**
         * Reads a bucket. Should be a constructor reference.
         *
         * @opensearch.internal
         */
        @FunctionalInterface
        public interface Reader<B extends Bucket<B>> {
            B read(StreamInput in, DocValueFormat format, boolean showDocCountError) throws IOException;
        }

        long bucketOrd;

        protected long docCount;
        protected long docCountError;
        protected InternalAggregations aggregations;
        protected final boolean showDocCountError;
        protected final DocValueFormat format;

        protected Bucket(
            long docCount,
            InternalAggregations aggregations,
            boolean showDocCountError,
            long docCountError,
            DocValueFormat formatter
        ) {
            this.showDocCountError = showDocCountError;
            this.format = formatter;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.docCountError = docCountError;
        }

        /**
         * Read from a stream.
         */
        protected Bucket(StreamInput in, DocValueFormat formatter, boolean showDocCountError) throws IOException {
            this.showDocCountError = showDocCountError;
            this.format = formatter;
            docCount = in.readVLong();
            docCountError = -1;
            if (showDocCountError) {
                docCountError = in.readLong();
            }
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public final void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(getDocCount());
            if (showDocCountError) {
                out.writeLong(docCountError);
            }
            aggregations.writeTo(out);
            writeTermTo(out);
        }

        protected abstract void writeTermTo(StreamOutput out) throws IOException;

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public long getDocCountError() {
            if (!showDocCountError) {
                throw new IllegalStateException("show_terms_doc_count_error is false");
            }
            return docCountError;
        }

        @Override
        public void setDocCountError(long docCountError) {
            this.docCountError = docCountError;
        }

        @Override
        public void setDocCountError(Function<Long, Long> updater) {
            this.docCountError = updater.apply(this.docCountError);
        }

        @Override
        public boolean showDocCountError() {
            return showDocCountError;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            keyToXContent(builder);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            if (showDocCountError) {
                builder.field(InternalTerms.DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME.getPreferredName(), getDocCountError());
            }
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        protected abstract XContentBuilder keyToXContent(XContentBuilder builder) throws IOException;

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Bucket<?> that = (Bucket<?>) obj;
            // No need to take format and showDocCountError, they are attributes
            // of the parent terms aggregation object that are only copied here
            // for serialization purposes
            return Objects.equals(docCount, that.docCount)
                && Objects.equals(docCountError, that.docCountError)
                && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), docCount, docCountError, aggregations);
        }
    }

    protected final BucketOrder reduceOrder;
    protected final BucketOrder order;
    protected final int requiredSize;
    protected final long minDocCount;

    /**
     * Creates a new {@link InternalTerms}
     * @param name The name of the aggregation
     * @param reduceOrder The {@link BucketOrder} that should be used to merge shard results.
     * @param order The {@link BucketOrder} that should be used to sort the final reduce.
     * @param requiredSize The number of top buckets.
     * @param minDocCount The minimum number of documents allowed per bucket.
     * @param metadata The metadata associated with the aggregation.
     */
    protected InternalTerms(
        String name,
        BucketOrder reduceOrder,
        BucketOrder order,
        int requiredSize,
        long minDocCount,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.reduceOrder = reduceOrder;
        this.order = order;
        this.requiredSize = requiredSize;
        this.minDocCount = minDocCount;
    }

    /**
     * Read from a stream.
     */
    protected InternalTerms(StreamInput in) throws IOException {
        super(in);
        reduceOrder = InternalOrder.Streams.readOrder(in);
        order = InternalOrder.Streams.readOrder(in);
        requiredSize = readSize(in);
        minDocCount = in.readVLong();
    }

    @Override
    protected final void doWriteTo(StreamOutput out) throws IOException {
        reduceOrder.writeTo(out);
        order.writeTo(out);
        writeSize(requiredSize, out);
        out.writeVLong(minDocCount);
        writeTermTypeInfoTo(out);
    }

    protected abstract void writeTermTypeInfoTo(StreamOutput out) throws IOException;

    @Override
    public abstract List<B> getBuckets();

    @Override
    public abstract B getBucketByKey(String term);

    private BucketOrder getReduceOrder(List<InternalAggregation> aggregations) {
        BucketOrder thisReduceOrder = null;
        for (InternalAggregation aggregation : aggregations) {
            @SuppressWarnings("unchecked")
            InternalTerms<A, B> terms = (InternalTerms<A, B>) aggregation;
            if (terms.getBuckets().size() == 0) {
                continue;
            }
            if (thisReduceOrder == null) {
                thisReduceOrder = terms.reduceOrder;
            } else if (thisReduceOrder.equals(terms.reduceOrder) == false) {
                return order;
            }
        }
        return thisReduceOrder != null ? thisReduceOrder : order;
    }

    private long getDocCountError(InternalTerms<?, ?> terms) {
        int size = terms.getBuckets().size();
        if (size == 0 || size < terms.getShardSize() || isKeyOrder(terms.order)) {
            return 0;
        } else if (InternalOrder.isCountDesc(terms.order)) {
            if (terms.getDocCountError() > 0) {
                // If there is an existing docCountError for this agg then
                // use this as the error for this aggregation
                return terms.getDocCountError();
            } else {
                // otherwise use the doc count of the last term in the
                // aggregation
                return terms.getBuckets().stream().mapToLong(MultiBucketsAggregation.Bucket::getDocCount).min().getAsLong();
            }
        } else {
            return -1;
        }
    }

    private List<B> reduceMergeSort(List<InternalAggregation> aggregations, BucketOrder thisReduceOrder, ReduceContext reduceContext) {
        assert isKeyOrder(thisReduceOrder);
        final Comparator<MultiBucketsAggregation.Bucket> cmp = thisReduceOrder.comparator();
        final PriorityQueue<IteratorAndCurrent<B>> pq = new PriorityQueue<IteratorAndCurrent<B>>(aggregations.size()) {
            @Override
            protected boolean lessThan(IteratorAndCurrent<B> a, IteratorAndCurrent<B> b) {
                return cmp.compare(a.current(), b.current()) < 0;
            }
        };
        for (InternalAggregation aggregation : aggregations) {
            @SuppressWarnings("unchecked")
            InternalTerms<A, B> terms = (InternalTerms<A, B>) aggregation;
            if (terms.getBuckets().isEmpty() == false) {
                assert reduceOrder.equals(reduceOrder);
                pq.add(new IteratorAndCurrent(terms.getBuckets().iterator()));
            }
        }
        List<B> reducedBuckets = new ArrayList<>();
        // list of buckets coming from different shards that have the same key
        List<B> currentBuckets = new ArrayList<>();
        B lastBucket = null;
        while (pq.size() > 0) {
            final IteratorAndCurrent<B> top = pq.top();
            assert lastBucket == null || cmp.compare(top.current(), lastBucket) >= 0;
            if (lastBucket != null && cmp.compare(top.current(), lastBucket) != 0) {
                // the key changes, reduce what we already buffered and reset the buffer for current buckets
                final B reduced = reduceBucket(currentBuckets, reduceContext);
                reducedBuckets.add(reduced);
                currentBuckets.clear();
            }
            lastBucket = top.current();
            currentBuckets.add(top.current());
            if (top.hasNext()) {
                top.next();
                assert cmp.compare(top.current(), lastBucket) > 0 : "shards must return data sorted by key";
                pq.updateTop();
            } else {
                pq.pop();
            }
        }

        if (currentBuckets.isEmpty() == false) {
            final B reduced = reduceBucket(currentBuckets, reduceContext);
            reducedBuckets.add(reduced);
        }
        return reducedBuckets;
    }

    private List<B> reduceLegacy(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        Map<Object, List<B>> bucketMap = new HashMap<>();
        for (InternalAggregation aggregation : aggregations) {
            @SuppressWarnings("unchecked")
            InternalTerms<A, B> terms = (InternalTerms<A, B>) aggregation;
            if (terms.getBuckets().isEmpty() == false) {
                for (B bucket : terms.getBuckets()) {
                    List<B> bucketList = bucketMap.get(bucket.getKey());
                    if (bucketList == null) {
                        bucketList = new ArrayList<>();
                        bucketMap.put(bucket.getKey(), bucketList);
                    }
                    bucketList.add(bucket);
                }
            }
        }
        List<B> reducedBuckets = new ArrayList<>();
        for (List<B> sameTermBuckets : bucketMap.values()) {
            final B b = reduceBucket(sameTermBuckets, reduceContext);
            reducedBuckets.add(b);
        }
        return reducedBuckets;
    }

    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        long sumDocCountError = 0;
        long otherDocCount = 0;
        InternalTerms<A, B> referenceTerms = null;
        for (InternalAggregation aggregation : aggregations) {
            @SuppressWarnings("unchecked")
            InternalTerms<A, B> terms = (InternalTerms<A, B>) aggregation;
            if (referenceTerms == null && aggregation.getClass().equals(UnmappedTerms.class) == false) {
                referenceTerms = terms;
            }
            if (referenceTerms != null
                && referenceTerms.getClass().equals(terms.getClass()) == false
                && terms.getClass().equals(UnmappedTerms.class) == false) {
                // control gets into this loop when the same field name against which the query is executed
                // is of different types in different indices.
                throw new AggregationExecutionException(
                    "Merging/Reducing the aggregations failed when computing the aggregation ["
                        + referenceTerms.getName()
                        + "] because the field you gave in the aggregation query existed as two different "
                        + "types in two different indices"
                );
            }
            otherDocCount += terms.getSumOfOtherDocCounts();
            final long thisAggDocCountError = getDocCountError(terms);
            if (sumDocCountError != -1) {
                if (thisAggDocCountError == -1) {
                    sumDocCountError = -1;
                } else {
                    sumDocCountError += thisAggDocCountError;
                }
            }
            setDocCountError(thisAggDocCountError);
            for (B bucket : terms.getBuckets()) {
                // If there is already a doc count error for this bucket
                // subtract this aggs doc count error from it to make the
                // new value for the bucket. This then means that when the
                // final error for the bucket is calculated below we account
                // for the existing error calculated in a previous reduce.
                // Note that if the error is unbounded (-1) this will be fixed
                // later in this method.
                bucket.setDocCountError(docCountError -> docCountError - thisAggDocCountError);
            }
        }

        final List<B> reducedBuckets;
        /**
         * Buckets returned by a partial reduce or a shard response are sorted by key.
         * That allows to perform a merge sort when reducing multiple aggregations together.
         * For backward compatibility, we disable the merge sort and use ({@link InternalTerms#reduceLegacy} if any of
         * the provided aggregations use a different {@link InternalTerms#reduceOrder}.
         */
        BucketOrder thisReduceOrder = getReduceOrder(aggregations);
        if (isKeyOrder(thisReduceOrder)) {
            // extract the primary sort in case this is a compound order.
            thisReduceOrder = InternalOrder.key(isKeyAsc(thisReduceOrder) ? true : false);
            reducedBuckets = reduceMergeSort(aggregations, thisReduceOrder, reduceContext);
        } else {
            reducedBuckets = reduceLegacy(aggregations, reduceContext);
        }
        final B[] list;
        if (reduceContext.isFinalReduce()) {
            final int size = Math.min(requiredSize, reducedBuckets.size());
            // final comparator
            final BucketPriorityQueue<B> ordered = new BucketPriorityQueue<>(size, order.comparator());
            for (B bucket : reducedBuckets) {
                if (sumDocCountError == -1) {
                    bucket.setDocCountError(-1);
                } else {
                    final long finalSumDocCountError = sumDocCountError;
                    bucket.setDocCountError(docCountError -> docCountError + finalSumDocCountError);
                }
                if (bucket.getDocCount() >= minDocCount) {
                    B removed = ordered.insertWithOverflow(bucket);
                    if (removed != null) {
                        otherDocCount += removed.getDocCount();
                        reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(removed));
                    } else {
                        reduceContext.consumeBucketsAndMaybeBreak(1);
                    }
                } else {
                    reduceContext.consumeBucketsAndMaybeBreak(-countInnerBucket(bucket));
                }
            }
            list = createBucketsArray(ordered.size());
            for (int i = ordered.size() - 1; i >= 0; i--) {
                list[i] = ordered.pop();
            }
        } else {
            // we can prune the list on partial reduce if the aggregation is ordered by key
            // and not filtered (minDocCount == 0)
            int size = isKeyOrder(order) && minDocCount == 0 ? Math.min(requiredSize, reducedBuckets.size()) : reducedBuckets.size();
            list = createBucketsArray(size);
            for (int i = 0; i < size; i++) {
                reduceContext.consumeBucketsAndMaybeBreak(1);
                list[i] = reducedBuckets.get(i);
                if (sumDocCountError == -1) {
                    list[i].setDocCountError(-1);
                } else {
                    final long fSumDocCountError = sumDocCountError;
                    list[i].setDocCountError(docCountError -> docCountError + fSumDocCountError);
                }
            }
        }
        long docCountError;
        if (sumDocCountError == -1) {
            docCountError = -1;
        } else {
            docCountError = aggregations.size() == 1 ? 0 : sumDocCountError;
        }
        return create(name, Arrays.asList(list), reduceContext.isFinalReduce() ? order : thisReduceOrder, docCountError, otherDocCount);
    }

    @Override
    protected B reduceBucket(List<B> buckets, ReduceContext context) {
        assert buckets.size() > 0;
        long docCount = 0;
        // For the per term doc count error we add up the errors from the
        // shards that did not respond with the term. To do this we add up
        // the errors from the shards that did respond with the terms and
        // subtract that from the sum of the error from all shards
        long docCountError = 0;
        List<InternalAggregations> aggregationsList = new ArrayList<>(buckets.size());
        for (B bucket : buckets) {
            docCount += bucket.getDocCount();
            if (docCountError != -1) {
                if (bucket.showDocCountError() == false || bucket.getDocCountError() == -1) {
                    docCountError = -1;
                } else {
                    docCountError += bucket.getDocCountError();
                }
            }
            aggregationsList.add((InternalAggregations) bucket.getAggregations());
        }
        InternalAggregations aggs = InternalAggregations.reduce(aggregationsList, context);
        return createBucket(docCount, aggs, docCountError, buckets.get(0));
    }

    protected abstract void setDocCountError(long docCountError);

    protected abstract int getShardSize();

    protected abstract A create(String name, List<B> buckets, BucketOrder reduceOrder, long docCountError, long otherDocCount);

    /**
     * Create an array to hold some buckets. Used in collecting the results.
     */
    protected abstract B[] createBucketsArray(int size);

    abstract B createBucket(long docCount, InternalAggregations aggs, long docCountError, B prototype);

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalTerms<?, ?> that = (InternalTerms<?, ?>) obj;
        return Objects.equals(minDocCount, that.minDocCount)
            && Objects.equals(reduceOrder, that.reduceOrder)
            && Objects.equals(order, that.order)
            && Objects.equals(requiredSize, that.requiredSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), minDocCount, reduceOrder, order, requiredSize);
    }

    protected static XContentBuilder doXContentCommon(
        XContentBuilder builder,
        Params params,
        long docCountError,
        long otherDocCount,
        List<? extends AbstractInternalBucket> buckets
    ) throws IOException {
        builder.field(DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME.getPreferredName(), docCountError);
        builder.field(SUM_OF_OTHER_DOC_COUNTS.getPreferredName(), otherDocCount);
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (AbstractInternalBucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }
}
