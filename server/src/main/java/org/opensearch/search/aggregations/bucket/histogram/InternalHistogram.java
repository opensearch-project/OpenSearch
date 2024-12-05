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

import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.PriorityQueue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.DocValueFormat;
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
import java.util.Collections;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;

/**
 * Implementation of {@link Histogram}.
 *
 * @opensearch.internal
 */
public final class InternalHistogram extends InternalMultiBucketAggregation<InternalHistogram, InternalHistogram.Bucket>
    implements
        Histogram,
        HistogramFactory {
    /**
     * Bucket for an internal histogram agg
     *
     * @opensearch.internal
     */
    public static class Bucket extends InternalMultiBucketAggregation.InternalBucket implements Histogram.Bucket, KeyComparable<Bucket> {

        final double key;
        final long docCount;
        final InternalAggregations aggregations;
        private final transient boolean keyed;
        protected final transient DocValueFormat format;

        public Bucket(double key, long docCount, boolean keyed, DocValueFormat format, InternalAggregations aggregations) {
            this.format = format;
            this.keyed = keyed;
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
        }

        /**
         * Read from a stream.
         */
        public Bucket(StreamInput in, boolean keyed, DocValueFormat format) throws IOException {
            this.format = format;
            this.keyed = keyed;
            key = in.readDouble();
            docCount = in.readVLong();
            aggregations = InternalAggregations.readFrom(in);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || obj.getClass() != Bucket.class) {
                return false;
            }
            Bucket that = (Bucket) obj;
            // No need to take the keyed and format parameters into account,
            // they are already stored and tested on the InternalHistogram object
            return key == that.key && docCount == that.docCount && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), key, docCount, aggregations);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(key);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
        }

        @Override
        public String getKeyAsString() {
            return format.format(key).toString();
        }

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            String keyAsString = format.format(key).toString();
            if (keyed) {
                builder.startObject(keyAsString);
            } else {
                builder.startObject();
            }
            if (format != DocValueFormat.RAW) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), keyAsString);
            }
            builder.field(CommonFields.KEY.getPreferredName(), key);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public int compareKey(Bucket other) {
            return Double.compare(key, other.key);
        }

        public DocValueFormat getFormatter() {
            return format;
        }

        public boolean getKeyed() {
            return keyed;
        }
    }

    /**
     * Information about an empty bucket
     *
     * @opensearch.internal
     */
    public static class EmptyBucketInfo {

        final double interval, offset, minBound, maxBound;
        final InternalAggregations subAggregations;

        public EmptyBucketInfo(double interval, double offset, double minBound, double maxBound, InternalAggregations subAggregations) {
            this.interval = interval;
            this.offset = offset;
            this.minBound = minBound;
            this.maxBound = maxBound;
            this.subAggregations = subAggregations;
        }

        EmptyBucketInfo(StreamInput in) throws IOException {
            this(in.readDouble(), in.readDouble(), in.readDouble(), in.readDouble(), InternalAggregations.readFrom(in));
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(interval);
            out.writeDouble(offset);
            out.writeDouble(minBound);
            out.writeDouble(maxBound);
            subAggregations.writeTo(out);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            EmptyBucketInfo that = (EmptyBucketInfo) obj;
            return interval == that.interval
                && offset == that.offset
                && minBound == that.minBound
                && maxBound == that.maxBound
                && Objects.equals(subAggregations, that.subAggregations);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), interval, offset, minBound, maxBound, subAggregations);
        }
    }

    private final List<Bucket> buckets;
    private final BucketOrder order;
    private final DocValueFormat format;
    private final boolean keyed;
    private final long minDocCount;
    final EmptyBucketInfo emptyBucketInfo;

    public InternalHistogram(
        String name,
        List<Bucket> buckets,
        BucketOrder order,
        long minDocCount,
        EmptyBucketInfo emptyBucketInfo,
        DocValueFormat formatter,
        boolean keyed,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.buckets = buckets;
        this.order = order;
        assert (minDocCount == 0) == (emptyBucketInfo != null);
        this.minDocCount = minDocCount;
        this.emptyBucketInfo = emptyBucketInfo;
        this.format = formatter;
        this.keyed = keyed;
    }

    /**
     * Stream from a stream.
     */
    public InternalHistogram(StreamInput in) throws IOException {
        super(in);
        order = InternalOrder.Streams.readHistogramOrder(in, false);
        minDocCount = in.readVLong();
        if (minDocCount == 0) {
            emptyBucketInfo = new EmptyBucketInfo(in);
        } else {
            emptyBucketInfo = null;
        }
        format = in.readNamedWriteable(DocValueFormat.class);
        keyed = in.readBoolean();
        buckets = in.readList(stream -> new Bucket(stream, keyed, format));
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        InternalOrder.Streams.writeHistogramOrder(order, out, false);
        out.writeVLong(minDocCount);
        if (minDocCount == 0) {
            emptyBucketInfo.writeTo(out);
        }
        out.writeNamedWriteable(format);
        out.writeBoolean(keyed);
        out.writeList(buckets);
    }

    @Override
    public String getWriteableName() {
        return HistogramAggregationBuilder.NAME;
    }

    @Override
    public List<InternalHistogram.Bucket> getBuckets() {
        return Collections.unmodifiableList(buckets);
    }

    long getMinDocCount() {
        return minDocCount;
    }

    BucketOrder getOrder() {
        return order;
    }

    @Override
    public InternalHistogram create(List<Bucket> buckets) {
        return new InternalHistogram(name, buckets, order, minDocCount, emptyBucketInfo, format, keyed, metadata);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.key, prototype.docCount, prototype.keyed, prototype.format, aggregations);
    }

    private List<Bucket> reduceBuckets(List<InternalAggregation> aggregations, ReduceContext reduceContext) {

        final PriorityQueue<IteratorAndCurrent<Bucket>> pq = new PriorityQueue<IteratorAndCurrent<Bucket>>(aggregations.size()) {
            @Override
            protected boolean lessThan(IteratorAndCurrent<Bucket> a, IteratorAndCurrent<Bucket> b) {
                return Double.compare(a.current().key, b.current().key) < 0;
            }
        };
        for (InternalAggregation aggregation : aggregations) {
            InternalHistogram histogram = (InternalHistogram) aggregation;
            if (histogram.buckets.isEmpty() == false) {
                pq.add(new IteratorAndCurrent(histogram.buckets.iterator()));
            }
        }

        List<Bucket> reducedBuckets = new ArrayList<>();
        if (pq.size() > 0) {
            // list of buckets coming from different shards that have the same key
            List<Bucket> currentBuckets = new ArrayList<>();
            double key = pq.top().current().key;

            do {
                final IteratorAndCurrent<Bucket> top = pq.top();

                if (Double.compare(top.current().key, key) != 0) {
                    // The key changes, reduce what we already buffered and reset the buffer for current buckets.
                    // Using Double.compare instead of != to handle NaN correctly.
                    final Bucket reduced = reduceBucket(currentBuckets, reduceContext);
                    if (reduced.getDocCount() >= minDocCount || reduceContext.isFinalReduce() == false) {
                        reducedBuckets.add(reduced);
                    }
                    currentBuckets.clear();
                    key = top.current().key;
                }

                currentBuckets.add(top.current());

                if (top.hasNext()) {
                    top.next();
                    assert Double.compare(top.current().key, key) > 0 : "shards must return data sorted by key";
                    pq.updateTop();
                } else {
                    pq.pop();
                }
            } while (pq.size() > 0);

            if (currentBuckets.isEmpty() == false) {
                final Bucket reduced = reduceBucket(currentBuckets, reduceContext);
                if (reduced.getDocCount() >= minDocCount || reduceContext.isFinalReduce() == false) {
                    reducedBuckets.add(reduced);
                }
            }
        }

        return reducedBuckets;
    }

    @Override
    protected Bucket reduceBucket(List<Bucket> buckets, ReduceContext context) {
        assert buckets.size() > 0;
        List<InternalAggregations> aggregations = new ArrayList<>(buckets.size());
        long docCount = 0;
        for (Bucket bucket : buckets) {
            docCount += bucket.docCount;
            aggregations.add((InternalAggregations) bucket.getAggregations());
        }
        InternalAggregations aggs = InternalAggregations.reduce(aggregations, context);
        return createBucket(buckets.get(0).key, docCount, aggs);
    }

    private double nextKey(double key) {
        return round(key + emptyBucketInfo.interval + emptyBucketInfo.interval / 2);
    }

    private double round(double key) {
        return Math.floor((key - emptyBucketInfo.offset) / emptyBucketInfo.interval) * emptyBucketInfo.interval + emptyBucketInfo.offset;
    }

    private void addEmptyBuckets(List<Bucket> list, ReduceContext reduceContext) {
        ListIterator<Bucket> iter = list.listIterator();

        // first adding all the empty buckets *before* the actual data (based on th extended_bounds.min the user requested)
        InternalAggregations reducedEmptySubAggs = InternalAggregations.reduce(
            Collections.singletonList(emptyBucketInfo.subAggregations),
            reduceContext
        );

        if (iter.hasNext() == false) {
            // fill with empty buckets
            for (double key = round(emptyBucketInfo.minBound); key <= emptyBucketInfo.maxBound; key = nextKey(key)) {
                iter.add(new Bucket(key, 0, keyed, format, reducedEmptySubAggs));
                reduceContext.consumeBucketsAndMaybeBreak(0);
            }
        } else {
            Bucket first = list.get(iter.nextIndex());
            if (Double.isFinite(emptyBucketInfo.minBound)) {
                // fill with empty buckets until the first key
                for (double key = round(emptyBucketInfo.minBound); key < first.key; key = nextKey(key)) {
                    iter.add(new Bucket(key, 0, keyed, format, reducedEmptySubAggs));
                    reduceContext.consumeBucketsAndMaybeBreak(0);
                }
            }

            // now adding the empty buckets within the actual data,
            // e.g. if the data series is [1,2,3,7] there are 3 empty buckets that will be created for 4,5,6
            Bucket lastBucket = null;
            do {
                Bucket nextBucket = list.get(iter.nextIndex());
                if (lastBucket != null) {
                    double key = nextKey(lastBucket.key);
                    while (key < nextBucket.key) {
                        iter.add(new Bucket(key, 0, keyed, format, reducedEmptySubAggs));
                        reduceContext.consumeBucketsAndMaybeBreak(0);
                        key = nextKey(key);
                    }
                    assert key == nextBucket.key || Double.isNaN(nextBucket.key) : "key: " + key + ", nextBucket.key: " + nextBucket.key;
                }
                lastBucket = iter.next();
            } while (iter.hasNext());

            // finally, adding the empty buckets *after* the actual data (based on the extended_bounds.max requested by the user)
            for (double key = nextKey(lastBucket.key); key <= emptyBucketInfo.maxBound; key = nextKey(key)) {
                iter.add(new Bucket(key, 0, keyed, format, reducedEmptySubAggs));
                reduceContext.consumeBucketsAndMaybeBreak(0);
            }
        }
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        List<Bucket> reducedBuckets = reduceBuckets(aggregations, reduceContext);
        if (reduceContext.isFinalReduce()) {
            if (minDocCount == 0) {
                addEmptyBuckets(reducedBuckets, reduceContext);
            }
            if (InternalOrder.isKeyDesc(order)) {
                // we just need to reverse here...
                List<Bucket> reverse = new ArrayList<>(reducedBuckets);
                Collections.reverse(reverse);
                reducedBuckets = reverse;
            } else if (InternalOrder.isKeyAsc(order) == false) {
                // nothing to do when sorting by key ascending, as data is already sorted since shards return
                // sorted buckets and the merge-sort performed by reduceBuckets maintains order.
                // otherwise, sorted by compound order or sub-aggregation, we need to fall back to a costly n*log(n) sort
                CollectionUtil.introSort(reducedBuckets, order.comparator());
            }
        }
        reduceContext.consumeBucketsAndMaybeBreak(reducedBuckets.size());
        return new InternalHistogram(getName(), reducedBuckets, order, minDocCount, emptyBucketInfo, format, keyed, getMetadata());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (keyed) {
            builder.startObject(CommonFields.BUCKETS.getPreferredName());
        } else {
            builder.startArray(CommonFields.BUCKETS.getPreferredName());
        }
        for (Bucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        if (keyed) {
            builder.endObject();
        } else {
            builder.endArray();
        }
        return builder;
    }

    // HistogramFactory method impls

    @Override
    public Number getKey(MultiBucketsAggregation.Bucket bucket) {
        return ((Bucket) bucket).key;
    }

    @Override
    public Number nextKey(Number key) {
        return nextKey(key.doubleValue());
    }

    @Override
    public InternalAggregation createAggregation(List<MultiBucketsAggregation.Bucket> buckets) {
        // convert buckets to the right type
        List<Bucket> buckets2 = new ArrayList<>(buckets.size());
        for (Object b : buckets) {
            buckets2.add((Bucket) b);
        }
        buckets2 = Collections.unmodifiableList(buckets2);
        return new InternalHistogram(name, buckets2, order, minDocCount, emptyBucketInfo, format, keyed, getMetadata());
    }

    @Override
    public Bucket createBucket(Number key, long docCount, InternalAggregations aggregations) {
        return new Bucket(key.doubleValue(), docCount, keyed, format, aggregations);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalHistogram that = (InternalHistogram) obj;
        return Objects.equals(buckets, that.buckets)
            && Objects.equals(emptyBucketInfo, that.emptyBucketInfo)
            && Objects.equals(format, that.format)
            && Objects.equals(keyed, that.keyed)
            && Objects.equals(minDocCount, that.minDocCount)
            && Objects.equals(order, that.order);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), buckets, emptyBucketInfo, format, keyed, minDocCount, order);
    }
}
