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

package org.opensearch.search.aggregations.bucket.composite;

import org.apache.lucene.util.BytesRef;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.opensearch.search.aggregations.KeyComparable;
import org.opensearch.search.aggregations.bucket.missing.MissingOrder;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Internal coordination class for composite aggs
 *
 * @opensearch.internal
 */
public class InternalComposite extends InternalMultiBucketAggregation<InternalComposite, InternalComposite.InternalBucket>
    implements
        CompositeAggregation {

    private final int size;
    private final List<InternalBucket> buckets;
    private final CompositeKey afterKey;
    private final int[] reverseMuls;
    private final MissingOrder[] missingOrders;
    private final List<String> sourceNames;
    private final List<DocValueFormat> formats;

    private final boolean earlyTerminated;

    InternalComposite(
        String name,
        int size,
        List<String> sourceNames,
        List<DocValueFormat> formats,
        List<InternalBucket> buckets,
        CompositeKey afterKey,
        int[] reverseMuls,
        MissingOrder[] missingOrders,
        boolean earlyTerminated,
        Map<String, Object> metadata
    ) {
        super(name, metadata);
        this.sourceNames = sourceNames;
        this.formats = formats;
        this.buckets = buckets;
        this.afterKey = afterKey;
        this.size = size;
        this.reverseMuls = reverseMuls;
        this.missingOrders = missingOrders;
        this.earlyTerminated = earlyTerminated;
    }

    public InternalComposite(StreamInput in) throws IOException {
        super(in);
        this.size = in.readVInt();
        this.sourceNames = in.readStringList();
        this.formats = new ArrayList<>(sourceNames.size());
        for (int i = 0; i < sourceNames.size(); i++) {
            formats.add(in.readNamedWriteable(DocValueFormat.class));
        }
        this.reverseMuls = in.readIntArray();
        this.missingOrders = in.readArray(MissingOrder::readFromStream, MissingOrder[]::new);
        this.buckets = in.readList((input) -> new InternalBucket(input, sourceNames, formats, reverseMuls, missingOrders));
        this.afterKey = in.readBoolean() ? new CompositeKey(in) : null;
        this.earlyTerminated = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(size);
        out.writeStringCollection(sourceNames);
        for (DocValueFormat format : formats) {
            out.writeNamedWriteable(format);
        }
        out.writeIntArray(reverseMuls);
        out.writeArray((output, order) -> order.writeTo(output), missingOrders);
        out.writeList(buckets);
        out.writeBoolean(afterKey != null);
        if (afterKey != null) {
            afterKey.writeTo(out);
        }
        out.writeBoolean(earlyTerminated);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return CompositeAggregation.toXContentFragment(this, builder, params);
    }

    @Override
    public String getWriteableName() {
        return CompositeAggregationBuilder.NAME;
    }

    @Override
    public InternalComposite create(List<InternalBucket> newBuckets) {
        /**
         * This is used by pipeline aggregations to filter/remove buckets so we
         * keep the <code>afterKey</code> of the original aggregation in order
         * to be able to retrieve the next page even if all buckets have been filtered.
         */
        return new InternalComposite(
            name,
            size,
            sourceNames,
            formats,
            newBuckets,
            afterKey,
            reverseMuls,
            missingOrders,
            earlyTerminated,
            getMetadata()
        );
    }

    @Override
    public InternalBucket createBucket(InternalAggregations aggregations, InternalBucket prototype) {
        return new InternalBucket(
            prototype.sourceNames,
            prototype.formats,
            prototype.key,
            prototype.reverseMuls,
            prototype.missingOrders,
            prototype.docCount,
            aggregations
        );
    }

    public int getSize() {
        return size;
    }

    @Override
    public List<InternalBucket> getBuckets() {
        return buckets;
    }

    /**
     * The formats used when writing the keys. Package private for testing.
     */
    List<DocValueFormat> getFormats() {
        return formats;
    }

    @Override
    public Map<String, Object> afterKey() {
        if (afterKey != null) {
            return new ArrayMap(sourceNames, formats, afterKey.values());
        }
        return null;
    }

    // Visible for tests
    boolean isTerminatedEarly() {
        return earlyTerminated;
    }

    // Visible for tests
    int[] getReverseMuls() {
        return reverseMuls;
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        PriorityQueue<BucketIterator> pq = new PriorityQueue<>(aggregations.size());
        boolean earlyTerminated = false;
        for (InternalAggregation agg : aggregations) {
            InternalComposite sortedAgg = (InternalComposite) agg;
            earlyTerminated |= sortedAgg.earlyTerminated;
            BucketIterator it = new BucketIterator(sortedAgg.buckets);
            if (it.next() != null) {
                pq.add(it);
            }
        }
        InternalBucket lastBucket = null;
        List<InternalBucket> buckets = new ArrayList<>();
        List<InternalBucket> result = new ArrayList<>();
        while (pq.size() > 0) {
            BucketIterator bucketIt = pq.poll();
            if (lastBucket != null && bucketIt.current.compareKey(lastBucket) != 0) {
                InternalBucket reduceBucket = reduceBucket(buckets, reduceContext);
                buckets.clear();
                result.add(reduceBucket);
                if (result.size() >= size) {
                    break;
                }
            }
            lastBucket = bucketIt.current;
            buckets.add(bucketIt.current);
            if (bucketIt.next() != null) {
                pq.add(bucketIt);
            }
        }
        if (buckets.size() > 0) {
            InternalBucket reduceBucket = reduceBucket(buckets, reduceContext);
            result.add(reduceBucket);
        }

        List<DocValueFormat> reducedFormats = formats;
        CompositeKey lastKey = null;
        if (result.size() > 0) {
            lastBucket = result.get(result.size() - 1);
            /* Attach the formats from the last bucket to the reduced composite
             * so that we can properly format the after key. */
            reducedFormats = lastBucket.formats;
            lastKey = lastBucket.getRawKey();
        }
        reduceContext.consumeBucketsAndMaybeBreak(result.size());
        return new InternalComposite(
            name,
            size,
            sourceNames,
            reducedFormats,
            result,
            lastKey,
            reverseMuls,
            missingOrders,
            earlyTerminated,
            metadata
        );
    }

    @Override
    protected InternalBucket reduceBucket(List<InternalBucket> buckets, ReduceContext context) {
        assert buckets.size() > 0;
        List<InternalAggregations> aggregations = new ArrayList<>(buckets.size());
        long docCount = 0;
        for (InternalBucket bucket : buckets) {
            docCount += bucket.docCount;
            aggregations.add(bucket.aggregations);
        }
        InternalAggregations aggs = InternalAggregations.reduce(aggregations, context);
        /* Use the formats from the bucket because they'll be right to format
         * the key. The formats on the InternalComposite doing the reducing are
         * just whatever formats make sense for *its* index. This can be real
         * trouble when the index doing the reducing is unmapped. */
        List<DocValueFormat> reducedFormats = buckets.get(0).formats;
        return new InternalBucket(sourceNames, reducedFormats, buckets.get(0).key, reverseMuls, missingOrders, docCount, aggs);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalComposite that = (InternalComposite) obj;
        return Objects.equals(size, that.size)
            && Objects.equals(buckets, that.buckets)
            && Objects.equals(afterKey, that.afterKey)
            && Arrays.equals(reverseMuls, that.reverseMuls)
            && Arrays.equals(missingOrders, that.missingOrders);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), size, buckets, afterKey, Arrays.hashCode(reverseMuls), Arrays.hashCode(missingOrders));
    }

    /**
     * The bucket iterator
     *
     * @opensearch.internal
     */
    private static class BucketIterator implements Comparable<BucketIterator> {
        final Iterator<InternalBucket> it;
        InternalBucket current;

        private BucketIterator(List<InternalBucket> buckets) {
            this.it = buckets.iterator();
        }

        @Override
        public int compareTo(BucketIterator other) {
            return current.compareKey(other.current);
        }

        InternalBucket next() {
            return current = it.hasNext() ? it.next() : null;
        }
    }

    /**
     * Internal bucket for the internal composite agg
     *
     * @opensearch.internal
     */
    public static class InternalBucket extends InternalMultiBucketAggregation.InternalBucket
        implements
            CompositeAggregation.Bucket,
            KeyComparable<InternalBucket> {

        private final CompositeKey key;
        private final long docCount;
        private final InternalAggregations aggregations;
        private final transient int[] reverseMuls;
        private final transient MissingOrder[] missingOrders;
        private final transient List<String> sourceNames;
        private final transient List<DocValueFormat> formats;

        InternalBucket(
            List<String> sourceNames,
            List<DocValueFormat> formats,
            CompositeKey key,
            int[] reverseMuls,
            MissingOrder[] missingOrders,
            long docCount,
            InternalAggregations aggregations
        ) {
            this.key = key;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.reverseMuls = reverseMuls;
            this.missingOrders = missingOrders;
            this.sourceNames = sourceNames;
            this.formats = formats;
        }

        InternalBucket(
            StreamInput in,
            List<String> sourceNames,
            List<DocValueFormat> formats,
            int[] reverseMuls,
            MissingOrder[] missingOrders
        ) throws IOException {
            this.key = new CompositeKey(in);
            this.docCount = in.readVLong();
            this.aggregations = InternalAggregations.readFrom(in);
            this.reverseMuls = reverseMuls;
            this.missingOrders = missingOrders;
            this.sourceNames = sourceNames;
            this.formats = formats;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            key.writeTo(out);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), docCount, key, aggregations);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            InternalBucket that = (InternalBucket) obj;
            return Objects.equals(docCount, that.docCount)
                && Objects.equals(key, that.key)
                && Objects.equals(aggregations, that.aggregations);
        }

        @Override
        public Map<String, Object> getKey() {
            // returns the formatted key in a map
            return new ArrayMap(sourceNames, formats, key.values());
        }

        // get the raw key (without formatting to preserve the natural order).
        // visible for testing
        CompositeKey getRawKey() {
            return key;
        }

        @Override
        public String getKeyAsString() {
            StringBuilder builder = new StringBuilder();
            builder.append('{');
            for (int i = 0; i < key.size(); i++) {
                if (i > 0) {
                    builder.append(", ");
                }
                builder.append(sourceNames.get(i));
                builder.append('=');
                builder.append(formatObject(key.get(i), formats.get(i)));
            }
            builder.append('}');
            return builder.toString();
        }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() {
            return aggregations;
        }

        /**
         * The formats used when writing the keys. Package private for testing.
         */
        List<DocValueFormat> getFormats() {
            return formats;
        }

        @Override
        public int compareKey(InternalBucket other) {
            for (int i = 0; i < key.size(); i++) {
                // lambda function require final variable.
                final int index = i;
                int result = missingOrders[i].compare(() -> key.get(index) == null, () -> other.key.get(index) == null, reverseMuls[i]);
                if (MissingOrder.unknownOrder(result) == false) {
                    if (result == 0) {
                        continue;
                    } else {
                        return result;
                    }
                }
                assert key.get(i).getClass() == other.key.get(i).getClass();
                @SuppressWarnings("unchecked")
                int cmp = key.get(i).compareTo(other.key.get(i)) * reverseMuls[i];
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            /**
             * See {@link CompositeAggregation#bucketToXContent}
             */
            throw new UnsupportedOperationException("not implemented");
        }
    }

    /**
     * Format <code>obj</code> using the provided {@link DocValueFormat}.
     * If the format is equals to {@link DocValueFormat#RAW}, the object is returned as is
     * for numbers and a string for {@link BytesRef}s.
     */
    static Object formatObject(Object obj, DocValueFormat format) {
        if (obj == null) {
            return null;
        }
        if (obj.getClass() == BytesRef.class) {
            BytesRef value = (BytesRef) obj;
            if (format == DocValueFormat.RAW) {
                return value.utf8ToString();
            } else {
                return format.format(value);
            }
        } else if (obj.getClass() == Long.class) {
            long value = (long) obj;
            if (format == DocValueFormat.RAW) {
                return value;
            } else {
                return format.format(value);
            }
        } else if (obj.getClass() == Double.class) {
            double value = (double) obj;
            if (format == DocValueFormat.RAW) {
                return value;
            } else {
                return format.format(value);
            }
        }
        return obj;
    }

    /**
     * An array map used for the internal composite agg
     *
     * @opensearch.internal
     */
    static class ArrayMap extends AbstractMap<String, Object> implements Comparable<ArrayMap> {
        final List<String> keys;
        final Comparable[] values;
        final List<DocValueFormat> formats;

        ArrayMap(List<String> keys, List<DocValueFormat> formats, Comparable[] values) {
            assert keys.size() == values.length && keys.size() == formats.size();
            this.keys = keys;
            this.formats = formats;
            this.values = values;
        }

        @Override
        public int size() {
            return values.length;
        }

        @Override
        public Object get(Object key) {
            for (int i = 0; i < keys.size(); i++) {
                if (key.equals(keys.get(i))) {
                    return formatObject(values[i], formats.get(i));
                }
            }
            return null;
        }

        @Override
        public Set<Entry<String, Object>> entrySet() {
            return new AbstractSet<Entry<String, Object>>() {
                @Override
                public Iterator<Entry<String, Object>> iterator() {
                    return new Iterator<Entry<String, Object>>() {
                        int pos = 0;

                        @Override
                        public boolean hasNext() {
                            return pos < values.length;
                        }

                        @Override
                        public Entry<String, Object> next() {
                            SimpleEntry<String, Object> entry = new SimpleEntry<>(
                                keys.get(pos),
                                formatObject(values[pos], formats.get(pos))
                            );
                            ++pos;
                            return entry;
                        }
                    };
                }

                @Override
                public int size() {
                    return keys.size();
                }
            };
        }

        @Override
        public int compareTo(ArrayMap that) {
            if (that == this) {
                return 0;
            }

            int idx = 0;
            int max = Math.min(this.keys.size(), that.keys.size());
            while (idx < max) {
                int compare = compareNullables(keys.get(idx), that.keys.get(idx));
                if (compare == 0) {
                    compare = compareNullables(values[idx], that.values[idx]);
                }
                if (compare != 0) {
                    return compare;
                }
                idx++;
            }
            if (idx < keys.size()) {
                return 1;
            }
            if (idx < that.keys.size()) {
                return -1;
            }
            return 0;
        }
    }

    private static int compareNullables(Comparable a, Comparable b) {
        if (a == b) {
            return 0;
        }
        if (a == null) {
            return -1;
        }
        if (b == null) {
            return 1;
        }
        return a.compareTo(b);
    }
}
