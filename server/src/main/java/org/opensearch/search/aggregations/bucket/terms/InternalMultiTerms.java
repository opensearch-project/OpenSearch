/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.apache.lucene.util.BytesRef;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.AggregationExecutionException;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.KeyComparable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Result of the {@link MultiTermsAggregator}.
 *
 * @opensearch.internal
 */
public class InternalMultiTerms extends InternalTerms<InternalMultiTerms, InternalMultiTerms.Bucket> {
    /**
     * Internal Multi Terms Bucket.
     *
     * @opensearch.internal
     */
    public static class Bucket extends InternalTerms.AbstractInternalBucket implements KeyComparable<Bucket> {

        protected long bucketOrd;
        /**
         * list of terms values.
         */
        protected List<Object> termValues;
        protected long docCount;
        protected InternalAggregations aggregations;
        protected boolean showDocCountError;
        protected long docCountError;
        /**
         * A list of term's {@link DocValueFormat}.
         */
        protected final List<DocValueFormat> termFormats;

        private static final String PIPE = "|";

        /**
         * Create default {@link Bucket}.
         */
        public static Bucket EMPTY(boolean showTermDocCountError, List<DocValueFormat> formats) {
            return new Bucket(null, 0, null, showTermDocCountError, 0, formats);
        }

        public Bucket(
            List<Object> values,
            long docCount,
            InternalAggregations aggregations,
            boolean showDocCountError,
            long docCountError,
            List<DocValueFormat> formats
        ) {
            this.termValues = values;
            this.docCount = docCount;
            this.aggregations = aggregations;
            this.showDocCountError = showDocCountError;
            this.docCountError = docCountError;
            this.termFormats = formats;
        }

        public Bucket(StreamInput in, List<DocValueFormat> formats, boolean showDocCountError) throws IOException {
            this.termValues = in.readList(StreamInput::readGenericValue);
            this.docCount = in.readVLong();
            this.aggregations = InternalAggregations.readFrom(in);
            this.showDocCountError = showDocCountError;
            this.docCountError = -1;
            if (showDocCountError) {
                this.docCountError = in.readLong();
            }
            this.termFormats = formats;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CommonFields.KEY.getPreferredName(), getKey());
            builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), getKeyAsString());
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
            if (showDocCountError) {
                builder.field(DOC_COUNT_ERROR_UPPER_BOUND_FIELD_NAME.getPreferredName(), getDocCountError());
            }
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(termValues, StreamOutput::writeGenericValue);
            out.writeVLong(docCount);
            aggregations.writeTo(out);
            if (showDocCountError) {
                out.writeLong(docCountError);
            }
        }

        @Override
        public List<Object> getKey() {
            List<Object> keys = new ArrayList<>(termValues.size());
            for (int i = 0; i < termValues.size(); i++) {
                keys.add(formatObject(termValues.get(i), termFormats.get(i)));
            }
            return keys;
        }

        @Override
        public String getKeyAsString() {
            return getKey().stream().map(Object::toString).collect(Collectors.joining(PIPE));
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
        void setDocCountError(long docCountError) {
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
        public Number getKeyAsNumber() {
            throw new IllegalArgumentException("getKeyAsNumber is not supported by [" + MultiTermsAggregationBuilder.NAME + "]");
        }

        @Override
        public long getDocCountError() {
            if (!showDocCountError) {
                throw new IllegalStateException("show_terms_doc_count_error is false");
            }
            return docCountError;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            Bucket other = (Bucket) obj;
            if (showDocCountError && docCountError != other.docCountError) {
                return false;
            }
            return termValues.equals(other.termValues)
                && docCount == other.docCount
                && aggregations.equals(other.aggregations)
                && showDocCountError == other.showDocCountError;
        }

        @Override
        public int hashCode() {
            return Objects.hash(termValues, docCount, aggregations, showDocCountError, showDocCountError ? docCountError : 0);
        }

        @Override
        public int compareKey(Bucket other) {
            return new BucketComparator().compare(this.termValues, other.termValues);
        }

        /**
         * Visible for testing.
         *
         * @opensearch.internal
         */
        protected static class BucketComparator implements Comparator<List<Object>> {
            @SuppressWarnings({ "unchecked" })
            @Override
            public int compare(List<Object> thisObjects, List<Object> thatObjects) {
                if (thisObjects.size() != thatObjects.size()) {
                    throw new AggregationExecutionException(
                        "[" + MultiTermsAggregationBuilder.NAME + "] aggregations failed due to terms" + " size is different"
                    );
                }
                for (int i = 0; i < thisObjects.size(); i++) {
                    final Object thisObject = thisObjects.get(i);
                    final Object thatObject = thatObjects.get(i);
                    int ret = ((Comparable) thisObject).compareTo(thatObject);
                    if (ret != 0) {
                        return ret;
                    }
                }
                return 0;
            }
        }
    }

    private final int shardSize;
    private final boolean showTermDocCountError;
    private final long otherDocCount;
    private final List<DocValueFormat> termFormats;
    private final List<Bucket> buckets;
    private Map<String, Bucket> bucketMap;

    private long docCountError;

    public InternalMultiTerms(
        String name,
        BucketOrder reduceOrder,
        BucketOrder order,
        int requiredSize,
        long minDocCount,
        Map<String, Object> metadata,
        int shardSize,
        boolean showTermDocCountError,
        long otherDocCount,
        long docCountError,
        List<DocValueFormat> formats,
        List<Bucket> buckets
    ) {
        super(name, reduceOrder, order, requiredSize, minDocCount, metadata);
        this.shardSize = shardSize;
        this.showTermDocCountError = showTermDocCountError;
        this.otherDocCount = otherDocCount;
        this.termFormats = formats;
        this.buckets = buckets;
        this.docCountError = docCountError;
    }

    public InternalMultiTerms(StreamInput in) throws IOException {
        super(in);
        this.docCountError = in.readZLong();
        this.termFormats = in.readList(stream -> stream.readNamedWriteable(DocValueFormat.class));
        this.shardSize = readSize(in);
        this.showTermDocCountError = in.readBoolean();
        this.otherDocCount = in.readVLong();
        this.buckets = in.readList(steam -> new Bucket(steam, termFormats, showTermDocCountError));
    }

    @Override
    public String getWriteableName() {
        return MultiTermsAggregationBuilder.NAME;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        return doXContentCommon(builder, params, docCountError, otherDocCount, buckets);
    }

    @Override
    public InternalMultiTerms create(List<Bucket> buckets) {
        return new InternalMultiTerms(
            name,
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            metadata,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            docCountError,
            termFormats,
            buckets
        );
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(
            prototype.termValues,
            prototype.docCount,
            aggregations,
            prototype.showDocCountError,
            prototype.docCountError,
            prototype.termFormats
        );
    }

    @Override
    protected void writeTermTypeInfoTo(StreamOutput out) throws IOException {
        out.writeZLong(docCountError);
        out.writeCollection(termFormats, StreamOutput::writeNamedWriteable);
        writeSize(shardSize, out);
        out.writeBoolean(showTermDocCountError);
        out.writeVLong(otherDocCount);
        out.writeList(buckets);
    }

    @Override
    public List<Bucket> getBuckets() {
        return buckets;
    }

    @Override
    public Bucket getBucketByKey(String term) {
        if (bucketMap == null) {
            bucketMap = buckets.stream().collect(Collectors.toMap(InternalMultiTerms.Bucket::getKeyAsString, Function.identity()));
        }
        return bucketMap.get(term);
    }

    @Override
    public long getDocCountError() {
        return docCountError;
    }

    @Override
    public long getSumOfOtherDocCounts() {
        return otherDocCount;
    }

    @Override
    protected void setDocCountError(long docCountError) {
        this.docCountError = docCountError;
    }

    @Override
    protected int getShardSize() {
        return shardSize;
    }

    @Override
    protected InternalMultiTerms create(
        String name,
        List<Bucket> buckets,
        BucketOrder reduceOrder,
        long docCountError,
        long otherDocCount
    ) {
        return new InternalMultiTerms(
            name,
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            metadata,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            docCountError,
            termFormats,
            buckets
        );
    }

    @Override
    protected Bucket[] createBucketsArray(int size) {
        return new Bucket[size];
    }

    @Override
    Bucket createBucket(long docCount, InternalAggregations aggs, long docCountError, Bucket prototype) {
        return new Bucket(
            prototype.termValues,
            docCount,
            aggs,
            prototype.showDocCountError,
            prototype.docCountError,
            prototype.termFormats
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        InternalMultiTerms that = (InternalMultiTerms) obj;

        if (showTermDocCountError && docCountError != that.docCountError) {
            return false;
        }
        return Objects.equals(buckets, that.buckets)
            && Objects.equals(otherDocCount, that.otherDocCount)
            && Objects.equals(showTermDocCountError, that.showTermDocCountError)
            && Objects.equals(shardSize, that.shardSize)
            && Objects.equals(docCountError, that.docCountError);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), buckets, otherDocCount, showTermDocCountError, shardSize);
    }

    /**
     * Copy from InternalComposite
     *
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
}
