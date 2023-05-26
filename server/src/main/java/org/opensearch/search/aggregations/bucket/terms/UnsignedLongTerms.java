/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Result of the {@link TermsAggregator} when the field is some kind of whole number like a integer,
 * long, unsigned long or a date.
 *
 * @opensearch.internal
 */
public class UnsignedLongTerms extends InternalMappedTerms<UnsignedLongTerms, UnsignedLongTerms.Bucket> {
    public static final String NAME = "ulterms";

    /**
     * Bucket for long terms
     *
     * @opensearch.internal
     */
    public static class Bucket extends InternalTerms.Bucket<Bucket> {
        BigInteger term;

        public Bucket(
            BigInteger term,
            long docCount,
            InternalAggregations aggregations,
            boolean showDocCountError,
            long docCountError,
            DocValueFormat format
        ) {
            super(docCount, aggregations, showDocCountError, docCountError, format);
            this.term = term;
        }

        /**
         * Read from a stream.
         */
        public Bucket(StreamInput in, DocValueFormat format, boolean showDocCountError) throws IOException {
            super(in, format, showDocCountError);
            term = in.readBigInteger();
        }

        @Override
        protected void writeTermTo(StreamOutput out) throws IOException {
            out.writeBigInteger(term);
        }

        @Override
        public String getKeyAsString() {
            return format.format(term).toString();
        }

        @Override
        public Object getKey() {
            if (format == DocValueFormat.UNSIGNED_LONG_SHIFTED) {
                return format.format(term);
            } else {
                return term;
            }
        }

        @Override
        public Number getKeyAsNumber() {
            if (format == DocValueFormat.UNSIGNED_LONG_SHIFTED) {
                return (Number) format.format(term);
            } else {
                return term;
            }
        }

        @Override
        public int compareKey(Bucket other) {
            return term.compareTo(other.term);
        }

        @Override
        protected final XContentBuilder keyToXContent(XContentBuilder builder) throws IOException {
            if (format == DocValueFormat.UNSIGNED_LONG_SHIFTED) {
                builder.field(CommonFields.KEY.getPreferredName(), format.format(term));
            } else {
                builder.field(CommonFields.KEY.getPreferredName(), term);
            }
            if (format != DocValueFormat.RAW && format != DocValueFormat.UNSIGNED_LONG_SHIFTED && format != DocValueFormat.UNSIGNED_LONG) {
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), format.format(term).toString());
            }
            return builder;
        }

        @Override
        public boolean equals(Object obj) {
            return super.equals(obj) && Objects.equals(term, ((Bucket) obj).term);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), term);
        }
    }

    public UnsignedLongTerms(
        String name,
        BucketOrder reduceOrder,
        BucketOrder order,
        int requiredSize,
        long minDocCount,
        Map<String, Object> metadata,
        DocValueFormat format,
        int shardSize,
        boolean showTermDocCountError,
        long otherDocCount,
        List<Bucket> buckets,
        long docCountError
    ) {
        super(
            name,
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            metadata,
            format,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            buckets,
            docCountError
        );
    }

    /**
     * Read from a stream.
     */
    public UnsignedLongTerms(StreamInput in) throws IOException {
        super(in, Bucket::new);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public UnsignedLongTerms create(List<Bucket> buckets) {
        return new UnsignedLongTerms(
            name,
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            metadata,
            format,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            buckets,
            docCountError
        );
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(
            prototype.term,
            prototype.docCount,
            aggregations,
            prototype.showDocCountError,
            prototype.docCountError,
            prototype.format
        );
    }

    @Override
    protected UnsignedLongTerms create(String name, List<Bucket> buckets, BucketOrder reduceOrder, long docCountError, long otherDocCount) {
        return new UnsignedLongTerms(
            name,
            reduceOrder,
            order,
            requiredSize,
            minDocCount,
            getMetadata(),
            format,
            shardSize,
            showTermDocCountError,
            otherDocCount,
            buckets,
            docCountError
        );
    }

    @Override
    protected Bucket[] createBucketsArray(int size) {
        return new Bucket[size];
    }

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        boolean unsignedLongFormat = false;
        boolean rawFormat = false;
        for (InternalAggregation agg : aggregations) {
            if (agg instanceof DoubleTerms) {
                return agg.reduce(aggregations, reduceContext);
            }
            if (agg instanceof UnsignedLongTerms) {
                if (((UnsignedLongTerms) agg).format == DocValueFormat.RAW) {
                    rawFormat = true;
                } else if (((UnsignedLongTerms) agg).format == DocValueFormat.UNSIGNED_LONG_SHIFTED) {
                    unsignedLongFormat = true;
                } else if (((UnsignedLongTerms) agg).format == DocValueFormat.UNSIGNED_LONG) {
                    unsignedLongFormat = true;
                }
            }
        }
        if (rawFormat && unsignedLongFormat) { // if we have mixed formats, convert results to double format
            List<InternalAggregation> newAggs = new ArrayList<>(aggregations.size());
            for (InternalAggregation agg : aggregations) {
                if (agg instanceof UnsignedLongTerms) {
                    DoubleTerms dTerms = UnsignedLongTerms.convertUnsignedLongTermsToDouble((UnsignedLongTerms) agg, format);
                    newAggs.add(dTerms);
                } else {
                    newAggs.add(agg);
                }
            }
            return newAggs.get(0).reduce(newAggs, reduceContext);
        }
        return super.reduce(aggregations, reduceContext);
    }

    @Override
    Bucket createBucket(long docCount, InternalAggregations aggs, long docCountError, UnsignedLongTerms.Bucket prototype) {
        return new Bucket(prototype.term, docCount, aggs, prototype.showDocCountError, docCountError, format);
    }

    /**
     * Converts a {@link UnsignedLongTerms} into a {@link DoubleTerms}, returning the value of the specified long terms as doubles.
     */
    static DoubleTerms convertUnsignedLongTermsToDouble(UnsignedLongTerms unsignedLongTerms, DocValueFormat decimalFormat) {
        List<UnsignedLongTerms.Bucket> buckets = unsignedLongTerms.getBuckets();
        List<DoubleTerms.Bucket> newBuckets = new ArrayList<>();
        for (Terms.Bucket bucket : buckets) {
            newBuckets.add(
                new DoubleTerms.Bucket(
                    bucket.getKeyAsNumber().doubleValue(),
                    bucket.getDocCount(),
                    (InternalAggregations) bucket.getAggregations(),
                    unsignedLongTerms.showTermDocCountError,
                    unsignedLongTerms.showTermDocCountError ? bucket.getDocCountError() : 0,
                    decimalFormat
                )
            );
        }
        return new DoubleTerms(
            unsignedLongTerms.getName(),
            unsignedLongTerms.reduceOrder,
            unsignedLongTerms.order,
            unsignedLongTerms.requiredSize,
            unsignedLongTerms.minDocCount,
            unsignedLongTerms.metadata,
            unsignedLongTerms.format,
            unsignedLongTerms.shardSize,
            unsignedLongTerms.showTermDocCountError,
            unsignedLongTerms.otherDocCount,
            newBuckets,
            unsignedLongTerms.docCountError
        );
    }
}
