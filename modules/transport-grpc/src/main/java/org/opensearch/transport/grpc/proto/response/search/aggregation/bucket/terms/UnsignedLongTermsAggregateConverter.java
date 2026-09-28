/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.Aggregate;
import org.opensearch.protobufs.UnsignedLongTermsAggregate;
import org.opensearch.protobufs.UnsignedLongTermsBucket;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.UnsignedLongTerms;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils;
import org.opensearch.transport.grpc.spi.AggregateProtoConverter;
import org.opensearch.transport.grpc.spi.AggregateProtoConverterRegistry;

import java.io.IOException;

/**
 * Converter for {@link UnsignedLongTerms} aggregations to Protocol Buffer Aggregate messages.
 */
public class UnsignedLongTermsAggregateConverter implements AggregateProtoConverter {

    private AggregateProtoConverterRegistry registry;

    /**
     * Creates a new UnsignedLongTermsAggregateConverter.
     */
    public UnsignedLongTermsAggregateConverter() {}

    @Override
    public Class<? extends InternalAggregation> getHandledAggregationType() {
        return UnsignedLongTerms.class;
    }

    @Override
    public void setRegistry(AggregateProtoConverterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public Aggregate.Builder toProto(InternalAggregation aggregation) throws IOException {
        UnsignedLongTerms unsignedLongTerms = (UnsignedLongTerms) aggregation;
        UnsignedLongTermsAggregate.Builder termsBuilder = UnsignedLongTermsAggregate.newBuilder();

        AggregateProtoUtils.addMetadata(termsBuilder::setMeta, unsignedLongTerms);

        termsBuilder.setDocCountErrorUpperBound(unsignedLongTerms.getDocCountError());
        termsBuilder.setSumOtherDocCount(unsignedLongTerms.getSumOfOtherDocCounts());

        for (UnsignedLongTerms.Bucket bucket : unsignedLongTerms.getBuckets()) {
            termsBuilder.addBuckets(convertBucket(bucket));
        }

        return Aggregate.newBuilder().setUlterms(termsBuilder.build());
    }

    /**
     * Mirroring {@link UnsignedLongTerms.Bucket#keyToXContent(XContentBuilder)}
     */
    private UnsignedLongTermsBucket convertBucket(UnsignedLongTerms.Bucket bucket) throws IOException {
        UnsignedLongTermsBucket.Builder builder = UnsignedLongTermsBucket.newBuilder();

        // BigInteger.longValue() preserves the uint64 bit pattern for proto's uint64 field.
        builder.setKey(((Number) bucket.getKey()).longValue());

        if (bucket.getFormat() != DocValueFormat.RAW
            && bucket.getFormat() != DocValueFormat.UNSIGNED_LONG
            && bucket.getFormat() != DocValueFormat.UNSIGNED_LONG_SHIFTED) {
            builder.setKeyAsString(bucket.getKeyAsString());
        }

        builder.setDocCount(bucket.getDocCount());
        if (bucket.showDocCountError()) {
            builder.setDocCountErrorUpperBound(bucket.getDocCountError());
        }

        for (Aggregation subAgg : bucket.getAggregations()) {
            if (subAgg instanceof InternalAggregation internalAgg) {
                builder.putAggregate(subAgg.getName(), AggregateProtoUtils.toProto(internalAgg, registry));
            } else {
                throw new IllegalStateException(
                    "Unexpected aggregation type in terms bucket sub-aggregations: "
                        + subAgg.getClass().getName()
                        + " (name="
                        + subAgg.getName()
                        + ")"
                );
            }
        }

        return builder.build();
    }
}
