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
import org.opensearch.protobufs.LongTermsAggregate;
import org.opensearch.protobufs.LongTermsBucket;
import org.opensearch.protobufs.LongTermsBucketKey;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils;
import org.opensearch.transport.grpc.spi.AggregateProtoConverter;
import org.opensearch.transport.grpc.spi.AggregateProtoConverterRegistry;

import java.io.IOException;

/**
 * Converter for {@link LongTerms} aggregations to Protocol Buffer Aggregate messages.
 */
public class LongTermsAggregateConverter implements AggregateProtoConverter {

    private AggregateProtoConverterRegistry registry;

    /**
     * Creates a new LongTermsAggregateConverter.
     */
    public LongTermsAggregateConverter() {}

    @Override
    public Class<? extends InternalAggregation> getHandledAggregationType() {
        return LongTerms.class;
    }

    @Override
    public void setRegistry(AggregateProtoConverterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public Aggregate.Builder toProto(InternalAggregation aggregation) throws IOException {
        LongTerms longTerms = (LongTerms) aggregation;
        LongTermsAggregate.Builder termsBuilder = LongTermsAggregate.newBuilder();

        AggregateProtoUtils.addMetadata(termsBuilder::setMeta, longTerms);

        termsBuilder.setDocCountErrorUpperBound(longTerms.getDocCountError());
        termsBuilder.setSumOtherDocCount(longTerms.getSumOfOtherDocCounts());

        for (LongTerms.Bucket bucket : longTerms.getBuckets()) {
            termsBuilder.addBuckets(convertBucket(bucket));
        }

        return Aggregate.newBuilder().setLterms(termsBuilder.build());
    }

    /**
     * Mirroring {@link LongTerms.Bucket#keyToXContent(XContentBuilder)}
     */
    private LongTermsBucket convertBucket(LongTerms.Bucket bucket) throws IOException {
        LongTermsBucket.Builder builder = LongTermsBucket.newBuilder();

        Object key = bucket.getKey();
        if (key instanceof Long) {
            builder.setKey(LongTermsBucketKey.newBuilder().setSigned((long) key));
        } else {
            builder.setKey(LongTermsBucketKey.newBuilder().setUnsigned(key.toString()));
        }

        if (bucket.getFormat() != DocValueFormat.RAW && bucket.getFormat() != DocValueFormat.UNSIGNED_LONG_SHIFTED) {
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
