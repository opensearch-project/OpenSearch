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
import org.opensearch.protobufs.StringTermsAggregate;
import org.opensearch.protobufs.StringTermsBucket;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils;
import org.opensearch.transport.grpc.spi.AggregateProtoConverter;
import org.opensearch.transport.grpc.spi.AggregateProtoConverterRegistry;

import java.io.IOException;

/**
 * Converter for {@link StringTerms} aggregations to Protocol Buffer Aggregate messages.
 */
public class StringTermsAggregateConverter implements AggregateProtoConverter {

    private AggregateProtoConverterRegistry registry;

    /**
     * Creates a new StringTermsAggregateConverter.
     */
    public StringTermsAggregateConverter() {}

    @Override
    public Class<? extends InternalAggregation> getHandledAggregationType() {
        return StringTerms.class;
    }

    @Override
    public void setRegistry(AggregateProtoConverterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public Aggregate.Builder toProto(InternalAggregation aggregation) throws IOException {
        StringTerms stringTerms = (StringTerms) aggregation;
        StringTermsAggregate.Builder termsBuilder = StringTermsAggregate.newBuilder();

        AggregateProtoUtils.addMetadata(termsBuilder::setMeta, stringTerms);

        termsBuilder.setDocCountErrorUpperBound(stringTerms.getDocCountError());
        termsBuilder.setSumOtherDocCount(stringTerms.getSumOfOtherDocCounts());

        for (StringTerms.Bucket bucket : stringTerms.getBuckets()) {
            termsBuilder.addBuckets(convertBucket(bucket));
        }

        return Aggregate.newBuilder().setSterms(termsBuilder.build());
    }

    /**
     * Mirroring {@link StringTerms.Bucket#keyToXContent(XContentBuilder)}
     */
    private StringTermsBucket convertBucket(StringTerms.Bucket bucket) throws IOException {
        StringTermsBucket.Builder builder = StringTermsBucket.newBuilder();

        builder.setKey(bucket.getKeyAsString());

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
