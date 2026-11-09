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
import org.opensearch.protobufs.DoubleTermsAggregate;
import org.opensearch.protobufs.DoubleTermsBucket;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils;
import org.opensearch.transport.grpc.spi.AggregateProtoConverter;
import org.opensearch.transport.grpc.spi.AggregateProtoConverterRegistry;

import java.io.IOException;

/**
 * Converter for {@link DoubleTerms} aggregations to Protocol Buffer Aggregate messages.
 */
public class DoubleTermsAggregateConverter implements AggregateProtoConverter {

    private AggregateProtoConverterRegistry registry;

    /**
     * Creates a new DoubleTermsAggregateConverter.
     */
    public DoubleTermsAggregateConverter() {}

    @Override
    public Class<? extends InternalAggregation> getHandledAggregationType() {
        return DoubleTerms.class;
    }

    @Override
    public void setRegistry(AggregateProtoConverterRegistry registry) {
        this.registry = registry;
    }

    @Override
    public Aggregate.Builder toProto(InternalAggregation aggregation) throws IOException {
        DoubleTerms doubleTerms = (DoubleTerms) aggregation;
        DoubleTermsAggregate.Builder termsBuilder = DoubleTermsAggregate.newBuilder();

        AggregateProtoUtils.addMetadata(termsBuilder::setMeta, doubleTerms);

        termsBuilder.setDocCountErrorUpperBound(doubleTerms.getDocCountError());
        termsBuilder.setSumOtherDocCount(doubleTerms.getSumOfOtherDocCounts());

        for (DoubleTerms.Bucket bucket : doubleTerms.getBuckets()) {
            termsBuilder.addBuckets(convertBucket(bucket));
        }

        return Aggregate.newBuilder().setDterms(termsBuilder.build());
    }

    /**
     * Mirroring {@link DoubleTerms.Bucket#keyToXContent(XContentBuilder)}
     */
    private DoubleTermsBucket convertBucket(DoubleTerms.Bucket bucket) throws IOException {
        DoubleTermsBucket.Builder builder = DoubleTermsBucket.newBuilder();

        builder.setKey((double) bucket.getKey());

        if (bucket.getFormat() != DocValueFormat.RAW) {
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
