/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.protobufs.Aggregate;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.bucket.terms.UnsignedLongTerms;
import org.opensearch.transport.grpc.spi.AggregateProtoConverter;

import java.io.IOException;

/**
 * Converter for {@link UnsignedLongTerms} aggregations to Protocol Buffer Aggregate messages.
 */
public class UnsignedLongTermsAggregateConverter implements AggregateProtoConverter {

    /**
     * Creates a new UnsignedLongTermsAggregateConverter.
     */
    public UnsignedLongTermsAggregateConverter() {}

    @Override
    public Class<? extends InternalAggregation> getHandledAggregationType() {
        return UnsignedLongTerms.class;
    }

    @Override
    public Aggregate.Builder toProto(InternalAggregation aggregation) throws IOException {
        return Aggregate.newBuilder().setUlterms(UnsignedLongTermsAggregateProtoUtils.toProto((UnsignedLongTerms) aggregation));
    }
}
