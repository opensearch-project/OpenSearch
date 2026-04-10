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
import org.opensearch.search.aggregations.bucket.terms.UnmappedTerms;
import org.opensearch.transport.grpc.spi.AggregateProtoConverter;

import java.io.IOException;

/**
 * Converter for {@link UnmappedTerms} aggregations to Protocol Buffer Aggregate messages.
 */
public class UnmappedTermsAggregateConverter implements AggregateProtoConverter {

    /**
     * Creates a new UnmappedTermsAggregateConverter.
     */
    public UnmappedTermsAggregateConverter() {}

    @Override
    public Class<? extends InternalAggregation> getHandledAggregationType() {
        return UnmappedTerms.class;
    }

    @Override
    public Aggregate.Builder toProto(InternalAggregation aggregation) throws IOException {
        return Aggregate.newBuilder().setUmterms(UnmappedTermsAggregateProtoUtils.toProto(aggregation));
    }
}
