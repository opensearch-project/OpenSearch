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
import org.opensearch.search.aggregations.bucket.terms.DoubleTerms;
import org.opensearch.transport.grpc.spi.AggregateProtoConverter;

import java.io.IOException;

/**
 * Converter for {@link DoubleTerms} aggregations to Protocol Buffer Aggregate messages.
 */
public class DoubleTermsAggregateConverter implements AggregateProtoConverter {

    /**
     * Creates a new DoubleTermsAggregateConverter.
     */
    public DoubleTermsAggregateConverter() {}

    @Override
    public Class<? extends InternalAggregation> getHandledAggregationType() {
        return DoubleTerms.class;
    }

    @Override
    public Aggregate.Builder toProto(InternalAggregation aggregation) throws IOException {
        return Aggregate.newBuilder().setDterms(DoubleTermsAggregateProtoUtils.toProto((DoubleTerms) aggregation));
    }
}
