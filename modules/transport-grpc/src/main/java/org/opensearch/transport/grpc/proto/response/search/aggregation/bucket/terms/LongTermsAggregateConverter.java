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
import org.opensearch.search.aggregations.bucket.terms.LongTerms;
import org.opensearch.transport.grpc.spi.AggregateProtoConverter;

import java.io.IOException;

/**
 * Converter for {@link LongTerms} aggregations to Protocol Buffer Aggregate messages.
 */
public class LongTermsAggregateConverter implements AggregateProtoConverter {

    /**
     * Creates a new LongTermsAggregateConverter.
     */
    public LongTermsAggregateConverter() {}

    @Override
    public Class<? extends InternalAggregation> getHandledAggregationType() {
        return LongTerms.class;
    }

    @Override
    public Aggregate.Builder toProto(InternalAggregation aggregation) throws IOException {
        return Aggregate.newBuilder().setLterms(LongTermsAggregateProtoUtils.toProto((LongTerms) aggregation));
    }
}
