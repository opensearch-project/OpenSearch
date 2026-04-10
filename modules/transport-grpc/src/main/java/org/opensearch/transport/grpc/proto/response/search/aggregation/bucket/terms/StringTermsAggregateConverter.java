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
import org.opensearch.search.aggregations.bucket.terms.StringTerms;
import org.opensearch.transport.grpc.spi.AggregateProtoConverter;

import java.io.IOException;

/**
 * Converter for {@link StringTerms} aggregations to Protocol Buffer Aggregate messages.
 */
public class StringTermsAggregateConverter implements AggregateProtoConverter {

    /**
     * Creates a new StringTermsAggregateConverter.
     */
    public StringTermsAggregateConverter() {}

    @Override
    public Class<? extends InternalAggregation> getHandledAggregationType() {
        return StringTerms.class;
    }

    @Override
    public Aggregate.Builder toProto(InternalAggregation aggregation) throws IOException {
        return Aggregate.newBuilder().setSterms(StringTermsAggregateProtoUtils.toProto((StringTerms) aggregation));
    }
}
