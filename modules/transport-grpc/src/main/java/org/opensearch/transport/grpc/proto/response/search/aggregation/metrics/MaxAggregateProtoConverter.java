/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.metrics;

import org.opensearch.protobufs.Aggregate;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.transport.grpc.spi.AggregateProtoConverter;

import java.io.IOException;

/**
 * Converter for {@link InternalMax} aggregations to Protocol Buffer Aggregate messages.
 * Delegates the actual conversion logic to {@link MaxAggregateProtoUtils}.
 */
public class MaxAggregateProtoConverter implements AggregateProtoConverter {

    /**
     * Creates a new MaxAggregateProtoConverter.
     */
    public MaxAggregateProtoConverter() {}

    @Override
    public Class<? extends InternalAggregation> getHandledAggregationType() {
        return InternalMax.class;
    }

    @Override
    public Aggregate.Builder toProto(InternalAggregation aggregation) throws IOException {
        Aggregate aggregate = MaxAggregateProtoUtils.toProto((InternalMax) aggregation);
        return aggregate.toBuilder();
    }
}
