/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.spi;

import org.opensearch.protobufs.Aggregate;
import org.opensearch.search.aggregations.InternalAggregation;

import java.io.IOException;

/**
 * SPI interface for converting OpenSearch InternalAggregation objects to Protocol Buffer Aggregate messages.
 * Follows the same pattern as {@link AggregationBuilderProtoConverter} for request-side conversions.
 */
public interface AggregateProtoConverter {

    /**
     * Returns the InternalAggregation subclass this converter handles.
     *
     * @return The class type of the aggregation this converter supports
     */
    Class<? extends InternalAggregation> getHandledAggregationType();

    /**
     * Converts an InternalAggregation to its Protocol Buffer Aggregate.Builder representation.
     * Returns a builder to allow the registry to construct the final Aggregate.
     * Mirrors REST-side {@link org.opensearch.search.aggregations.InternalAggregation#toXContent}
     *
     * @param aggregation The InternalAggregation to convert (guaranteed to be of the handled type)
     * @return An Aggregate.Builder with the aggregation-specific fields populated
     * @throws IOException if an error occurs during protobuf conversion
     */
    Aggregate.Builder toProto(InternalAggregation aggregation) throws IOException;
}
