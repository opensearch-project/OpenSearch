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
 * SPI interface for the aggregate converter registry.
 * Provides the main entry point for converting InternalAggregation objects to Protocol Buffer Aggregate messages.
 */
public interface AggregateProtoConverterRegistry {

    /**
     * Converts an InternalAggregation to its Protocol Buffer Aggregate representation.
     * Handles metadata and delegates to the appropriate converter.
     *
     * @param aggregation The InternalAggregation to convert (must not be null)
     * @return The corresponding Protocol Buffer Aggregate message
     * @throws IllegalArgumentException if aggregation is null or type is not supported
     * @throws IOException if an error occurs during protobuf conversion
     */
    Aggregate toProto(InternalAggregation aggregation) throws IOException;
}
