/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation;

import org.opensearch.protobufs.Aggregate;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.transport.grpc.spi.AggregateProtoConverterRegistry;

import java.io.IOException;

/**
 * Converts InternalAggregation to Aggregate protobuf.
 * Uses a registry pattern for extensible converter dispatch.
 */
public class AggregateProtoUtils {

    private static AggregateProtoConverterRegistry registry = new AggregateProtoConverterRegistryImpl();

    private AggregateProtoUtils() {
        // Utility class - no instances
    }

    /**
     * Sets the registry for testing or custom converter registration.
     *
     * @param registry The registry to use (must not be null)
     */
    public static void setRegistry(AggregateProtoConverterRegistry registry) {
        if (registry == null) {
            throw new IllegalArgumentException("Registry must not be null");
        }
        AggregateProtoUtils.registry = registry;
    }

    /**
     * Converts an Aggregation to Aggregate protobuf.
     *
     * <p>Delegates to the registry for extensible converter dispatch.
     * Mirrors REST-side {@link org.opensearch.search.aggregations.InternalAggregation#toXContent}.
     *
     * @param aggregation The OpenSearch aggregation (must not be null)
     * @return The corresponding Protocol Buffer Aggregate message
     * @throws IllegalArgumentException if aggregation is null, not an InternalAggregation, or type is not supported
     * @throws IOException if an error occurs during protobuf conversion
     * @see org.opensearch.search.aggregations.InternalAggregation#toXContent
     */
    public static Aggregate toProto(Aggregation aggregation) throws IOException {
        if (aggregation == null) {
            throw new IllegalArgumentException("Aggregation must not be null");
        }

        if (!(aggregation instanceof InternalAggregation)) {
            throw new IllegalArgumentException("Only InternalAggregation types are supported");
        }

        return registry.toProto((InternalAggregation) aggregation);
    }
}
