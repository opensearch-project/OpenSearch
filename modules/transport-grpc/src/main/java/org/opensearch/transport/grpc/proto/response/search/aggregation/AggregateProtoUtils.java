/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation;

import org.opensearch.protobufs.Aggregate;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.transport.grpc.proto.response.common.ObjectMapProtoUtils;
import org.opensearch.transport.grpc.spi.AggregateProtoConverterRegistry;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * Converts InternalAggregation to Aggregate protobuf.
 * Uses a registry pattern for extensible converter dispatch.
 */
public class AggregateProtoUtils {

    private AggregateProtoUtils() {
        // Utility class - no instances
    }

    /**
     * Converts an Aggregation to Aggregate protobuf.
     *
     * <p>Delegates to the registry for extensible converter dispatch.
     * Mirrors REST-side {@link InternalAggregation#toXContent}.
     *
     * @param aggregation The OpenSearch aggregation (must not be null)
     * @param registry The converter registry for dispatching to the appropriate converter
     * @return The corresponding Protocol Buffer Aggregate message
     * @throws IllegalArgumentException if aggregation is null, not an InternalAggregation, or type is not supported
     * @throws IOException if an error occurs during protobuf conversion
     * @see InternalAggregation#toXContent
     */
    public static Aggregate toProto(Aggregation aggregation, AggregateProtoConverterRegistry registry) throws IOException {
        if (aggregation == null) {
            throw new IllegalArgumentException("Aggregation must not be null");
        }

        if (!(aggregation instanceof InternalAggregation)) {
            throw new IllegalArgumentException("Only InternalAggregation types are supported");
        }

        return registry.toProto((InternalAggregation) aggregation);
    }

    /**
     * Adds metadata from an InternalAggregation to a typed aggregate builder
     * via the provided setter function.
     *
     * @param metaSetter the setMeta method reference on the typed builder
     * @param aggregation the source aggregation
     */
    public static void addMetadata(Consumer<ObjectMap> metaSetter, InternalAggregation aggregation) {
        if (aggregation.getMetadata() != null && !aggregation.getMetadata().isEmpty()) {
            ObjectMap.Value metaValue = ObjectMapProtoUtils.toProto(aggregation.getMetadata());
            if (metaValue.hasObjectMap()) {
                metaSetter.accept(metaValue.getObjectMap());
            }
        }
    }

}
