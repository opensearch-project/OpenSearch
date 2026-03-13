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
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.transport.grpc.proto.response.common.ObjectMapProtoUtils;
import org.opensearch.transport.grpc.proto.response.search.aggregation.metrics.MaxAggregateProtoUtils;
import org.opensearch.transport.grpc.proto.response.search.aggregation.metrics.MinAggregateProtoUtils;

import java.io.IOException;

/**
 * Converts InternalAggregation to Aggregate protobuf.
 */
public class AggregateProtoUtils {

    private AggregateProtoUtils() {
        // Utility class - no instances
    }

    /**
     * Converts an Aggregation to Aggregate protobuf.
     *
     * <p>Dispatches to specific converters and handles metadata centrally.
     * Mirrors REST-side {@link org.opensearch.search.aggregations.InternalAggregation#toXContent}.
     *
     * @param aggregation The OpenSearch aggregation (must not be null)
     * @return The corresponding Protocol Buffer Aggregate message
     * @throws IllegalArgumentException if aggregation is null or type is not supported
     * @throws IOException if an error occurs during protobuf conversion
     * @see org.opensearch.search.aggregations.InternalAggregation#toXContent
     */
    public static Aggregate toProto(Aggregation aggregation) throws IOException {
        if (aggregation == null) {
            throw new IllegalArgumentException("Aggregation must not be null");
        }

        Aggregate.Builder builder = Aggregate.newBuilder();

        if (aggregation.getMetadata() != null && !aggregation.getMetadata().isEmpty()) {
            ObjectMap.Value metaValue = ObjectMapProtoUtils.toProto(aggregation.getMetadata());
            if (metaValue.hasObjectMap()) {
                builder.setMeta(metaValue.getObjectMap());
            }
        }

        if (aggregation instanceof InternalMin) {
            builder.mergeFrom(MinAggregateProtoUtils.toProto((InternalMin) aggregation));
        } else if (aggregation instanceof InternalMax) {
            builder.mergeFrom(MaxAggregateProtoUtils.toProto((InternalMax) aggregation));
        } else {
            throw new IllegalArgumentException("Unsupported aggregation type: " + aggregation.getClass().getName());
        }

        return builder.build();
    }
}
