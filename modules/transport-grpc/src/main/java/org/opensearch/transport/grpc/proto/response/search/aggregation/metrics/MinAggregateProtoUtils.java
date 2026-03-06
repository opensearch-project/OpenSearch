/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.metrics;

import org.opensearch.protobufs.MinAggregate;
import org.opensearch.protobufs.NullValue;
import org.opensearch.protobufs.SingleMetricAggregateBaseAllOfValue;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.metrics.InternalMin;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils;

/**
 * Utility class for converting InternalMin aggregation results to MinAggregate protocol buffer format.
 *
 * <p>This utility mirrors the REST API serialization logic found in {@link InternalMin#doXContentBody}
 * but produces Protocol Buffer messages instead of XContent (JSON/YAML). The conversion handles:
 * <ul>
 *   <li>Minimum numeric value with special handling for empty result sets (POSITIVE_INFINITY → NULL_VALUE)</li>
 *   <li>Formatted string representation (value_as_string) - only included when format != RAW</li>
 *   <li>Aggregation metadata</li>
 * </ul>
 *
 * <p>The field mapping between REST API and gRPC protobuf:
 * <pre>
 * REST (XContent)              gRPC (Protobuf)
 * ---------------              ---------------
 * "value"                  →   value (SingleMetricAggregateBaseAllOfValue)
 * "value_as_string"        →   value_as_string (optional, format-dependent)
 * metadata                 →   meta (ObjectMap)
 * </pre>
 *
 * @see InternalMin
 * @see InternalMin#doXContentBody
 * @see MinAggregate
 */
public class MinAggregateProtoUtils {

    private MinAggregateProtoUtils() {
        // Utility class
    }

    /**
     * Converts an InternalMin aggregation result to a MinAggregate protobuf message.
     *
     * <p>Mirrors {@link InternalMin#doXContentBody} structure exactly.
     *
     * @param internalMin The InternalMin aggregation result from OpenSearch
     * @return A MinAggregate protobuf message with all applicable fields populated
     * @see InternalMin#doXContentBody
     * @see MinAggregate
     */
    public static MinAggregate toProto(InternalMin internalMin) {
        MinAggregate.Builder builder = MinAggregate.newBuilder();

        double min = internalMin.getValue();
        boolean hasValue = !Double.isInfinite(min);

        SingleMetricAggregateBaseAllOfValue.Builder valueBuilder = SingleMetricAggregateBaseAllOfValue.newBuilder();
        if (hasValue) {
            valueBuilder.setDouble(min);
        } else {
            valueBuilder.setNullValue(NullValue.NULL_VALUE_NULL);
        }
        builder.setValue(valueBuilder.build());

        if (hasValue && internalMin.getFormat() != DocValueFormat.RAW) {
            builder.setValueAsString(internalMin.getValueAsString());
        }

        AggregateProtoUtils.setMetadataIfPresent(internalMin.getMetadata(), builder::setMeta);

        return builder.build();
    }
}
