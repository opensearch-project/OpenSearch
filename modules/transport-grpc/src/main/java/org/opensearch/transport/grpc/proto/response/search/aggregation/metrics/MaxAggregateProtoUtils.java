/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.metrics;

import org.opensearch.protobufs.MaxAggregate;
import org.opensearch.protobufs.NullValue;
import org.opensearch.protobufs.SingleMetricAggregateBaseAllOfValue;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.metrics.InternalMax;
import org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils;

/**
 * Utility class for converting InternalMax aggregation results to MaxAggregate protocol buffer format.
 *
 * <p>This utility mirrors the REST API serialization logic found in {@link InternalMax#doXContentBody}
 * but produces Protocol Buffer messages instead of XContent (JSON/YAML). The conversion handles:
 * <ul>
 *   <li>Maximum numeric value with special handling for empty result sets (NEGATIVE_INFINITY → NULL_VALUE)</li>
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
 * @see InternalMax
 * @see InternalMax#doXContentBody
 * @see MaxAggregate
 */
public class MaxAggregateProtoUtils {

    private MaxAggregateProtoUtils() {
        // Utility class
    }

    /**
     * Converts an InternalMax aggregation result to a MaxAggregate protobuf message.
     *
     * <p>Mirrors {@link InternalMax#doXContentBody} structure exactly.
     *
     * @param internalMax The InternalMax aggregation result from OpenSearch
     * @return A MaxAggregate protobuf message with all applicable fields populated
     * @see InternalMax#doXContentBody
     * @see MaxAggregate
     */
    public static MaxAggregate toProto(InternalMax internalMax) {
        MaxAggregate.Builder builder = MaxAggregate.newBuilder();

        // Line 100: boolean hasValue = !Double.isInfinite(max);
        double max = internalMax.getValue();
        boolean hasValue = !Double.isInfinite(max);

        // Line 101: builder.field(CommonFields.VALUE.getPreferredName(), hasValue ? max : null);
        SingleMetricAggregateBaseAllOfValue.Builder valueBuilder = SingleMetricAggregateBaseAllOfValue.newBuilder();
        if (hasValue) {
            valueBuilder.setDouble(max);
        } else {
            valueBuilder.setNullValue(NullValue.NULL_VALUE_NULL);
        }
        builder.setValue(valueBuilder.build());

        if (hasValue && internalMax.getFormat() != DocValueFormat.RAW) {
            builder.setValueAsString(internalMax.getValueAsString());
        }

        AggregateProtoUtils.setMetadataIfPresent(internalMax.getMetadata(), builder::setMeta);

        return builder.build();
    }
}
