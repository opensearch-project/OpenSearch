/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation.metrics;

import org.opensearch.protobufs.MaxAggregation;
import org.opensearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.opensearch.transport.grpc.proto.request.common.ScriptProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.aggregation.support.ValuesSourceAggregationProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.aggregation.support.ValuesSourceProtoFields;
import org.opensearch.transport.grpc.proto.response.common.FieldValueProtoUtils;

/**
 * Utility class for converting MaxAggregation Protocol Buffers to MaxAggregationBuilder.
 */
public class MaxAggregationProtoUtils {

    private MaxAggregationProtoUtils() {
        // Utility class
    }

    /**
     * Converts a Protocol Buffer MaxAggregation to a MaxAggregationBuilder.
     *
     * <p>This method parallels the REST parsing logic in {@link MaxAggregationBuilder#PARSER},
     * processing fields in the exact same sequence to ensure consistent validation and behavior.
     *
     * @param name The name of the aggregation
     * @param maxAggProto The Protocol Buffer MaxAggregation to convert
     * @return A configured MaxAggregationBuilder
     * @throws IllegalArgumentException if required fields are missing or validation fails
     */
    public static MaxAggregationBuilder fromProto(String name, MaxAggregation maxAggProto) {
        if (maxAggProto == null) {
            throw new IllegalArgumentException("MaxAggregation proto must not be null");
        }

        MaxAggregationBuilder builder = new MaxAggregationBuilder(name);

        ValuesSourceProtoFields fields = ValuesSourceProtoFields.builder()
            .field(maxAggProto.hasField() ? maxAggProto.getField() : null)
            .missing(maxAggProto.hasMissing() ? FieldValueProtoUtils.fromProto(maxAggProto.getMissing()) : null)
            .valueType(maxAggProto.hasValueType() ? maxAggProto.getValueType() : null)
            .format(maxAggProto.hasFormat() ? maxAggProto.getFormat() : null)
            .script(maxAggProto.hasScript() ? ScriptProtoUtils.parseFromProtoRequest(maxAggProto.getScript()) : null)
            .build();

        ValuesSourceAggregationProtoUtils.declareFields(
            builder,
            fields,
            /* scriptable= */ true,
            /* formattable= */ true,
            /* timezoneAware= */ false
        );

        return builder;
    }
}
