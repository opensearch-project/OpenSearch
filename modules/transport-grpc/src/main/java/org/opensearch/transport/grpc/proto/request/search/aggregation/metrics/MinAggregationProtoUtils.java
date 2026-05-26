/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation.metrics;

import org.opensearch.protobufs.MinAggregation;
import org.opensearch.search.aggregations.metrics.MinAggregationBuilder;
import org.opensearch.transport.grpc.proto.request.common.ScriptProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.aggregation.support.ValuesSourceAggregationProtoUtils;
import org.opensearch.transport.grpc.proto.request.search.aggregation.support.ValuesSourceProtoFields;
import org.opensearch.transport.grpc.proto.response.common.FieldValueProtoUtils;

/**
 * Utility class for converting MinAggregation Protocol Buffers to MinAggregationBuilder.
 */
public class MinAggregationProtoUtils {

    private MinAggregationProtoUtils() {
        // Utility class
    }

    /**
     * Converts a Protocol Buffer MinAggregation to a MinAggregationBuilder.
     *
     * <p>This method parallels the REST parsing logic in {@link MinAggregationBuilder#PARSER},
     * processing fields in the exact same sequence to ensure consistent validation and behavior.
     *
     * @param name The name of the aggregation
     * @param minAggProto The Protocol Buffer MinAggregation to convert
     * @return A configured MinAggregationBuilder
     * @throws IllegalArgumentException if required fields are missing or validation fails
     */
    public static MinAggregationBuilder fromProto(String name, MinAggregation minAggProto) {
        if (minAggProto == null) {
            throw new IllegalArgumentException("MinAggregation proto must not be null");
        }

        MinAggregationBuilder builder = new MinAggregationBuilder(name);

        ValuesSourceProtoFields fields = ValuesSourceProtoFields.builder()
            .field(minAggProto.hasField() ? minAggProto.getField() : null)
            .missing(minAggProto.hasMissing() ? FieldValueProtoUtils.fromProto(minAggProto.getMissing()) : null)
            .valueType(minAggProto.hasValueType() ? minAggProto.getValueType() : null)
            .format(minAggProto.hasFormat() ? minAggProto.getFormat() : null)
            .script(minAggProto.hasScript() ? ScriptProtoUtils.parseFromProtoRequest(minAggProto.getScript()) : null)
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
