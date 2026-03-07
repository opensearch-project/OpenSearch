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
import org.opensearch.transport.grpc.proto.request.search.aggregation.support.ValuesSourceAggregationProtoUtils;

/**
 * Utility class for converting MaxAggregation Protocol Buffers to MaxAggregationBuilder objects.
 *
 * <p>Field processing follows the exact sequence defined in {@link MaxAggregationBuilder#PARSER}
 * to ensure identical behavior with REST API parsing. This includes:
 * <ol>
 *   <li>ValuesSourceAggregationBuilder fields (field, missing, value_type, format, script)</li>
 * </ol>
 *
 * @see MaxAggregationBuilder#PARSER
 * @see org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder#declareFields
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
     * @param name The name of the aggregation (from parent container map key)
     * @param maxAggProto The Protocol Buffer MaxAggregation to convert
     * @return A configured MaxAggregationBuilder
     * @throws IllegalArgumentException if required fields are missing or validation fails
     * @see MaxAggregationBuilder#PARSER
     */
    public static MaxAggregationBuilder fromProto(String name, MaxAggregation maxAggProto) {
        if (maxAggProto == null) {
            throw new IllegalArgumentException("MaxAggregation proto must not be null");
        }
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Aggregation name must not be null or empty");
        }

        MaxAggregationBuilder builder = new MaxAggregationBuilder(name);

        // ========================================
        // ValuesSourceAggregationBuilder common fields
        // ========================================
        // @see ValuesSourceAggregationBuilder#declareFields called from MaxAggregationBuilder.PARSER
        // For max aggregation: scriptable=true, formattable=true, timezoneAware=false, fieldRequired=true

        // Always-declared fields (ValuesSourceAggregationBuilder lines 83-103)
        ValuesSourceAggregationProtoUtils.parseField(builder, maxAggProto.hasField(), maxAggProto.getField());
        ValuesSourceAggregationProtoUtils.parseMissing(builder, maxAggProto.hasMissing(), maxAggProto.getMissing());
        ValuesSourceAggregationProtoUtils.parseValueType(builder, maxAggProto.hasValueType(), maxAggProto.getValueType());

        // Conditional fields based on configuration (ValuesSourceAggregationBuilder lines 105-141)
        ValuesSourceAggregationProtoUtils.parseConditionalFields(
            builder,
            maxAggProto.hasFormat(),
            maxAggProto.getFormat(),
            maxAggProto.hasScript(),
            maxAggProto.getScript(),
            maxAggProto.hasField() && !maxAggProto.getField().isEmpty(),
            /* scriptable= */ true,
            /* formattable= */ true,
            /* timezoneAware= */ false,
            /* fieldRequired= */ true
        );

        return builder;
    }
}
