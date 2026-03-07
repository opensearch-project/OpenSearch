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
import org.opensearch.transport.grpc.proto.request.search.aggregation.support.ValuesSourceAggregationProtoUtils;

/**
 * Utility class for converting MinAggregation Protocol Buffers to MinAggregationBuilder objects.
 *
 * <p>Field processing follows the exact sequence defined in {@link MinAggregationBuilder#PARSER}
 * to ensure identical behavior with REST API parsing. This includes:
 * <ol>
 *   <li>ValuesSourceAggregationBuilder fields (field, missing, value_type, format, script)</li>
 * </ol>
 *
 * @see MinAggregationBuilder#PARSER
 * @see org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder#declareFields
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
     * @param name The name of the aggregation (from parent container map key)
     * @param minAggProto The Protocol Buffer MinAggregation to convert
     * @return A configured MinAggregationBuilder
     * @throws IllegalArgumentException if required fields are missing or validation fails
     * @see MinAggregationBuilder#PARSER
     */
    public static MinAggregationBuilder fromProto(String name, MinAggregation minAggProto) {
        if (minAggProto == null) {
            throw new IllegalArgumentException("MinAggregation proto must not be null");
        }
        if (name == null || name.isEmpty()) {
            throw new IllegalArgumentException("Aggregation name must not be null or empty");
        }

        MinAggregationBuilder builder = new MinAggregationBuilder(name);

        // ========================================
        // ValuesSourceAggregationBuilder common fields
        // ========================================
        // @see ValuesSourceAggregationBuilder#declareFields called from MinAggregationBuilder.PARSER
        // For min aggregation: scriptable=true, formattable=true, timezoneAware=false, fieldRequired=true

        // Always-declared fields
        ValuesSourceAggregationProtoUtils.parseField(builder, minAggProto.hasField(), minAggProto.getField());
        ValuesSourceAggregationProtoUtils.parseMissing(builder, minAggProto.hasMissing(), minAggProto.getMissing());
        ValuesSourceAggregationProtoUtils.parseValueType(builder, minAggProto.hasValueType(), minAggProto.getValueType());

        // Conditional fields based on configuration
        ValuesSourceAggregationProtoUtils.parseConditionalFields(
            builder,
            minAggProto.hasFormat(),
            minAggProto.getFormat(),
            minAggProto.hasScript(),
            minAggProto.getScript(),
            minAggProto.hasField() && !minAggProto.getField().isEmpty(),
            /* scriptable= */ true,
            /* formattable= */ true,
            /* timezoneAware= */ false,
            /* fieldRequired= */ true
        );

        return builder;
    }
}
