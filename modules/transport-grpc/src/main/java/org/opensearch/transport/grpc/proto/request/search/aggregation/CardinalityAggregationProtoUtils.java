/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation;

import org.opensearch.protobufs.CardinalityAggregation;
import org.opensearch.protobufs.CardinalityExecutionMode;
import org.opensearch.protobufs.FieldValue;
import org.opensearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.opensearch.transport.grpc.proto.request.common.ScriptProtoUtils;
import org.opensearch.transport.grpc.proto.response.common.FieldValueProtoUtils;
import org.opensearch.transport.grpc.util.ProtobufEnumUtils;

import java.lang.reflect.Method;

/**
 * Utility class for converting CardinalityAggregation Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of cardinality aggregations
 * into their corresponding OpenSearch CardinalityAggregationBuilder implementations.
 */
public class CardinalityAggregationProtoUtils {

    private CardinalityAggregationProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer CardinalityAggregation to an OpenSearch CardinalityAggregationBuilder.
     *
     * <p>This method is the gRPC equivalent of {@link CardinalityAggregationBuilder#PARSER}, which parses
     * cardinality aggregations from REST/JSON requests via {@code fromXContent}. Similar to how the parser
     * reads JSON fields, this method extracts values from the Protocol Buffer representation and creates
     * a properly configured CardinalityAggregationBuilder.
     *
     * <p>The REST-side serialization via {@link CardinalityAggregationBuilder#doXContentBody} produces
     * JSON that conceptually mirrors the protobuf structure used here.
     *
     * @param name The name of the aggregation
     * @param cardinalityAggProto The Protocol Buffer CardinalityAggregation object
     * @return A configured CardinalityAggregationBuilder instance
     * @throws IllegalArgumentException if the field value type is not supported
     * @see CardinalityAggregationBuilder#PARSER
     * @see CardinalityAggregationBuilder#doXContentBody
     */
    public static CardinalityAggregationBuilder fromProto(String name, CardinalityAggregation cardinalityAggProto) {
        CardinalityAggregationBuilder builder = new CardinalityAggregationBuilder(name);

        // Set field if present
        if (cardinalityAggProto.hasField()) {
            builder.field(cardinalityAggProto.getField());
        }

        // Set missing value if present
        if (cardinalityAggProto.hasMissing()) {
            FieldValue missingValue = cardinalityAggProto.getMissing();
            Object missing = FieldValueProtoUtils.fromProto(missingValue, false);
            builder.missing(missing);
        }

        // Set script if present
        if (cardinalityAggProto.hasScript()) {
            try {
                builder.script(ScriptProtoUtils.parseFromProtoRequest(cardinalityAggProto.getScript()));
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse script for cardinality aggregation", e);
            }
        }

        // Set precision threshold if present
        if (cardinalityAggProto.hasPrecisionThreshold()) {
            builder.precisionThreshold(cardinalityAggProto.getPrecisionThreshold());
        }

        // Set execution hint if present (added in OpenSearch 2.19.1)
        // Use reflection for forward-compatibility: if the OpenSearch version doesn't support
        // this field yet, it will be silently ignored rather than causing a compilation error.
        if (cardinalityAggProto.hasExecutionHint()) {
            String executionHint = convertExecutionMode(cardinalityAggProto.getExecutionHint());
            if (executionHint != null) {
                try {
                    Method method = CardinalityAggregationBuilder.class.getMethod("executionHint", String.class);
                    method.invoke(builder, executionHint);
                } catch (NoSuchMethodException e) {
                    // Method doesn't exist in OpenSearch < 2.19.1 - silently ignore
                } catch (Exception e) {
                    throw new IllegalArgumentException("Failed to set execution hint for cardinality aggregation", e);
                }
            }
        }

        return builder;
    }

    /**
     * Converts a Protocol Buffer CardinalityExecutionMode to its string representation.
     * Supports execution hints added in OpenSearch 2.19.1.
     *
     * @param mode The Protocol Buffer CardinalityExecutionMode
     * @return The string representation of the execution mode, or null if unspecified
     */
    private static String convertExecutionMode(CardinalityExecutionMode mode) {
        if (mode == CardinalityExecutionMode.CARDINALITY_EXECUTION_MODE_UNSPECIFIED) {
            return null;
        }
        return ProtobufEnumUtils.convertToString(mode);
    }
}
