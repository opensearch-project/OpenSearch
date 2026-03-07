/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation.support;

import org.opensearch.script.Script;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.transport.grpc.proto.request.common.ScriptProtoUtils;
import org.opensearch.transport.grpc.proto.response.common.FieldValueProtoUtils;
import org.opensearch.transport.grpc.util.ProtobufEnumUtils;

/**
 * Utility class for parsing common fields from ValuesSource-based aggregation Protocol Buffer messages.
 *
 * <p>This class mirrors {@link ValuesSourceAggregationBuilder#declareFields}, providing centralized parsing
 * logic for common fields shared across all value-source aggregations (field, missing, value_type, format,
 * script, timezone).
 *
 * <p>In REST, {@link ValuesSourceAggregationBuilder#declareFields} is called by each aggregation's PARSER
 * to declare common fields. The parameters (scriptable, formattable, timezoneAware, fieldRequired) control
 * which fields are declared. This utility provides static helper methods that aggregation proto utils classes
 * call to parse these common fields from protobuf messages.
 *
 * @see ValuesSourceAggregationBuilder#declareFields
 * @see org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder#PARSER
 * @see org.opensearch.search.aggregations.metrics.MinAggregationBuilder#PARSER
 * @see org.opensearch.search.aggregations.metrics.MaxAggregationBuilder#PARSER
 */
public class ValuesSourceAggregationProtoUtils {

    private ValuesSourceAggregationProtoUtils() {
        // Utility class
    }

    /**
     * Parses conditional ValuesSource fields based on configuration flags.
     *
     * <p>Mirrors {@link ValuesSourceAggregationBuilder#declareFields} conditional logic, handling fields
     * that are only declared based on configuration flags (format, script, timezone).
     *
     * <p>Field validation mirrors REST's declareRequiredFieldSet logic:
     * <ul>
     *   <li>If scriptable=true AND fieldRequired=true: At least ONE of [field, script] required</li>
     *   <li>If scriptable=false AND fieldRequired=true: field required</li>
     *   <li>If fieldRequired=false: Both optional</li>
     * </ul>
     *
     * @param builder The aggregation builder to populate
     * @param hasFormat Whether proto has format
     * @param format The format string from proto
     * @param hasScript Whether proto has script
     * @param scriptProto The script proto
     * @param hasField Whether proto has field (needed for validation)
     * @param scriptable Whether script field is supported
     * @param formattable Whether format field is supported
     * @param timezoneAware Whether timezone field is supported (currently not implemented)
     * @param fieldRequired Whether at least one of [field, script] is required
     * @throws IllegalArgumentException if validation fails
     */
    public static void parseConditionalFields(
        ValuesSourceAggregationBuilder<?> builder,
        // Format parameters
        boolean hasFormat,
        String format,
        // Script parameters
        boolean hasScript,
        org.opensearch.protobufs.Script scriptProto,
        // Validation parameter
        boolean hasField,
        // Configuration flags (matching REST)
        boolean scriptable,
        boolean formattable,
        boolean timezoneAware,
        boolean fieldRequired
    ) {
        // Conditional: format
        if (formattable) {
            parseFormat(builder, hasFormat, format);
        }

        // Conditional: script and validation
        if (scriptable) {
            parseScript(builder, hasScript, scriptProto);
            // Required field validation - at least ONE of [field, script] is required
            if (fieldRequired && !hasField && !hasScript) {
                throw new IllegalArgumentException(
                    "Required one of fields [field, script], but none were specified."
                );
            }
        } else {
            // Required field validation - only field is required (script not supported)
            if (fieldRequired && !hasField) {
                throw new IllegalArgumentException(
                    "Required field [field] was not specified."
                );
            }
        }

        // Conditional: timezone
        if (timezoneAware) {
            // TODO: Implement parseTimeZone when timezone support is needed
            // Currently no aggregations use this
        }
    }

    /**
     * Parses the 'field' from protobuf message.
     *
     * <p>Mirrors {@link ValuesSourceAggregationBuilder#declareFields} which declares the field parameter.
     *
     * @param builder The aggregation builder to populate
     * @param hasField Whether the proto has a field value
     * @param field The field value from proto
     */
    public static void parseField(
        ValuesSourceAggregationBuilder<?> builder,
        boolean hasField,
        String field
    ) {
        if (hasField && !field.isEmpty()) {
            builder.field(field);
        }
    }

    /**
     * Parses the 'missing' value from protobuf message.
     *
     * <p>Mirrors {@link ValuesSourceAggregationBuilder#declareFields} which declares the missing parameter.
     *
     * @param builder The aggregation builder to populate
     * @param hasMissing Whether the proto has a missing value
     * @param missingProto The missing value proto
     */
    public static void parseMissing(
        ValuesSourceAggregationBuilder<?> builder,
        boolean hasMissing,
        org.opensearch.protobufs.FieldValue missingProto
    ) {
        if (hasMissing) {
            // Don't convert strings to BytesRef for missing values - keep them as String objects
            // to match REST API behavior and ensure proper formatting in aggregation results
            Object missingValue = FieldValueProtoUtils.fromProto(missingProto, false);
            builder.missing(missingValue);
        }
    }

    /**
     * Parses the 'value_type' from protobuf message.
     *
     * <p>Mirrors {@link ValuesSourceAggregationBuilder#declareFields} which declares the value_type parameter
     * and uses {@link ValueType#lenientParse} for parsing.
     *
     * @param builder The aggregation builder to populate
     * @param hasValueType Whether the proto has a value type
     * @param valueTypeProto The value type enum from proto
     * @throws IllegalArgumentException if value type cannot be parsed
     */
    public static void parseValueType(
        ValuesSourceAggregationBuilder<?> builder,
        boolean hasValueType,
        Enum<?> valueTypeProto
    ) {
        if (hasValueType) {
            String valueTypeStr = ProtobufEnumUtils.convertToString(valueTypeProto);
            if (valueTypeStr != null) {
                ValueType valueType = ValueType.lenientParse(valueTypeStr);
                if (valueType == null) {
                    // Match REST behavior: throw exception for unknown value type
                    throw new IllegalArgumentException("Unknown value type [" + valueTypeStr + "]");
                }
                builder.userValueTypeHint(valueType);
            }
        }
    }

    /**
     * Parses the 'format' from protobuf message (only if formattable=true).
     *
     * <p>Mirrors {@link ValuesSourceAggregationBuilder#declareFields} conditional format declaration.
     *
     * @param builder The aggregation builder to populate
     * @param hasFormat Whether the proto has a format value
     * @param format The format string from proto
     */
    public static void parseFormat(
        ValuesSourceAggregationBuilder<?> builder,
        boolean hasFormat,
        String format
    ) {
        if (hasFormat && !format.isEmpty()) {
            builder.format(format);
        }
    }

    /**
     * Parses the 'script' from protobuf message (only if scriptable=true).
     *
     * <p>Mirrors {@link ValuesSourceAggregationBuilder#declareFields} conditional script declaration
     * and uses {@link Script#parse} for parsing.
     *
     * @param builder The aggregation builder to populate
     * @param hasScript Whether the proto has a script
     * @param scriptProto The script proto message
     * @throws IllegalArgumentException if script parsing fails
     */
    public static void parseScript(
        ValuesSourceAggregationBuilder<?> builder,
        boolean hasScript,
        org.opensearch.protobufs.Script scriptProto
    ) {
        if (hasScript) {
            try {
                Script script = ScriptProtoUtils.parseFromProtoRequest(scriptProto);
                builder.script(script);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse script for aggregation", e);
            }
        }
    }

    // TODO: Add parseTimeZone() when timezone support is needed
    // Mirrors ValuesSourceAggregationBuilder#declareFields conditional timezone declaration
}
