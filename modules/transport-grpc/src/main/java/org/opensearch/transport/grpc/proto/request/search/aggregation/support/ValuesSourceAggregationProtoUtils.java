/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation.support;

import org.opensearch.core.ParseField;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.transport.grpc.util.ProtobufEnumUtils;

/**
 * Utility class for parsing common fields from ValuesSource-based aggregation Protocol Buffer messages.
 */
public class ValuesSourceAggregationProtoUtils {

    private ValuesSourceAggregationProtoUtils() {
        // Utility class
    }

    /**
     * Declares common fields for ValuesSource aggregations, mirroring {@link ValuesSourceAggregationBuilder#declareFields}.
     *
     * <p>Directly sets fields on the builder, mirroring REST's ObjectParser pattern.
     *
     * @param builder The aggregation builder to set fields on
     * @param fields Wrapper containing all ValuesSource field values from proto
     * @param scriptable Whether script field is supported
     * @param formattable Whether format field is supported
     * @param timezoneAware Whether timezone field is supported
     * @see ValuesSourceAggregationBuilder#declareFields
     */
    public static void declareFields(
        ValuesSourceAggregationBuilder<?> builder,
        ValuesSourceProtoFields fields,
        boolean scriptable,
        boolean formattable,
        boolean timezoneAware
    ) {
        declareFields(builder, fields, scriptable, formattable, timezoneAware, true);
    }

    /**
     * Declares common fields for ValuesSource aggregations, mirroring {@link ValuesSourceAggregationBuilder#declareFields}.
     *
     * <p>Directly sets field values when present, mirroring REST's ObjectParser behavior without the abstraction layer.
     *
     * @param builder The aggregation builder to set fields on
     * @param fields Wrapper containing all ValuesSource field values from proto
     * @param scriptable Whether script field is supported
     * @param formattable Whether format field is supported
     * @param timezoneAware Whether timezone field is supported
     * @param fieldRequired Whether field or script is required
     * @see ValuesSourceAggregationBuilder#declareFields
     */
    public static void declareFields(
        ValuesSourceAggregationBuilder<?> builder,
        ValuesSourceProtoFields fields,
        boolean scriptable,
        boolean formattable,
        boolean timezoneAware,
        boolean fieldRequired
    ) {
        // Field declaration - mirrors REST ObjectParser.declareField behavior
        if (fields.getField() != null) {
            try {
                builder.field(fields.getField());
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse [field]", e);
            }
        }

        // Missing value declaration
        if (fields.getMissing() != null) {
            try {
                builder.missing(fields.getMissing());
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse [missing]", e);
            }
        }

        // Value type declaration with transformation
        if (fields.getValueTypeProto() != null) {
            try {
                String valueTypeStr = ProtobufEnumUtils.convertToString(fields.getValueTypeProto());
                if (valueTypeStr != null) {
                    ValueType valueType = ValueType.lenientParse(valueTypeStr);
                    if (valueType == null) {
                        throw new IllegalArgumentException("Unknown value type [" + valueTypeStr + "]");
                    }
                    builder.userValueTypeHint(valueType);
                }
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to parse [value_type]", e);
            }
        }

        // Format declaration (conditional)
        if (formattable) {
            if (fields.getFormat() != null) {
                try {
                    builder.format(fields.getFormat());
                } catch (Exception e) {
                    throw new IllegalArgumentException("Failed to parse [format]", e);
                }
            }
        }

        // Field/script requirement validation
        if (scriptable) {
            if (fields.getScript() != null) {
                try {
                    builder.script(fields.getScript());
                } catch (Exception e) {
                    throw new IllegalArgumentException("Failed to parse [script]", e);
                }
            }

            if (fieldRequired) {
                // throw exception when neither field nor script is specified
                if (fields.getField() == null && fields.getScript() == null) {
                    throw new IllegalArgumentException(
                        "Required one of fields ["
                            + ParseField.CommonFields.FIELD.getPreferredName()
                            + ", "
                            + Script.SCRIPT_PARSE_FIELD.getPreferredName()
                            + "], but none were specified."
                    );
                }
            }
        } else {
            if (fieldRequired) {
                if (fields.getField() == null) {
                    throw new IllegalArgumentException(
                        "Required field [" + ParseField.CommonFields.FIELD.getPreferredName() + "] was not specified."
                    );
                }
            }
        }

        if (timezoneAware) {
            throw new UnsupportedOperationException(
                "Timezone field is not yet supported in gRPC aggregations"
            );
        }
    }
}
