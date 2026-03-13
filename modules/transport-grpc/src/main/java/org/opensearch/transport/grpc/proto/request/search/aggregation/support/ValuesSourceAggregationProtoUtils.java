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
import org.opensearch.transport.grpc.proto.request.common.ObjectParserProtoUtils;
import org.opensearch.transport.grpc.util.ProtobufEnumUtils;

import java.util.function.Function;

/**
 * Utility class for parsing common fields from ValuesSource-based aggregation Protocol Buffer messages.
 * Mirrors {@link ValuesSourceAggregationBuilder#declareFields} using {@link ObjectParserProtoUtils}.
 */
public class ValuesSourceAggregationProtoUtils {

    private ValuesSourceAggregationProtoUtils() {
        // Utility class
    }

    /**
     * Declares common fields for ValuesSource aggregations, mirroring {@link ValuesSourceAggregationBuilder#declareFields}.
     *
     * <p>Uses {@link ObjectParserProtoUtils} for explicit, declarative field parsing that mirrors REST's ObjectParser pattern.
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
     * <p>Uses {@link ObjectParserProtoUtils} for explicit, declarative field parsing that mirrors REST's ObjectParser pattern.
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
        ObjectParserProtoUtils.declareField(
            builder,
            ValuesSourceAggregationBuilder::field,
            fields.getField(),
            Function.identity(),
            ParseField.CommonFields.FIELD.getPreferredName()
        );

        ObjectParserProtoUtils.declareField(
            builder,
            ValuesSourceAggregationBuilder::missing,
            fields.getMissing(),
            Function.identity(),
            ParseField.CommonFields.MISSING.getPreferredName()
        );

        ObjectParserProtoUtils.declareField(
            builder,
            ValuesSourceAggregationBuilder::userValueTypeHint,
            fields.getValueTypeProto(),
            proto -> {
                String valueTypeStr = ProtobufEnumUtils.convertToString(proto);
                if (valueTypeStr == null) {
                    return null;
                }
                ValueType valueType = ValueType.lenientParse(valueTypeStr);
                if (valueType == null) {
                    throw new IllegalArgumentException("Unknown value type [" + valueTypeStr + "]");
                }
                return valueType;
            },
            ValueType.VALUE_TYPE.getPreferredName()
        );

        if (formattable) {
            ObjectParserProtoUtils.declareField(
                builder,
                ValuesSourceAggregationBuilder::format,
                fields.getFormat(),
                Function.identity(),
                ParseField.CommonFields.FORMAT.getPreferredName()
            );
        }

        if (scriptable) {
            ObjectParserProtoUtils.declareField(
                builder,
                ValuesSourceAggregationBuilder::script,
                fields.getScript(),
                Function.identity(),
                Script.SCRIPT_PARSE_FIELD.getPreferredName()
            );

            if (fieldRequired) {
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
