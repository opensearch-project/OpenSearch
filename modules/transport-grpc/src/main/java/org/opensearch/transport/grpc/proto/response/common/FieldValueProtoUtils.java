/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.common;

import org.opensearch.protobufs.FieldValue;

import static org.opensearch.index.query.AbstractQueryBuilder.maybeConvertToBytesRef;

/**
 * Utility class for converting between generic Java objects and Protocol Buffer FieldValue type.
 * This class provides methods to transform Java objects of various types (primitives, strings,
 * maps, etc.) into their corresponding Protocol Buffer representations for gRPC communication,
 * and vice versa.
 */
public class FieldValueProtoUtils {

    private FieldValueProtoUtils() {

    }

    /**
     * Converts a generic Java Object to its Protocol Buffer FieldValue representation.
     * This method handles various Java types (Integer, Long, Double, Float, String, Boolean, Enum, Map)
     * and converts them to the appropriate FieldValue type.
     *
     * @param javaObject The Java object to convert
     * @return A Protocol Buffer FieldValue representation of the Java object
     * @throws IllegalArgumentException if the Java object type cannot be converted
     */
    public static FieldValue toProto(Object javaObject) {
        FieldValue.Builder fieldValueBuilder = FieldValue.newBuilder();
        toProto(javaObject, fieldValueBuilder);
        return fieldValueBuilder.build();
    }

    /**
     * Converts a generic Java Object to its Protocol Buffer FieldValue representation.
     * It handles various Java types (Integer, Long, Double, Float, String, Boolean, Enum, Map)
     * and converts them to the appropriate FieldValue type.
     *
     * @param javaObject The Java object to convert
     * @param fieldValueBuilder The builder to populate with the Java object data
     * @throws IllegalArgumentException if the Java object type cannot be converted
     */
    public static void toProto(Object javaObject, FieldValue.Builder fieldValueBuilder) {
        if (javaObject == null) {
            throw new IllegalArgumentException("Cannot convert null to FieldValue");
        }

        switch (javaObject) {
            case String s -> fieldValueBuilder.setString(s);
            case Integer i -> {
                org.opensearch.protobufs.GeneralNumber.Builder num = org.opensearch.protobufs.GeneralNumber.newBuilder();
                num.setInt32Value(i);
                fieldValueBuilder.setGeneralNumber(num.build());
            }
            case Long l -> {
                org.opensearch.protobufs.GeneralNumber.Builder num = org.opensearch.protobufs.GeneralNumber.newBuilder();
                num.setInt64Value(l);
                fieldValueBuilder.setGeneralNumber(num.build());
            }
            case Double d -> {
                org.opensearch.protobufs.GeneralNumber.Builder num = org.opensearch.protobufs.GeneralNumber.newBuilder();
                num.setDoubleValue(d);
                fieldValueBuilder.setGeneralNumber(num.build());
            }
            case Float f -> {
                org.opensearch.protobufs.GeneralNumber.Builder num = org.opensearch.protobufs.GeneralNumber.newBuilder();
                num.setFloatValue(f);
                fieldValueBuilder.setGeneralNumber(num.build());
            }
            case Boolean b -> fieldValueBuilder.setBool(b);
            case Enum<?> e -> fieldValueBuilder.setString(e.toString());
            default -> throw new IllegalArgumentException("Cannot convert " + javaObject + " to FieldValue");
        }
    }

    /**
     * Converts a Protocol Buffer FieldValue to its corresponding Java object representation.
     * This method handles various FieldValue types (GeneralNumber, String, Boolean, NullValue)
     * and converts them to the appropriate Java types. String values are processed through
     * maybeConvertToBytesRef for consistency with OpenSearch query processing.
     *
     * @param fieldValue The Protocol Buffer FieldValue to convert
     * @return A Java object representation of the FieldValue, or null if the FieldValue represents null
     * @throws IllegalArgumentException if the FieldValue type is not recognized
     */
    public static Object fromProto(FieldValue fieldValue) {
        return fromProto(fieldValue, true);
    }

    /**
     * Converts a Protocol Buffer FieldValue to its corresponding Java object representation.
     * This method handles various FieldValue types (GeneralNumber, String, Boolean, NullValue)
     * and converts them to the appropriate Java types.
     *
     * @param fieldValue The Protocol Buffer FieldValue to convert
     * @param convertStringsToBytesRef Whether to process string values through maybeConvertToBytesRef
     * @return A Java object representation of the FieldValue, or null if the FieldValue represents null
     * @throws IllegalArgumentException if the FieldValue type is not recognized
     */
    public static Object fromProto(FieldValue fieldValue, boolean convertStringsToBytesRef) {
        if (fieldValue == null) {
            return null;
        }

        if (fieldValue.hasGeneralNumber()) {
            org.opensearch.protobufs.GeneralNumber generalNumber = fieldValue.getGeneralNumber();
            switch (generalNumber.getValueCase()) {
                case INT32_VALUE:
                    return generalNumber.getInt32Value();
                case INT64_VALUE:
                    return generalNumber.getInt64Value();
                case FLOAT_VALUE:
                    return generalNumber.getFloatValue();
                case DOUBLE_VALUE:
                    return generalNumber.getDoubleValue();
                default:
                    throw new IllegalArgumentException("Unsupported general number type: " + generalNumber.getValueCase());
            }
        } else if (fieldValue.hasString()) {
            return convertStringsToBytesRef ? maybeConvertToBytesRef(fieldValue.getString()) : fieldValue.getString();
        } else if (fieldValue.hasBool()) {
            return fieldValue.getBool();
        } else if (fieldValue.hasNullValue()) {
            return null;
        } else {
            throw new IllegalArgumentException("FieldValue type not recognized");
        }
    }
}
