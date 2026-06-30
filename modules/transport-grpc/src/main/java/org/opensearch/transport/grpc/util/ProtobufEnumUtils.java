/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.util;

import java.util.Locale;

/**
 * Utility class for converting protobuf enum values to OpenSearch string representations.
 * This class provides a simple method to convert protobuf enums by removing the message
 * name prefix and converting to snake_case.
 *
 * Examples:
 * - SORT_ORDER_ASC → "asc"
 * - FIELD_TYPE_LONG → "long"
 * - SORT_MODE_MIN → "min"
 *
 * @opensearch.internal
 */
public class ProtobufEnumUtils {

    private ProtobufEnumUtils() {
        // Utility class
    }

    /**
     * Converts protobuf enum to string by removing the prefix and converting to lowercase.
     *
     * @param protobufEnum The protobuf enum value
     * @return The converted string value in lowercase, or null if input is null
     */
    public static String convertToString(Enum<?> protobufEnum) {
        if (protobufEnum == null) {
            return null;
        }

        String enumName = protobufEnum.name();

        String messageName = protobufEnum.getClass().getSimpleName();

        String prefix = camelCaseToSnakeCase(messageName).toUpperCase(Locale.ROOT) + "_";

        if (enumName.startsWith(prefix)) {
            return enumName.substring(prefix.length()).toLowerCase(Locale.ROOT);
        }

        return enumName.toLowerCase(Locale.ROOT);
    }

    /**
     * Converts CamelCase to snake_case.
     */
    private static String camelCaseToSnakeCase(String camelCase) {
        return camelCase.replaceAll("([a-z])([A-Z])", "$1_$2");
    }
}
