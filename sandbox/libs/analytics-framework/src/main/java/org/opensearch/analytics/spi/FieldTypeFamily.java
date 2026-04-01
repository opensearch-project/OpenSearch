/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Logical field type families for capability matching.
 * Groups OpenSearch {@code MappedFieldType.typeName()} strings into
 * coarse categories that backends declare support for.
 *
 * @opensearch.internal
 */
public enum FieldTypeFamily {
    NUMERIC,
    KEYWORD,
    TEXT,
    DATE,
    BOOLEAN,
    IP,
    GEO_POINT,
    GEO_SHAPE,
    BINARY,
    NESTED,
    OBJECT,
    RANGE,
    COMPLETION;

    /**
     * Maps an OpenSearch mapping type string ({@code MappedFieldType.typeName()})
     * to a FieldTypeFamily. Returns null if the type is not recognized.
     */
    public static FieldTypeFamily fromMappingType(String mappingType) {
        if (mappingType == null) {
            return null;
        }
        return switch (mappingType) {
            case "integer", "long", "short", "byte", "float", "double",
                 "half_float", "scaled_float", "unsigned_long" -> NUMERIC;
            case "keyword", "constant_keyword", "wildcard" -> KEYWORD;
            case "text", "match_only_text" -> TEXT;
            case "date", "date_nanos" -> DATE;
            case "boolean" -> BOOLEAN;
            case "ip" -> IP;
            case "geo_point", "point" -> GEO_POINT;
            case "geo_shape", "shape" -> GEO_SHAPE;
            case "binary" -> BINARY;
            case "nested" -> NESTED;
            case "object", "flat_object" -> OBJECT;
            case "integer_range", "float_range", "long_range",
                 "double_range", "date_range", "ip_range" -> RANGE;
            case "completion" -> COMPLETION;
            default -> null;
        };
    }
}
