/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.type.SqlTypeName;

import java.util.EnumSet;
import java.util.Set;

/**
 * Granular field types for capability matching. Each value maps to an OpenSearch
 * mapping type string. Convenience grouping methods for common families.
 *
 * @opensearch.internal
 */
public enum FieldType {
    // Numeric
    INTEGER("integer"),
    LONG("long"),
    SHORT("short"),
    BYTE("byte"),
    FLOAT("float"),
    DOUBLE("double"),
    HALF_FLOAT("half_float"),
    SCALED_FLOAT("scaled_float"),
    UNSIGNED_LONG("unsigned_long"),

    // Keyword
    KEYWORD("keyword"),
    CONSTANT_KEYWORD("constant_keyword"),
    WILDCARD_FIELD("wildcard"),

    // Text
    TEXT("text"),
    MATCH_ONLY_TEXT("match_only_text"),

    // Date
    DATE("date"),
    DATE_NANOS("date_nanos"),

    // Singular
    BOOLEAN("boolean"),
    IP("ip"),
    GEO_POINT("geo_point"),
    POINT("point"),
    GEO_SHAPE("geo_shape"),
    SHAPE("shape"),
    BINARY("binary"),
    NESTED("nested"),
    OBJECT("object"),
    FLAT_OBJECT("flat_object"),
    COMPLETION("completion"),

    // ── Composite ────────────────────────────────────────────────────
    /**
     * Array-typed expression result. Used for the return-type slot of array-producing scalar
     * functions (PPL {@code array(…)}, {@code array_slice}, {@code array_distinct}). Has no
     * OpenSearch mapping equivalent — arrays in OpenSearch are multi-value fields with the
     * underlying element type, not a separate type. The mapping string is {@code "array"} as a
     * placeholder; {@link #fromMappingType} keeps working unchanged because no source
     * advertises that mapping string.
     */
    ARRAY("array"),
    /**
     * Models Calcite MAP return types for scalar functions such as PPL {@code parse}
     * (parse → {@code map<varchar, varchar>} of named groups). No corresponding OpenSearch
     * mapping type; {@link #fromMappingType} won't match {@code "map"} because no real OS
     * mapping uses that name.
     */
    MAP("map");

    private final String mappingType;

    FieldType(String mappingType) {
        this.mappingType = mappingType;
    }

    public String getMappingType() {
        return mappingType;
    }

    /** All numeric field types. */
    public static Set<FieldType> numeric() {
        return EnumSet.of(INTEGER, LONG, SHORT, BYTE, FLOAT, DOUBLE, HALF_FLOAT, SCALED_FLOAT, UNSIGNED_LONG);
    }

    /** All keyword-like field types. */
    public static Set<FieldType> keyword() {
        return EnumSet.of(KEYWORD, CONSTANT_KEYWORD, WILDCARD_FIELD);
    }

    /** All text field types. */
    public static Set<FieldType> text() {
        return EnumSet.of(TEXT, MATCH_ONLY_TEXT);
    }

    /** All date field types. */
    public static Set<FieldType> date() {
        return EnumSet.of(DATE, DATE_NANOS);
    }

    /** Maps an OpenSearch mapping type string to a FieldType. Returns null if not recognized. */
    public static FieldType fromMappingType(String type) {
        if (type == null) {
            return null;
        }
        for (FieldType fieldType : values()) {
            if (fieldType.mappingType.equals(type)) {
                return fieldType;
            }
        }
        return null;
    }

    /** Maps a Calcite SqlTypeName to a FieldType. Returns null if not recognized. */
    public static FieldType fromSqlTypeName(SqlTypeName sqlTypeName) {
        if (sqlTypeName == null) {
            return null;
        }
        return switch (sqlTypeName) {
            case TINYINT -> BYTE;
            case SMALLINT -> SHORT;
            case INTEGER -> FieldType.INTEGER;
            case BIGINT -> LONG;
            case FLOAT, REAL -> FieldType.FLOAT;
            case DOUBLE, DECIMAL -> FieldType.DOUBLE;
            case CHAR, VARCHAR -> KEYWORD;
            case DATE -> FieldType.DATE;
            case TIME, TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE -> FieldType.DATE;
            case BOOLEAN -> FieldType.BOOLEAN;
            case BINARY, VARBINARY -> FieldType.BINARY;
            case ARRAY -> FieldType.ARRAY;
            case MAP -> FieldType.MAP;
            default -> null;
        };
    }
}
