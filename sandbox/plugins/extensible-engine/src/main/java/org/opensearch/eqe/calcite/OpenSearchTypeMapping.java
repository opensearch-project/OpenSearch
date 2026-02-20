/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.eqe.calcite;

import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Maps OpenSearch field type strings to Calcite {@link SqlTypeName}.
 *
 * // TODO replace this with sql plugin type mappings
 */
public final class OpenSearchTypeMapping {

    private OpenSearchTypeMapping() {}

    /**
     * Map an OpenSearch field type string to a Calcite SqlTypeName.
     */
    public static SqlTypeName mapFieldType(String fieldType) {
        if (fieldType == null) return SqlTypeName.VARCHAR;
        switch (fieldType.toLowerCase()) {
            case "long":           return SqlTypeName.BIGINT;
            case "integer":        return SqlTypeName.INTEGER;
            case "short":          return SqlTypeName.SMALLINT;
            case "byte":           return SqlTypeName.TINYINT;
            case "unsigned_long":  return SqlTypeName.DECIMAL;
            case "double":
            case "scaled_float":   return SqlTypeName.DOUBLE;
            case "float":
            case "half_float":     return SqlTypeName.FLOAT;
            case "boolean":        return SqlTypeName.BOOLEAN;
            case "date":
            case "date_nanos":     return SqlTypeName.TIMESTAMP;
            case "binary":         return SqlTypeName.VARBINARY;
            default:               return SqlTypeName.VARCHAR;
        }
    }
}
