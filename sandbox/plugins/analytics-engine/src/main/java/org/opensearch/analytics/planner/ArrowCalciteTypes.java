/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Bidirectional Arrow ↔ Calcite type converter for single types.
 * This is the sole authority for type reconciliation between the
 * {@code AggregateFunction.intermediateFields} Arrow types and
 * Calcite's {@code RelDataType} system in the decomposition resolver.
 */
public final class ArrowCalciteTypes {

    private ArrowCalciteTypes() {}

    /**
     * Convert an Arrow type to the corresponding Calcite {@link RelDataType}.
     */
    public static RelDataType toCalcite(ArrowType t, RelDataTypeFactory f) {
        return switch (t) {
            case ArrowType.Int i when i.getBitWidth() == 64 -> f.createSqlType(SqlTypeName.BIGINT);
            case ArrowType.Int i when i.getBitWidth() == 32 -> f.createSqlType(SqlTypeName.INTEGER);
            case ArrowType.FloatingPoint fp when fp.getPrecision() == FloatingPointPrecision.DOUBLE -> f.createSqlType(SqlTypeName.DOUBLE);
            case ArrowType.FloatingPoint fp when fp.getPrecision() == FloatingPointPrecision.SINGLE -> f.createSqlType(SqlTypeName.REAL);
            case ArrowType.Utf8 u -> f.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
            case ArrowType.Binary b -> f.createSqlType(SqlTypeName.VARBINARY, Integer.MAX_VALUE);
            case ArrowType.Bool b -> f.createSqlType(SqlTypeName.BOOLEAN);
            default -> throw new IllegalArgumentException("Unsupported Arrow type: " + t);
        };
    }

    /**
     * Convert a Calcite {@link RelDataType} to the corresponding Arrow type.
     */
    public static ArrowType toArrow(RelDataType t) {
        return switch (t.getSqlTypeName()) {
            case BIGINT -> new ArrowType.Int(64, true);
            case INTEGER -> new ArrowType.Int(32, true);
            case DOUBLE -> new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case REAL, FLOAT -> new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case VARCHAR, CHAR -> ArrowType.Utf8.INSTANCE;
            case VARBINARY, BINARY -> ArrowType.Binary.INSTANCE;
            case BOOLEAN -> ArrowType.Bool.INSTANCE;
            default -> throw new IllegalArgumentException("Unsupported Calcite type: " + t.getSqlTypeName());
        };
    }
}
