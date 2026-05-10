/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.stage;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

/**
 * Translates a Calcite {@link RelDataType} (row type) to an Arrow {@link Schema}.
 * Used by distributed stages to declare their exchange-point schema when registering
 * {@code StreamingTable} partitions with the native execution engine.
 *
 * <p>All fields are nullable for MVP.
 */
final class ArrowSchemaFromCalcite {

    private ArrowSchemaFromCalcite() {}

    /**
     * Convert a Calcite row type to an Arrow schema. All fields are nullable.
     *
     * @param rowType the Calcite row type from a RelNode fragment
     * @return the corresponding Arrow schema
     */
    public static Schema arrowSchemaFromRowType(RelDataType rowType) {
        List<Field> fields = new ArrayList<>();
        for (RelDataTypeField f : rowType.getFieldList()) {
            fields.add(toArrowField(f.getName(), f.getType()));
        }
        return new Schema(fields);
    }

    /**
     * Build an Arrow {@link Field} from a Calcite type. For scalar types this is a
     * leaf field with the appropriate {@link ArrowType}; for ARRAY this is a
     * {@code List<T>} whose single child is the recursively-converted element type
     * (Arrow names the child {@code $data$} by convention — kept here for parity with
     * Arrow's own builders so downstream tooling that walks list children by name
     * doesn't break).
     */
    private static Field toArrowField(String name, RelDataType type) {
        SqlTypeName sqlTypeName = type.getSqlTypeName();
        if (sqlTypeName == SqlTypeName.ARRAY) {
            RelDataType elementType = type.getComponentType();
            if (elementType == null) {
                throw new IllegalArgumentException(
                    "ARRAY type with no component type for field [" + name + "]; cannot derive list element schema"
                );
            }
            Field elementField = toArrowField("$data$", elementType);
            return new Field(name, new FieldType(true, ArrowType.List.INSTANCE, null), List.of(elementField));
        }
        ArrowType arrowType = toArrowType(sqlTypeName);
        return new Field(name, new FieldType(true, arrowType, null), null);
    }

    private static ArrowType toArrowType(SqlTypeName sqlTypeName) {
        switch (sqlTypeName) {
            case BIGINT:
                return new ArrowType.Int(64, true);
            case INTEGER:
                return new ArrowType.Int(32, true);
            case SMALLINT:
                return new ArrowType.Int(16, true);
            case TINYINT:
                return new ArrowType.Int(8, true);
            case DOUBLE:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case FLOAT:
            case REAL:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case BOOLEAN:
                return ArrowType.Bool.INSTANCE;
            case VARCHAR:
            case CHAR:
                return ArrowType.Utf8.INSTANCE;
            case VARBINARY:
            case BINARY:
                return ArrowType.Binary.INSTANCE;
            case DATE:
                return new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY);
            case TIME:
                return new ArrowType.Time(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, 32);
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new ArrowType.Timestamp(org.apache.arrow.vector.types.TimeUnit.MILLISECOND, null);
            default:
                throw new IllegalArgumentException("Unsupported Calcite SQL type: " + sqlTypeName);
        }
    }
}
