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
 * Used to derive the target schema for {@code RowBatchToArrowConverter} from the
 * child stage's resolved fragment row type.
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
            fields.add(fieldFromCalcite(f));
        }
        return new Schema(fields);
    }

    public static Field fieldFromCalcite(RelDataTypeField f) {
        ArrowType arrowType = toArrowType(f.getType().getSqlTypeName());
        return new Field(f.getName(), new FieldType(true, arrowType, null), null);
    }

    private static ArrowType toArrowType(SqlTypeName sqlTypeName) {
        switch (sqlTypeName) {
            case BIGINT:
                return new ArrowType.Int(64, true);
            case INTEGER:
                return new ArrowType.Int(32, true);
            case DOUBLE:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case FLOAT:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case BOOLEAN:
                return ArrowType.Bool.INSTANCE;
            case VARCHAR:
            case CHAR:
                return ArrowType.Utf8.INSTANCE;
            case VARBINARY:
            case BINARY:
                return ArrowType.Binary.INSTANCE;
            default:
                throw new IllegalArgumentException("Unsupported Calcite SQL type: " + sqlTypeName);
        }
    }
}
