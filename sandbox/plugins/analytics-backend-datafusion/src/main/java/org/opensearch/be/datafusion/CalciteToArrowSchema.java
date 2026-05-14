/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
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
 * Minimal Calcite {@link RelDataType} → Arrow {@link Schema} converter. Used by the M1
 * broadcast dispatcher to build a fallback schema for the build-side capture sink so an
 * all-empty build side still registers a memtable with the right row type on the probe.
 *
 * <p>Covers the common types analytics-engine emits today (numerics, varchar, boolean, date,
 * timestamp). Anything outside the whitelist falls back to nullable {@code Utf8}, which is
 * acceptable because the fallback schema is only used when the actual broadcast payload has
 * no rows — the join produces zero matches and the column's storage type doesn't matter.
 *
 * <p>If a future query needs a richer type (e.g. struct, list, map) on a broadcast build, the
 * fallback path will register a memtable with a coerced {@code Utf8} column for that field.
 * That can mismatch the probe-side join's expected column type and will surface as a plan
 * error from DataFusion — at which point the solution is to add the missing case here, not to
 * silently coerce. The catch-all is a deliberate no-row-correctness floor, not a
 * type-system contract.
 *
 * @opensearch.internal
 */
final class CalciteToArrowSchema {

    private CalciteToArrowSchema() {}

    static Schema convert(RelDataType rowType) {
        List<Field> fields = new ArrayList<>(rowType.getFieldCount());
        for (RelDataTypeField field : rowType.getFieldList()) {
            fields.add(new Field(field.getName(), toFieldType(field.getType()), null));
        }
        return new Schema(fields);
    }

    private static FieldType toFieldType(RelDataType type) {
        boolean nullable = type.isNullable();
        return FieldType.nullable(toArrowType(type)); // nullable=true is safe; it's a fallback
            // — see class javadoc. Override callers that care about non-null can do their own check.
    }

    private static ArrowType toArrowType(RelDataType type) {
        SqlTypeName sql = type.getSqlTypeName();
        if (sql == null) {
            return new ArrowType.Utf8();
        }
        switch (sql) {
            case TINYINT:
                return new ArrowType.Int(8, true);
            case SMALLINT:
                return new ArrowType.Int(16, true);
            case INTEGER:
                return new ArrowType.Int(32, true);
            case BIGINT:
                return new ArrowType.Int(64, true);
            case FLOAT:
            case REAL:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
            case DOUBLE:
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case BOOLEAN:
                return ArrowType.Bool.INSTANCE;
            case CHAR:
            case VARCHAR:
                return new ArrowType.Utf8();
            case BINARY:
            case VARBINARY:
                return new ArrowType.Binary();
            case DATE:
                return new ArrowType.Date(DateUnit.DAY);
            case TIME:
                return new ArrowType.Time(TimeUnit.MILLISECOND, 32);
            case TIMESTAMP:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case DECIMAL: {
                int precision = type.getPrecision();
                int scale = type.getScale();
                // Arrow's Decimal128 supports up to precision 38; cap conservatively for the fallback path.
                int safePrecision = precision <= 0 ? 38 : Math.min(precision, 38);
                int safeScale = scale < 0 ? 0 : Math.min(scale, safePrecision);
                return new ArrowType.Decimal(safePrecision, safeScale, 128);
            }
            default:
                // See class javadoc — last-resort coercion for the fallback-only path.
                return new ArrowType.Utf8();
        }
    }
}
