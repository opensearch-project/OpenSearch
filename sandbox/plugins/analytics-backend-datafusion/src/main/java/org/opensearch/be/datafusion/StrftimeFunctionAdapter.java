/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Rewrites PPL {@code strftime(value, format)} → local {@link #STRFTIME}, which Isthmus
 * serializes under Substrait extension name {@code "strftime"} (matched by the Rust UDF
 * in {@code rust/src/udf/strftime.rs}). Folds every numeric source onto a single Float64
 * signature via {@code CAST AS DOUBLE}; timestamp/date inputs forward verbatim (the Rust
 * UDF's {@code coerce_types} canonicalizes them to {@code Timestamp(Microsecond, None)}).
 *
 * @opensearch.internal
 */
class StrftimeFunctionAdapter implements ScalarFunctionAdapter {

    static final SqlFunction STRFTIME = new SqlFunction(
        "strftime",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR,
        null,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.CHARACTER),
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        List<RexNode> operands = original.getOperands();
        if (operands.size() != 2) {
            return original;
        }
        RexNode value = operands.get(0);
        RexNode format = operands.get(1);
        SqlTypeName valueType = value.getType().getSqlTypeName();

        // Fold every numeric (and string — IT covers the Calcite auto-coerce path) source onto
        // a single Float64 signature; DOUBLE preserves fractional-seconds precision. Timestamp /
        // date / time inputs forward verbatim — the Rust coerce_types canonicalizes them.
        RexNode normalizedValue;
        if (isIntegralNumeric(valueType)
            || valueType == SqlTypeName.FLOAT
            || valueType == SqlTypeName.REAL
            || valueType == SqlTypeName.DECIMAL
            || SqlTypeName.CHAR_TYPES.contains(valueType)) {
            normalizedValue = castTo(value, SqlTypeName.DOUBLE, cluster);
        } else {
            normalizedValue = value;
        }

        return cluster.getRexBuilder().makeCall(original.getType(), STRFTIME, List.of(normalizedValue, format));
    }

    private static boolean isIntegralNumeric(SqlTypeName type) {
        return type == SqlTypeName.TINYINT || type == SqlTypeName.SMALLINT || type == SqlTypeName.INTEGER || type == SqlTypeName.BIGINT;
    }

    private static RexNode castTo(RexNode operand, SqlTypeName target, RelOptCluster cluster) {
        if (operand.getType().getSqlTypeName() == target) {
            return operand;
        }
        RelDataTypeFactory factory = cluster.getTypeFactory();
        RelDataType targetType = factory.createTypeWithNullability(factory.createSqlType(target), operand.getType().isNullable());
        return cluster.getRexBuilder().makeCast(targetType, operand);
    }
}
