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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Adapter for PPL's {@code conv(n, fromBase, toBase)} — base conversion. Rewrites the PPL UDF
 * (registered upstream under the name {@code "CONVERT"} by {@code ConvFunction.toUDF}) to a
 * locally-declared {@link SqlFunction} whose substrait sig is mapped to the {@code conv} Rust UDF
 * (see {@code rust/src/udf/conv.rs} and the YAML signature in
 * {@code src/main/resources/opensearch_scalar_functions.yaml}).
 *
 * <p>Semantics mirror {@code Long.toString(Long.parseLong(n, fromBase), toBase)} — same as PPL's
 * {@code ConvFunction.conv}. PPL accepts a string OR a numeric first arg ({@code conv(intCol, 10,
 * 16)}), but the Rust UDF takes a string; on the Substrait/pushdown path there is no implicit
 * cast, so a numeric {@code n} fails signature resolution ("conv: arg 0 expected string, got
 * Int32"). Cast a numeric {@code n} to VARCHAR here so the Rust UDF always receives a string —
 * mirroring how {@code StrftimeFunctionAdapter} folds numeric inputs onto its canonical type.
 *
 * @opensearch.internal
 */
class ConvAdapter implements ScalarFunctionAdapter {

    static final SqlOperator LOCAL_CONV_OP = new SqlFunction(
        "conv",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000_NULLABLE,
        null,
        OperandTypes.family(SqlTypeFamily.ANY, SqlTypeFamily.INTEGER, SqlTypeFamily.INTEGER),
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        List<RexNode> operands = original.getOperands();
        if (operands.size() != 3) {
            return cluster.getRexBuilder().makeCall(original.getType(), LOCAL_CONV_OP, operands);
        }
        RexNode n = operands.get(0);
        RexNode converted = SqlTypeName.NUMERIC_TYPES.contains(n.getType().getSqlTypeName()) ? castToVarchar(n, cluster) : n;
        return cluster.getRexBuilder().makeCall(original.getType(), LOCAL_CONV_OP, List.of(converted, operands.get(1), operands.get(2)));
    }

    private static RexNode castToVarchar(RexNode operand, RelOptCluster cluster) {
        RelDataTypeFactory factory = cluster.getTypeFactory();
        RelDataType varchar = factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.VARCHAR), operand.getType().isNullable());
        RexBuilder rexBuilder = cluster.getRexBuilder();
        return rexBuilder.makeCast(varchar, operand, true, false);
    }
}
