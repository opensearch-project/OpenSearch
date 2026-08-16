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
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.ArrayList;
import java.util.List;

/**
 * Normalises operand types for {@code SqlLibraryOperators.CONCAT_FUNCTION} (variadic CONCAT).
 * Substrait's {@code concat} extension declares CONSISTENT parameter consistency, but Calcite
 * leaves short string literals as {@code CHAR}/FixedChar alongside VARCHAR field refs — isthmus
 * rejects the mixed-type call. This adapter casts every non-VARCHAR operand to VARCHAR
 * (preserving nullability) so substrait sees a uniform-typed variadic call.
 */
class ConcatVariadicAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        List<RexNode> operands = original.getOperands();
        if (operands.isEmpty()) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
        boolean changed = false;
        List<RexNode> normalised = new ArrayList<>(operands.size());
        for (RexNode operand : operands) {
            SqlTypeName sqlType = operand.getType().getSqlTypeName();
            if (sqlType == SqlTypeName.VARCHAR) {
                normalised.add(operand);
                continue;
            }
            // CHAR / FixedChar / other string token → cast to VARCHAR, preserve nullability.
            RelDataType varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
            RelDataType nullableVarchar = typeFactory.createTypeWithNullability(varchar, operand.getType().isNullable());
            normalised.add(rexBuilder.makeCast(nullableVarchar, operand));
            changed = true;
        }
        if (!changed) {
            return original;
        }
        return rexBuilder.makeCall(original.getType(), original.getOperator(), normalised);
    }
}
