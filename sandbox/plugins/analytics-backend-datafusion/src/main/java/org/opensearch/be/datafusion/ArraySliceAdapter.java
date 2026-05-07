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
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.ArrayList;
import java.util.List;

/**
 * Adapter for Calcite's {@link SqlLibraryOperators#ARRAY_SLICE} — passes the
 * call through unchanged but coerces the index operands (positions 1, 2, and
 * optionally 3) to {@code BIGINT}. PPL's parser types positive integer literals
 * as {@code DECIMAL(20,0)} (precision wide enough to hold any 64-bit unsigned
 * value), but DataFusion's {@code array_slice} signature accepts only integer
 * indices and refuses to coerce decimal arguments — failing with
 * {@code "No function matches the given name and argument types
 * 'array_slice(List(Int32), Decimal128(20, 0), Decimal128(22, 0))'"}.
 *
 * <p>The array operand at index 0 is left untouched.
 *
 * @opensearch.internal
 */
class ArraySliceAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataTypeFactory typeFactory = cluster.getTypeFactory();
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        List<RexNode> operands = original.getOperands();
        List<RexNode> coerced = new ArrayList<>(operands.size());
        for (int i = 0; i < operands.size(); i++) {
            RexNode operand = operands.get(i);
            if (i == 0 || operand.getType().getSqlTypeName() == SqlTypeName.BIGINT) {
                coerced.add(operand);
            } else {
                RelDataType nullableBigint = typeFactory.createTypeWithNullability(bigint, operand.getType().isNullable());
                coerced.add(rexBuilder.makeCast(nullableBigint, operand, true, false));
            }
        }
        return rexBuilder.makeCall(original.getType(), original.getOperator(), coerced);
    }
}
