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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.ArrayList;
import java.util.List;

/**
 * Rename + operand-coerce adapter for PPL's {@code mvappend(arg1, arg2, …)}.
 *
 * <p>The Rust UDF (`udf::mvappend`) handles operands as a uniform stream where
 * every operand is either {@code element_type} (scalar) or
 * {@code List<element_type>} (array) for a single inferred {@code element_type}.
 * The Calcite call's return type is {@code ARRAY<componentType>}; this adapter
 * casts each scalar operand to {@code componentType} and each array operand to
 * {@code ARRAY<componentType>} before substrait emission, so the UDF sees a
 * single element type across all positions.
 *
 * <p>Mixed-type {@code mvappend} calls (PPL widens to {@code ARRAY<ANY>}) end
 * up with a Calcite {@code ANY} component type which substrait can't serialize
 * — those fail at substrait conversion before reaching this adapter, and
 * aren't handled by it.
 *
 * <p>Same templated machinery as {@link MvzipAdapter} / {@link MvfindAdapter}:
 * the locally-declared operator is the referent of the
 * {@link io.substrait.isthmus.expression.FunctionMappings.Sig} entry in
 * {@link DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS}.
 *
 * @opensearch.internal
 */
class MvappendAdapter implements ScalarFunctionAdapter {

    static final SqlOperator LOCAL_MVAPPEND_OP = new SqlFunction(
        "mvappend",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.ARG0,
        null,
        OperandTypes.VARIADIC,
        SqlFunctionCategory.SYSTEM
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataType arrayType = original.getType();
        RelDataType componentType = arrayType.getComponentType();
        if (componentType == null) {
            // Defensive — Calcite always assigns ARRAY<X> as mvappend's return type. Pass
            // operands through untouched and let substrait surface the error if anything's
            // off.
            return rexBuilder.makeCall(arrayType, LOCAL_MVAPPEND_OP, original.getOperands());
        }
        List<RexNode> coerced = new ArrayList<>(original.getOperands().size());
        for (RexNode operand : original.getOperands()) {
            RelDataType operandType = operand.getType();
            if (operandType.getComponentType() != null) {
                // Array operand — cast to ARRAY<componentType> if its element type differs.
                RelDataType targetArray = cluster.getTypeFactory().createArrayType(componentType, -1);
                if (operandType.equals(targetArray)) {
                    coerced.add(operand);
                } else {
                    coerced.add(rexBuilder.makeCast(targetArray, operand, true, false));
                }
            } else {
                // Scalar operand — cast to componentType if its type differs.
                if (operandType.equals(componentType)) {
                    coerced.add(operand);
                } else {
                    coerced.add(rexBuilder.makeCast(componentType, operand, true, false));
                }
            }
        }
        return rexBuilder.makeCall(arrayType, LOCAL_MVAPPEND_OP, coerced);
    }
}
