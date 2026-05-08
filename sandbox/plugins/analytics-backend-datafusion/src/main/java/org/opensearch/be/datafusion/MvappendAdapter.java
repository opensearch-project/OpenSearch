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
            return rexBuilder.makeCall(arrayType, LOCAL_MVAPPEND_OP, original.getOperands());
        }
        // Substrait's variadic {@code any1} parameter requires every operand at the same
        // variadic position to share a type. PPL's {@code mvappend(arg, …)} accepts a mix
        // of bare scalars and arrays, which substrait's signature matcher rejects with
        // {@code Unable to convert call mvappend(list<…>, scalar, …)}. Normalize every
        // operand to {@code ARRAY<componentType>} — array operands cast their element
        // type if it differs; scalar operands wrap in a {@code make_array(…)} singleton
        // call. The Rust UDF then sees a uniform {@code list<any1>} variadic.
        RelDataType targetArrayType = cluster.getTypeFactory().createArrayType(componentType, -1);
        List<RexNode> coerced = new ArrayList<>(original.getOperands().size());
        for (RexNode operand : original.getOperands()) {
            RelDataType operandType = operand.getType();
            if (operandType.getComponentType() != null) {
                // Array operand — cast to ARRAY<componentType> if its element type differs.
                if (operandType.equals(targetArrayType)) {
                    coerced.add(operand);
                } else {
                    coerced.add(rexBuilder.makeCast(targetArrayType, operand, true, false));
                }
            } else {
                // Scalar operand — first cast to componentType (so the singleton array's
                // element type matches), then wrap in make_array so substrait sees a list.
                RexNode casted = operandType.equals(componentType) ? operand : rexBuilder.makeCast(componentType, operand, true, false);
                coerced.add(rexBuilder.makeCall(targetArrayType, MakeArrayAdapter.LOCAL_MAKE_ARRAY_OP, List.of(casted)));
            }
        }
        return rexBuilder.makeCall(arrayType, LOCAL_MVAPPEND_OP, coerced);
    }
}
