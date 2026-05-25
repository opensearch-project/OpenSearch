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
 * Rename adapter for PPL's {@code array(a, b, …)} constructor — rewrites to a
 * locally-declared {@link SqlFunction} named {@code make_array}, which is
 * DataFusion's native array constructor (no UDF registration required on the
 * Rust side; isthmus emits a Substrait scalar function call with that name and
 * DataFusion's substrait consumer maps it to {@code make_array} natively).
 *
 * <p>Unlike {@link org.opensearch.analytics.spi.AbstractNameMappingAdapter},
 * this adapter also CASTs each operand to the array's inferred element type
 * before emission. PPL's {@code ArrayFunctionImpl} returns
 * {@code ARRAY<commonElementType>} (Calcite type-widens to find the common
 * element type), but it does NOT widen the individual operand types — so a
 * call like {@code array(1, 1.5)} produces a RexCall whose operand types are
 * {@code (INTEGER, DECIMAL(2,1))} but whose return type is {@code ARRAY<DOUBLE>}.
 * Substrait's variadic {@code make_array(any1)} signature requires consistent
 * argument types ({@link io.substrait.expression.VariadicParameterConsistencyValidator})
 * and throws an AssertionError that fatally exits the JVM otherwise — so we
 * widen each operand to the call's component type before substrait sees it.
 *
 * <p>Same machinery as {@link UnixTimestampAdapter}: locally-declared operator
 * is the referent of the {@link io.substrait.isthmus.expression.FunctionMappings.Sig}
 * in {@link DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS}.
 *
 * @opensearch.internal
 */
class MakeArrayAdapter implements ScalarFunctionAdapter {

    /**
     * Locally-declared target operator. Name matches DataFusion's native {@code make_array}.
     * Return type inference is a placeholder — {@link #adapt} explicitly carries the
     * original call's array return type forward.
     */
    static final SqlOperator LOCAL_MAKE_ARRAY_OP = new SqlFunction(
        "make_array",
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
        RelDataType elementType = arrayType.getComponentType();
        if (elementType == null) {
            // Defensive — Calcite's array() always infers a component type. If somehow
            // missing, fall through with original operands and let substrait fail.
            return rexBuilder.makeCall(arrayType, LOCAL_MAKE_ARRAY_OP, original.getOperands());
        }
        List<RexNode> widened = new ArrayList<>(original.getOperands().size());
        for (RexNode operand : original.getOperands()) {
            if (operand.getType().equals(elementType)) {
                widened.add(operand);
            } else {
                widened.add(rexBuilder.makeCast(elementType, operand, true, false));
            }
        }
        return rexBuilder.makeCall(arrayType, LOCAL_MAKE_ARRAY_OP, widened);
    }
}
