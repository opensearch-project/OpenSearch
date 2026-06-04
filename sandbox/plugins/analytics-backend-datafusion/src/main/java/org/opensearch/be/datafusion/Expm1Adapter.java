/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.NumericToDoubleAdapter;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;

/**
 * Rewrites PPL's {@code EXPM1(x)} UDF (defined as {@code exp(x) - 1}) into the
 * equivalent {@code MINUS(EXP(x), 1)} expression tree. DataFusion's substrait
 * consumer recognises {@code exp} and {@code subtract} natively, but has no
 * direct {@code expm1} scalar function; lowering the UDF before Substrait
 * serialisation keeps the plan expressible in standard Substrait primitives.
 *
 * <p>For very small inputs {@code exp(x) - 1} has worse precision than the
 * dedicated {@code Math.expm1} implementation, but PPL's semantic is already
 * the naive subtraction (see {@code PPLBuiltinOperators.EXPM1}) so behaviour
 * is preserved.
 *
 * @opensearch.internal
 */
class Expm1Adapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        // Defensive: only rewrite the EXPM1 UDF. Any other call passes through.
        if (!original.getOperator().getName().equalsIgnoreCase("EXPM1")) {
            return original;
        }
        if (original.getOperands().size() != 1) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        // Widen the operand to DOUBLE before threading it into the synthesised EXP call.
        // BackendPlanAdapter has already visited this subtree, so the inner EXP we produce
        // here won't be re-adapted by NumericToDoubleAdapter — we have to widen explicitly.
        RexNode arg = NumericToDoubleAdapter.widenToDoubleIfNumeric(original.getOperands().get(0), cluster);
        RexNode exp = rexBuilder.makeCall(original.getType(), SqlStdOperatorTable.EXP, List.of(arg));
        RexNode one = rexBuilder.makeExactLiteral(BigDecimal.ONE);
        return rexBuilder.makeCall(SqlStdOperatorTable.MINUS, exp, one);
    }
}
