/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

/**
 * Bridges integer-input rounding calls (FLOOR, CEIL, SIGN, TRUNCATE) over to DataFusion's
 * fp64-only UDFs while preserving PPL's "same type as input" return-type contract.
 *
 * <p>For an integer first operand, rewrites {@code fn(int_col [, ...])} as
 * {@code CAST(fn(CAST(int_col AS DOUBLE) [, ...]) AS <original_type>)}. The outer CAST keeps
 * the call's type identical to {@code original.getType()}, so downstream rowTypes don't
 * shift; the inner CAST widens the first operand so isthmus binds the standard fp64 impl
 * (DataFusion's UDFs reject {@code Int32}). For non-integer operands the call is rebuilt
 * only when {@code target} renames the operator (e.g. SIGN → signum), otherwise passed
 * through.
 *
 * <p>Why we need this: without it, Calcite's wire schema says {@code Int32} but the producer's
 * actual physical output is {@code Float64}, and on multi-shard plans the consumer's
 * {@code ensure_schema_compatibility} check rejects the bind.
 *
 * <p>Trailing operands (e.g. the {@code scale} of {@code TRUNCATE(value, scale)}) pass through
 * untouched. Recursion is safe: once the first operand is widened to DOUBLE, the operand-type
 * guard short-circuits any re-visit.
 *
 * @opensearch.internal
 */
public class IntegerRoundingCastAdapter implements ScalarFunctionAdapter {

    private final SqlOperator target;

    public IntegerRoundingCastAdapter(SqlOperator target) {
        this.target = target;
    }

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().isEmpty()) {
            return original;
        }
        RexNode firstOperand = original.getOperands().get(0);
        SqlTypeName firstType = firstOperand.getType().getSqlTypeName();
        boolean firstIsInteger = SqlTypeName.INT_TYPES.contains(firstType);
        boolean operatorRename = original.getOperator() != target;

        // Already-wide operand and same operator — nothing to do.
        if (!firstIsInteger && !operatorRename) {
            return original;
        }
        // Already-wide operand but operator needs renaming (e.g. SIGN → signum). Rebuild
        // with the renamed operator; no widening/cast needed.
        if (!firstIsInteger) {
            return cluster.getRexBuilder().makeCall(original.getType(), target, original.getOperands());
        }

        // Integer first operand: widen to DOUBLE, rebuild the call (using `target`, which may
        // be a rename), and restore the original return type via the outer CAST.
        RelDataTypeFactory factory = cluster.getTypeFactory();
        RelDataType doubleType = factory.createTypeWithNullability(
            factory.createSqlType(SqlTypeName.DOUBLE),
            firstOperand.getType().isNullable()
        );
        RexNode widenedFirst = cluster.getRexBuilder().makeCast(doubleType, firstOperand);
        java.util.List<RexNode> rebuiltOperands = new java.util.ArrayList<>(original.getOperands().size());
        rebuiltOperands.add(widenedFirst);
        for (int i = 1; i < original.getOperands().size(); i++) {
            rebuiltOperands.add(original.getOperands().get(i));
        }
        RexNode innerCall = cluster.getRexBuilder().makeCall(doubleType, target, rebuiltOperands);
        return cluster.getRexBuilder().makeCast(original.getType(), innerCall);
    }
}
