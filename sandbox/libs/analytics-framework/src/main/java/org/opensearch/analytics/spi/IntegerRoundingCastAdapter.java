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
 * Calcite types {@code FLOOR(int_col)}, {@code CEIL}, {@code SIGN}, {@code TRUNCATE} as
 * {@code int} (ARG0 inference), but DataFusion's UDFs only accept fp32/fp64 and produce
 * {@code Float64}. On multi-shard plans the substrait schema check rejects the resulting
 * wire/physical disagreement. Adjusting Calcite's return type would break the user-facing
 * "same type as input" PPL contract; instead this adapter wraps the call in casts so wire
 * and physical output agree without changing the outer-visible type.
 *
 * <p>Rewrites {@code fn(int_col [, ...])} as
 * {@code CAST(fn(CAST(int_col AS DOUBLE) [, ...]) AS <original_type>)}. Trailing operands
 * (e.g. {@code TRUNCATE}'s {@code scale}) pass through untouched; non-integer first operands
 * are rebuilt only when {@code target} renames the operator (e.g. SIGN → signum).
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
