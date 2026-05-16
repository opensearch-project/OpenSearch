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

import java.util.ArrayList;
import java.util.List;

/**
 * Casts every numeric operand of a {@link RexCall} to {@code DOUBLE} before
 * rebuilding the call with a configured target {@link SqlOperator}. Use this
 * when the backend's substrait extension YAML declares a single {@code fp64}
 * impl per arity but Calcite hands the call operands as {@code i8 / i16 /
 * i32 / i64 / fp32 / DECIMAL} — without widening, isthmus' signature matcher
 * fails with {@code "Unable to convert call X(i32?)"} because there's no
 * matching key.
 *
 * <p>Widening rules:
 * <ul>
 *   <li>{@code DOUBLE} → unchanged.</li>
 *   <li>{@code INT_TYPES} ({@code TINYINT, SMALLINT, INTEGER, BIGINT}),
 *       {@code FLOAT, REAL, DECIMAL} → wrapped in {@code CAST AS DOUBLE},
 *       preserving the operand's nullability.</li>
 *   <li>Anything else (string, date, boolean, struct, …) → unchanged. Adapters
 *       that need non-numeric handling should subclass or compose with their
 *       own logic rather than rely on this adapter.</li>
 * </ul>
 *
 * <p>The target operator is supplied at construction time. Two common shapes:
 * <ul>
 *   <li><b>Pure widening</b> — pass the same {@link SqlOperator} that came in.
 *       The call is rebuilt with widened operands but the operator is
 *       unchanged. Used for math functions like {@code EXP} / {@code LN} /
 *       {@code POWER} where Calcite's operator matches the substrait name.</li>
 *   <li><b>Widen + rename</b> — pass a different {@link SqlOperator}
 *       (typically a locally-declared {@code SqlFunction} pointing at a Rust
 *       UDF). Used for PPL functions that don't have a Calcite stdlib operator
 *       e.g. {@code MAKEDATE}, {@code MAKETIME}, {@code FROM_UNIXTIME}.</li>
 * </ul>
 *
 * <p>The return type of the rebuilt call is taken from {@code original.getType()},
 * not recomputed — Calcite's type system already determined the expected return
 * (e.g. fp64 for transcendental fns, TIMESTAMP for {@code from_unixtime}) and
 * we don't want widening operands to perturb the outer-visible schema.
 *
 * @opensearch.internal
 */
public class NumericToDoubleAdapter implements ScalarFunctionAdapter {

    private final SqlOperator target;

    public NumericToDoubleAdapter(SqlOperator target) {
        this.target = target;
    }

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        List<RexNode> rewritten = new ArrayList<>(original.getOperands().size());
        for (RexNode operand : original.getOperands()) {
            rewritten.add(widenToDoubleIfNumeric(operand, cluster));
        }
        return cluster.getRexBuilder().makeCall(original.getType(), target, rewritten);
    }

    /**
     * Exposed for adapters that perform a structural rewrite (e.g. lowering
     * {@code EXPM1(x)} to {@code EXP(x) - 1}) and need to widen the operand
     * before threading it into the synthesised inner call — the synthesised
     * call is created after BackendPlanAdapter
     * has already visited this subtree, so it won't be re-adapted.
     */
    public static RexNode widenToDoubleIfNumeric(RexNode operand, RelOptCluster cluster) {
        SqlTypeName type = operand.getType().getSqlTypeName();
        if (type == SqlTypeName.DOUBLE) {
            return operand;
        }
        if (SqlTypeName.INT_TYPES.contains(type) || type == SqlTypeName.FLOAT || type == SqlTypeName.REAL || type == SqlTypeName.DECIMAL) {
            RelDataTypeFactory factory = cluster.getTypeFactory();
            RelDataType doubleType = factory.createTypeWithNullability(
                factory.createSqlType(SqlTypeName.DOUBLE),
                operand.getType().isNullable()
            );
            return cluster.getRexBuilder().makeCast(doubleType, operand);
        }
        return operand;
    }
}
