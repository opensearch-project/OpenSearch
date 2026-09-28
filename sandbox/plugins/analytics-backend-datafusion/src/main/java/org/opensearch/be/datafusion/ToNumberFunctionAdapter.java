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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Rewrites PPL {@code tonumber(string[, base])} into a DataFusion-compatible expression.
 *
 * <p>Per the
 * <a href="https://docs.opensearch.org/latest/sql-and-ppl/ppl/functions/conversion/#tonumber">
 * PPL {@code tonumber} docs</a>:
 *
 * <blockquote>
 *   {@code tonumber(string[, base])} — converts the string value to a number. If the
 *   {@code base} parameter is omitted, base 10 is assumed. Returns NULL when the string
 *   cannot be parsed.
 * </blockquote>
 *
 * @opensearch.internal
 */
class ToNumberFunctionAdapter implements ScalarFunctionAdapter {

    static final SqlFunction TONUMBER = new SqlFunction(
        "tonumber",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.DOUBLE,
        null,
        OperandTypes.family(),
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        List<RexNode> operands = original.getOperands();
        if (operands.isEmpty()) {
            return original;
        }
        RexNode value = operands.get(0);

        // 1-arg — implied base 10. DataFusion's built-in CAST(str AS DOUBLE) returns NULL on
        // parse failure.
        if (operands.size() == 1) {
            return makeSafeDoubleCast(value, cluster);
        }

        // 2-arg — rebuild as tonumber(CAST(value AS VARCHAR), CAST(base AS INTEGER))
        if (operands.size() == 2) {
            RexNode base = operands.get(1);
            RexNode normalizedValue = castTo(value, SqlTypeName.VARCHAR, cluster);
            RexNode normalizedBase = castTo(base, SqlTypeName.INTEGER, cluster);
            return cluster.getRexBuilder().makeCall(original.getType(), TONUMBER, List.of(normalizedValue, normalizedBase));
        }

        return original;
    }

    /**
     * Casts {@code operand} to {@code target} while preserving its nullability. Returns the
     * operand unchanged when it's already the target type so we don't layer redundant CASTs.
     */
    private static RexNode castTo(RexNode operand, SqlTypeName target, RelOptCluster cluster) {
        if (operand.getType().getSqlTypeName() == target) {
            return operand;
        }
        RelDataTypeFactory factory = cluster.getTypeFactory();
        RelDataType targetType = factory.createTypeWithNullability(factory.createSqlType(target), operand.getType().isNullable());
        return cluster.getRexBuilder().makeCast(targetType, operand);
    }

    /**
     * Wraps the single operand in a SAFE_CAST to DOUBLE. SAFE_CAST serialises as a substrait
     * cast with {@code FAILURE_BEHAVIOR_RETURN_NULL}, which DataFusion maps to
     * {@code try_cast} — so parse failures yield NULL instead of raising.
     */
    private static RexNode makeSafeDoubleCast(RexNode value, RelOptCluster cluster) {
        RelDataTypeFactory factory = cluster.getTypeFactory();
        RelDataType doubleType = factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.DOUBLE), true);
        // RexBuilder.makeCast(type, exp, matchNullability, safe) — the `safe` flag produces a
        // SqlKind.SAFE_CAST call instead of a plain CAST.
        return cluster.getRexBuilder().makeCast(doubleType, value, true, true);
    }
}
