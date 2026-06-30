/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.planner.adapter;

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
 * Adapter for numeric-conversion family:
 * {@code num} / {@code auto} / {@code memk} / {@code rmcomma} / {@code rmunit}
 *
 * <p>All five share the same Calcite shape — unary call whose single operand is of any type,
 * returning a nullable DOUBLE. The adapter normalizes the operand to VARCHAR, then rebuilds
 * the call with a per-function synthetic {@link SqlFunction}.
 * Each synthetic operator is paired with its Substrait extension name.
 *
 * @opensearch.internal
 */
public class NumericConversionFunctionAdapter implements ScalarFunctionAdapter {

    /** Synthetic operator for {@code num(x)}. Paired with extension name {@code "num"}. */
    public static final SqlFunction NUM = unaryDoubleUdf("num");

    /** Synthetic operator for {@code auto(x)}. Paired with extension name {@code "auto"}. */
    public static final SqlFunction AUTO = unaryDoubleUdf("auto");

    /** Synthetic operator for {@code memk(x)}. Paired with extension name {@code "memk"}. */
    public static final SqlFunction MEMK = unaryDoubleUdf("memk");

    /** Synthetic operator for {@code rmcomma(x)}. Paired with extension name {@code "rmcomma"}. */
    public static final SqlFunction RMCOMMA = unaryDoubleUdf("rmcomma");

    /** Synthetic operator for {@code rmunit(x)}. Paired with extension name {@code "rmunit"}. */
    public static final SqlFunction RMUNIT = unaryDoubleUdf("rmunit");

    /** Synthetic operator for {@code dur2sec(x)}. Paired with extension name {@code "dur2sec"}. */
    public static final SqlFunction DUR2SEC = unaryDoubleUdf("dur2sec");

    /** Synthetic operator for {@code mstime(x)}. Paired with extension name {@code "mstime"}. */
    public static final SqlFunction MSTIME = unaryDoubleUdf("mstime");

    private final SqlFunction target;

    public NumericConversionFunctionAdapter(SqlFunction target) {
        this.target = target;
    }

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 1) {
            return original;
        }
        RexNode value = original.getOperands().get(0);
        RexNode normalized = coerceToVarchar(value, cluster);
        return cluster.getRexBuilder().makeCall(original.getType(), target, List.of(normalized));
    }

    /**
     * Coerces {@code operand} to VARCHAR. Returns the operand unchanged when it's already a
     * character type so we don't apply redundant CAST.
     */
    private static RexNode coerceToVarchar(RexNode operand, RelOptCluster cluster) {
        SqlTypeName src = operand.getType().getSqlTypeName();
        if (src == SqlTypeName.VARCHAR || src == SqlTypeName.CHAR) {
            return operand;
        }
        RelDataTypeFactory factory = cluster.getTypeFactory();
        RelDataType varcharType = factory.createTypeWithNullability(
            factory.createSqlType(SqlTypeName.VARCHAR),
            operand.getType().isNullable()
        );
        return cluster.getRexBuilder().makeCast(varcharType, operand);
    }

    /**
     * Locally-declared {@code name(varchar) → fp64} operator.
     */
    private static SqlFunction unaryDoubleUdf(String name) {
        return new SqlFunction(
            name,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.DOUBLE_NULLABLE,
            null,
            OperandTypes.family(),
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
    }
}
