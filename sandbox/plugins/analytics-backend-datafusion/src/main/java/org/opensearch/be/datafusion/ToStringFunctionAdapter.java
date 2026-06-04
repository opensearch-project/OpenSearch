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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;
import java.util.Locale;

/**
 * Rewrites PPL {@code tostring(value[, format])} into a DataFusion-compatible expression.
 *
 * <p>Per the
 * <a href="https://docs.opensearch.org/latest/sql-and-ppl/ppl/functions/conversion/#tostring">
 * PPL {@code tostring} docs</a>:
 *
 * <blockquote>
 *   {@code tostring(value[, format])} — converts the value to a string representation.
 *   If a format is provided, converts numbers to the specified format type. For Boolean
 *   values, converts to {@code TRUE} or {@code FALSE}. The {@code format} parameter is
 *   only used when {@code value} is a number and is ignored for Booleans.
 * </blockquote>
 *
 * <p>Handles two arrival shapes:
 * <ol>
 *   <li>Native {@code tostring(value[, format])} — dispatched as-is.</li>
 *   <li>{@code NUMBER_TO_STRING(num)} — PPL's {@code ExtendedRexBuilder.makeCast} override
 *       intercepts {@code CAST(num AS VARCHAR)} for approximate-numeric / decimal source types
 *       and rewrites it into a call to {@code PPLBuiltinOperators.NUMBER_TO_STRING}. That
 *       PPL-plugin-defined UDF isn't in any Substrait catalog, so isthmus cannot resolve it.
 *       We treat it as the single-arg {@code tostring} shape and lower it to a plain VARCHAR
 *       CAST.
 *   </li>
 * </ol>
 *
 * @opensearch.internal
 */
class ToStringFunctionAdapter implements ScalarFunctionAdapter {

    /**
     * Target numeric type for a given PPL format mode. {@link #COMMAS} preserves fractional
     * precision because it renders rounded to 2 decimals. All other modes fold to BIGINT because
     * their output is defined on the integer part of the value (cf. PPL docs: binary/hex/duration
     * are integer conversions).
     */
    private enum Format {
        HEX("hex", SqlTypeName.BIGINT),
        BINARY("binary", SqlTypeName.BIGINT),
        COMMAS("commas", /* preserveFractional */ null),
        DURATION("duration", SqlTypeName.BIGINT),
        DURATION_MILLIS("duration_millis", SqlTypeName.BIGINT);

        final String literal;
        /**
         * Target type for the numeric argument, or {@code null} for {@link #COMMAS} which
         * picks BIGINT vs DOUBLE based on the source type.
         */
        final SqlTypeName fixedTarget;

        Format(String literal, SqlTypeName fixedTarget) {
            this.literal = literal;
            this.fixedTarget = fixedTarget;
        }

        /** Case-insensitive lookup matching the PPL spec. Returns {@code null} when unknown. */
        static Format from(String modeLiteral) {
            if (modeLiteral == null) return null;
            String lower = modeLiteral.toLowerCase(Locale.ROOT);
            for (Format f : values()) {
                if (f.literal.equals(lower)) return f;
            }
            return null;
        }

        /**
         * Choose the target type for the numeric argument given the source RexNode type.
         * For every mode except {@link #COMMAS} this is a fixed BIGINT; COMMAS preserves
         * fractional types by routing through DOUBLE and widens integers to BIGINT.
         */
        SqlTypeName targetFor(SqlTypeName source) {
            if (fixedTarget != null) {
                return fixedTarget;
            }
            return isFractional(source) ? SqlTypeName.DOUBLE : SqlTypeName.BIGINT;
        }
    }

    /**
     * Synthetic {@code tostring} operator used when we rebuild the 2-arg call. It mirrors the
     * shape of the PPL operator but is keyed on the literal name {@code "tostring"} — which is
     * the name the Rust UDF registers under and the YAML extension declares. A dedicated operator
     * gives the isthmus name-based resolver a deterministic hook; we don't have to rely on the
     * incoming RexCall's operator being correctly named.
     */
    static final SqlFunction TOSTRING = new SqlFunction(
        "tostring",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR,
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

        // NUMBER_TO_STRING is PPL's intercepted numeric-to-varchar cast. Treat it identically to
        // the 1-arg tostring shape: lower to a plain CAST(value AS VARCHAR) that isthmus /
        // DataFusion can serialise.
        if (ScalarFunction.NUMBER_TO_STRING.name().equalsIgnoreCase(original.getOperator().getName())) {
            return makeVarcharCast(original, value, cluster);
        }

        // tostring renders booleans as the uppercase literals TRUE / FALSE (format arg is ignored for booleans).
        if (value.getType().getSqlTypeName() == SqlTypeName.BOOLEAN) {
            return makeBooleanToString(original, value, cluster);
        }

        // 1-arg: tostring(x) → CAST(x AS VARCHAR)
        if (operands.size() == 1) {
            return makeVarcharCast(original, value, cluster);
        }

        // 2-arg: tostring(x, format). Only rewrite when the format arg is a string literal with
        // a known mode; otherwise pass the call through so the downstream planner fails loudly.
        if (operands.size() == 2 && operands.get(1) instanceof RexLiteral formatLit && isStringLiteral(formatLit)) {
            Format mode = Format.from(formatLit.getValueAs(String.class));
            if (mode != null) {
                return rebuildCall(original, value, formatLit, mode, cluster);
            }
        }

        return original;
    }

    /**
     * Lower a BOOLEAN-valued {@code tostring} call to
     * {@code CASE WHEN value THEN 'TRUE' WHEN NOT value THEN 'FALSE' END}.
     */
    private static RexNode makeBooleanToString(RexCall original, RexNode value, RelOptCluster cluster) {
        RelDataTypeFactory factory = cluster.getTypeFactory();
        RelDataType varcharType = factory.createTypeWithNullability(
            factory.createSqlType(SqlTypeName.VARCHAR),
            original.getType().isNullable()
        );
        RexNode trueLit = cluster.getRexBuilder().makeLiteral("TRUE");
        RexNode falseLit = cluster.getRexBuilder().makeLiteral("FALSE");
        RexNode notValue = cluster.getRexBuilder().makeCall(SqlStdOperatorTable.NOT, value);
        return cluster.getRexBuilder()
            .makeCall(
                varcharType,
                SqlStdOperatorTable.CASE,
                List.of(value, trueLit, notValue, falseLit, cluster.getRexBuilder().makeNullLiteral(varcharType))
            );
    }

    /**
     * Rebuild the 2-arg call as {@code tostring(CAST(value AS <target>), formatLit)}. The CAST
     * ensures the numeric argument matches the Rust UDF's declared BIGINT/FLOAT64 signatures;
     * the format literal is forwarded verbatim so the UDF's per-row dispatch sees the exact
     * mode string the caller supplied.
     */
    private static RexNode rebuildCall(RexCall original, RexNode value, RexLiteral formatLit, Format mode, RelOptCluster cluster) {
        SqlTypeName target = mode.targetFor(value.getType().getSqlTypeName());
        RexNode normalized = castTo(value, target, cluster);
        return cluster.getRexBuilder().makeCall(original.getType(), TOSTRING, List.of(normalized, formatLit));
    }

    private static boolean isStringLiteral(RexLiteral literal) {
        SqlTypeName sqlType = literal.getType().getSqlTypeName();
        return sqlType == SqlTypeName.CHAR || sqlType == SqlTypeName.VARCHAR;
    }

    private static boolean isFractional(SqlTypeName type) {
        return type == SqlTypeName.FLOAT || type == SqlTypeName.DOUBLE || type == SqlTypeName.REAL || type == SqlTypeName.DECIMAL;
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

    private static RexNode makeVarcharCast(RexCall original, RexNode value, RelOptCluster cluster) {
        RelDataTypeFactory factory = cluster.getTypeFactory();
        RelDataType varcharType = factory.createTypeWithNullability(
            factory.createSqlType(SqlTypeName.VARCHAR),
            original.getType().isNullable()
        );
        return cluster.getRexBuilder().makeCast(varcharType, value);
    }
}
