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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Adapter for the PPL {@code rex field=f "(?<g>...)"} extract command's
 * single-match form. Rewrites the SQL plugin's
 * {@code PPLBuiltinOperators.REX_EXTRACT} call (a custom Calcite UDF named
 * {@code "REX_EXTRACT"}) to {@link #LOCAL_REX_EXTRACT_OP}, which {@link
 * DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS} maps to the
 * {@code rex_extract} Substrait extension.
 *
 * <p>Pattern and group operands MUST be string literals — the regex is
 * compiled per-call on the Rust side, not per-row. Column-valued pattern or
 * group is rejected at plan time with {@code IllegalArgumentException} so
 * users see the error immediately rather than as a silent NULL row at
 * runtime. (The Rust UDF also handles column-valued operands defensively for
 * future use cases, but the SQL plugin's PPL Calcite visitor never emits
 * such calls today.)
 *
 * <p>Mirrors the {@link ConvertTzAdapter} shape: one local target operator,
 * a literal-validation step, and a {@code rexBuilder.makeCall} preserving
 * the original call's return type so the enclosing project's row type cache
 * stays consistent.
 *
 * @opensearch.internal
 */
class RexExtractAdapter implements ScalarFunctionAdapter {

    /**
     * Locally-declared target operator for the rewrite. {@link SqlKind#OTHER_FUNCTION}
     * so it doesn't collide with any Calcite built-in. {@link OperandTypes#STRING_STRING_STRING}
     * keeps validation permissive — the Rust UDF's {@code coerce_types} does the
     * real type vetting.
     */
    static final SqlOperator LOCAL_REX_EXTRACT_OP = new SqlFunction(
        "rex_extract",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000_NULLABLE,
        null,
        OperandTypes.STRING_STRING_STRING,
        SqlFunctionCategory.STRING
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 3) {
            throw new IllegalArgumentException(
                "rex_extract: expected 3 operands (input, pattern, group), got " + original.getOperands().size()
            );
        }
        validateLiteral(original.getOperands().get(1), "pattern");
        validateLiteral(original.getOperands().get(2), "group");
        RexBuilder rexBuilder = cluster.getRexBuilder();
        return rexBuilder.makeCall(original.getType(), LOCAL_REX_EXTRACT_OP, original.getOperands());
    }

    /**
     * Reject non-literal operands at plan time. Both pattern and group must be
     * known up front because the regex compiler runs once per query — a
     * column-valued operand would either recompile per row (catastrophic) or
     * silently degrade by parsing the first row's value as a constant.
     */
    static void validateLiteral(RexNode operand, String name) {
        if (!(operand instanceof RexLiteral literal)) {
            throw new IllegalArgumentException("rex_extract: '" + name + "' must be a string literal, got " + operand.getKind());
        }
        SqlTypeName typeName = literal.getType().getSqlTypeName();
        if (typeName != SqlTypeName.CHAR && typeName != SqlTypeName.VARCHAR) {
            throw new IllegalArgumentException("rex_extract: '" + name + "' must be a string literal, got " + typeName);
        }
    }
}
