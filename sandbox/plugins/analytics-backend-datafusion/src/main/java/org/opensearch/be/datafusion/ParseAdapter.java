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
import java.util.Locale;

/**
 * Cat-3b adapter for PPL's {@code parse <field> '<regex>'} command. Two jobs at
 * plan time:
 *
 * <ol>
 *   <li><b>Literal validation.</b> The pattern + method operands must both be
 *       non-null string literals. The Rust UDF compiles the pattern once per
 *       call so a column-valued pattern is not supported, and a non-literal
 *       method operand is meaningless. Both fail fast at plan time with a
 *       clear error mentioning which slot is wrong.</li>
 *   <li><b>Method gating.</b> Reject {@code grok} and {@code patterns}
 *       up front — only {@code regex} has a Rust implementation today. The
 *       error names both the rejected method and the supported alternative so
 *       users know whether to switch the method or stay on the legacy
 *       engine.</li>
 * </ol>
 *
 * <p>After validation the call is rewritten to {@link #LOCAL_PARSE_OP}, whose
 * {@code FunctionMappings.Sig} in {@link DataFusionFragmentConvertor} resolves
 * to the {@code parse} Rust UDF. The original call's return type
 * (a {@code MAP<VARCHAR, VARCHAR>} from {@code ParseFunction}) is preserved
 * via {@code rexBuilder.makeCall(original.getType(), ...)} so the enclosing
 * {@code Project} rowType stays consistent (see
 * {@link org.opensearch.analytics.spi.AbstractNameMappingAdapter} javadoc for
 * background).
 *
 * @opensearch.internal
 */
class ParseAdapter implements ScalarFunctionAdapter {

    /**
     * Locally-declared target operator for the rewrite. {@link SqlKind#OTHER_FUNCTION}
     * so it doesn't collide with any Calcite built-in. The return type is
     * inherited from the original RexCall via {@code makeCall}, so the
     * {@code ReturnTypes.VARCHAR_2000} placeholder here is never observed.
     */
    static final SqlOperator LOCAL_PARSE_OP = new SqlFunction(
        "parse",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000,
        null,
        OperandTypes.STRING_STRING_STRING,
        SqlFunctionCategory.STRING
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        List<RexNode> operands = original.getOperands();
        if (operands.size() != 3) {
            throw new IllegalArgumentException("parse: expected 3 operands (input, pattern, method), got " + operands.size());
        }
        // Slot 1 (pattern) and slot 2 (method) must be non-null string literals.
        // The Rust UDF compiles the regex once at runtime entry, so a column-
        // valued pattern would be ill-defined; the method literal is gated to
        // 'regex' below.
        requireStringLiteral(operands.get(1), "pattern");
        String method = requireStringLiteral(operands.get(2), "method");

        if (!"regex".equals(method.toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "parse: method '%s' is not supported by the analytics engine; only 'regex' is supported "
                        + "(use 'grok' / 'patterns' on the legacy engine)",
                    method
                )
            );
        }

        // Preserve the original return type — see ConvertTzAdapter for the
        // Project.isValid-compatible-types rationale.
        return rexBuilder.makeCall(original.getType(), LOCAL_PARSE_OP, operands);
    }

    /**
     * Ensures {@code operand} is a non-null VARCHAR/CHAR literal. Returns the
     * literal's string value. Any other shape (column ref, function call,
     * NULL literal, non-string literal) throws with a slot label so users see
     * which argument is wrong.
     */
    private static String requireStringLiteral(RexNode operand, String slotLabel) {
        if (!(operand instanceof RexLiteral literal)) {
            throw new IllegalArgumentException("parse: " + slotLabel + " must be a string literal (no column references / expressions)");
        }
        SqlTypeName typeName = literal.getType().getSqlTypeName();
        if (typeName != SqlTypeName.CHAR && typeName != SqlTypeName.VARCHAR) {
            throw new IllegalArgumentException("parse: " + slotLabel + " must be a string literal, got type " + typeName);
        }
        String value = literal.getValueAs(String.class);
        if (value == null) {
            throw new IllegalArgumentException("parse: " + slotLabel + " must be a non-null string literal");
        }
        return value;
    }
}
