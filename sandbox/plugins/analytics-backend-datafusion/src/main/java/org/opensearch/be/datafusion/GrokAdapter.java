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
 * Cat-3b adapter for PPL's {@code grok <field> '<grok-pattern>'} command — the
 * grok-syntax sibling of {@link ParseAdapter}. The SQL plugin lowers both
 * {@code grok} and {@code parse} through the same {@code ParseFunction} UDF,
 * differing only by the third operand: {@code 'grok'} vs {@code 'regex'}. So the
 * RexCall reaching this adapter is {@code GROK(input, '<grok-pattern>', 'grok')}.
 *
 * <p>One job at plan time: <b>literal validation</b> of the grok pattern operand.
 * The Rust UDF compiles (resolves + matches) the grok pattern once per call, so a
 * column-valued pattern is not supported and is rejected up front. The method
 * operand is always the {@code 'grok'} literal emitted by the SQL plugin's grok
 * lowering — unlike {@link ParseAdapter} (whose {@code parse} operator can carry
 * {@code regex}/{@code grok}/{@code patterns}), this adapter is bound to the GROK
 * operator by name in the backend's {@code scalarFunctionAdapters} map, so it is
 * only ever invoked for grok and does not re-check the method.
 *
 * <p>Unlike {@code parse}, the grok pattern is <b>not</b> a raw regex: the
 * recursive {@code %{NAME:field}} resolution and the 108-line default grok
 * dictionary live in the {@code grok} Rust UDF, because the SQL plugin's grok
 * library is not on OpenSearch core's classpath. The resolved regex relies on
 * lookbehind / atomic groups (e.g. {@code BASE10NUM}), so the Rust UDF matches
 * with {@code fancy-regex} rather than the {@code regex} crate that backs
 * {@code parse}.
 *
 * <p>After validation the call is rewritten to {@link #LOCAL_GROK_OP}, whose
 * {@code FunctionMappings.Sig} in {@link DataFusionFragmentConvertor} resolves to
 * the {@code grok} Rust UDF. The original call's return type
 * ({@code MAP<VARCHAR, VARCHAR>} from {@code ParseFunction}) is preserved via
 * {@code rexBuilder.makeCall(original.getType(), ...)} so the enclosing
 * {@code Project} rowType stays consistent.
 *
 * @opensearch.internal
 */
class GrokAdapter implements ScalarFunctionAdapter {

    /**
     * Locally-declared target operator for the rewrite. {@link SqlKind#OTHER_FUNCTION}
     * so it doesn't collide with any Calcite built-in. The return type is inherited
     * from the original RexCall via {@code makeCall}, so the
     * {@code ReturnTypes.VARCHAR_2000} placeholder here is never observed.
     */
    static final SqlOperator LOCAL_GROK_OP = new SqlFunction(
        "grok",
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
            throw new IllegalArgumentException("grok: expected 3 operands (input, pattern, method), got " + operands.size());
        }
        // The grok pattern (slot 1) must be a non-null string literal: the Rust UDF
        // resolves + compiles it once per call, so a column-valued pattern would be
        // ill-defined. The method operand (slot 2) is always the 'grok' literal emitted
        // by the SQL plugin's grok lowering — this adapter is bound to the GROK operator
        // by name in the backend's scalarFunctionAdapters map, so it is only ever invoked
        // for grok and does not re-check the method.
        requireStringLiteral(operands.get(1), "pattern");

        // Preserve the original return type — see ParseAdapter / ConvertTzAdapter for
        // the Project.isValid-compatible-types rationale.
        return rexBuilder.makeCall(original.getType(), LOCAL_GROK_OP, operands);
    }

    /**
     * Ensures {@code operand} is a non-null VARCHAR/CHAR literal. Returns the
     * literal's string value. Any other shape (column ref, function call, NULL
     * literal, non-string literal) throws with a slot label so users see which
     * argument is wrong.
     */
    private static String requireStringLiteral(RexNode operand, String slotLabel) {
        if (!(operand instanceof RexLiteral literal)) {
            throw new IllegalArgumentException("grok: " + slotLabel + " must be a string literal (no column references / expressions)");
        }
        SqlTypeName typeName = literal.getType().getSqlTypeName();
        if (typeName != SqlTypeName.CHAR && typeName != SqlTypeName.VARCHAR) {
            throw new IllegalArgumentException("grok: " + slotLabel + " must be a string literal, got type " + typeName);
        }
        String value = literal.getValueAs(String.class);
        if (value == null) {
            throw new IllegalArgumentException("grok: " + slotLabel + " must be a non-null string literal");
        }
        return value;
    }
}
