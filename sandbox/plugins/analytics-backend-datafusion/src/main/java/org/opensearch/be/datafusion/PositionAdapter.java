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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;

/**
 * Adapts PPL {@code POSITION(substr IN str[, start])} to DataFusion's {@code strpos(str, substr)}.
 *
 * <p>PPL emits a 2-arg {@code POSITION(substr, str)} for {@code locate(substr, str)} /
 * {@code position(substr IN str)}, and a 3-arg {@code POSITION(substr, str, start)} for
 * PPL's 3-arg {@code locate(substr, str, start)} (PPL's frontend maps both surface spellings
 * into {@link SqlKind#POSITION}). DataFusion's {@code strpos} is
 * {@code (str, substr)} with no {@code start} parameter, so:
 *
 * <ul>
 *   <li><b>2-arg form:</b> swap operands → {@code strpos(str, substr)}.</li>
 *   <li><b>3-arg form:</b> decompose as
 *       {@code CASE WHEN strpos(substring(str, start), substr) = 0
 *               THEN 0
 *               ELSE strpos(substring(str, start), substr) + start - 1
 *               END}.
 *       Preserves 1-indexed semantics and returns 0 when the substring isn't found.</li>
 * </ul>
 *
 * @opensearch.internal
 */
class PositionAdapter implements ScalarFunctionAdapter {

    /** Locally-declared {@code strpos} operator. The
     *  {@link io.substrait.isthmus.expression.FunctionMappings.Sig} entry in
     *  {@link DataFusionFragmentConvertor#ADDITIONAL_SCALAR_SIGS} pairs it with the
     *  {@code strpos} extension name declared in {@code opensearch_scalar_functions.yaml}. */
    static final SqlFunction STRPOS = new SqlFunction(
        "strpos",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.INTEGER,
        null,
        OperandTypes.ANY_ANY,
        SqlFunctionCategory.STRING
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        List<RexNode> operands = original.getOperands();
        if (operands.size() < 2 || operands.size() > 3) {
            return original;
        }

        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode substr = operands.get(0);
        RexNode str = operands.get(1);

        if (operands.size() == 2) {
            // Simple swap: POSITION(substr, str) → strpos(str, substr)
            return rexBuilder.makeCall(original.getType(), STRPOS, List.of(str, substr));
        }

        // 3-arg: POSITION(substr, str, start) → decompose via substring.
        RexNode start = operands.get(2);
        RelDataTypeFactory typeFactory = cluster.getTypeFactory();
        RelDataType intType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.INTEGER), true);

        // tail = substring(str, start)
        RexNode tail = rexBuilder.makeCall(SqlStdOperatorTable.SUBSTRING, str, start);
        // posInTail = strpos(tail, substr) — 1-indexed, 0 when not found.
        RexNode posInTail = rexBuilder.makeCall(STRPOS, tail, substr);

        RexNode zero = rexBuilder.makeExactLiteral(BigDecimal.ZERO, intType);
        RexNode one = rexBuilder.makeExactLiteral(BigDecimal.ONE, intType);
        RexNode isZero = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, posInTail, zero);
        RexNode adjusted = rexBuilder.makeCall(
            SqlStdOperatorTable.MINUS,
            rexBuilder.makeCall(SqlStdOperatorTable.PLUS, posInTail, start),
            one
        );

        // CASE WHEN posInTail = 0 THEN 0 ELSE posInTail + start - 1 END
        return rexBuilder.makeCall(intType, SqlStdOperatorTable.CASE, List.of(isZero, zero, adjusted));
    }
}
