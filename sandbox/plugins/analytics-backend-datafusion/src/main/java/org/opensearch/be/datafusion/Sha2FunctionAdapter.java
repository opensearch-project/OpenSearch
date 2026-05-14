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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;
import java.util.Set;

/**
 * Adapts PPL {@code sha2(input, bitLen)} to a pure DataFusion expression.
 *
 * <p>PPL surface (per
 * <a href="https://docs.opensearch.org/latest/sql-and-ppl/ppl/functions/cryptographic/">
 * cryptographic functions docs</a>):
 * <ul>
 *   <li>{@code bitLen} must be 224, 256, 384, or 512.</li>
 *   <li>Returns the hex-encoded digest as a string.</li>
 * </ul>
 *
 * <p>Rewrite: {@code sha2(input, N)} → {@code encode(digest(input, 'shaN'), 'hex')}.
 * {@link #DIGEST} resolves to DataFusion's native {@code digest(input, algorithm)}
 * (returns {@code Binary}), and {@link #ENCODE} resolves to DataFusion's native
 * {@code encode(binary, format)} (returns {@code Utf8} — the hex string).
 *
 * @opensearch.internal
 */
class Sha2FunctionAdapter implements ScalarFunctionAdapter {

    /** Valid bit lengths */
    private static final Set<Integer> SUPPORTED_BIT_LENGTHS = Set.of(224, 256, 384, 512);

    /**
     * Synthetic {@code digest(input, algorithm) → binary} operator. Paired with the
     * {@code "digest"} Substrait extension name
     */
    static final SqlFunction DIGEST = new SqlFunction(
        "digest",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.explicit(SqlTypeName.VARBINARY),
        null,
        OperandTypes.family(),
        SqlFunctionCategory.STRING
    );

    /**
     * Synthetic {@code encode(binary, format) → varchar} operator. Paired with the
     * {@code "encode"} Substrait extension name
     */
    static final SqlFunction ENCODE = new SqlFunction(
        "encode",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR,
        null,
        OperandTypes.family(),
        SqlFunctionCategory.STRING
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        List<RexNode> operands = original.getOperands();
        if (operands.size() != 2) {
            return original;
        }
        RexNode input = operands.get(0);
        RexNode bitLenOp = operands.get(1);
        if (!(bitLenOp instanceof RexLiteral literal)) {
            return original;
        }
        BigDecimal bitLenValue = literal.getValueAs(BigDecimal.class);
        if (bitLenValue == null) {
            return original;
        }
        int bitLen;
        try {
            bitLen = bitLenValue.intValueExact();
        } catch (ArithmeticException e) {
            return original;
        }
        if (!SUPPORTED_BIT_LENGTHS.contains(bitLen)) {
            return original;
        }

        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataTypeFactory typeFactory = cluster.getTypeFactory();
        RelDataType binaryType = typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.VARBINARY),
            input.getType().isNullable()
        );

        RexNode algorithmLit = rexBuilder.makeLiteral("sha" + bitLen);
        RexNode hexFormatLit = rexBuilder.makeLiteral("hex");
        RexNode digest = rexBuilder.makeCall(binaryType, DIGEST, List.of(input, algorithmLit));
        return rexBuilder.makeCall(original.getType(), ENCODE, List.of(digest, hexFormatLit));
    }
}
