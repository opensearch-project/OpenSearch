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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Adapter for {@code ctime(value[, format])} / {@code mktime(value[, format])}
 *
 * <p>Shape normalization:
 * <ul>
 *   <li>Value operand coerced to VARCHAR. {@code ctime} / {@code mktime} both accept numeric
 *       epoch seconds directly; rendering them as strings lets the Rust UDFs share one string
 *       input path with identical null-propagation semantics. Numeric columns become
 *       {@code CAST(value AS VARCHAR)} — DataFusion produces the canonical decimal
 *       representation, which the UDF's {@code toEpochSeconds} parses back to double.</li>
 *   <li>Format operand filled in with default {@code "%m/%d/%Y %H:%M:%S"} when the
 *       incoming call is unary, so the Rust UDF always sees a 2-arg signature.</li>
 * </ul>
 *
 * @opensearch.internal
 */
public class TimeConversionFunctionAdapter implements ScalarFunctionAdapter {

    public static final String DEFAULT_FORMAT = "%m/%d/%Y %H:%M:%S";

    /** Synthetic operator for {@code ctime(value, format) → varchar} */
    public static final SqlFunction CTIME = binaryUdf("ctime", ReturnTypes.VARCHAR_NULLABLE);

    /** Synthetic operator for {@code mktime(value, format) → fp64} */
    public static final SqlFunction MKTIME = binaryUdf("mktime", ReturnTypes.DOUBLE_NULLABLE);

    private final SqlFunction target;

    public TimeConversionFunctionAdapter(SqlFunction target) {
        this.target = target;
    }

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        List<RexNode> operands = original.getOperands();
        if (operands.isEmpty() || operands.size() > 2) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode normalizedValue = coerceToVarchar(operands.get(0), cluster);
        RexNode format = operands.size() == 2 ? operands.get(1) : rexBuilder.makeLiteral(DEFAULT_FORMAT);
        return rexBuilder.makeCall(original.getType(), target, List.of(normalizedValue, format));
    }

    /**
     * Coerces {@code operand} to VARCHAR. Returns the operand unchanged when it's already a
     * character type so we don't layer redundant CASTs.
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

    private static SqlFunction binaryUdf(String name, SqlReturnTypeInference returnTypeInference) {
        return new SqlFunction(
            name,
            SqlKind.OTHER_FUNCTION,
            returnTypeInference,
            null,
            OperandTypes.family(),
            SqlFunctionCategory.USER_DEFINED_FUNCTION
        );
    }
}
