/* SPDX-License-Identifier: Apache-2.0 */
package org.opensearch.be.datafusion;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.schema.IpType;
import org.opensearch.analytics.planner.rel.OperatorAnnotation;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.ArrayList;
import java.util.List;

/**
 * Coerces comparison operands to types that isthmus can bind to Substrait signatures:
 *
 * <ul>
 *   <li><b>{@link IpType} operands</b> &mdash; cast to plain {@code VARBINARY} so isthmus
 *       generates the standard Substrait {@code equal} / {@code not_equal} / {@code lt} /
 *       {@code gt} / {@code lte} / {@code gte} function instead of a UDT-specific function
 *       name (e.g. {@code EQUALS_IP}) that has no Substrait signature.</li>
 *   <li><b>Character operands vs temporal</b> &mdash; coerce to {@code TIMESTAMP} when isthmus
 *       has no Substrait sig for the call: char-vs-temporal (decorrelation folded the PPL
 *       {@code TIMESTAMP} UDF to a bare string), or TIME-vs-{DATE,TIMESTAMP} (Calcite wraps
 *       TIME with {@code to_timestamp(precision_time<P>)} which has no yaml impl).</li>
 * </ul>
 *
 * @opensearch.internal
 */
class ComparisonTemporalCoercionAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        List<RexNode> operands = original.getOperands();
        if (operands.size() != 2) {
            return original;
        }
        RexNode left = operands.get(0);
        RexNode right = operands.get(1);

        // ── IpType: cast to plain VARBINARY before Substrait conversion ──────
        RexNode newLeft = castIpTypeToVarbinary(left, cluster);
        RexNode newRight = castIpTypeToVarbinary(right, cluster);
        if (newLeft != left || newRight != right) {
            return rebuild(original, newLeft, newRight, cluster);
        }

        // ── Temporal coercion ────────────────────────────────────────────────
        // TIME vs DATE/TIMESTAMP — rewrite the TIME side to today-anchored TIMESTAMP
        RexNode timeLeft = coerceTimeIfApplicable(left, right, cluster);
        RexNode timeRight = coerceTimeIfApplicable(right, left, cluster);
        if (timeLeft != left || timeRight != right) {
            return rebuild(original, timeLeft, timeRight, cluster);
        }

        boolean leftChar = isCharacter(left);
        boolean rightChar = isCharacter(right);
        boolean leftTemporal = isTemporal(left);
        boolean rightTemporal = isTemporal(right);
        if (leftChar && rightTemporal) {
            newLeft = DatePartAdapters.coerceCharacterOperandToTimestamp(left, cluster);
        } else if (rightChar && leftTemporal) {
            newRight = DatePartAdapters.coerceCharacterOperandToTimestamp(right, cluster);
        } else {
            return original;
        }
        if (newLeft == left && newRight == right) {
            return original;
        }
        return rebuild(original, newLeft, newRight, cluster);
    }

    /** Cast an {@link IpType} operand to plain {@code VARBINARY}, or return the operand unchanged. */
    private static RexNode castIpTypeToVarbinary(RexNode node, RelOptCluster cluster) {
        if (!(node.getType() instanceof IpType)) {
            return node;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataType varbinary = cluster.getTypeFactory()
            .createTypeWithNullability(
                cluster.getTypeFactory().createSqlType(SqlTypeName.VARBINARY),
                node.getType().isNullable()
            );
        return rexBuilder.makeAbstractCast(varbinary, node);
    }

    /** TIME (or wrapped TIME) compared with DATE/TIMESTAMP — rewrite to today-anchored TIMESTAMP, preserving any outer annotation. */
    private static RexNode coerceTimeIfApplicable(RexNode self, RexNode other, RelOptCluster cluster) {
        RexNode timeInner = unwrapToTime(self);
        if (timeInner == null) {
            return self;
        }
        if (unwrapToTime(other) != null || !isDateOrTimestampOrWrapped(other)) {
            return self;
        }
        RexNode rewritten = DatePartAdapters.coerceCharacterOperandToTimestamp(timeInner, cluster);
        if (self instanceof OperatorAnnotation annotation && annotation.unwrap() != null) {
            return annotation.withAdaptedOriginal(rewritten);
        }
        return rewritten;
    }

    /** True for a raw DATE/TIMESTAMP, or a {@code to_timestamp(date)}/{@code CAST(date AS TIMESTAMP)} wrapper. */
    private static boolean isDateOrTimestampOrWrapped(RexNode node) {
        RexNode stripped = stripAnnotation(node);
        if (isDateOrTimestamp(stripped)) {
            return true;
        }
        if (stripped instanceof RexCall call && call.getOperands().size() == 1) {
            return isDateOrTimestamp(stripAnnotation(call.getOperands().get(0)))
                && (call.getOperator() == DateTimeAdapters.LOCAL_TO_TIMESTAMP_OP || call.isA(SqlKind.CAST));
        }
        return false;
    }

    /** Returns the TIME-typed inner if node is raw TIME or a single-operand to_timestamp/CAST around TIME; else null. Strips OperatorAnnotation at every level — recursion threads annotations through intermediate nodes. */
    private static RexNode unwrapToTime(RexNode node) {
        RexNode stripped = stripAnnotation(node);
        if (isTime(stripped)) {
            return stripped;
        }
        if (stripped instanceof RexCall call && call.getOperands().size() == 1) {
            RexNode inner = stripAnnotation(call.getOperands().get(0));
            if (isTime(inner) && (call.getOperator() == DateTimeAdapters.LOCAL_TO_TIMESTAMP_OP || call.isA(SqlKind.CAST))) {
                return inner;
            }
        }
        return null;
    }

    private static RexNode stripAnnotation(RexNode node) {
        while (node instanceof OperatorAnnotation annotation && annotation.unwrap() != null) {
            node = annotation.unwrap();
        }
        return node;
    }

    private static RexNode rebuild(RexCall original, RexNode newLeft, RexNode newRight, RelOptCluster cluster) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        List<RexNode> coerced = new ArrayList<>(2);
        coerced.add(newLeft);
        coerced.add(newRight);
        return rexBuilder.makeCall(original.getType(), original.getOperator(), coerced);
    }

    private static boolean isCharacter(RexNode node) {
        return SqlTypeFamily.CHARACTER.contains(node.getType());
    }

    private static boolean isTime(RexNode node) {
        SqlTypeName name = node.getType().getSqlTypeName();
        return name == SqlTypeName.TIME || name == SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE;
    }

    private static boolean isDateOrTimestamp(RexNode node) {
        SqlTypeName name = node.getType().getSqlTypeName();
        return name == SqlTypeName.DATE || name == SqlTypeName.TIMESTAMP || name == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
    }

    private static boolean isTemporal(RexNode node) {
        RelDataType type = node.getType();
        SqlTypeName name = type.getSqlTypeName();
        return name == SqlTypeName.TIMESTAMP
            || name == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE
            || name == SqlTypeName.DATE
            || name == SqlTypeName.TIME
            || name == SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE;
    }
}