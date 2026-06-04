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
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.planner.rel.OperatorAnnotation;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

/**
 * Adapter for PPL's {@code WIDTH_BUCKET}, the lowering target for {@code bin <field> bins=N}.
 *
 * <p>Numeric values: rename to the {@code width_bucket} Rust UDF.
 *
 * <p>Timestamp values: the UDF doesn't accept timestamps and substrait can't
 * subtract two {@code precision_timestamp}s. Rewrite to bucket arithmetic in
 * epoch-seconds space, anchored at {@code MIN(ts) OVER ()}:
 * {@code bucket_start = origin + floor((ts − origin) / stride) × stride}.
 *
 * @opensearch.internal
 */
class WidthBucketAdapter implements ScalarFunctionAdapter {

    /** Binds to the {@code width_bucket} Rust UDF via {@code ADDITIONAL_SCALAR_SIGS}. */
    static final SqlOperator LOCAL_WIDTH_BUCKET_OP = new SqlFunction(
        "width_bucket",
        SqlKind.OTHER_FUNCTION,
        ReturnTypes.VARCHAR_2000_NULLABLE,
        null,
        OperandTypes.family(SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC, SqlTypeFamily.NUMERIC),
        SqlFunctionCategory.USER_DEFINED_FUNCTION
    );

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().isEmpty()) {
            return original;
        }
        SqlTypeName firstOperandType = original.getOperands().get(0).getType().getSqlTypeName();
        if (isTimeBased(firstOperandType)) {
            return adaptTimeBased(original, cluster);
        }
        return adaptNumeric(original, cluster);
    }

    private static RexNode adaptNumeric(RexCall original, RelOptCluster cluster) {
        return cluster.getRexBuilder().makeCall(original.getType(), LOCAL_WIDTH_BUCKET_OP, original.getOperands());
    }

    private static RexNode adaptTimeBased(RexCall original, RelOptCluster cluster) {
        Optional<MaxMinOverPair> rangePair = decomposeMinusOverRange(original);
        OptionalInt binsOpt = extractBinsLiteral(original);
        if (rangePair.isPresent() && binsOpt.isPresent() && binsOpt.getAsInt() > 0) {
            return adaptTimeBasedDataAware(original, cluster, rangePair.get(), binsOpt.getAsInt());
        }
        return original;
    }

    private static RexNode adaptTimeBasedDataAware(RexCall original, RelOptCluster cluster, MaxMinOverPair pair, int bins) {
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataTypeFactory typeFactory = cluster.getTypeFactory();
        RexNode tsCol = original.getOperands().get(0);
        RelDataType bigint = typeFactory.createSqlType(SqlTypeName.BIGINT);
        RelDataType fp64 = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        RexNode binsLit = rexBuilder.makeBigintLiteral(BigDecimal.valueOf(bins));

        // Convert all three timestamps to i64 epoch seconds.
        RexNode tsSeconds = rexBuilder.makeCall(bigint, UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, List.of(tsCol));
        RexNode minSeconds = rexBuilder.makeCall(bigint, UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, List.of(pair.minOver()));
        RexNode maxSeconds = rexBuilder.makeCall(bigint, UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, List.of(pair.maxOver()));

        // stride = (max - min) / N
        RexNode rangeSeconds = rexBuilder.makeCall(bigint, SqlStdOperatorTable.MINUS, List.of(maxSeconds, minSeconds));
        RexNode strideSeconds = rexBuilder.makeCall(bigint, SqlStdOperatorTable.DIVIDE, List.of(rangeSeconds, binsLit));

        // bucket_start = floor((ts - min) / stride) * stride + min — integer division floors.
        RexNode tsMinusMin = rexBuilder.makeCall(bigint, SqlStdOperatorTable.MINUS, List.of(tsSeconds, minSeconds));
        RexNode bucketIndex = rexBuilder.makeCall(bigint, SqlStdOperatorTable.DIVIDE, List.of(tsMinusMin, strideSeconds));
        RexNode bucketOffset = rexBuilder.makeCall(bigint, SqlStdOperatorTable.MULTIPLY, List.of(bucketIndex, strideSeconds));
        RexNode bucketStartSeconds = rexBuilder.makeCall(bigint, SqlStdOperatorTable.PLUS, List.of(bucketOffset, minSeconds));

        // from_unixtime takes fp64; outer cast restores the original return type.
        RexNode bucketStartDouble = rexBuilder.makeCast(fp64, bucketStartSeconds, true);
        RexNode bucketStartTimestamp = rexBuilder.makeCall(RustUdfDateTimeAdapters.LOCAL_FROM_UNIXTIME_OP, List.of(bucketStartDouble));
        return rexBuilder.makeCast(original.getType(), bucketStartTimestamp, true);
    }

    private static OptionalInt extractBinsLiteral(RexCall call) {
        if (call.getOperands().size() > 1
            && call.getOperands().get(1) instanceof RexLiteral binsLit
            && binsLit.getValue() instanceof Number num) {
            return OptionalInt.of(num.intValue());
        }
        return OptionalInt.empty();
    }

    /** Match operand 2 against {@code MINUS(MAX OVER (), MIN OVER ())}. */
    private static Optional<MaxMinOverPair> decomposeMinusOverRange(RexCall call) {
        if (call.getOperands().size() < 3) {
            return Optional.empty();
        }
        // analytics-engine wraps operands in OperatorAnnotation before the adapter runs;
        // peel so the pattern match sees the underlying call shape.
        RexNode op2 = unwrapAnnotations(call.getOperands().get(2));
        if (!(op2 instanceof RexCall minusCall) || minusCall.getKind() != SqlKind.MINUS) {
            return Optional.empty();
        }
        if (minusCall.getOperands().size() != 2) {
            return Optional.empty();
        }
        RexNode lhs = unwrapAnnotations(minusCall.getOperands().get(0));
        RexNode rhs = unwrapAnnotations(minusCall.getOperands().get(1));
        if (!(lhs instanceof RexOver maxOver) || !isAggKind(maxOver, SqlKind.MAX)) {
            return Optional.empty();
        }
        if (!(rhs instanceof RexOver minOver) || !isAggKind(minOver, SqlKind.MIN)) {
            return Optional.empty();
        }
        // Conservative: require single-operand window aggs (lowered shape is OVER ()).
        if (maxOver.getOperands().size() != 1 || minOver.getOperands().size() != 1) {
            return Optional.empty();
        }
        return Optional.of(new MaxMinOverPair(maxOver, minOver));
    }

    /** Peel {@link OperatorAnnotation} wrappers so pattern-matching sees the underlying call. */
    private static RexNode unwrapAnnotations(RexNode node) {
        RexNode current = node;
        while (current instanceof OperatorAnnotation annotation && annotation.unwrap() != null) {
            current = annotation.unwrap();
        }
        return current;
    }

    private static boolean isAggKind(RexOver over, SqlKind kind) {
        SqlAggFunction agg = over.getAggOperator();
        return agg != null && agg.getKind() == kind;
    }

    private static boolean isTimeBased(SqlTypeName tn) {
        return tn == SqlTypeName.TIMESTAMP || tn == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE || tn == SqlTypeName.DATE;
    }

    /** Holder for the pattern-matched {@code MAX OVER ()} / {@code MIN OVER ()} pair. */
    private record MaxMinOverPair(RexOver maxOver, RexOver minOver) {
    }
}
