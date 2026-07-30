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
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * Rewrites PPL {@code t1 - t2} on TIMESTAMP / DATE operands as
 * {@code to_unixtime(t1) - to_unixtime(t2)} (seconds), since Substrait's default
 * catalog has no {@code subtract(precision_timestamp, ...)} binding. Numeric operands
 * pass through unchanged.
 *
 * @opensearch.internal
 */
class MinusAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperator() != SqlStdOperatorTable.MINUS || original.getOperands().size() != 2) {
            return original;
        }
        RexNode left = original.getOperands().get(0);
        RexNode right = original.getOperands().get(1);
        if (!isDateOrTimestamp(left.getType()) || !isDateOrTimestamp(right.getType())) {
            return original;
        }
        // Leave MINUS(MAX OVER (), MIN OVER ()) for WidthBucketAdapter — it pattern-matches
        // that exact shape to lower `bin <ts> bins=N` into integer-seconds math.
        if (left instanceof RexOver && right instanceof RexOver) {
            return original;
        }

        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode leftSeconds = rexBuilder.makeCall(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, left);
        RexNode rightSeconds = rexBuilder.makeCall(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, right);
        RexNode diffSeconds = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, leftSeconds, rightSeconds);

        // DATE-DATE → integer day-count (legacy convention). Calcite infers the return type
        // as DATE, so returning BIGINT directly prevents ArrowValues from formatting
        // epoch-day 5 as the date "1970-01-06".
        if (left.getType().getSqlTypeName() == SqlTypeName.DATE && right.getType().getSqlTypeName() == SqlTypeName.DATE) {
            RelDataType bigint = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
            RexNode secsPerDay = rexBuilder.makeLiteral(java.math.BigDecimal.valueOf(86_400L), bigint);
            return rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, diffSeconds, secsPerDay);
        }

        // PPL's MINUS(DATETIME, DATETIME) infers TIMESTAMP at the call site; lift the
        // BIGINT seconds back through from_unixtime so Project.isValid type-matches.
        RelDataType fp64 = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE);
        RexNode diffDouble = rexBuilder.makeCast(fp64, diffSeconds, true);
        RexNode asTimestamp = rexBuilder.makeCall(RustUdfDateTimeAdapters.LOCAL_FROM_UNIXTIME_OP, diffDouble);
        return rexBuilder.makeCast(original.getType(), asTimestamp, true);
    }

    private static boolean isDateOrTimestamp(RelDataType type) {
        SqlTypeName name = type.getSqlTypeName();
        return name == SqlTypeName.TIMESTAMP || name == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE || name == SqlTypeName.DATE;
    }
}
