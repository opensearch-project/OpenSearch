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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.NumericToDoubleAdapter;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * PPL {@code ADDTIME(a, b)} / {@code SUBTIME(a, b)} — add (or subtract) the <em>time-of-day</em> of
 * {@code b} to/from the temporal value {@code a}.
 *
 * <p>Return type (already inferred onto the call by the PPL frontend): {@code TIME} when {@code a}
 * is a {@code TIME}, otherwise {@code TIMESTAMP}. {@code b} contributes only its seconds-of-day,
 * {@code delta = to_unixtime(anchor(b)) MOD 86400}.
 *
 * <ul>
 *   <li><b>TIMESTAMP result:</b> {@code from_unixtime(to_unixtime(a) ± delta)} — whole-second epoch
 *       arithmetic, rebuilt into a timestamp.</li>
 *   <li><b>TIME result</b> (a is TIME): {@code maketime} of {@code secondsOfDay(a) ± delta},
 *       normalized into a day — see {@link TimeOfDayLowering#secondsToTime}.</li>
 * </ul>
 *
 * <p>TIME operands are anchored to a TIMESTAMP first because {@code to_unixtime} rejects an Arrow
 * {@code Time64} (see {@link TimeOfDayLowering#secondsOfDay}). {@code from_unixtime} takes fp64.
 *
 * @opensearch.internal
 */
class AddSubTimeAdapter implements ScalarFunctionAdapter {

    private final boolean isAdd;

    AddSubTimeAdapter(boolean isAdd) {
        this.isAdd = isAdd;
    }

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 2) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode a = original.getOperands().get(0);
        RexNode b = original.getOperands().get(1);
        RexNode delta = TimeOfDayLowering.secondsOfDay(b, cluster);
        org.apache.calcite.sql.SqlOperator addsub = isAdd ? SqlStdOperatorTable.PLUS : SqlStdOperatorTable.MINUS;

        if (original.getType().getSqlTypeName() == SqlTypeName.TIME) {
            RexNode combined = rexBuilder.makeCall(addsub, TimeOfDayLowering.secondsOfDay(a, cluster), delta);
            return TimeOfDayLowering.secondsToTime(combined, original.getType(), cluster);
        }

        // TIMESTAMP result: from_unixtime(to_unixtime(a) ± delta).
        RexNode anchoredA = DatePartAdapters.coerceCharacterOperandToTimestamp(a, cluster);
        RexNode aEpoch = rexBuilder.makeCall(UnixTimestampAdapter.LOCAL_TO_UNIXTIME_OP, anchoredA);
        RexNode epoch = rexBuilder.makeCall(addsub, aEpoch, delta);
        RexNode epochDouble = NumericToDoubleAdapter.widenToDoubleIfNumeric(epoch, cluster);
        RelDataType tsType = rexBuilder.getTypeFactory()
            .createTypeWithNullability(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP), true);
        RexNode ts = rexBuilder.makeCall(tsType, RustUdfDateTimeAdapters.LOCAL_FROM_UNIXTIME_OP, List.of(epochDouble));
        if (ts.getType().equals(original.getType())) {
            return ts;
        }
        return rexBuilder.makeCast(original.getType(), ts, true);
    }
}
