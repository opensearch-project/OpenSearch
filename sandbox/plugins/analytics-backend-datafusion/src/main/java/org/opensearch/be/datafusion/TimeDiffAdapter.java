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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * PPL {@code TIMEDIFF(a, b)} (engine label {@code TIME_DIFF}) — the difference between two
 * time-of-day values as a {@code TIME}: {@code maketime} of
 * {@code secondsOfDay(a) - secondsOfDay(b)}, normalized into a day (e.g.
 * {@code TIMEDIFF('23:59:59', '13:00:00') = '10:59:59'}).
 *
 * <p>Both operands are anchored to a TIMESTAMP before {@code to_unixtime} (which rejects an Arrow
 * {@code Time64}); the result is rebuilt with {@code maketime} rather than a {@code CAST AS TIME}
 * that DataFusion's optimizer rejects. See {@link TimeOfDayLowering}.
 *
 * @opensearch.internal
 */
class TimeDiffAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 2) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RexNode diff = rexBuilder.makeCall(
            SqlStdOperatorTable.MINUS,
            TimeOfDayLowering.secondsOfDay(original.getOperands().get(0), cluster),
            TimeOfDayLowering.secondsOfDay(original.getOperands().get(1), cluster)
        );
        return TimeOfDayLowering.secondsToTime(diff, original.getType(), cluster);
    }
}
