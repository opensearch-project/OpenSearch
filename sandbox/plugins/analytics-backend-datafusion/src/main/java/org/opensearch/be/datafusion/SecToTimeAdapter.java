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
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.ScalarFunctionAdapter;

import java.util.List;

/**
 * PPL {@code sec_to_time(n)} — the time-of-day {@code n} seconds past midnight (returns TIME).
 *
 * <p>Lowering: cast {@code n} to BIGINT and hand it to {@link TimeOfDayLowering#secondsToTime}, which
 * normalizes into {@code [0, 86400)} and builds a {@code Time64} via the {@code maketime(h, m, s)}
 * Rust UDF. MySQL wraps second counts past a day (e.g. {@code sec_to_time(123456) = 10:17:36}); the
 * helper's floor-mod normalization provides that wrap. A direct {@code from_unixtime(n)} +
 * {@code CAST AS TIME} can't be used — DataFusion's optimizer rejects {@code CAST from Timestamp to
 * Time} — so {@code maketime} produces the {@code Time64} directly.
 *
 * @opensearch.internal
 */
class SecToTimeAdapter implements ScalarFunctionAdapter {

    @Override
    public RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage, RelOptCluster cluster) {
        if (original.getOperands().size() != 1) {
            return original;
        }
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelDataType bigint = rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);
        RexNode seconds = rexBuilder.makeCast(bigint, original.getOperands().get(0), true);
        return TimeOfDayLowering.secondsToTime(seconds, original.getType(), cluster);
    }
}
