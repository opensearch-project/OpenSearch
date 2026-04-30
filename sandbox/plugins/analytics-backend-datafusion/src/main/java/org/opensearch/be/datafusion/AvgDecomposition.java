/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.AggregateDecomposition;

import java.util.List;

/**
 * Decomposes AVG(x) into PARTIAL: SUM(x), COUNT(x) and FINAL: CAST(SUM) / COUNT.
 *
 * <p>Avoids sending AVG to DataFusion in partial mode where its intermediate
 * state ({@code STRUCT<i64,i64>}) is incompatible with the flat Arrow schema
 * used by the streaming reduce sink.
 */
public class AvgDecomposition implements AggregateDecomposition {

    public static final AvgDecomposition INSTANCE = new AvgDecomposition();

    private AvgDecomposition() {}

    @Override
    public List<AggregateCall> partialCalls(AggregateCall originalCall) {
        String baseName = originalCall.name != null ? originalCall.name : "avg";
        // Use SUM for the partial sum.
        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            originalCall.isDistinct(), false, originalCall.ignoreNulls(),
            List.of(), originalCall.getArgList(), originalCall.filterArg,
            originalCall.distinctKeys, originalCall.collation,
            originalCall.type,
            baseName + "$sum"
        );
        AggregateCall countCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            originalCall.isDistinct(), false, originalCall.ignoreNulls(),
            List.of(), originalCall.getArgList(), originalCall.filterArg,
            originalCall.distinctKeys, originalCall.collation,
            originalCall.type,
            baseName + "$count"
        );
        return List.of(sumCall, countCall);
    }

    @Override
    public RexNode finalExpression(RexBuilder rexBuilder, List<RexNode> partialRefs) {
        // CAST(SUM(partial_sums) AS DOUBLE) / SUM(partial_counts)
        RexNode castSum = rexBuilder.makeCast(
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE), partialRefs.get(0)
        );
        return rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, castSum, partialRefs.get(1));
    }
}
