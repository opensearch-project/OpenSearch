/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.analytics.spi.AggregateDecomposition;

import java.util.List;

/**
 * Decomposes AVG into SUM + COUNT for distributed execution.
 *
 * <p>PARTIAL phase emits two columns: {@code [SUM(x): DOUBLE, COUNT(x): BIGINT]}.
 * FINAL phase computes {@code SUM_ref / COUNT_ref} as a scalar expression.
 */
public class AvgDecomposition implements AggregateDecomposition {

    public static final AvgDecomposition INSTANCE = new AvgDecomposition();

    @Override
    public List<AggregateCall> partialCalls(AggregateCall originalCall, org.apache.calcite.rel.RelNode input) {
        AggregateCall sumCall = AggregateCall.create(
            SqlStdOperatorTable.SUM,
            false, false, false,
            originalCall.getArgList(), -1, null, RelCollations.EMPTY,
            1, input, null,
            originalCall.name == null ? "sum$" : originalCall.name + "$sum"
        );
        AggregateCall countCall = AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false, false, false,
            originalCall.getArgList(), -1, null, RelCollations.EMPTY,
            1, input, null,
            originalCall.name == null ? "count$" : originalCall.name + "$count"
        );
        return List.of(sumCall, countCall);
    }

    @Override
    public RexNode finalExpression(RexBuilder rexBuilder, List<RexNode> partialRefs) {
        RexNode sumRef = partialRefs.get(0);
        RexNode countRef = partialRefs.get(1);
        RexNode countAsDouble = rexBuilder.makeCast(
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.DOUBLE),
            countRef
        );
        return rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, sumRef, countAsDouble);
    }
}
