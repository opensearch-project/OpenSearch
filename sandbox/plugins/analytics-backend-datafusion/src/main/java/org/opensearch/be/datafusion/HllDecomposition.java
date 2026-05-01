/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.opensearch.analytics.spi.AggregateDecomposition;

import java.util.List;

/**
 * Decomposes APPROX_COUNT_DISTINCT (HLL) for distributed execution.
 *
 * <p>PARTIAL phase emits one column: {@code APPROX_COUNT_DISTINCT(x): BIGINT}.
 * DataFusion's {@code StripFinalAggregateRule} ensures the shard runs only the
 * Partial AggregateExec, emitting binary HLL sketch bytes.
 *
 * <p>FINAL phase: the FINAL AggregateExec merges the HLL sketches (via
 * {@code StripPartialAggregateRule} on the coordinator). The final expression
 * is an identity passthrough — the FINAL aggregate produces the merged count.
 */
public class HllDecomposition implements AggregateDecomposition {

    public static final HllDecomposition INSTANCE = new HllDecomposition();

    @Override
    public List<AggregateCall> partialCalls(AggregateCall originalCall, org.apache.calcite.rel.RelNode input) {
        // Same function — DataFusion's physical optimizer rules handle partial/final split
        return List.of(AggregateCall.create(
            SqlStdOperatorTable.APPROX_COUNT_DISTINCT,
            true, true, false,
            originalCall.getArgList(), -1, null, RelCollations.EMPTY,
            1, input, null,
            originalCall.name
        ));
    }

    @Override
    public RexNode finalExpression(RexBuilder rexBuilder, List<RexNode> partialRefs) {
        // Identity: the FINAL AggregateExec produces the merged count directly
        return partialRefs.get(0);
    }
}
