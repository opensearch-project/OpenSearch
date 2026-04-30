/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Describes how a backend decomposes an aggregate function into partial and final phases
 * for distributed execution across shards.
 *
 * <p>When {@link AggregateCapability#decomposition()} is null, the planner applies
 * Calcite's standard decomposition (e.g. AVG → SUM/COUNT, STDDEV_POP → SUM(x²)+SUM(x)+COUNT).
 *
 * <p>When non-null, the planner uses this decomposition during plan forking resolution,
 * after a single backend has been chosen for the aggregate operator. The decomposition
 * rewrites the PARTIAL aggregate's output schema and the FINAL aggregate's input schema
 * as a paired operation — they must be consistent within the same plan alternative.
 *
 * <p>Examples:
 * <ul>
 *   <li>COVAR_POP(x, y): partial emits SUM(x*y), SUM(x), SUM(y), COUNT;
 *       final expression: (SUM(x*y) - SUM(x)*SUM(y)/COUNT) / COUNT</li>
 *   <li>HLL distinct count: partial emits a single HLL sketch accumulator;
 *       final expression: HLL_MERGE(sketches) → cardinality estimate</li>
 * </ul>
 *
 * @opensearch.internal
 */
public interface AggregateDecomposition {

    /**
     * The aggregate calls emitted by the PARTIAL phase for the given original call.
     * These replace the original aggregate call in the PARTIAL operator and define
     * the columns flowing through the exchange to the FINAL operator.
     *
     * <p>The returned calls must use types compatible with
     * Calcite's type system so the exchange row type is well-defined.
     *
     * @param originalCall the original aggregate call being decomposed
     */
    List<AggregateCall> partialCalls(AggregateCall originalCall);

    /**
     * Expression over the partial results that produces the final aggregated value.
     * {@code partialRefs} are {@link org.apache.calcite.rex.RexInputRef} nodes
     * referencing the columns emitted by {@link #partialCalls(AggregateCall)} in order.
     *
     * <p>For AVG: {@code partialRefs.get(0) / partialRefs.get(1)} (SUM / COUNT).
     * For HLL: a call to the backend's HLL_MERGE function over {@code partialRefs.get(0)}.
     */
    RexNode finalExpression(RexBuilder rexBuilder, List<RexNode> partialRefs);
}
