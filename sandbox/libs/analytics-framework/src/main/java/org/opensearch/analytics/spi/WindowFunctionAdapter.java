/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;

import java.util.List;

/**
 * Per-function adapter that transforms a backend-agnostic window function
 * {@link RexOver} into a backend-compatible form. Registered by backends
 * alongside their capability declarations, keyed by {@link WindowFunction}.
 *
 * <p>Examples — DataFusion:
 * <ul>
 *   <li>PPL {@code ARG_MIN(value, ts)} → {@code FIRST_VALUE(value) ORDER BY ts ASC}
 *       (DataFusion has no built-in arg_min UDAF)</li>
 *   <li>PPL {@code DISTINCT_COUNT_APPROX(x)} → {@code APPROX_COUNT_DISTINCT(x)}
 *       (substrait function-name re-bind to a DataFusion built-in)</li>
 * </ul>
 *
 * <p>Operands, PARTITION BY, and ORDER BY have already been recursively adapted
 * by {@code BackendPlanAdapter} when this is called — adapters receive them as
 * pre-adapted inputs and decide how to rewire them around the new operator.
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface WindowFunctionAdapter {

    /**
     * Adapt the given {@link RexOver} for backend compatibility.
     *
     * @param original          the original RexOver (use for type, frame bounds, distinct/ignore-nulls flags)
     * @param adaptedOperands   operands after recursive scalar adaptation
     * @param adaptedPartitions partition-by expressions after recursive scalar adaptation
     * @param adaptedOrderKeys  order-by collations after recursive scalar adaptation
     * @param cluster           provides {@code getRexBuilder()} for constructing new RexNodes
     * @return the adapted expression (typically a fresh {@link RexOver}, but Calcite's
     *         {@code RexBuilder.makeOver} returns the wider {@link RexNode} type, so the
     *         contract permits any RexNode in case an adapter wants to wrap or unwrap)
     */
    RexNode adapt(
        RexOver original,
        List<RexNode> adaptedOperands,
        List<RexNode> adaptedPartitions,
        List<RexFieldCollation> adaptedOrderKeys,
        RelOptCluster cluster
    );
}
