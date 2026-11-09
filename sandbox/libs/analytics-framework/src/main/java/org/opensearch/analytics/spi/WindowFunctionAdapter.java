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
 * Per-function adapter that rewrites a backend-agnostic window {@link RexOver} into a backend-compatible form.
 * Registered by backends keyed by {@link WindowFunction}. Operands, PARTITION BY, and ORDER BY arrive
 * already-adapted from {@code BackendPlanAdapter}'s scalar-recursion pass.
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
