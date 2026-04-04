/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.planner.FieldStorageInfo;
import org.opensearch.analytics.spi.OperatorCapability;

import java.util.List;

/**
 * Marker interface for all OpenSearch custom RelNodes that carry backend assignment
 * and per-column storage metadata.
 *
 * <p>Each node computes {@link #getOutputFieldStorage()} from its input's metadata.
 * Parent operators read this to make backend routing decisions.
 *
 * <p>{@link #getViableBackends()} lists all backends that could execute this operator
 * (including via delegation). Consumed during plan forking to generate one complete
 * plan per viable backend.
 *
 * @opensearch.internal
 */
public interface OpenSearchRelNode {

    /** All backends that could execute this operator, including via delegation. */
    List<String> getViableBackends();

    /** Per-column storage metadata aligned with this node's output row type field order. */
    List<FieldStorageInfo> getOutputFieldStorage();

    /** Returns annotations inside this operator (predicates, calls, expressions). Empty if none. */
    default List<OperatorAnnotation> getAnnotations() {
        return List.of();
    }

    /**
     * Creates a copy with viableBackends narrowed to the given backend and
     * annotations replaced with the resolved versions.
     *
     * @param backend              the chosen backend for this operator
     * @param children             resolved child RelNodes
     * @param resolvedAnnotations  annotations narrowed to single backends, same order as {@link #getAnnotations()}
     */
    RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations);

    // TODO: add RelNode stripAnnotations(List<RelNode> strippedChildren) — returns a clean standard
    // Calcite RelNode (e.g., LogicalFilter, LogicalAggregate) with viableBackends dropped and
    // annotations unwrapped to original expressions. Needed before handing to backend's FragmentConvertor.

    /**
     * Whether this operator's annotations may benefit from a different backend
     * than its child's. Controls plan branching in PlanForker — operators that
     * return true branch across annotation backend groups. Others inherit the
     * pipeline backend for annotation resolution, reducing plan count.
     *
     * <p>Currently only Filter returns true (index access vs column scan).
     * All other operators inherit the pipeline backend.
     *
     * <p>TODO: hook in external benchmark-driven configuration to control branching
     * per operator type. E.g., if benchmarks show backend X aggregates 3x faster
     * than Y for a given data shape, generate an alternative with X for aggregate.
     */
    default boolean hasDelegationAdvantage() {
        return false;
    }

    /**
     * The operator capability type for this node. Used by PlanForker to check
     * arrow compatibility when considering cross-backend operator assignment.
     * Returns null for nodes that don't map to a standard operator (e.g., exchange nodes).
     */
    default OperatorCapability getOperatorCapability() {
        return null;
    }
}
