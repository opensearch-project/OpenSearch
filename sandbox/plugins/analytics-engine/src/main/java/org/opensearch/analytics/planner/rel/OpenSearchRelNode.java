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

    /**
     * Returns a clean standard Calcite RelNode with viableBackends dropped and
     * annotations unwrapped to original expressions. Used before handing to
     * backend's FragmentConvertor.
     *
     * <p>TODO: add delegation-aware conversion. When an annotation's backend differs
     * from the operator's backend, extract the expression, ask the delegate backend
     * to convert it, and pass (delegationId, delegateBytes) to the primary backend.
     *
     * @param strippedChildren children already stripped
     */
    RelNode stripAnnotations(List<RelNode> strippedChildren);

    /**
     * The operator capability type for this node. Returns null for nodes that
     * don't map to a standard operator (e.g., exchange nodes, StageInputScan).
     */
    default OperatorCapability getOperatorCapability() {
        return null;
    }
}
