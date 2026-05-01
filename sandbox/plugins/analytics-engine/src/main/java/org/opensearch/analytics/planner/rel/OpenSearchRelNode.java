/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.apache.calcite.rel.RelNode;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FragmentConvertor;

import java.util.List;

/**
 * Marker interface for all OpenSearch custom RelNodes that carry backend assignment
 * and per-column storage metadata.
 *
 * <p>TODO: consider making this an abstract class storing {@code viableBackends} centrally,
 * with a default {@link #copyResolved} that returns {@code this} when already narrowed to
 * a single backend — avoids unnecessary copies during plan forking.
 *
 * <p>TODO: when JMH benchmarks show RelNode copy/strip as a hotspot, consider
 * preserving the original LogicalXxx fragment alongside the marked fragment through
 * the DAG to avoid reconstruction via {@link #copyResolved} and {@link #stripAnnotations}.
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
     * <p>{@code children} contains the resolved inputs in the same order as
     * the node's inputs. Single-input operators (Filter, Aggregate, Sort)
     * use {@code children.getFirst()}; future multi-input operators (Join) will
     * use multiple entries.
     *
     * @param backend              the chosen backend for this operator
     * @param children             resolved child RelNodes
     * @param resolvedAnnotations  annotations narrowed to single backends, same order as {@link #getAnnotations()}
     */
    RelNode copyResolved(String backend, List<RelNode> children, List<OperatorAnnotation> resolvedAnnotations);

    /**
     * Returns a clean standard Calcite RelNode with viableBackends dropped and
     * annotations unwrapped to original expressions. Passed to the backend's
     * {@link FragmentConvertor}.
     *
     * <p>{@code strippedChildren} follows the same ordering convention as
     * {@code children} in {@link #copyResolved}.
     *
     * @param strippedChildren children already stripped
     */
    RelNode stripAnnotations(List<RelNode> strippedChildren);
}
