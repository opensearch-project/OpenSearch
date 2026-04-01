/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.rel;

import org.opensearch.analytics.planner.FieldStorageInfo;

import java.util.List;

/**
 * Marker interface for all OpenSearch custom RelNodes that carry backend assignment
 * and per-column storage metadata.
 *
 * <p>Each node computes {@link #getOutputFieldStorage()} from its input's metadata.
 * Parent operators read this to make backend routing decisions.
 *
 * <p>{@link #getBackend()} is the default/preferred backend chosen by the marking rule.
 * {@link #getViableBackends()} lists all backends that could execute this operator
 * (including via delegation). Consumed by {@code BackendResolver.generateCandidatePlans()}
 * during StagePlan alternative generation.
 *
 * @opensearch.internal
 */
public interface OpenSearchRelNode {

    String UNRESOLVED = "unresolved";

    /** Default/preferred backend chosen by the marking rule. */
    String getBackend();

    /** All backends that could execute this operator, including via delegation. */
    List<String> getViableBackends();

    /** Per-column storage metadata aligned with this node's output row type field order. */
    List<FieldStorageInfo> getOutputFieldStorage();
}
