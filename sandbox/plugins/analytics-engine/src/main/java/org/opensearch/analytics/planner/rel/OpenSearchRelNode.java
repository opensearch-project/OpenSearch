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
 * <p>{@link #getViableBackends()} lists all backends that could execute this operator
 * (including via delegation). The first entry is the default/preferred backend.
 * Consumed during plan forking to generate one complete plan per viable backend.
 *
 * @opensearch.internal
 */
public interface OpenSearchRelNode {

    /** All backends that could execute this operator, including via delegation. */
    List<String> getViableBackends();

    /** Per-column storage metadata aligned with this node's output row type field order. */
    List<FieldStorageInfo> getOutputFieldStorage();
}
