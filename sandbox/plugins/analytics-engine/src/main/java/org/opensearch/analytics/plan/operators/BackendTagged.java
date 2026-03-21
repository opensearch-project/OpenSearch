/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.plan.operators;

import org.apache.calcite.rel.RelNode;

/**
 * Marker interface for all OpenSearch custom RelNode operators.
 * Enables the backend resolution phase (Phase 5) to walk the tree
 * without instanceof chains.
 */
public interface BackendTagged {

    /** Returns the current backend tag, e.g. "unresolved", "datafusion", "lucene". */
    String getBackendTag();

    /**
     * Returns a copy of this operator with the given backend tag applied.
     * Return type is RelNode because each subtype is a different class.
     */
    RelNode withBackendTag(String tag);
}
