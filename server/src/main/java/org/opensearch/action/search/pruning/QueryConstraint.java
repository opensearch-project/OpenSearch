/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search.pruning;

/**
 * Mandatory query-side condition that can be evaluated against index-level field domains.
 */
public interface QueryConstraint {
    /**
     * Field name constrained by the query.
     */
    String field();
}
