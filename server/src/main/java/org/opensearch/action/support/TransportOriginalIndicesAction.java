/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.action.ActionRequest;

/**
 * Optional contract for transport actions that can expose the original index expressions
 * as provided by the user (e.g., aliases, wildcards, concrete names, etc.).
 * <p>
 * This is useful for internal follow-up requests (such as scroll) that no longer carry an
 * {@link org.opensearch.action.IndicesRequest}, but still need to preserve the user-facing
 * index expressions for downstream components (e.g., request metadata, filters).
 */
public interface TransportOriginalIndicesAction<Request extends ActionRequest> {

    /**
     * Returns the original user-provided index expressions (aliases/wildcards/concrete),
     * if they can be determined for this request. Otherwise, returns null/empty.
     *
     * Implementations should return the expressions exactly as supplied by the user (patterns/aliases),
     *  NOT the expanded concrete index names resolved during execution.
     */
    String[] originalIndices(Request request);
}
