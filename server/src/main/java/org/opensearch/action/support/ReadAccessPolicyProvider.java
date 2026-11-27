/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.common.annotation.ExperimentalApi;

/** Supplies effective read-access policies for the current request. */
@ExperimentalApi
@FunctionalInterface
public interface ReadAccessPolicyProvider {

    /**
     * Returns the effective document-level policy for the concrete indices in {@code context} and the current read request.
     * For a user read, call this method after authorization with that request's user authorization context installed on
     * the calling thread. An internal read must carry its system request context. The returned policy must retain the
     * request-specific authorization information it needs so its methods can be used on another thread during the same
     * request without the original thread context. Callers must not cache it for another request or user;
     * implementations may return the stateless {@link DocumentLevelReadAccessPolicy#unrestricted()} policy when appropriate.
     */
    DocumentLevelReadAccessPolicy getDocumentLevelReadAccessPolicy(ReadAccessContext context);
}
