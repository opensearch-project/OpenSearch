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

    /** Returns the effective document-level policy for {@code context}. */
    DocumentLevelReadAccessPolicy getDocumentLevelReadAccessPolicy(ReadAccessContext context);
}
