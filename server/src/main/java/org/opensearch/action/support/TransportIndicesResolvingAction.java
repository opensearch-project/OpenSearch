/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support;

import org.opensearch.action.ActionRequest;
import org.opensearch.cluster.metadata.ResolvedIndices;

/**
 * An additional interface that should be implemented by TransportAction implementations which need to resolve
 * IndicesRequests or other action requests which specify indices. This interface allows other components to retrieve
 * precise information about the indices an action is going to operate on. This is particularly useful for access
 * control implementations, but can be also used for other purposes, such as monitoring, audit logging, etc.
 * <p>
 * Classes implementing this interface should make sure that the reported indices are also actually the indices
 * the action will operate on. The best way to achieve this, is to move the index extraction code from the execute
 * methods into reusable methods and to depend on these both for execution and reporting.
 */
public interface TransportIndicesResolvingAction<Request extends ActionRequest> {

    /**
     * Returns the actual indices the action will operate on, given the specified request and cluster state.
     */
    ResolvedIndices resolveIndices(Request request);
}
