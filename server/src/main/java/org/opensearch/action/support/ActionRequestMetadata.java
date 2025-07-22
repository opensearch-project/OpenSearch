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
import org.opensearch.core.action.ActionResponse;

import java.util.Optional;

/**
 * This class can be used to provide metadata about action requests to ActionFilter implementations.
 * At the moment, this class provides information about the requested indices of a request, but it can be
 * extended to transport further metadata.
 */
public class ActionRequestMetadata<Request extends ActionRequest, Response extends ActionResponse> {

    /**
     * Returns an empty meta data object which will just report unknown results.
     */
    public static <Request extends ActionRequest, Response extends ActionResponse> ActionRequestMetadata<Request, Response> empty() {
        @SuppressWarnings("unchecked")
        ActionRequestMetadata<Request, Response> result = (ActionRequestMetadata<Request, Response>) EMPTY;
        return result;
    }

    private static final ActionRequestMetadata<?, ?> EMPTY = new ActionRequestMetadata<>(null, null);

    private final TransportAction<Request, Response> transportAction;
    private final Request request;

    private ResolvedIndices resolvedIndices;
    private boolean resolvedIndicesInitialized;

    ActionRequestMetadata(TransportAction<Request, Response> transportAction, Request request) {
        this.transportAction = transportAction;
        this.request = request;
    }

    /**
     * If the current action request references indices, this method actually referenced indices. That means that any
     * expressions or patterns will be resolved.
     * <p>
     * If the request cannot reference indices OR if the respective action does not support resolving of requests,
     * this returns an empty Optional.
     */
    public Optional<ResolvedIndices> resolvedIndices() {
        if (!(transportAction instanceof TransportIndicesResolvingAction<?>)) {
            return Optional.empty();
        }

        if (this.resolvedIndicesInitialized) {
            return Optional.of(this.resolvedIndices);
        } else {
            return resolveIndices();
        }
    }

    /**
     * Performs the actual index resolution. Index resolution can be relatively costly on big clusters, so we
     * perform it lazily only when requested.
     */
    private Optional<ResolvedIndices> resolveIndices() {
        @SuppressWarnings("unchecked")
        TransportIndicesResolvingAction<Request> indicesResolvingAction = (TransportIndicesResolvingAction<Request>) this.transportAction;
        ResolvedIndices result = indicesResolvingAction.resolveIndices(request);

        this.resolvedIndices = result;
        this.resolvedIndicesInitialized = true;
        return Optional.of(result);
    }
}
