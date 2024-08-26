/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.shiro;

import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;

import java.util.List;
import java.util.Objects;

/**
 * Delegating RestHandler that delegates all implementations to original handler
 */
public class DelegatingRestHandler implements RestHandler {

    protected final RestHandler delegate;

    public DelegatingRestHandler(RestHandler delegate) {
        Objects.requireNonNull(delegate, "RestHandler delegate can not be null");
        this.delegate = delegate;
    }

    @Override
    public void handleRequest(RestRequest request, RestChannel channel, NodeClient client) throws Exception {
        delegate.handleRequest(request, channel, client);
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return delegate.canTripCircuitBreaker();
    }

    @Override
    public boolean supportsContentStream() {
        return delegate.supportsContentStream();
    }

    @Override
    public boolean allowsUnsafeBuffers() {
        return delegate.allowsUnsafeBuffers();
    }

    @Override
    public List<Route> routes() {
        return delegate.routes();
    }

    @Override
    public List<DeprecatedRoute> deprecatedRoutes() {
        return delegate.deprecatedRoutes();
    }

    @Override
    public List<ReplacedRoute> replacedRoutes() {
        return delegate.replacedRoutes();
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return delegate.allowSystemIndexAccessByDefault();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    @Override
    public boolean supportsStreaming() {
        return delegate.supportsStreaming();
    }
}
