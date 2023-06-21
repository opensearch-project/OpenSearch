/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.rest;

import org.opensearch.client.node.ProtobufNodeClient;
import org.opensearch.core.xcontent.XContent;
import org.opensearch.rest.RestRequest.Method;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Handler for REST requests
*
* @opensearch.api
*/
@FunctionalInterface
public interface ProtobufRestHandler {

    /**
     * Handles a rest request.
    * @param request The request to handle
    * @param channel The channel to write the request response to
    * @param client A client to use to make internal requests on behalf of the original request
    */
    void handleRequest(RestRequest request, RestChannel channel, ProtobufNodeClient client) throws Exception;

    default boolean canTripCircuitBreaker() {
        return true;
    }

    /**
     * Indicates if the ProtobufRestHandler supports content as a stream. A stream would be multiple objects delineated by
    * {@link XContent#streamSeparator()}. If a handler returns true this will affect the types of content that can be sent to
    * this endpoint.
    */
    default boolean supportsContentStream() {
        return false;
    }

    /**
     * Indicates if the ProtobufRestHandler supports working with pooled buffers. If the request handler will not escape the return
    * {@link RestRequest#content()} or any buffers extracted from it then there is no need to make a copies of any pooled buffers in the
    * {@link RestRequest} instance before passing a request to this handler. If this instance does not support pooled/unsafe buffers
    * {@link RestRequest#ensureSafeBuffers()} should be called on any request before passing it to {@link #handleRequest}.
    *
    * @return true iff the handler supports requests that make use of pooled buffers
    */
    default boolean allowsUnsafeBuffers() {
        return false;
    }

    /**
     * The list of {@link Route}s that this ProtobufRestHandler is responsible for handling.
    */
    default List<Route> routes() {
        return Collections.emptyList();
    }

    /**
     * A list of routes handled by this ProtobufRestHandler that are deprecated and do not have a direct
    * replacement. If changing the {@code path} or {@code method} of a route,
    * use {@link #replacedRoutes()}.
    */
    default List<DeprecatedRoute> deprecatedRoutes() {
        return Collections.emptyList();
    }

    /**
     * A list of routes handled by this ProtobufRestHandler that have had their {@code path} and/or
    * {@code method} changed. The pre-existing {@code route} will be registered
    * as deprecated alongside the updated {@code route}.
    */
    default List<ReplacedRoute> replacedRoutes() {
        return Collections.emptyList();
    }

    /**
     * Controls whether requests handled by this class are allowed to to access system indices by default.
    * @return {@code true} if requests handled by this class should be allowed to access system indices.
    */
    default boolean allowSystemIndexAccessByDefault() {
        return false;
    }

    static ProtobufRestHandler wrapper(ProtobufRestHandler delegate) {
        return new Wrapper(delegate);
    }

    /**
     * Wrapper for a handler.
    *
    * @opensearch.internal
    */
    class Wrapper implements ProtobufRestHandler {
        private final ProtobufRestHandler delegate;

        public Wrapper(ProtobufRestHandler delegate) {
            this.delegate = Objects.requireNonNull(delegate, "ProtobufRestHandler delegate can not be null");
        }

        @Override
        public String toString() {
            return delegate.toString();
        }

        @Override
        public void handleRequest(RestRequest request, RestChannel channel, ProtobufNodeClient client) throws Exception {
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
    }

    /**
     * Route for the request.
    *
    * @opensearch.internal
    */
    class Route {

        private final String path;
        private final Method method;

        public Route(Method method, String path) {
            this.path = path;
            this.method = method;
        }

        public String getPath() {
            return path;
        }

        public Method getMethod() {
            return method;
        }
    }

    /**
     * Represents an API that has been deprecated and is slated for removal.
    */
    class DeprecatedRoute extends Route {

        private final String deprecationMessage;

        public DeprecatedRoute(Method method, String path, String deprecationMessage) {
            super(method, path);
            this.deprecationMessage = deprecationMessage;
        }

        public String getDeprecationMessage() {
            return deprecationMessage;
        }
    }

    /**
     * Represents an API that has had its {@code path} or {@code method} changed. Holds both the
    * new and previous {@code path} and {@code method} combination.
    */
    class ReplacedRoute extends Route {

        private final String deprecatedPath;
        private final Method deprecatedMethod;

        /**
         * Construct replaced routes using new and deprocated methods and new and deprecated paths
        * @param method route method
        * @param path new route path
        * @param deprecatedMethod deprecated method
        * @param deprecatedPath deprecated path
        */
        public ReplacedRoute(Method method, String path, Method deprecatedMethod, String deprecatedPath) {
            super(method, path);
            this.deprecatedMethod = deprecatedMethod;
            this.deprecatedPath = deprecatedPath;
        }

        /**
         * Construct replaced routes using route method, new and deprecated paths
        * This constructor can be used when both new and deprecated paths use the same method
        * @param method route method
        * @param path new route path
        * @param deprecatedPath deprecated path
        */
        public ReplacedRoute(Method method, String path, String deprecatedPath) {
            this(method, path, method, deprecatedPath);
        }

        /**
         * Construct replaced routes using route, new and deprecated prefixes
        * @param route route
        * @param prefix new route prefix
        * @param deprecatedPrefix deprecated prefix
        */
        public ReplacedRoute(Route route, String prefix, String deprecatedPrefix) {
            this(route.getMethod(), prefix + route.getPath(), deprecatedPrefix + route.getPath());
        }

        public String getDeprecatedPath() {
            return deprecatedPath;
        }

        public Method getDeprecatedMethod() {
            return deprecatedMethod;
        }
    }

    /**
     * Construct replaced routes using routes template and prefixes for new and deprecated paths
    * @param routes routes
    * @param prefix new prefix
    * @param deprecatedPrefix deprecated prefix
    * @return new list of API routes prefixed with the prefix string
    */
    static List<ReplacedRoute> replaceRoutes(List<Route> routes, final String prefix, final String deprecatedPrefix) {
        return routes.stream().map(route -> new ReplacedRoute(route, prefix, deprecatedPrefix)).collect(Collectors.toList());
    }
}
