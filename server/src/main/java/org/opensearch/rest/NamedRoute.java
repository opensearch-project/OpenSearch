/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.opensearch.OpenSearchException;
import org.opensearch.transport.TransportService;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * A named Route
 *
 * @opensearch.internal
 */
public class NamedRoute extends RestHandler.Route {

    private static final String VALID_ACTION_NAME_PATTERN = "^[a-zA-Z0-9:/*_]*$";
    static final int MAX_LENGTH_OF_ACTION_NAME = 250;

    private final String uniqueName;
    private final Set<String> actionNames;

    private Function<RestRequest, RestResponse> handler;

    /**
     * Builder class for constructing instances of {@link NamedRoute}.
     */
    public static class Builder {
        private RestRequest.Method method;
        private String path;
        private String uniqueName;
        private final Set<String> legacyActionNames = new HashSet<>();
        private Function<RestRequest, RestResponse> handler;

        /**
         * Sets the REST method for the route.
         *
         * @param method the REST method for the route
         * @return the builder instance
         */
        public Builder method(RestRequest.Method method) {
            requireNonNull(method, "REST method must not be null.");
            this.method = method;
            return this;
        }

        /**
         * Sets the URL path for the route.
         *
         * @param path the URL path for the route
         * @return the builder instance
         */
        public Builder path(String path) {
            requireNonNull(path, "REST path must not be null.");
            this.path = path;
            return this;
        }

        /**
         * Sets the name for the route.
         *
         * @param name the name for the route
         * @return the builder instance
         */
        public Builder uniqueName(String name) {
            requireNonNull(name, "REST route name must not be null.");
            this.uniqueName = name;
            return this;
        }

        /**
         * Sets the legacy action names for the route.
         *
         * @param legacyActionNames the legacy action names for the route
         * @return the builder instance
         */
        public Builder legacyActionNames(Set<String> legacyActionNames) {
            this.legacyActionNames.addAll(validateLegacyActionNames(legacyActionNames));
            return this;
        }

        /**
         * Sets the handler for this route
         *
         * @param handler the handler for this route
         * @return the builder instance
         */
        public Builder handler(Function<RestRequest, RestResponse> handler) {
            requireNonNull(handler, "Route handler must not be null.");
            this.handler = handler;
            return this;
        }

        /**
         * Builds a new instance of {@link NamedRoute} based on the provided parameters.
         *
         * @return a new instance of {@link NamedRoute}
         * @throws OpenSearchException if the route name is invalid
         */
        public NamedRoute build() {
            checkIfFieldsAreSet();
            return new NamedRoute(this);
        }

        /**
         * Checks if all builder fields are set before creating a new NamedRoute object
         */
        private void checkIfFieldsAreSet() {
            if (method == null || path == null || uniqueName == null) {
                throw new IllegalStateException("REST method, path and uniqueName are required.");
            }
        }

        private Set<String> validateLegacyActionNames(Set<String> legacyActionNames) {
            if (legacyActionNames == null) {
                return new HashSet<>();
            }
            for (String actionName : legacyActionNames) {
                if (!TransportService.isValidActionName(actionName)) {
                    throw new OpenSearchException(
                        "Invalid action name [" + actionName + "]. It must start with one of: " + TransportService.VALID_ACTION_PREFIXES
                    );
                }
            }
            return legacyActionNames;
        }

    }

    private NamedRoute(Builder builder) {
        super(builder.method, builder.path);
        if (!isValidRouteName(builder.uniqueName)) {
            throw new OpenSearchException(
                "Invalid route name specified. The route name may include the following characters"
                    + " 'a-z', 'A-Z', '0-9', ':', '/', '*', '_' and be less than "
                    + MAX_LENGTH_OF_ACTION_NAME
                    + " characters"
            );
        }
        this.uniqueName = builder.uniqueName;
        this.actionNames = Set.copyOf(builder.legacyActionNames);
        this.handler = builder.handler;
    }

    public boolean isValidRouteName(String routeName) {
        return routeName != null
            && !routeName.isBlank()
            && routeName.length() <= MAX_LENGTH_OF_ACTION_NAME
            && routeName.matches(VALID_ACTION_NAME_PATTERN);
    }

    /**
     * The name of the Route. Must be unique across Route.
     */
    public String name() {
        return this.uniqueName;
    }

    /**
     * The legacy transport Action name to match against this route to support authorization in REST layer.
     * MUST be unique across all Routes
     */
    public Set<String> actionNames() {
        return this.actionNames;
    }

    /**
     * The handler associated with this route
     * @return the handler associated with this route
     */
    public Function<RestRequest, RestResponse> handler() {
        return handler;
    }

    @Override
    public String toString() {
        return "NamedRoute [method=" + method + ", path=" + path + ", name=" + uniqueName + ", actionNames=" + actionNames + "]";
    }
}
