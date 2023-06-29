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

    public static class Builder {
        private RestRequest.Method method;
        private String path;
        private String uniqueName;
        private Set<String> legacyActionNames;

        public Builder method(RestRequest.Method method) {
            this.method = method;
            return this;
        }

        public Builder path(String path) {
            this.path = path;
            return this;
        }

        public Builder uniqueName(String name) {
            this.uniqueName = name;
            return this;
        }

        public Builder legacyActionNames(Set<String> legacyActionNames) {
            this.legacyActionNames = legacyActionNames;
            return this;
        }

        public NamedRoute build() {
            return new NamedRoute(this);
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
        this.actionNames = validateLegacyActionNames(builder.legacyActionNames);
    }

    public boolean isValidRouteName(String routeName) {
        return routeName != null
            && !routeName.isBlank()
            && routeName.length() <= MAX_LENGTH_OF_ACTION_NAME
            && routeName.matches(VALID_ACTION_NAME_PATTERN);
    }

    private Set<String> validateLegacyActionNames(Set<String> legacyActionNames) {
        if (legacyActionNames == null) {
            return new HashSet<>();
        }
        for (String actionName : legacyActionNames) {
            if (!TransportService.isValidActionName(actionName)) {
                throw new OpenSearchException(
                    "invalid action name [" + actionName + "] must start with one of: " + TransportService.VALID_ACTION_PREFIXES
                );
            }
        }
        return legacyActionNames;
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

    @Override
    public String toString() {
        return "NamedRoute [method=" + method + ", path=" + path + ", name=" + uniqueName + ", actionNames=" + actionNames + "]";
    }
}
