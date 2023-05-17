/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.opensearch.OpenSearchException;

/**
 * A named Route
 *
 * @opensearch.internal
 */
public class NamedRoute extends RestHandler.Route {
    private static final String VALID_ACTION_NAME_PATTERN = "^[a-zA-Z0-9:/*_]*$";
    static final int MAX_LENGTH_OF_ACTION_NAME = 250;

    private final String name;

    public boolean isValidRouteName(String routeName) {
        if (routeName == null || routeName.isBlank() || routeName.length() > MAX_LENGTH_OF_ACTION_NAME) {
            return false;
        }
        return routeName.matches(VALID_ACTION_NAME_PATTERN);
    }

    public NamedRoute(RestRequest.Method method, String path, String name) {
        super(method, path);
        if (!isValidRouteName(name)) {
            throw new OpenSearchException(
                "Invalid route name specified. The route name may include the following characters"
                    + " 'a-z', 'A-Z', '0-9', ':', '/', '*', '_' and be less than "
                    + MAX_LENGTH_OF_ACTION_NAME
                    + " characters"
            );
        }
        this.name = name;
    }

    /**
     * The name of the Route. Must be unique across Route.
     */
    public String name() {
        return this.name;
    }

    @Override
    public String toString() {
        return "NamedRoute [method=" + method + ", path=" + path + ", name=" + name + "]";
    }
}
