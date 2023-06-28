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
public class ReplacedNamedRoute extends RestHandler.ReplacedRoute {

    private static final String VALID_ACTION_NAME_PATTERN = "^[a-zA-Z0-9:/*_]*$";
    static final int MAX_LENGTH_OF_ACTION_NAME = 250;

    private String name;

    private Set<String> actionNames;

    public boolean isValidRouteName(String routeName) {
        if (routeName == null || routeName.isBlank() || routeName.length() > MAX_LENGTH_OF_ACTION_NAME) {
            return false;
        }
        return routeName.matches(VALID_ACTION_NAME_PATTERN);
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

    public ReplacedNamedRoute(
        RestRequest.Method method,
        String path,
        RestRequest.Method deprecatedMethod,
        String deprecatedPath,
        String name
    ) {
        super(method, path, deprecatedMethod, deprecatedPath);
        if (!isValidRouteName(name)) {
            throw new OpenSearchException(
                "Invalid route name specified. The route name may include the following characters"
                    + " 'a-z', 'A-Z', '0-9', ':', '/', '*', '_' and be less than "
                    + MAX_LENGTH_OF_ACTION_NAME
                    + " characters"
            );
        }
        this.name = name;
        this.actionNames = new HashSet<>();
    }

    /**
     * Allows registering a legacyName to match against transport action
     * @param method - The REST method for this route
     * @param path - the URL for this route
     * @param name - the shortname for this route
     * @param legacyActionNames - list of names of the transport action this route will be matched against
     */
    public ReplacedNamedRoute(
        RestRequest.Method method,
        String path,
        RestRequest.Method deprecatedMethod,
        String deprecatedPath,
        String name,
        Set<String> legacyActionNames
    ) {
        this(method, path, deprecatedMethod, deprecatedPath, name);
        this.actionNames = validateLegacyActionNames(legacyActionNames);
    }

    /**
     * Allows registering a legacyName to match against transport action
     * @param method - The REST method for this route
     * @param path - the URL for this route
     * @param name - the shortname for this route
     * @param legacyActionNames - list of names of the transport action this route will be matched against
     */
    public ReplacedNamedRoute(RestRequest.Method method, String path, String deprecatedPath, String name, Set<String> legacyActionNames) {
        this(method, path, method, deprecatedPath, name, legacyActionNames);
    }

    public ReplacedNamedRoute(RestHandler.Route route, String prefix, String deprecatedPrefix, String name, Set<String> legacyActionNames) {
        this(route.getMethod(), prefix + route.getPath(), deprecatedPrefix + route.getPath(), name, legacyActionNames);
    }

    /**
     * The name of the Route. Must be unique across Route.
     */
    public String name() {
        return this.name;
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
        return "ReplacedNamedRoute [method="
            + method
            + ", path="
            + path
            + ", deprecatedMethod="
            + getDeprecatedMethod()
            + ", deprecatedPath="
            + getDeprecatedPath()
            + ", name= "
            + name
            + ", actionNames= "
            + actionNames
            + "]";
    }
}
