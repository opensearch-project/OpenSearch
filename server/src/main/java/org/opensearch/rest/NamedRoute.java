/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.transport.TransportService;

/**
 * A named Route
 *
 * @opensearch.internal
 */
public class NamedRoute extends RestHandler.Route {

    private static final Logger logger = LogManager.getLogger(NamedRoute.class);
    private static final String VALID_ACTION_NAME_PATTERN = "^[a-zA-Z0-9:/*_]*$";
    static final int MAX_LENGTH_OF_ACTION_NAME = 250;

    private String name;

    private String legacyActionName;

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
     * Allows registering a legacyName to match against transport action
     * @param method - The REST method for this route
     * @param path - the URL for this route
     * @param name - the shortname for this route
     * @param legacyActionName - name of the transport action this route will be matched against
     */
    public NamedRoute(RestRequest.Method method, String path, String name, String legacyActionName) {
        this(method, path, name);
        if (!TransportService.isValidActionName(legacyActionName)) {
            logger.warn("invalid action name [" + legacyActionName + "] must start with one of: " + TransportService.VALID_ACTION_PREFIXES);
        }
        this.legacyActionName = legacyActionName;
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
    public String legacyActionName() {
        return this.legacyActionName;
    }

    @Override
    public String toString() {
        if(legacyActionName == null) {
            return "NamedRoute [method=" + method + ", path=" + path + ", name=" + name + "]";
        }
        return "NamedRoute [method=" + method + ", path=" + path + ", name=" + name + ", legacyActionName=" + legacyActionName + "]";
    }
}
