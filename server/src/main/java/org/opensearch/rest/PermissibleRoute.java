/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

/**
 * A named Route
 *
 * @opensearch.api
 */
public class PermissibleRoute extends RestHandler.Route {

    private final String name;
    public PermissibleRoute(RestRequest.Method method, String path, String name) {
        super(method, path);
        this.name = name;
    }

    /**
     * The name of the Route. Must be unique across Route.
     */
    public String name() {
        return this.name;
    }
}
