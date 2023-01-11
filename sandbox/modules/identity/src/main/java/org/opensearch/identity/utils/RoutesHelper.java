/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.utils;

import org.opensearch.rest.RestHandler;

import java.util.Arrays;
import java.util.List;

public class RoutesHelper {

    /**
     * Add prefixes(_opendistro... and _plugins...) to rest API routes
     * @param routes routes
     * @return new list of API routes prefixed with _opendistro... and _plugins...
     *Total number of routes is expanded as twice as the number of routes passed in
     */
    public static List<RestHandler.Route> addRoutesPrefix(List<RestHandler.Route> routes) {
        return addRoutesPrefix(routes, "/_opendistro/_security/api", "/_plugins/_security/api");
    }

    /**
     * Add customized prefix(_opendistro... and _plugins...)to API rest routes
     * @param routes routes
     * @param prefixes all api prefix
     * @return new list of API routes prefixed with the strings listed in prefixes
     * Total number of routes will be expanded len(prefixes) as much comparing to the list passed in
     */
    public static List<RestHandler.Route> addRoutesPrefix(List<RestHandler.Route> routes, final String... prefixes) {
        return routes.stream()
            .flatMap(r -> Arrays.stream(prefixes).map(p -> new RestHandler.Route(r.getMethod(), p + r.getPath())))
            .collect(ImmutableList.toImmutableList());
    }

    /**
     * Add prefixes(_plugins...) to rest API routes
     * @param deprecatedRoutes Routes being deprecated
     * @return new list of API routes prefixed with _opendistro... and _plugins...
     *Total number of routes is expanded as twice as the number of routes passed in
     */
    public static List<RestHandler.DeprecatedRoute> addDeprecatedRoutesPrefix(List<RestHandler.DeprecatedRoute> deprecatedRoutes) {
        return addDeprecatedRoutesPrefix(deprecatedRoutes, "/_opendistro/_security/api", "/_plugins/_security/api");
    }

    /**
     * Add customized prefix(_opendistro... and _plugins...)to API rest routes
     * @param deprecatedRoutes Routes being deprecated
     * @param prefixes all api prefix
     * @return new list of API routes prefixed with the strings listed in prefixes
     * Total number of routes will be expanded len(prefixes) as much comparing to the list passed in
     */
    public static List<RestHandler.DeprecatedRoute> addDeprecatedRoutesPrefix(
        List<RestHandler.DeprecatedRoute> deprecatedRoutes,
        final String... prefixes
    ) {
        return deprecatedRoutes.stream()
            .flatMap(
                r -> Arrays.stream(prefixes)
                    .map(p -> new RestHandler.DeprecatedRoute(r.getMethod(), p + r.getPath(), r.getDeprecationMessage()))
            )
            .collect(ImmutableList.toImmutableList());
    }
}
