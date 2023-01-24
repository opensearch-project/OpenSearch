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
import java.util.stream.Collectors;

public class RoutesHelper {

    /**
     * Add prefixes to rest API routes for identity
     * @param routes routes
     * @return new list of API routes prefixed with _identity/api...
     */
    public static List<RestHandler.Route> addRoutesPrefix(List<RestHandler.Route> routes) {
        return addRoutesPrefix(routes, "_identity/api");
    }

    /**
     * Add customized prefix to API rest routes
     * @param routes routes
     * @param prefixes prefix to be applied to all APIs
     * @return new list of API routes prefixed with the strings listed in prefixes
     */
    public static List<RestHandler.Route> addRoutesPrefix(List<RestHandler.Route> routes, final String... prefixes) {
        return routes.stream()
            .flatMap(r -> Arrays.stream(prefixes).map(p -> new RestHandler.Route(r.getMethod(), p + r.getPath())))
            .collect(Collectors.toUnmodifiableList());
    }
}
