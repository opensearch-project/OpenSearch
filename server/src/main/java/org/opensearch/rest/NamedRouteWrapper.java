/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import java.util.Set;

/**
 * The interface serves as a contract for different types of named routes
 */
public interface NamedRouteWrapper {
    /**
     * The name of the Route. Must be unique across route .
     * @return the name of this handler
     */
    String name();

    /**
     * The action names associate with the Route.
     * @return the set of action names registered for this route
     */
    Set<String> actionNames();
}
