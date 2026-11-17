/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.rest.RestHandler.Route.Property.ADMINISTRATIVE;
import static org.opensearch.rest.RestRequest.Method.GET;

public class RestHandlerTests extends OpenSearchTestCase {

    public void testRouteHasNoPropertiesByDefault() {
        RestHandler.Route route = new RestHandler.Route(GET, "foo/bar");

        assertFalse(route.hasProperty(ADMINISTRATIVE));
    }

    public void testRouteWithProperty() {
        RestHandler.Route route = new RestHandler.Route(GET, "foo/bar", ADMINISTRATIVE);

        assertTrue(route.hasProperty(ADMINISTRATIVE));
    }
}
