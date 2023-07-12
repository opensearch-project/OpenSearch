/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.OpenSearchTestCase;

public class RouteHandlerTests extends OpenSearchTestCase {
    public void testUnnamedRouteHandler() {
        RouteHandler rh = new RouteHandler(
            RestRequest.Method.GET,
            "/foo/bar",
            req -> new ExtensionRestResponse(req, RestStatus.OK, "content")
        );

        assertEquals(null, rh.name());
    }

    public void testNamedRouteHandler() {
        RouteHandler rh = new RouteHandler(
            "foo",
            RestRequest.Method.GET,
            "/foo/bar",
            req -> new ExtensionRestResponse(req, RestStatus.OK, "content")
        );

        assertEquals("foo", rh.name());
    }
}
