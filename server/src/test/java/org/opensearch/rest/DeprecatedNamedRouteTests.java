/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.opensearch.OpenSearchException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class DeprecatedNamedRouteTests extends OpenSearchTestCase {

    public void testValidRouteName() {
        DeprecatedNamedRoute deprecatedNamedRoute = new DeprecatedNamedRoute(
            RestRequest.Method.GET,
            "/test",
            "Deprecation message",
            "route_name"
        );
        assertTrue(deprecatedNamedRoute.isValidRouteName("valid_route"));
    }

    public void testInvalidRouteName_Null() {
        DeprecatedNamedRoute deprecatedNamedRoute = new DeprecatedNamedRoute(
            RestRequest.Method.GET,
            "/test",
            "Deprecation message",
            "route_name"
        );
        assertFalse(deprecatedNamedRoute.isValidRouteName(null));
    }

    public void testInvalidRouteName_Blank() {
        DeprecatedNamedRoute deprecatedNamedRoute = new DeprecatedNamedRoute(
            RestRequest.Method.GET,
            "/test",
            "Deprecation message",
            "route_name"
        );
        assertFalse(deprecatedNamedRoute.isValidRouteName(""));
    }

    public void testInvalidRouteName_ExceedsMaxLength() {
        DeprecatedNamedRoute deprecatedNamedRoute = new DeprecatedNamedRoute(
            RestRequest.Method.GET,
            "/test",
            "Deprecation message",
            "route_name"
        );
        String longRouteName = String.join("", Collections.nCopies(DeprecatedNamedRoute.MAX_LENGTH_OF_ACTION_NAME + 1, "a"));
        assertFalse(deprecatedNamedRoute.isValidRouteName(longRouteName));
    }

    public void testInvalidRouteName_InvalidCharacters() {
        DeprecatedNamedRoute deprecatedNamedRoute = new DeprecatedNamedRoute(
            RestRequest.Method.GET,
            "/test",
            "Deprecation message",
            "route_name"
        );
        assertFalse(deprecatedNamedRoute.isValidRouteName("invalid_name@"));
    }

    public void testConstructor_InvalidRouteName() {
        assertThrows(OpenSearchException.class, () -> {
            new DeprecatedNamedRoute(RestRequest.Method.GET, "/test", "Deprecation message", "invalid_name@");
        });
    }

    public void testConstructor_ValidRouteName() {
        Set<String> actionNames = new HashSet<>();
        actionNames.add("cluster:admin/action1");
        actionNames.add("cluster:admin/action2");

        DeprecatedNamedRoute deprecatedNamedRoute = new DeprecatedNamedRoute(
            RestRequest.Method.GET,
            "/test",
            "Deprecation message",
            "valid_name",
            actionNames
        );

        assertEquals("valid_name", deprecatedNamedRoute.name());
        assertEquals(actionNames, deprecatedNamedRoute.actionNames());
        assertEquals("Deprecation message", deprecatedNamedRoute.getDeprecationMessage());
    }
}
