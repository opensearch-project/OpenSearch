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

import java.util.Set;
import java.util.function.Function;

import static org.opensearch.rest.NamedRoute.MAX_LENGTH_OF_ACTION_NAME;
import static org.opensearch.rest.RestRequest.Method.GET;

public class NamedRouteTests extends OpenSearchTestCase {

    public void testNamedRouteWithEmptyName() {
        try {
            NamedRoute r = new NamedRoute.Builder().method(GET).path("foo/bar").uniqueName("").build();
            fail("Expected NamedRoute to throw exception on empty name provided");
        } catch (OpenSearchException e) {
            assertTrue(e.getMessage().contains("Invalid route name specified"));
        }
    }

    public void testNamedRouteWithNameContainingSpace() {
        try {
            NamedRoute r = new NamedRoute.Builder().method(GET).path("foo/bar").uniqueName("foo bar").build();
            fail("Expected NamedRoute to throw exception on name containing space name provided");
        } catch (OpenSearchException e) {
            assertTrue(e.getMessage().contains("Invalid route name specified"));
        }
    }

    public void testNamedRouteWithNameContainingInvalidCharacters() {
        try {
            NamedRoute r = new NamedRoute.Builder().method(GET).path("foo/bar").uniqueName("foo@bar!").build();
            fail("Expected NamedRoute to throw exception on name containing invalid characters name provided");
        } catch (OpenSearchException e) {
            assertTrue(e.getMessage().contains("Invalid route name specified"));
        }
    }

    public void testNamedRouteWithNameOverMaximumLength() {
        try {
            String repeated = new String(new char[MAX_LENGTH_OF_ACTION_NAME + 1]).replace("\0", "x");
            NamedRoute r = new NamedRoute.Builder().method(GET).path("foo/bar").uniqueName(repeated).build();
            fail("Expected NamedRoute to throw exception on name over maximum length supplied");
        } catch (OpenSearchException e) {
            assertTrue(e.getMessage().contains("Invalid route name specified"));
        }
    }

    public void testNamedRouteWithValidActionName() {
        try {
            NamedRoute r = new NamedRoute.Builder().method(GET).path("foo/bar").uniqueName("foo:bar").build();
        } catch (OpenSearchException e) {
            fail("Did not expect NamedRoute to throw exception on valid action name");
        }
    }

    public void testNamedRouteWithValidActionNameWithForwardSlash() {
        try {
            NamedRoute r = new NamedRoute.Builder().method(GET).path("foo/bar").uniqueName("foo:bar:baz").build();
        } catch (OpenSearchException e) {
            fail("Did not expect NamedRoute to throw exception on valid action name");
        }
    }

    public void testNamedRouteWithValidActionNameWithWildcard() {
        try {
            NamedRoute r = new NamedRoute.Builder().method(GET).path("foo/bar").uniqueName("foo:bar/*").build();
        } catch (OpenSearchException e) {
            fail("Did not expect NamedRoute to throw exception on valid action name");
        }
    }

    public void testNamedRouteWithValidActionNameWithUnderscore() {
        try {
            NamedRoute r = new NamedRoute.Builder().method(GET).path("foo/bar").uniqueName("foo:bar_baz").build();
        } catch (OpenSearchException e) {
            fail("Did not expect NamedRoute to throw exception on valid action name");
        }
    }

    public void testNamedRouteWithNullLegacyActionNames() {
        try {
            NamedRoute r = new NamedRoute.Builder().method(GET).path("foo/bar").uniqueName("foo:bar").legacyActionNames(null).build();
            assertTrue(r.actionNames().isEmpty());
        } catch (OpenSearchException e) {
            fail("Did not expect NamedRoute to pass with an invalid legacy action name");
        }
    }

    public void testNamedRouteWithInvalidLegacyActionNames() {
        try {
            NamedRoute r = new NamedRoute.Builder().method(GET)
                .path("foo/bar")
                .uniqueName("foo:bar")
                .legacyActionNames(Set.of("foo:bar-legacy"))
                .build();
            fail("Did not expect NamedRoute to pass with an invalid legacy action name");
        } catch (OpenSearchException e) {
            assertTrue(e.getMessage().contains("Invalid action name [foo:bar-legacy]. It must start with one of:"));
        }
    }

    public void testNamedRouteWithHandler() {
        Function<RestRequest, RestResponse> fooHandler = restRequest -> null;
        try {
            NamedRoute r = new NamedRoute.Builder().method(GET).path("foo/bar").uniqueName("foo:bar_baz").handler(fooHandler).build();
            assertEquals(r.handler(), fooHandler);
        } catch (OpenSearchException e) {
            fail("Did not expect NamedRoute to throw exception");
        }
    }

    public void testNamedRouteNullChecks() {
        try {
            NamedRoute r = new NamedRoute.Builder().method(null).path("foo/bar").uniqueName("foo:bar_baz").build();
            fail("Expected NamedRoute to throw exception as method should not be null");
        } catch (NullPointerException e) {
            assertEquals("REST method must not be null.", e.getMessage());
        }

        try {
            NamedRoute r = new NamedRoute.Builder().method(GET).path(null).uniqueName("foo:bar_baz").build();
            fail("Expected NamedRoute to throw exception as path should not be null");
        } catch (NullPointerException e) {
            assertEquals("REST path must not be null.", e.getMessage());
        }

        try {
            NamedRoute r = new NamedRoute.Builder().method(GET).path("foo/bar").uniqueName(null).build();
            fail("Expected NamedRoute to throw exception as route name should not be null");
        } catch (NullPointerException e) {
            assertEquals("REST route name must not be null.", e.getMessage());
        }

        try {
            NamedRoute r = new NamedRoute.Builder().method(GET).path("foo/bar").uniqueName("foo:bar_baz").handler(null).build();
            fail("Expected NamedRoute to throw exception as handler should not be null");
        } catch (NullPointerException e) {
            assertEquals("Route handler must not be null.", e.getMessage());
        }
    }

    public void testNamedRouteEmptyBuild() {
        try {
            NamedRoute r = new NamedRoute.Builder().build();
            fail("Expected NamedRoute to throw exception as fields should not be null");
        } catch (IllegalStateException e) {
            assertEquals("REST method, path and uniqueName are required.", e.getMessage());
        }

    }

}
