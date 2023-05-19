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

import static org.opensearch.rest.NamedRoute.MAX_LENGTH_OF_ACTION_NAME;

public class NamedRouteTests extends OpenSearchTestCase {

    public void testNamedRouteWithNullName() {
        try {
            NamedRoute r = new NamedRoute(RestRequest.Method.GET, "foo/bar", null);
            fail("Expected NamedRoute to throw exception on null name provided");
        } catch (OpenSearchException e) {
            assertTrue(e.getMessage().contains("Invalid route name specified"));
        }
    }

    public void testNamedRouteWithEmptyName() {
        try {
            NamedRoute r = new NamedRoute(RestRequest.Method.GET, "foo/bar", "");
            fail("Expected NamedRoute to throw exception on empty name provided");
        } catch (OpenSearchException e) {
            assertTrue(e.getMessage().contains("Invalid route name specified"));
        }
    }

    public void testNamedRouteWithNameContainingSpace() {
        try {
            NamedRoute r = new NamedRoute(RestRequest.Method.GET, "foo/bar", "foo bar");
            fail("Expected NamedRoute to throw exception on name containing space name provided");
        } catch (OpenSearchException e) {
            assertTrue(e.getMessage().contains("Invalid route name specified"));
        }
    }

    public void testNamedRouteWithNameContainingInvalidCharacters() {
        try {
            NamedRoute r = new NamedRoute(RestRequest.Method.GET, "foo/bar", "foo@bar!");
            fail("Expected NamedRoute to throw exception on name containing invalid characters name provided");
        } catch (OpenSearchException e) {
            assertTrue(e.getMessage().contains("Invalid route name specified"));
        }
    }

    public void testNamedRouteWithNameOverMaximumLength() {
        try {
            String repeated = new String(new char[MAX_LENGTH_OF_ACTION_NAME + 1]).replace("\0", "x");
            NamedRoute r = new NamedRoute(RestRequest.Method.GET, "foo/bar", repeated);
            fail("Expected NamedRoute to throw exception on name over maximum length supplied");
        } catch (OpenSearchException e) {
            assertTrue(e.getMessage().contains("Invalid route name specified"));
        }
    }

    public void testNamedRouteWithValidActionName() {
        try {
            NamedRoute r = new NamedRoute(RestRequest.Method.GET, "foo/bar", "foo:bar");
        } catch (OpenSearchException e) {
            fail("Did not expect NamedRoute to throw exception on valid action name");
        }
    }

    public void testNamedRouteWithValidActionNameWithForwardSlash() {
        try {
            NamedRoute r = new NamedRoute(RestRequest.Method.GET, "foo/bar", "foo:bar/baz");
        } catch (OpenSearchException e) {
            fail("Did not expect NamedRoute to throw exception on valid action name");
        }
    }

    public void testNamedRouteWithValidActionNameWithWildcard() {
        try {
            NamedRoute r = new NamedRoute(RestRequest.Method.GET, "foo/bar", "foo:bar/*");
        } catch (OpenSearchException e) {
            fail("Did not expect NamedRoute to throw exception on valid action name");
        }
    }

    public void testNamedRouteWithValidActionNameWithUnderscore() {
        try {
            NamedRoute r = new NamedRoute(RestRequest.Method.GET, "foo/bar", "foo:bar_baz");
        } catch (OpenSearchException e) {
            fail("Did not expect NamedRoute to throw exception on valid action name");
        }
    }
}
