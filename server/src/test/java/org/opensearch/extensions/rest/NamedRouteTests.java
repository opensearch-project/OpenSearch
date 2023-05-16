/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.extensions.rest;

import org.junit.Test;
import org.opensearch.OpenSearchException;
import org.opensearch.rest.NamedRoute;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;

public class NamedRouteTests extends OpenSearchTestCase {

    @Test
    public void testNamedRouteWithNullName() {
        try {
            NamedRoute r = new NamedRoute(RestRequest.Method.GET, "foo/bar", null);
            fail("Expected NamedRoute to throw exception on null name provided");
        } catch (OpenSearchException e) {
            assertTrue(e.getMessage().contains("Invalid route name specified"));
        }
    }

    @Test
    public void testNamedRouteWithEmptyName() {
        try {
            NamedRoute r = new NamedRoute(RestRequest.Method.GET, "foo/bar", "");
            fail("Expected NamedRoute to throw exception on empty name provided");
        } catch (OpenSearchException e) {
            assertTrue(e.getMessage().contains("Invalid route name specified"));
        }
    }

    @Test
    public void testNamedRouteWithNameContainingSpace() {
        try {
            NamedRoute r = new NamedRoute(RestRequest.Method.GET, "foo/bar", "foo bar");
            fail("Expected NamedRoute to throw exception on name containing space name provided");
        } catch (OpenSearchException e) {
            assertTrue(e.getMessage().contains("Invalid route name specified"));
        }
    }

    @Test
    public void testNamedRouteWithNameContainingInvalidCharacters() {
        try {
            NamedRoute r = new NamedRoute(RestRequest.Method.GET, "foo/bar", "foo@bar!");
            fail("Expected NamedRoute to throw exception on name containing invalid characters name provided");
        } catch (OpenSearchException e) {
            assertTrue(e.getMessage().contains("Invalid route name specified"));
        }
    }

    @Test
    public void testNamedRouteWithNameOverMaximumLength() {
        try {
            String repeated = new String(new char[251]).replace("\0", "x");
            NamedRoute r = new NamedRoute(RestRequest.Method.GET, "foo/bar", repeated);
            fail("Expected NamedRoute to throw exception on name over maximum length supplied");
        } catch (OpenSearchException e) {
            assertTrue(e.getMessage().contains("Invalid route name specified"));
        }
    }

    @Test
    public void testNamedRouteWithValidActionName() {
        try {
            NamedRoute r = new NamedRoute(RestRequest.Method.GET, "foo/bar", "foo:bar");
        } catch (OpenSearchException e) {
            fail("Did not expect NamedRoute to throw exception on valid action name");
        }
    }

    @Test
    public void testNamedRouteWithValidActionNameWithForwardSlash() {
        try {
            NamedRoute r = new NamedRoute(RestRequest.Method.GET, "foo/bar", "foo:bar/baz");
        } catch (OpenSearchException e) {
            fail("Did not expect NamedRoute to throw exception on valid action name");
        }
    }

    @Test
    public void testNamedRouteWithValidActionNameWithWildcard() {
        try {
            NamedRoute r = new NamedRoute(RestRequest.Method.GET, "foo/bar", "foo:bar/*");
        } catch (OpenSearchException e) {
            fail("Did not expect NamedRoute to throw exception on valid action name");
        }
    }

    @Test
    public void testNamedRouteWithValidActionNameWithUnderscore() {
        try {
            NamedRoute r = new NamedRoute(RestRequest.Method.GET, "foo/bar", "foo:bar_baz");
        } catch (OpenSearchException e) {
            fail("Did not expect NamedRoute to throw exception on valid action name");
        }
    }
}
