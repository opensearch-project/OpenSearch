/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest;

import org.junit.Before;
import org.opensearch.OpenSearchException;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashSet;
import java.util.Set;

import static org.opensearch.rest.NamedRoute.MAX_LENGTH_OF_ACTION_NAME;

public class ReplacedNamedRouteTests extends OpenSearchTestCase {

    private ReplacedNamedRoute replacedNamedRoute;

    @Before
    public void setup() {
        RestRequest.Method method = RestRequest.Method.GET;
        String path = "/test";
        RestRequest.Method deprecatedMethod = RestRequest.Method.POST;
        String deprecatedPath = "/deprecated";
        String name = "testRoute";
        Set<String> legacyActionNames = new HashSet<>();
        legacyActionNames.add("cluster:admin/legacyAction");

        replacedNamedRoute = new ReplacedNamedRoute(method, path, deprecatedMethod, deprecatedPath, name, legacyActionNames);
    }

    public void testIsValidRouteName() {
        ReplacedNamedRoute replacedNamedRoute = new ReplacedNamedRoute(
            RestRequest.Method.GET,
            "foo/bar",
            "deprecatedFoo/bar",
            "foo:bar",
            null
        );

        assertFalse(replacedNamedRoute.isValidRouteName(null));
        assertFalse(replacedNamedRoute.isValidRouteName(""));
        assertFalse(replacedNamedRoute.isValidRouteName(" "));
        assertFalse(replacedNamedRoute.isValidRouteName(new String(new char[MAX_LENGTH_OF_ACTION_NAME + 1]).replace("\0", "x")));
        assertFalse(replacedNamedRoute.isValidRouteName("invalid$route$name"));

        assertTrue(replacedNamedRoute.isValidRouteName("validRouteName"));
        assertTrue(replacedNamedRoute.isValidRouteName("valid:Route:Name"));
        assertTrue(replacedNamedRoute.isValidRouteName("valid/Route/Name"));
        assertTrue(replacedNamedRoute.isValidRouteName("valid*Route*Name"));
        assertTrue(replacedNamedRoute.isValidRouteName("valid_Route_Name"));
    }

    public void testConstructor_InvalidRouteName() {
        assertThrows(OpenSearchException.class, () -> { new ReplacedNamedRoute(null, null, null, null, "invalid$route$name"); });
    }

    public void testConstructor_ValidRouteName() {

        try {
            ReplacedNamedRoute rr = new ReplacedNamedRoute(null, null, null, null, "validRouteName");
        } catch (OpenSearchException e) {
            fail("Did not expect ReplacedNamedRoute to throw exception on valid route name");
        }
    }

    public void testActionNames() {
        Set<String> actionNames = replacedNamedRoute.actionNames();
        assertNotNull(actionNames);
        assertEquals(1, actionNames.size());
        assertTrue(actionNames.contains("cluster:admin/legacyAction"));
    }

    public void testToString() {
        String expectedString = "ReplacedNamedRoute [method=GET, path=/test, deprecatedMethod=POST, "
            + "deprecatedPath=/deprecated, name= testRoute, actionNames= [cluster:admin/legacyAction]]";

        assertEquals(expectedString, replacedNamedRoute.toString());
    }
}
