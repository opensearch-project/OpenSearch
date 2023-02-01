/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.remotecluster;

import org.junit.Before;
import org.junit.Test;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.identity.ConfigConstants;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Tests REST API for users against remote cluster
 */
public class UserIT extends OpenSearchRestTestCase {
    private final String ENDPOINT;

    protected String getEndpointPrefix() {
        return ConfigConstants.IDENTITY_REST_REQUEST_PREFIX;
    }

    public UserIT() {
        ENDPOINT = getEndpointPrefix() + "/api";
    }

    @Before
    public void init() throws Exception {
        ensureIdentityIndexExists();
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        // TODO: is this required and will it affect other tests?
        return true; // setting true to reuse same spun up cluster to run tests
    }

    protected void ensureIdentityIndexExists() throws IOException {
        // this will fail if default index name is changed in remote cluster
        String identityIndex = ConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX;
        Request request = new Request("GET", "/" + identityIndex);
        Response response = adminClient().performRequest(request);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        Map<String, Object> responseAsMap = entityAsMap(response);
        assertTrue(responseAsMap.containsKey(identityIndex));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testInternalUsersApi() throws Exception {

        String username = "test-create";

        // Create a user
        Request request = new Request("PUT", ENDPOINT + "/internalusers/" + username);
        request.setJsonEntity("{ \"password\" : \"test-create\" }\n");
        Response response = client().performRequest(request);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        Map<String, Object> createResponse = entityAsMap(response);
        List<Map<String, Object>> usersCreated = (List<Map<String, Object>>) createResponse.get("users");
        assertEquals(usersCreated.size(), 1);
        assertEquals(usersCreated.get(0).get("successful"), true);
        assertEquals(usersCreated.get(0).get("username"), username);

        // Read a user

        // Read all users

        // Update a user

        // Update multiple users

        // Delete a user

    }

}
