/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.remotecluster;

import org.junit.Before;

import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.identity.IdentityConfigConstants;
import org.opensearch.identity.rest.IdentityRestConstants;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Tests REST API for users against remote cluster
 */
public class UserIT extends OpenSearchRestTestCase {
    private final String identityIndex = IdentityConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX;
    private final String ENDPOINT;

    protected String getEndpointPrefix() {
        return IdentityRestConstants.IDENTITY_REST_REQUEST_PREFIX;
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
        return true; // setting true so identity index is not deleted upon test completion
    }

    /**
     * This warning is expected to be thrown as we are accessing identity index directly
     * @return the warning message to be expected
     */
    private RequestOptions systemIndexWarning() {
        return expectWarnings(
            "this request accesses system indices: ["
                + identityIndex
                + "], but in a future major version, direct access to system indices will be prevented by default"
        );
    }

    protected void ensureIdentityIndexExists() throws IOException {
        // this will fail if default index name is changed in remote cluster
        Request request = new Request("GET", "/" + identityIndex);
        request.setOptions(systemIndexWarning());
        Response response = adminClient().performRequest(request);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        Map<String, Object> responseAsMap = entityAsMap(response);
        assertTrue(responseAsMap.containsKey(identityIndex));
    }

    @SuppressWarnings("unchecked")
    public void testInternalUsersApi() throws Exception {

        String username = "test-create";

        // Create a user
        String createMessage = username + " created successfully.";
        Request request = new Request("PUT", ENDPOINT + "/users/" + username);
        request.setJsonEntity("{ \"password\" : \"test-create\" }\n");
        request.setOptions(systemIndexWarning());
        Response response = client().performRequest(request);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        Map<String, Object> createResponse = entityAsMap(response);
        List<Map<String, Object>> usersCreated = (List<Map<String, Object>>) createResponse.get("users");
        assertEquals(usersCreated.size(), 1);
        assertEquals(usersCreated.get(0).get("successful"), true);
        assertEquals(usersCreated.get(0).get("username"), username);
        assertEquals(usersCreated.get(0).get("message"), createMessage);

        // Read a user

        // Read all users

        // Update a user
        String updateMessage = username + " updated successfully.";
        request = new Request("PUT", ENDPOINT + "/users/" + username);
        request.setJsonEntity("{ \"password\" : \"test-create\" }\n");
        request.setOptions(systemIndexWarning());
        response = client().performRequest(request);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        Map<String, Object> updateResponse = entityAsMap(response);
        List<Map<String, Object>> usersUpdated = (List<Map<String, Object>>) updateResponse.get("users");
        assertEquals(usersUpdated.size(), 1);
        assertEquals(usersUpdated.get(0).get("successful"), true);
        assertEquals(usersUpdated.get(0).get("username"), username);
        assertEquals(usersUpdated.get(0).get("message"), updateMessage);

        // Update multiple users

        // Delete a user

    }

}
