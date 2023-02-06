/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.identity.rest.IdentityRestConstants;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.Map;

import static org.opensearch.test.rest.OpenSearchRestTestCase.entityAsMap;

/**
 * Tests REST API for users against local cluster
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class UserCrudIT extends HttpSmokeTestCaseWithIdentity {
    private static final String ENDPOINT = IdentityRestConstants.IDENTITY_REST_API_REQUEST_PREFIX;;

    public UserCrudIT() {}

    @Before
    public void startClusterWithIdentityIndex() throws Exception {
        startNodesWithIdentityIndex();
    }

    @SuppressWarnings("unchecked")
    public void testUsersRestApi() throws Exception {

        final Map<String, String> emptyMap = Map.of();
        final List<String> emptyList = List.of();

        String username = "test-create";
        String createContent = "{ \"password\" : \"test\","
            + " \"attributes\": { \"attribute1\": \"value1\"},"
            + " \"permissions\": [\"indices:admin:create\"]"
            + " }\n";

        String updateContent = "{ \"password\" : \"test\","
            + " \"attributes\": { \"attribute1\": \"value2\"},"
            + " \"permissions\": [\"indices:admin:update\"]"
            + " }\n";

        Map<String, String> expectedAttributes = Map.of("attribute1", "value2");
        List<String> expectedPermissions = List.of("indices:admin:create");

        // Create a user
        String createSuccessMessage = username + " created successfully.";
        Request createRequest = new Request("PUT", ENDPOINT + "/users/" + username);
        createRequest.setJsonEntity(createContent);
        Response response = getRestClient().performRequest(createRequest);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        Map<String, Object> userCreated = entityAsMap(response);
        assertEquals(userCreated.size(), 3);
        assertEquals(userCreated.get("successful"), true);
        assertEquals(userCreated.get("username"), username);
        assertEquals(userCreated.get("message"), createSuccessMessage);

        // Update a user (same user in this case)
        String updateSuccessMessage = username + " updated successfully.";
        Request updateRequest = new Request("PUT", ENDPOINT + "/users/" + username);
        updateRequest.setJsonEntity(updateContent);
        response = getRestClient().performRequest(updateRequest);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        Map<String, Object> usersUpdated = entityAsMap(response);
        assertEquals(usersUpdated.size(), 3);
        assertEquals(usersUpdated.get("successful"), true);
        assertEquals(usersUpdated.get("username"), username);
        assertEquals(usersUpdated.get("message"), updateSuccessMessage);

        // GET a user
        Request getRequest = new Request("GET", ENDPOINT + "/users/" + username);
        response = getRestClient().performRequest(getRequest);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        Map<String, Object> getResponse = entityAsMap(response);
        Map<String, String> user = (Map<String, String>) getResponse.get(username);
        assertNotEquals(user, null);
        assertEquals(user.get("attributes"), expectedAttributes);
        assertEquals(user.get("permissions"), expectedPermissions);

        // GET all users
        Request mGetRequest = new Request("GET", ENDPOINT + "/users");
        response = getRestClient().performRequest(mGetRequest);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        Map<String, Object> mGetResponse = entityAsMap(response);
        List<Map<String, Object>> users = (List<Map<String, Object>>) mGetResponse.get("users");
        assertEquals(users.size(), 2);
        assertTrue(users.get(0).containsKey("admin"));
        Map<String, Object> user1 = (Map<String, Object>) users.get(0).get("admin");
        assertEquals(user1.get("attributes"), emptyMap);
        assertEquals(user1.get("permissions"), emptyList);
        assertTrue(users.get(1).containsKey(username));
        Map<String, Object> user2 = (Map<String, Object>) users.get(1).get(username);
        assertEquals(user2.get("attributes"), expectedAttributes);
        assertEquals(user2.get("permissions"), expectedPermissions);

        // DELETE a user
        String deletedMessage = username + " deleted successfully.";
        Request deleteRequest = new Request("DELETE", ENDPOINT + "/users/" + username);
        response = getRestClient().performRequest(deleteRequest);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        Map<String, Object> deletedUser = entityAsMap(response);
        assertEquals(deletedUser.size(), 2);
        assertEquals(deletedUser.get("successful"), true);
        assertEquals(deletedUser.get("message"), deletedMessage);
    }

}
