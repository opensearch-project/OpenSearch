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
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.List;
import java.util.Map;

import static org.opensearch.test.rest.OpenSearchRestTestCase.entityAsMap;

/**
 * Tests REST API for users against local cluster
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class UserCrudIT extends HttpSmokeTestCaseWithIdentity {
    private final String ENDPOINT;

    public UserCrudIT() {
        ENDPOINT = ConfigConstants.IDENTITY_REST_API_REQUEST_PREFIX;
    }

    @Before
    public void startClusterWithIdentityIndex() throws Exception {
        startNodesWithIdentityIndex();
    }

    @SuppressWarnings("unchecked")
    public void testUsersRestApi() throws Exception {

        String username = "test-create";
        // Create a user
        Request request = new Request("PUT", ENDPOINT + "/internalusers/" + username);
        request.setJsonEntity("{ \"password\" : \"test\" }\n");
        Response response = getRestClient().performRequest(request);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        Map<String, Object> createResponse = entityAsMap(response);
        List<Map<String, Object>> usersCreated = (List<Map<String, Object>>) createResponse.get("users");
        assertEquals(usersCreated.size(), 1);
        assertEquals(usersCreated.get(0).get("successful"), true);
        assertEquals(usersCreated.get(0).get("username"), username);

        // Add other api tests here
    }

}
