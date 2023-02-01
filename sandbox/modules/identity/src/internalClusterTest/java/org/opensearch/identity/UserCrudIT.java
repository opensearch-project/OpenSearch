/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

import static org.hamcrest.Matchers.is;

/**
 * Tests REST API for users against local cluster
 */
public class UserCrudIT extends HttpSmokeTestCaseWithIdentity {
    private final String ENDPOINT;

    public UserCrudIT() {
        ENDPOINT = ConfigConstants.IDENTITY_REST_API_REQUEST_PREFIX;
    }

    @Before
    public void startClusterWithIdentityIndex() throws Exception {
        startNodesWithIdentityIndex();
    }

    public void testUsersRestApi() throws Exception {

        // Create a user
        Request request = new Request("PUT", ENDPOINT + "/internalusers/test");
        request.setJsonEntity("{ \"password\" : \"test\" }\n");
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        // Add other api tests here
    }

}
