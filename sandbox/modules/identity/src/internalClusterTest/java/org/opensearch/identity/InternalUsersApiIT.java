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

public class InternalUsersApiIT extends HttpSmokeTestCaseWithIdentity {
    private final String ENDPOINT;

    protected String getEndpointPrefix() {
        return ConfigConstants.IDENTITY_REST_REQUEST_PREFIX;
    }

    public InternalUsersApiIT() {
        ENDPOINT = getEndpointPrefix() + "/api";
    }

    @Before
    public void initCluster() throws Exception {
        startNodes();
        ensureIdentityIndexIsGreen();
    }

    @Test
    public void testInternalUsersApi() throws Exception {

        // Create a user
        Request request = new Request("PUT", ENDPOINT + "/internalusers/test");
        request.setJsonEntity("{ \"password\" : \"test\" }\n");
        Response response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        // Read a user
        request = new Request("GET", ENDPOINT + "/internalusers/test");
        response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        // Read all users
        request = new Request("GET", ENDPOINT + "/internalusers");
        response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        // Update a user
        request = new Request("PATCH", ENDPOINT + "/internalusers/test");
        request.setJsonEntity("[{ \"op\": \"add\", \"path\": \"/password\", \"value\": \"neu\" }]");
        response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        // Update multiple users
        request = new Request("PATCH", ENDPOINT + "/internalusers");
        request.setJsonEntity("[{ \"op\": \"add\", \"path\": \"/test\", \"value\": {\"password\": \"new-password\" }}]");
        response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        // Delete a user
        request = new Request("DELETE", ENDPOINT + "/internalusers/test");
        response = getRestClient().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

    }

}
