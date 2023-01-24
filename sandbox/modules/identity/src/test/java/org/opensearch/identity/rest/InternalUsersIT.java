/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest;

import org.junit.Before;
import org.junit.Test;

import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.identity.ConfigConstants;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import static org.hamcrest.Matchers.is;

public class InternalUsersIT extends OpenSearchRestTestCase {
    private final String ENDPOINT;

    protected String getEndpointPrefix() {
        return ConfigConstants.IDENTITY_REST_REQUEST_PREFIX;
    }

    public InternalUsersIT() {
        ENDPOINT = getEndpointPrefix() + "/api";
    }

    @Before
    public void init() throws Exception {
        // see if there is a need to check that Identity index exists
    }

    @Test
    public void testInternalUsersApi() throws Exception {

        // Create a user
        Request request = new Request("PUT", ENDPOINT + "/internalusers/test");
        request.setJsonEntity("{ \"password\" : \"test\" }\n");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        // Read a user
        request = new Request("GET", ENDPOINT + "/internalusers/test");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        // Read all users
        request = new Request("GET", ENDPOINT + "/internalusers");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        // Update a user
        request = new Request("PATCH", ENDPOINT + "/internalusers/test");
        request.setJsonEntity("[{ \"op\": \"add\", \"path\": \"/password\", \"value\": \"neu\" }]");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        // Update multiple users
        request = new Request("PATCH", ENDPOINT + "/internalusers");
        request.setJsonEntity("[{ \"op\": \"add\", \"path\": \"/test\", \"value\": {\"password\": \"new-password\" }}]");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        // Delete a user
        request = new Request("DELETE", ENDPOINT + "/internalusers/test");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

    }

}
