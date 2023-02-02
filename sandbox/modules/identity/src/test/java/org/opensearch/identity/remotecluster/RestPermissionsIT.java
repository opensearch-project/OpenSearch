/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.remotecluster;

import org.junit.Before;

import org.opensearch.authn.StringPrincipal;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.identity.ConfigConstants;
import org.opensearch.identity.authz.PermissionStorage;
import org.opensearch.identity.rest.RestConstants;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.is;

/**
 * Integration test for permission granting REST API against a remote cluster
 */
public class RestPermissionsIT extends OpenSearchRestTestCase {

    protected String getEndpointPrefix() {
        return RestConstants.IDENTITY_REST_REQUEST_PREFIX;
    }

    public RestPermissionsIT() {}

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

    public void testPermissionsRestApi() throws Exception {

        final String createEndpoint = RestConstants.IDENTITY_CREATE_PERMISSION_ACTION;
        // Add a permission
        Request createRequest = new Request("POST", createEndpoint);
        createRequest.setJsonEntity("{ \"permissionString\" : \"cluster:admin\\read\", \"principalString\" : \"testPrincipal\" }\n");
        Response createResponse = client().performRequest(createRequest);
        assertThat(createResponse.getStatusLine().getStatusCode(), is(200));

        final String checkEndpoint = RestConstants.IDENTITY_READ_PERMISSION_ACTION;
        // Check for the added permission
        Request checkRequest = new Request("GET", checkEndpoint);
        checkRequest.setJsonEntity("{\"principalString\" : \"testPrincipal\" }\n");
        Response checkResponse = client().performRequest(checkRequest);
        assertThat(checkResponse.getStatusLine().getStatusCode(), is(200));
        assertTrue(checkResponse.getEntity().toString().contains("cluster:admin\\read"));

        // Check for the added permission in permission storage
        assertTrue(PermissionStorage.get(new StringPrincipal("testPrincipal")).contains("cluster:admin\\read"));

        final String deleteEndpoint = RestConstants.IDENTITY_READ_PERMISSION_ACTION;
        // Delete the added permission
        Request deleteRequest = new Request("DELETE", deleteEndpoint);
        deleteRequest.setJsonEntity("{ \"permissionString\" : \"cluster:admin\\read\", \"principalString\" : \"testPrincipal\" }\n");
        Response deleteResponse = client().performRequest(deleteRequest);
        assertThat(deleteResponse.getStatusLine().getStatusCode(), is(200));

        // Check the added permission is gone
        checkRequest = new Request("GET", checkEndpoint);
        checkRequest.setJsonEntity("{\"principalString\" : \"testPrincipal\" }\n");
        checkResponse = client().performRequest(checkRequest);
        assertThat(checkResponse.getStatusLine().getStatusCode(), is(200));
        assertFalse(checkResponse.getEntity().toString().contains("cluster:admin\\read"));

        // Check the added permission is removed from permission storage
        assertFalse(PermissionStorage.get(new StringPrincipal("testPrincipal")).contains("cluster:admin\\read"));

        // Add other api tests here

    }
}
