/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.junit.Before;
import org.opensearch.authn.StringPrincipal;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.identity.authz.PermissionStorage;
import org.opensearch.identity.rest.RestConfigConstants;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.hamcrest.Matchers.is;

/**
 * Tests REST API for users against local cluster
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PermissionApiIT extends HttpSmokeTestCaseWithIdentity {

    public PermissionApiIT() {}

    @Before
    public void startClusterWithIdentityIndex() throws Exception {
        startNodesWithIdentityIndex();
    }

    @SuppressWarnings("unchecked")
    public void testPermissionsRestApi() throws Exception {

        final String createEndpoint = RestConfigConstants.IDENTITY_CREATE_PERMISSION_ACTION;
        // Add a permission
        Request createRequest = new Request("POST", createEndpoint);
        createRequest.setJsonEntity("{ \"permissionString\" : \"cluster:admin\\read\", \"principalString\" : \"testPrincipal\" }\n");
        Response createResponse = getRestClient().performRequest(createRequest);
        assertThat(createResponse.getStatusLine().getStatusCode(), is(200));

        final String checkEndpoint = RestConfigConstants.IDENTITY_READ_PERMISSION_ACTION;
        // Check for the added permission
        Request checkRequest = new Request("GET", checkEndpoint);
        checkRequest.setJsonEntity("{\"principalString\" : \"testPrincipal\" }\n");
        Response checkResponse = getRestClient().performRequest(checkRequest);
        assertThat(checkResponse.getStatusLine().getStatusCode(), is(200));
        assertTrue(checkResponse.getEntity().toString().contains("cluster:admin\\read"));

        // Check for the added permission in permission storage
        assertTrue(PermissionStorage.get(new StringPrincipal("testPrincipal")).contains("cluster:admin\\read"));

        final String deleteEndpoint = RestConfigConstants.IDENTITY_READ_PERMISSION_ACTION;
        // Delete the added permission
        Request deleteRequest = new Request("DELETE", deleteEndpoint);
        deleteRequest.setJsonEntity("{ \"permissionString\" : \"cluster:admin\\read\", \"principalString\" : \"testPrincipal\" }\n");
        Response deleteResponse = getRestClient().performRequest(deleteRequest);
        assertThat(deleteResponse.getStatusLine().getStatusCode(), is(200));

        // Check the added permission is gone
        checkRequest = new Request("GET", checkEndpoint);
        checkRequest.setJsonEntity("{\"principalString\" : \"testPrincipal\" }\n");
        checkResponse = getRestClient().performRequest(checkRequest);
        assertThat(checkResponse.getStatusLine().getStatusCode(), is(200));
        assertFalse(checkResponse.getEntity().toString().contains("cluster:admin\\read"));

        // Check the added permission is removed from permission storage
        assertFalse(PermissionStorage.get(new StringPrincipal("testPrincipal")).contains("cluster:admin\\read"));

        // Add other api tests here

    }

}
