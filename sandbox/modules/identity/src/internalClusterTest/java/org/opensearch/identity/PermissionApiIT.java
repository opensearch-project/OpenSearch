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
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.identity.authz.PermissionStorage;
import org.opensearch.identity.rest.IdentityRestConstants;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Set;

import java.nio.charset.StandardCharsets;

import java.util.stream.Collectors;

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

        final RequestOptions authHeaderOptions = RequestOptions.DEFAULT.toBuilder()
            .addHeader("Authorization", "Basic YWRtaW46YWRtaW4=")
            .build(); // admin:admin

        // _identity_api_permissions
        final String endpoint = IdentityRestConstants.IDENTITY_API_PERMISSION_PREFIX;

        String username = "test";
        // Add a permission
        Request putRequest = new Request("PUT", endpoint + "/" + username);
        putRequest.setJsonEntity("{ \"permission\" : \"cluster.admin/read\"}\n");
        putRequest.setOptions(authHeaderOptions);
        Response putResponse = getRestClient().performRequest(putRequest);
        assertThat(putResponse.getStatusLine().getStatusCode(), is(200));

        Set<String> permissionsInStorage = PermissionStorage.get((new StringPrincipal(username)))
            .stream()
            .map(permission -> permission.getPermissionString())
            .collect(Collectors.toSet());

        // Check for the added permission
        Request checkRequest = new Request("GET", endpoint + "/" + username);
        checkRequest.setOptions(authHeaderOptions);
        Response checkResponse = getRestClient().performRequest(checkRequest);
        assertThat(checkResponse.getStatusLine().getStatusCode(), is(200));
        assertTrue(
            new String(checkResponse.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8).contains("cluster.admin/read")
        );

        // Check for the added permission in permission storage
        assertTrue(permissionsInStorage.contains("cluster.admin/read"));
        
        putRequest = new Request("PUT", endpoint + "/" + username);
        putRequest.setJsonEntity("{ \"permission\" : \":1:2:3\"}\n"); // Invalid permission
        putRequest.setOptions(authHeaderOptions);
        try {
            putResponse = getRestClient().performRequest(putRequest);
        } catch (ResponseException ex) {
            assertTrue(ex.getMessage().contains("All permissions must contain a permission type and action delimited"));
        }

        // Check for the added permission in permission storage
        assertFalse(permissionsInStorage.contains(":1:2:3"));

        // Delete the added permission
        Request deleteRequest = new Request("DELETE", endpoint + "/" + username);
        deleteRequest.setJsonEntity("{ \"permissionString\" : \"cluster.admin/read\"}\n");
        deleteRequest.setOptions(authHeaderOptions);
        Response deleteResponse = getRestClient().performRequest(deleteRequest);
        assertThat(deleteResponse.getStatusLine().getStatusCode(), is(200));

        // Check the added permission is gone
        checkRequest = new Request("GET", endpoint + "/" + username);
        checkRequest.setOptions(authHeaderOptions);
        checkResponse = getRestClient().performRequest(checkRequest);
        assertThat(checkResponse.getStatusLine().getStatusCode(), is(200));
        assertFalse(checkResponse.getEntity().toString().contains("cluster.admin/read"));

        // Check the added permission is removed from permission storage
        assertFalse(PermissionStorage.get(new StringPrincipal(username)).contains("cluster.admin/read"));
    }
}
