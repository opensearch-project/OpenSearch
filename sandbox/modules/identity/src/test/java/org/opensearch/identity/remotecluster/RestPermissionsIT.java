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
import org.opensearch.client.ResponseException;
import org.opensearch.identity.IdentityConfigConstants;
import org.opensearch.identity.authz.PermissionStorage;
import org.opensearch.identity.rest.IdentityRestConstants;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.is;

/**
 * Integration test for permission granting REST API against a remote cluster
 */
public class RestPermissionsIT extends OpenSearchRestTestCase {

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
        String identityIndex = IdentityConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX;
        Request request = new Request("GET", "/" + identityIndex);
        Response response = adminClient().performRequest(request);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        Map<String, Object> responseAsMap = entityAsMap(response);
        assertTrue(responseAsMap.containsKey(identityIndex));
    }

    public void testPermissionsRestApi() throws Exception {

        // _identity/api/permissions
        final String endpoint = IdentityRestConstants.PERMISSION_SUBPATH;

        String username = "test";
        // _identity/api/permissions/test
        Request putRequest = new Request("PUT", endpoint + "/" + username);
        putRequest.setJsonEntity("{ \"permission\" : \"cluster.admin/read\"}\n");
        putRequest.setOptions(IdentityRestTestCase.options());
        Response putResponse = client().performRequest(putRequest);
        assertThat(putResponse.getStatusLine().getStatusCode(), is(200));
        assertTrue(new String(putResponse.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8).contains("true"));

        // Check for the added permission
        Request checkRequest = new Request("GET", endpoint + "/" + username);
        Response checkResponse = client().performRequest(checkRequest);
        assertThat(checkResponse.getStatusLine().getStatusCode(), is(200));
        assertTrue(
            new String(checkResponse.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8).contains("cluster.admin/read")
        );

        // Check for the added permission in permission storage
        Set<String> permissionsInStorage = PermissionStorage.get((new StringPrincipal(username)))
            .stream()
            .map(permission -> permission.getPermissionString())
            .collect(Collectors.toSet());

        // Check for the added permission in permission storage
        assertTrue(permissionsInStorage.contains("cluster.admin/read"));

        putRequest = new Request("PUT", endpoint + "/" + username);
        putRequest.setJsonEntity("{ \"permission\" : \":1:2:3\"}\n"); // Invalid permission
        putRequest.setOptions(IdentityRestTestCase.options());
        try {
            putResponse = client().performRequest(putRequest);
        } catch (ResponseException ex) {
            assertTrue(ex.getMessage().contains("All permissions must contain a permission type and action delimited"));
        }

        // Check for the added permission in permission storage
        assertFalse(permissionsInStorage.contains(":1:2:3"));

        // Delete the added permission
        Request deleteRequest = new Request("DELETE", endpoint + "/" + username);
        deleteRequest.setJsonEntity("{ \"permissionString\" : \"cluster.admin/read\"}\n");
        Response deleteResponse = client().performRequest(deleteRequest);
        assertThat(deleteResponse.getStatusLine().getStatusCode(), is(200));

        // Check the added permission is gone
        checkRequest = new Request("GET", endpoint + "/" + username);
        checkResponse = client().performRequest(checkRequest);
        assertThat(checkResponse.getStatusLine().getStatusCode(), is(200));
        assertFalse(checkResponse.getEntity().toString().contains("cluster.admin/read"));

        // Check the added permission is removed from permission storage
        assertFalse(PermissionStorage.get(new StringPrincipal(username)).contains("cluster.admin/read"));
    }
}
