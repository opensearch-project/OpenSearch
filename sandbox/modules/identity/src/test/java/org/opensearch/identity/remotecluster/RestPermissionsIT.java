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
import org.opensearch.client.Response;
import org.opensearch.identity.ConfigConstants;
import org.opensearch.identity.rest.RestConstants;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.is;

/**
 * Integration test for permission granting REST API against a remote cluster
 */
public class RestPermissionsIT extends OpenSearchRestTestCase {

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

        // _identity/api/permissions
        final String endpoint = RestConstants.PERMISSION_SUBPATH;

        String username = "test";
        // _identity/api/permissions/test
        Request createRequest = new Request("PUT", endpoint + username);
        createRequest.setJsonEntity("{ \"permissionString\" : \"cluster.admin/read\"}\n");
        Response createResponse = client().performRequest(createRequest);
        assertThat(createResponse.getStatusLine().getStatusCode(), is(200));
    }
}
