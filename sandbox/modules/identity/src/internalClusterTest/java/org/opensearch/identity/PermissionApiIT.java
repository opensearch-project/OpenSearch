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
import org.opensearch.identity.rest.RestConstants;
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

        final String endpoint = RestConstants.IDENTITY_PERMISSION_SUFFIX;

        String username = "test";
        // Add a permission
        Request createRequest = new Request("PUT", endpoint + username + RestConstants.IDENTITY_PUT_PERMISSION_SUFFIX);
        createRequest.setJsonEntity("{ \"permissionString\" : \"cluster:admin/read\"}\n");
        Response createResponse = getRestClient().performRequest(createRequest);
        assertThat(createResponse.getStatusLine().getStatusCode(), is(200));

    }
}
