/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.rest.RestStatus;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class BasicAuthTests extends AbstractIdentityTestCase {
    public void testBasicAuthSuccess() throws Exception {
        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Basic YWRtaW46YWRtaW4=").build(); // admin:admin
        request.setOptions(options);

        Response response = getRestClient().performRequest(request);

        String content = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        assertEquals(RestStatus.OK.getStatus(), response.getStatusLine().getStatusCode());
        assertThat(content, containsString("green"));
    }

    public void testBasicAuthUnauthorized() throws Exception {
        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Basic bWFydmluOmdhbGF4eQ==").build(); // marvin:galaxy
        request.setOptions(options);
        request.addParameter("ignore", "401");

        Response response = getRestClient().performRequest(request);

        assertEquals(RestStatus.UNAUTHORIZED.getStatus(), response.getStatusLine().getStatusCode());
    }
}
