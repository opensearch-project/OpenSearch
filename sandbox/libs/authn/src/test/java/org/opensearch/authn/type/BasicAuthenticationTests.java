/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.authn.type;

import org.hamcrest.MatcherAssert;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class BasicAuthenticationTests extends OpenSearchRestTestCase {

    public void testClusterHealthWithValidAuthenticationHeader() throws IOException {
        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Basic YWRtaW46YWRtaW4=").build(); // admin:admin
        request.setOptions(options);
        Response response = client().performRequest(request);

        assertOK(response);

        // Standard cluster health response
        MatcherAssert.assertThat(entityAsMap(response).size(), equalTo(17));
        MatcherAssert.assertThat(entityAsMap(response).get("status"), equalTo("green"));

    }

    public void testClusterHealthWithNoHeader() throws IOException {
        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().build(); // admin:admin
        request.setOptions(options);
        Response response = client().performRequest(request);

        // allowed if no authorization header was passed. Should be updated once that is fixed
        assertOK(response);

        // Standard cluster health response
        MatcherAssert.assertThat(entityAsMap(response).size(), equalTo(17));
        MatcherAssert.assertThat(entityAsMap(response).get("status"), equalTo("green"));

    }

    public void testClusterHealthWithInvalidAuthenticationHeader() throws IOException {
        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Basic bWFydmluOmdhbGF4eQ==").build(); // marvin:galaxy
        request.setOptions(options);
        Response response = client().performRequest(request);

        // Should not fail, because current implementation allows a unauthorized request to pass
        // TODO: Update this to test for UNAUTHORIZED once that flow is implemented
        assertOK(response);

        // Standard cluster health response
        MatcherAssert.assertThat(entityAsMap(response).size(), equalTo(17));
        MatcherAssert.assertThat(entityAsMap(response).get("status"), equalTo("green"));

    }

    public void testClusterHealthWithCorruptAuthenticationHeader() throws IOException {
        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Basic bleh").build(); // marvin:galaxy
        request.setOptions(options);
        try {
            client().performRequest(request);
        } catch (ResponseException e) {
            MatcherAssert.assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(405));
        }

    }
}
