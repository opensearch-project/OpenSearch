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
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

// Testing authentication against remote cluster
public class BasicAuthenticationIT extends OpenSearchRestTestCase {

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

    public void testClusterHealthWithValidAuthenticationHeader_twoSemiColonPassword() throws IOException {
        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Basic dGVzdDp0ZTpzdA==").build(); // test:te:st
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

        // Should not fail, because current implementation allows a request with missing header to pass
        // TODO: Update this test to check for missing-header response, once that is implemented
        assertOK(response);

        // Standard cluster health response
        MatcherAssert.assertThat(entityAsMap(response).size(), equalTo(17));
        MatcherAssert.assertThat(entityAsMap(response).get("status"), equalTo("green"));
    }

    public void testClusterHealthWithInvalidAuthenticationHeader() throws IOException {
        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Basic bWFydmluOmdhbGF4eQ==").build(); // marvin:galaxy
        request.setOptions(options);
        try {
            client().performRequest(request);
        } catch (ResponseException re) {
            Map<String, Object> responseMap = entityAsMap(re.getResponse());
            MatcherAssert.assertThat(responseMap.size(), equalTo(2));
            MatcherAssert.assertThat(responseMap.get("status"), equalTo(401));
            MatcherAssert.assertThat(responseMap.get("error"), equalTo("marvin does not exist in internal realm."));
        }
    }

    public void testClusterHealthWithCorruptAuthenticationHeader() throws IOException {
        Request request = new Request("GET", "/_cluster/health");
        RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Basic bleh").build();
        try {
            client().performRequest(request);
        } catch (ResponseException e) {
            MatcherAssert.assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(405));
        }

    }
}
