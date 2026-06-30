/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.backwards;

import org.apache.hc.core5.http.HttpHost;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.opensearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

public class SearchingIT extends OpenSearchRestTestCase {
    public void testMultiGet() throws Exception {
        final Set<HttpHost> nodes = buildNodes();

        final MultiGetRequest multiGetRequest = new MultiGetRequest();
        multiGetRequest.add("index", "id1");

        try (RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(nodes.toArray(HttpHost[]::new)))) {
            MultiGetResponse response = client.mget(multiGetRequest, RequestOptions.DEFAULT);
            assertEquals(1, response.getResponses().length);

            assertTrue(response.getResponses()[0].isFailed());
            assertNotNull(response.getResponses()[0].getFailure());
            assertEquals(response.getResponses()[0].getFailure().getId(), "id1");
            assertEquals(response.getResponses()[0].getFailure().getIndex(), "index");
            assertThat(response.getResponses()[0].getFailure().getMessage(), containsString("no such index [index]"));
       }
    }

    private Set<HttpHost> buildNodes() throws IOException, URISyntaxException {
        Response response = client().performRequest(new Request("GET", "_nodes"));
        ObjectPath objectPath = ObjectPath.createFromResponse(response);
        Map<String, Object> nodesAsMap = objectPath.evaluate("nodes");
        final Set<HttpHost> nodes = new HashSet<>();
        for (String id : nodesAsMap.keySet()) {
            nodes.add(HttpHost.create((String) objectPath.evaluate("nodes." + id + ".http.publish_address")));
        }

        return nodes;
    }
}
