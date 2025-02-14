/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http;

import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import java.io.IOException;

import static org.apache.hc.core5.http.HttpStatus.SC_OK;
import static org.hamcrest.Matchers.containsString;

@ClusterScope(scope = Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 5, numClientNodes = 0)
public class HttpCatIT extends HttpSmokeTestCase {

    public void testdoCatRequest() throws IOException {
        try (RestClient restClient = getRestClient()) {
            int nodesCount = restClient.getNodes().size();
            assertEquals(5, nodesCount);

            // to make sure the timeout is working
            for (int i = 0; i < 5; i++) {
                sendRequest(restClient, 30, nodesCount);
            }

            // no timeout
            for (int i = 0; i < 5; i++) {
                sendRequest(restClient, -1, nodesCount);
            }

            for (int i = 1; i < 5; i++) {
                long timeout = randomInt(300);
                sendRequest(restClient, timeout, nodesCount);
            }
        }
    }

    private void sendRequest(RestClient restClient, long timeout, int nodesCount) {
        Request nodesRequest;
        if (timeout < 0) {
            nodesRequest = new Request("GET", "/_cat/nodes");
        } else {
            nodesRequest = new Request("GET", "/_cat/nodes?timeout=" + timeout + "ms");
        }
        try {
            Response response = restClient.performRequest(nodesRequest);
            assertEquals(SC_OK, response.getStatusLine().getStatusCode());
            String result = EntityUtils.toString(response.getEntity());
            String[] NodeInfos = result.split("\n");
            assertEquals(nodesCount, NodeInfos.length);
        } catch (IOException | ParseException e) {
            // it means that it costs too long to get ClusterState from the master.
            assertThat(e.getMessage(), containsString("There is not enough time to obtain nodesInfo metric from the cluster manager"));
        }
    }

}
