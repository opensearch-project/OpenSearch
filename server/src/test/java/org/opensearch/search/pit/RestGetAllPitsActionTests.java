/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pit;

import org.apache.lucene.util.SetOnce;
import org.opensearch.action.ActionListener;
import org.opensearch.action.search.GetAllPitNodesRequest;
import org.opensearch.action.search.GetAllPitNodesResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.search.RestGetAllPitsAction;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for rest get all PITs action
 */
public class RestGetAllPitsActionTests extends OpenSearchTestCase {

    public void testGetAllPits() throws Exception {
        SetOnce<Boolean> pitCalled = new SetOnce<>();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public void getAllPits(GetAllPitNodesRequest request, ActionListener<GetAllPitNodesResponse> listener) {
                pitCalled.set(true);
            }
        }) {
            RestGetAllPitsAction action = new RestGetAllPitsAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withPath("/_all").build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);
            assertThat(pitCalled.get(), equalTo(true));
        }
    }
}
