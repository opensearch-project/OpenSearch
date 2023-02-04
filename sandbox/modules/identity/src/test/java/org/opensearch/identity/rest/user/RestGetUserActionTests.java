/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.user;

import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.SetOnce;
import org.opensearch.common.collect.Map;
import org.opensearch.identity.rest.user.get.single.GetUserRequest;
import org.opensearch.identity.rest.user.get.single.RestGetUserAction;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests to verify the behavior of rest create user action
 */
public class RestGetUserActionTests extends OpenSearchTestCase {

    public void testGetUserRestAction() throws Exception {
        SetOnce<Boolean> getUserCalled = new SetOnce<>();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                GetUserRequest req = (GetUserRequest) request;
                getUserCalled.set(true);
                assertThat(req.getUsername(), equalTo("test"));
            }
        }) {
            RestGetUserAction action = new RestGetUserAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("name", "test")).build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);

            assertThat(getUserCalled.get(), equalTo(true));
        }
    }

}
