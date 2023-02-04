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
import org.opensearch.identity.rest.user.get.multi.MultiGetUserRequest;
import org.opensearch.identity.rest.user.get.multi.RestMultiGetUserAction;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests to verify the behavior of rest create user action
 */
public class RestMultiGetUserActionTests extends OpenSearchTestCase {

    public void testGetMultipleUsers() throws Exception {
        SetOnce<Boolean> mgetUserCalled = new SetOnce<>();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                MultiGetUserRequest req = (MultiGetUserRequest) request;
                mgetUserCalled.set(true);
            }
        }) {
            RestMultiGetUserAction action = new RestMultiGetUserAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);

            assertThat(mgetUserCalled.get(), equalTo(true));
        }
    }

}
