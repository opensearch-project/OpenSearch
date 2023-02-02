/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.rest.action.permission;

import org.opensearch.common.SetOnce;
import org.opensearch.action.ActionListener;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionResponse;
import org.opensearch.action.ActionType;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.collect.Map;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.identity.rest.action.permission.put.RestPutPermissionAction;
import org.opensearch.identity.rest.action.permission.put.PutPermissionRequest;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests to verify the behavior of rest create user action
 */
public class RestPermissionTests extends OpenSearchTestCase {
    public void testParsePutPermissionRequestWithInvalidJsonThrowsException() {
        RestPutPermissionAction action = new RestPutPermissionAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray("{invalid_json}"),
            XContentType.JSON
        ).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), equalTo("Failed to parse put permission request body"));
    }

    public void testParsePutPermissionWithValidJson() throws Exception {
        SetOnce<Boolean> putPermissionCalled = new SetOnce<>();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                PutPermissionRequest req = (PutPermissionRequest) request;
                putPermissionCalled.set(true);
                assertThat(req.getPermissionString(), equalTo("test"));
            }
        }) {
            RestPutPermissionAction action = new RestPutPermissionAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("permissionString", "test")).build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);

            assertThat(putPermissionCalled.get(), equalTo(true));
        }
    }
}
