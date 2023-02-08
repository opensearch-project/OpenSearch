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
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.collect.Map;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.identity.rest.user.put.PutUserRequest;
import org.opensearch.identity.rest.user.put.RestPutUserAction;
import org.opensearch.identity.utils.ErrorType;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;

import static org.hamcrest.Matchers.equalTo;

/**
 * Tests to verify the behavior of rest create user action
 */
public class RestPutUserActionTests extends OpenSearchTestCase {
    public void testParsePutUserRequestWithInvalidJsonThrowsException() {
        RestPutUserAction action = new RestPutUserAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray("{invalid_json}"),
            XContentType.JSON
        ).build();
        Exception e = expectThrows(IllegalArgumentException.class, () -> action.prepareRequest(request, null));
        assertThat(e.getMessage(), equalTo(ErrorType.BODY_NOT_PARSEABLE.getMessage() + "PUT"));
    }

    public void testPutUserWithValidJson() throws Exception {
        SetOnce<Boolean> putUserCalled = new SetOnce<>();
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                PutUserRequest req = (PutUserRequest) request;
                putUserCalled.set(true);
                assertEquals(req.getUsername(), "test");
                assertEquals(req.getPassword(), "test");
                assertEquals(req.getAttributes().size(), 1);
                assertEquals(req.getPermissions().size(), 1);
            }
        }) {
            String content = "{ \"password\" : \"test\","
                + " \"attributes\": { \"attribute1\": \"value1\"},"
                + " \"permissions\": [\"indices:admin:create\"]"
                + " }\n";

            RestPutUserAction action = new RestPutUserAction();
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("name", "test"))
                .withContent(new BytesArray(content), XContentType.JSON)
                .build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);
            action.handleRequest(request, channel, nodeClient);

            assertThat(putUserCalled.get(), equalTo(true));
        }
    }

}
