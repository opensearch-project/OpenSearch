/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.search;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionType;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.StreamSearchAction;
import org.opensearch.common.SetOnce;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;
import static org.hamcrest.Matchers.equalTo;

public class RestSearchActionTests extends OpenSearchTestCase {

    private NodeClient createMockNodeClient(SetOnce<ActionType<?>> capturedActionType) {
        return new NoOpNodeClient(this.getTestName()) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                capturedActionType.set(action);
                listener.onResponse(null);
                return new Task(1L, "test", action.name(), "test task", null, null);
            }

            @Override
            public String getLocalNodeId() {
                return "test-node";
            }
        };
    }

    private void testActionExecution(Map<String, String> params, ActionType<?> expectedAction) throws Exception {
        SetOnce<ActionType<?>> capturedActionType = new SetOnce<>();
        try (NodeClient nodeClient = createMockNodeClient(capturedActionType)) {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(params != null ? params : new HashMap<>())
                .build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);

            new RestSearchAction().handleRequest(request, channel, nodeClient);

            assertThat(capturedActionType.get(), equalTo(expectedAction));
        }
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamSearchWithFeatureFlagEnabled() throws Exception {
        testActionExecution(Map.of("stream", "true"), StreamSearchAction.INSTANCE);
    }

    public void testStreamSearchWithFeatureFlagDisabled() throws Exception {
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName())) {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withParams(Map.of("stream", "true")).build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);

            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> new RestSearchAction().handleRequest(request, channel, nodeClient)
            );
            assertThat(e.getMessage(), equalTo("You need to enable stream transport first to use stream search."));
        }
    }

    public void testRegularSearchWithoutStreamParameter() throws Exception {
        testActionExecution(null, SearchAction.INSTANCE);
    }

    public void testRegularSearchWithStreamParameterFalse() throws Exception {
        testActionExecution(Map.of("stream", "false"), SearchAction.INSTANCE);
    }
}
