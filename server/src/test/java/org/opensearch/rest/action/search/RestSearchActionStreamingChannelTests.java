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
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.StreamSearchAction;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.common.SetOnce;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.http.HttpChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestCancellableNodeClient;
import org.opensearch.rest.action.StreamingTerminalChannelReleasingListener;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.opensearch.action.search.StreamSearchTransportService.STREAM_SEARCH_ENABLED;
import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;
import static org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class RestSearchActionStreamingChannelTests extends OpenSearchTestCase {

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> entries = new ArrayList<>();
        entries.addAll(ClusterModule.getNamedXWriteables());
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.emptyList());
        entries.addAll(searchModule.getNamedXContents());
        return new NamedXContentRegistry(entries);
    }

    public void testStreamingTerminalChannelReleasingListenerCreation() {
        RestCancellableNodeClient mockClient = mock(RestCancellableNodeClient.class);
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).build();
        FakeRestChannel channel = new FakeRestChannel(restRequest, false, 0);
        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> mockDelegate = mock(ActionListener.class);

        StreamingTerminalChannelReleasingListener<SearchResponse> listener = new StreamingTerminalChannelReleasingListener<>(
            mockClient,
            channel,
            mockDelegate
        );

        assertNotNull(listener);
        assertEquals(channel, listener.getChannel());
        assertFalse(listener.isFinished());
    }

    public void testStreamingListenerBasicFunctionality() {
        RestCancellableNodeClient mockClient = mock(RestCancellableNodeClient.class);
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).build();
        FakeRestChannel channel = new FakeRestChannel(restRequest, false, 0);
        @SuppressWarnings("unchecked")
        ActionListener<SearchResponse> mockDelegate = mock(ActionListener.class);
        SearchResponse mockResponse = mock(SearchResponse.class);

        StreamingTerminalChannelReleasingListener<SearchResponse> listener = new StreamingTerminalChannelReleasingListener<>(
            mockClient,
            channel,
            mockDelegate
        );

        listener.onResponse(mockResponse);
        assertTrue(listener.isFinished());
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testStreamingRequestWiresStreamingTerminalChannelReleasingListener() throws Exception {
        ClusterSettings clusterSettings = createClusterSettingsWithStreamSearchEnabled();

        SearchSourceBuilder source = new SearchSourceBuilder();
        source.aggregation(AggregationBuilders.terms("test_terms").field("category"));
        XContentBuilder contentBuilder = XContentBuilder.builder(MediaTypeRegistry.JSON.xContent());
        source.toXContent(contentBuilder, EMPTY_PARAMS);
        BytesReference content = BytesReference.bytes(contentBuilder);
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(content, MediaTypeRegistry.JSON).build();

        SetOnce<ActionListener<SearchResponse>> capturedListener = new SetOnce<>();
        SetOnce<ActionType<?>> capturedAction = new SetOnce<>();

        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName()) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                capturedAction.set(action);
                listener.onResponse(null);
                return new Task(1L, "test", action.name(), "test task", null, null);
            }

            @Override
            public String getLocalNodeId() {
                return "test-node";
            }
        }) {
            RestSearchAction restSearchAction = new RestSearchAction(clusterSettings) {
                @Override
                protected RestCancellableNodeClient createRestCancellableNodeClient(NodeClient client, HttpChannel httpChannel) {
                    return new RestCancellableNodeClient(client, httpChannel) {
                        @Override
                        public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                            ActionType<Response> action,
                            Request request,
                            ActionListener<Response> listener
                        ) {
                            if (action == StreamSearchAction.INSTANCE) {
                                @SuppressWarnings("unchecked")
                                ActionListener<SearchResponse> searchListener = (ActionListener<SearchResponse>) listener;
                                capturedListener.set(searchListener);
                            }
                            super.doExecute(action, request, listener);
                        }
                    };
                }
            };

            FakeRestChannel channel = new FakeRestChannel(restRequest, false, 0);
            restSearchAction.handleRequest(restRequest, channel, nodeClient);

            assertThat(capturedAction.get(), equalTo(StreamSearchAction.INSTANCE));
            assertNotNull(capturedListener.get());
            assertThat(capturedListener.get(), instanceOf(StreamingTerminalChannelReleasingListener.class));
        }
    }

    private ClusterSettings createClusterSettingsWithStreamSearchEnabled() {
        Settings settings = Settings.builder().put(STREAM_SEARCH_ENABLED.getKey(), true).build();
        return new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    }
}
