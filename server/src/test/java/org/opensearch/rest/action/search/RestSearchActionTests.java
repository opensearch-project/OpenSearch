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
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.StreamSearchAction;
import org.opensearch.common.SetOnce;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.rest.RestRequest;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestChannel;
import org.opensearch.test.rest.FakeRestRequest;
import org.opensearch.transport.client.node.NodeClient;

import static org.opensearch.common.util.FeatureFlags.STREAM_SEARCH;
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

    private void testActionExecution(ActionType<?> expectedAction) throws Exception {
        SetOnce<ActionType<?>> capturedActionType = new SetOnce<>();
        try (NodeClient nodeClient = createMockNodeClient(capturedActionType)) {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);

            new RestSearchAction().handleRequest(request, channel, nodeClient);

            assertThat(capturedActionType.get(), equalTo(expectedAction));
        }
    }

    public void testWithSearchStreamFlagDisabled() throws Exception {
        // When SEARCH_STREAM flag is disabled, always use SearchAction
        testActionExecution(SearchAction.INSTANCE);
    }

    @LockFeatureFlag(STREAM_SEARCH)
    public void testWithStreamSearchEnabledButStreamTransportDisabled() throws Exception {
        // When SEARCH_STREAM is enabled but STREAM_TRANSPORT is disabled, should throw exception
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName())) {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);

            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> new RestSearchAction().handleRequest(request, channel, nodeClient)
            );
            assertThat(e.getMessage(), equalTo("You need to enable stream transport first to use stream search."));
        }
    }

    public void testWithStreamSearchAndTransportEnabled() throws Exception {
        // When both SEARCH_STREAM and STREAM_TRANSPORT are enabled, should use StreamSearchAction
        try (
            FeatureFlags.TestUtils.FlagWriteLock searchStreamLock = new FeatureFlags.TestUtils.FlagWriteLock(STREAM_SEARCH);
            FeatureFlags.TestUtils.FlagWriteLock streamTransportLock = new FeatureFlags.TestUtils.FlagWriteLock(STREAM_TRANSPORT)
        ) {
            testActionExecution(StreamSearchAction.INSTANCE);
        }
    }

    // Tests for canUseStreamSearch method
    public void testCanUseStreamSearchWithNullSource() {
        SearchRequest searchRequest = new SearchRequest();
        assertTrue(RestSearchAction.canUseStreamSearch(searchRequest));
    }

    public void testCanUseStreamSearchWithNoAggregations() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(QueryBuilders.matchAllQuery());
        searchRequest.source(source);
        assertTrue(RestSearchAction.canUseStreamSearch(searchRequest));
    }

    public void testCanUseStreamSearchWithSingleTermsAggregation() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.aggregation(AggregationBuilders.terms("test_terms").field("category"));
        searchRequest.source(source);
        assertTrue(RestSearchAction.canUseStreamSearch(searchRequest));
    }

    public void testCanUseStreamSearchWithMultipleAggregations() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.aggregation(AggregationBuilders.terms("test_terms").field("category"));
        source.aggregation(AggregationBuilders.avg("test_avg").field("price"));
        searchRequest.source(source);
        assertFalse(RestSearchAction.canUseStreamSearch(searchRequest));
    }

    public void testCanUseStreamSearchWithSingleNonTermsAggregation() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.aggregation(AggregationBuilders.avg("test_avg").field("price"));
        searchRequest.source(source);
        assertFalse(RestSearchAction.canUseStreamSearch(searchRequest));
    }

    public void testCanUseStreamSearchWithSingleHistogramAggregation() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.aggregation(AggregationBuilders.histogram("test_histogram").field("timestamp").interval(1000));
        searchRequest.source(source);
        assertFalse(RestSearchAction.canUseStreamSearch(searchRequest));
    }
}
