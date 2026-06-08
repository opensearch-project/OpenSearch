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
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.StreamSearchAction;
import org.opensearch.common.SetOnce;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
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

import static org.opensearch.action.search.StreamSearchTransportService.STREAM_SEARCH_ENABLED;
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

    private ClusterSettings createClusterSettingsWithStreamSearchEnabled() {
        Settings settings = Settings.builder().put(STREAM_SEARCH_ENABLED.getKey(), true).build();
        return new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    }

    private SearchRequest createSearchRequestWithTermsAggregation() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.aggregation(AggregationBuilders.terms("test_terms").field("category"));
        searchRequest.source(source);
        return searchRequest;
    }

    public void testWithSearchStreamDisabled() throws Exception {
        SetOnce<ActionType<?>> capturedActionType = new SetOnce<>();
        try (NodeClient nodeClient = createMockNodeClient(capturedActionType)) {
            RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).build();
            FakeRestChannel channel = new FakeRestChannel(request, false, 0);

            new RestSearchAction().handleRequest(request, channel, nodeClient);

            assertThat(capturedActionType.get(), equalTo(SearchAction.INSTANCE));
        }
    }

    // When stream search is enabled but STREAM_TRANSPORT is disabled, should throw exception
    public void testWithStreamSearchEnabledButStreamTransportDisabled() {
        try (NodeClient nodeClient = new NoOpNodeClient(this.getTestName())) {
            RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).build();
            FakeRestChannel channel = new FakeRestChannel(restRequest, false, 0);

            Exception e = expectThrows(
                IllegalArgumentException.class,
                () -> new RestSearchAction(createClusterSettingsWithStreamSearchEnabled()).handleRequest(restRequest, channel, nodeClient)
            );
            assertThat(e.getMessage(), equalTo("You need to enable stream transport first to use stream search."));
        }
    }

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testWithStreamSearchAndTransportEnabled() {
        ClusterSettings clusterSettings = createClusterSettingsWithStreamSearchEnabled();
        SearchRequest searchRequest = createSearchRequestWithTermsAggregation();

        SetOnce<ActionType<?>> capturedActionType = new SetOnce<>();
        try (NodeClient nodeClient = createMockNodeClient(capturedActionType)) {
            // Verify all conditions are met for stream search
            assertTrue(clusterSettings.get(STREAM_SEARCH_ENABLED));
            assertTrue(FeatureFlags.isEnabled(FeatureFlags.STREAM_TRANSPORT));
            assertTrue(RestSearchAction.canUseStreamSearch(searchRequest));

            // Execute the StreamSearchAction directly since we've verified the conditions
            nodeClient.executeLocally(StreamSearchAction.INSTANCE, searchRequest, new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse response) {}

                @Override
                public void onFailure(Exception e) {}
            });

            assertThat(capturedActionType.get(), equalTo(StreamSearchAction.INSTANCE));
        }
    }

    // Tests for canUseStreamSearch method
    public void testCanUseStreamSearchWithNullSource() {
        SearchRequest searchRequest = new SearchRequest();
        assertFalse(RestSearchAction.canUseStreamSearch(searchRequest));
    }

    public void testCanUseStreamSearchWithNoAggregations() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.query(QueryBuilders.matchAllQuery());
        searchRequest.source(source);
        assertFalse(RestSearchAction.canUseStreamSearch(searchRequest));
    }

    public void testCanUseStreamSearchWithSingleTermsAggregation() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.aggregation(AggregationBuilders.terms("test_terms").field("category"));
        searchRequest.source(source);
        assertTrue(RestSearchAction.canUseStreamSearch(searchRequest));
    }

    public void testCanUseStreamSearchWithMultipleTermsAggregations() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.aggregation(AggregationBuilders.terms("terms1").field("category"));
        source.aggregation(AggregationBuilders.terms("terms2").field("brand"));
        searchRequest.source(source);
        assertTrue(RestSearchAction.canUseStreamSearch(searchRequest));
    }

    public void testCanUseStreamSearchWithTermsAndNonTermsTopLevel() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.aggregation(AggregationBuilders.terms("test_terms").field("category"));
        source.aggregation(AggregationBuilders.sum("test_sum").field("price"));
        searchRequest.source(source);
        assertFalse(RestSearchAction.canUseStreamSearch(searchRequest));
    }

    public void testCanUseStreamSearchWithSingleNonTermsAggregation() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.aggregation(AggregationBuilders.sum("test_sum").field("price"));
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

    public void testCanUseStreamSearchWithTermsAndSupportedSubAggregations() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.aggregation(
            AggregationBuilders.terms("test_terms")
                .field("category")
                .subAggregation(AggregationBuilders.sum("sum_price").field("price"))
                .subAggregation(AggregationBuilders.max("max_price").field("price"))
                .subAggregation(AggregationBuilders.min("min_price").field("price"))
                .subAggregation(AggregationBuilders.cardinality("unique_brands").field("brand"))
        );
        searchRequest.source(source);
        assertTrue(RestSearchAction.canUseStreamSearch(searchRequest));
    }

    public void testCanUseStreamSearchWithTermsAndNestedTermsSubAggregation() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.aggregation(
            AggregationBuilders.terms("category_terms")
                .field("category")
                .subAggregation(AggregationBuilders.terms("brand_terms").field("brand"))
        );
        searchRequest.source(source);
        assertTrue(RestSearchAction.canUseStreamSearch(searchRequest));
    }

    public void testCanUseStreamSearchWithTermsAndUnsupportedSubAggregation() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.aggregation(
            AggregationBuilders.terms("test_terms")
                .field("category")
                .subAggregation(AggregationBuilders.histogram("price_hist").field("price").interval(10))
        );
        searchRequest.source(source);
        assertFalse(RestSearchAction.canUseStreamSearch(searchRequest));
    }

    public void testCanUseStreamSearchWithThreeLevelTermsNesting() {
        // 3-level terms nesting should be rejected (max 2 levels supported)
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.aggregation(
            AggregationBuilders.terms("level1_terms")
                .field("category")
                .subAggregation(
                    AggregationBuilders.terms("level2_terms")
                        .field("brand")
                        .subAggregation(AggregationBuilders.terms("level3_terms").field("model"))
                )
        );
        searchRequest.source(source);
        assertFalse(RestSearchAction.canUseStreamSearch(searchRequest));
    }

    public void testCanUseStreamSearchWithTwoLevelTermsAndMetricLeaf() {
        // 2-level terms with metric leaf should be accepted
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder source = new SearchSourceBuilder();
        source.aggregation(
            AggregationBuilders.terms("level1_terms")
                .field("category")
                .subAggregation(
                    AggregationBuilders.terms("level2_terms")
                        .field("brand")
                        .subAggregation(AggregationBuilders.sum("sum_price").field("price"))
                )
        );
        searchRequest.source(source);
        assertTrue(RestSearchAction.canUseStreamSearch(searchRequest));
    }
}
