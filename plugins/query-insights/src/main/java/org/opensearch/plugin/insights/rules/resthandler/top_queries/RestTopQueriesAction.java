/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.resthandler.top_queries;

import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesAction;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesRequest;
import org.opensearch.plugin.insights.rules.action.top_queries.TopQueriesResponse;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;

import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.plugin.insights.settings.QueryInsightsSettings.TOP_QUERIES_BASE_URI;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Rest action to get Top N queries by certain metric type
 *
 * @opensearch.api
 */
public class RestTopQueriesAction extends BaseRestHandler {
    /** The metric types that are allowed in top N queries */
    static final Set<String> ALLOWED_METRICS = MetricType.allMetricTypes().stream().map(MetricType::toString).collect(Collectors.toSet());

    /**
     * Constructor for RestTopQueriesAction
     */
    public RestTopQueriesAction() {}

    @Override
    public List<Route> routes() {
        return List.of(
            new Route(GET, TOP_QUERIES_BASE_URI),
            new Route(GET, String.format(Locale.ROOT, "%s/{nodeId}", TOP_QUERIES_BASE_URI))
        );
    }

    @Override
    public String getName() {
        return "query_insights_top_queries_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        final TopQueriesRequest topQueriesRequest = prepareRequest(request);
        topQueriesRequest.timeout(request.param("timeout"));

        return channel -> client.execute(TopQueriesAction.INSTANCE, topQueriesRequest, topQueriesResponse(channel));
    }

    static TopQueriesRequest prepareRequest(final RestRequest request) {
        final String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        final String metricType = request.param("type", MetricType.LATENCY.toString());
        if (!ALLOWED_METRICS.contains(metricType)) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "request [%s] contains invalid metric type [%s]", request.path(), metricType)
            );
        }
        return new TopQueriesRequest(MetricType.fromString(metricType), nodesIds);
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

    private RestResponseListener<TopQueriesResponse> topQueriesResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final TopQueriesResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
