/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */


package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.insights.top_queries.TopQueriesRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions.NodesResponseRestListener;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Transport action to get Top N queries by certain metric type
 *
 * @opensearch.api
 */
public class RestTopQueriesAction extends BaseRestHandler {
    /** The metric types that are allowed in top N queries */
    static final Set<String> ALLOWED_METRICS = TopQueriesRequest.Metric.allMetrics();

    public RestTopQueriesAction() {}

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(
                new Route(GET, "/_insights/top_queries"),
                new Route(GET, "/_insights/top_queries/{nodeId}")
            )
        );
    }

    @Override
    public String getName() {
        return "top_queries_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final TopQueriesRequest topQueriesRequest = prepareRequest(request);
        topQueriesRequest.timeout(request.param("timeout"));

        return channel -> client.admin().cluster().topQueries(topQueriesRequest, new NodesResponseRestListener<>(channel));
    }

    static TopQueriesRequest prepareRequest(final RestRequest request) {
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        String metricType = request.param("type", TopQueriesRequest.Metric.LATENCY.metricName()).toUpperCase();
        if (!ALLOWED_METRICS.contains(metricType)) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "request [%s] contains invalid metric type [%s]",
                    request.path(),
                    metricType
                )
            );
        }
        TopQueriesRequest topQueriesRequest = new TopQueriesRequest(nodesIds);
        topQueriesRequest.setMetricType(metricType);
        return topQueriesRequest;
    }

    @Override
    protected Set<String> responseParams() {
        return Settings.FORMAT_PARAMS;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
