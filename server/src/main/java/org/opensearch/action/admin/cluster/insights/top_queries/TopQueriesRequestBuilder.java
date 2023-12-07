/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */


package org.opensearch.action.admin.cluster.insights.top_queries;

import org.opensearch.action.support.nodes.NodesOperationRequestBuilder;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.common.annotation.PublicApi;

/**
 * Builder class for requesting cluster/node level top queries information.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class TopQueriesRequestBuilder extends NodesOperationRequestBuilder<TopQueriesRequest, TopQueriesResponse, TopQueriesRequestBuilder> {

    public TopQueriesRequestBuilder(OpenSearchClient client, TopQueriesAction action) {
        super(client, action, new TopQueriesRequest());
    }

    /**
     * set metric type for the request
     *
     * @param metric Name of metric as a string.
     * @return This, for request chaining.
     */
    public TopQueriesRequestBuilder setMetricType(String metric) {
        request.setMetricType(metric);
        return this;
    }
}
