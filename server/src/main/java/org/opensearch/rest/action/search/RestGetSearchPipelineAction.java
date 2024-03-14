/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.search;

import org.opensearch.action.search.GetSearchPipelineRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.core.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * REST action to retrieve search pipelines
 *
 *  @opensearch.internal
 */
public class RestGetSearchPipelineAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "search_get_pipeline_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_search/pipeline"), new Route(GET, "/_search/pipeline/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        GetSearchPipelineRequest request = new GetSearchPipelineRequest(Strings.splitStringByCommaToArray(restRequest.param("id")));
        request.clusterManagerNodeTimeout(restRequest.paramAsTime("cluster_manager_timeout", request.clusterManagerNodeTimeout()));
        return channel -> client.admin().cluster().getSearchPipeline(request, new RestStatusToXContentListener<>(channel));
    }
}
