/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.search;

import org.opensearch.action.search.DeleteSearchPipelineRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.DELETE;

/**
 * REST action to delete a search pipeline
 *
 *  @opensearch.internal
 */
public class RestDeleteSearchPipelineAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "search_delete_pipeline_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "/_search/pipeline/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest restRequest, NodeClient client) throws IOException {
        DeleteSearchPipelineRequest request = new DeleteSearchPipelineRequest(restRequest.param("id"));
        request.clusterManagerNodeTimeout(restRequest.paramAsTime("cluster_manager_timeout", request.clusterManagerNodeTimeout()));
        request.timeout(restRequest.paramAsTime("timeout", request.timeout()));
        return channel -> client.admin().cluster().deleteSearchPipeline(request, new RestToXContentListener<>(channel));
    }
}
