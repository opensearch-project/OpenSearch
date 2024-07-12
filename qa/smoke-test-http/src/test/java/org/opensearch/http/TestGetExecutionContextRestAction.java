/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http;

import org.opensearch.client.node.NodeClient;
import org.opensearch.client.node.PluginAwareNodeClient;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;
import java.util.Stack;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.rest.RestRequest.Method.POST;

public class TestGetExecutionContextRestAction extends BaseRestHandler {

    private final PluginAwareNodeClient pluginAwareClient;

    public TestGetExecutionContextRestAction(PluginAwareNodeClient pluginAwareClient) {
        this.pluginAwareClient = pluginAwareClient;
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_get_execution_context"));
    }

    @Override
    public String getName() {
        return "test_get_execution_context_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        String stashedContext;
        try (ThreadContext.StoredContext storedContext = pluginAwareClient.switchContext()) {
            stashedContext = pluginAwareClient.threadPool().getThreadContext().getHeader(ThreadContext.PLUGIN_EXECUTION_CONTEXT);
        }
        RestResponse response = new BytesRestResponse(RestStatus.OK, stashedContext);
        return channel -> channel.sendResponse(response);
    }
}
