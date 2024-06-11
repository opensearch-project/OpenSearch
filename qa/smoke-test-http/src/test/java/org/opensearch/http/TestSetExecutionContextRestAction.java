/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http;

import org.opensearch.client.node.NodeClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

public class TestSetExecutionContextRestAction extends BaseRestHandler {

    private final ThreadPool threadPool;

    public TestSetExecutionContextRestAction(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_set_execution_context"));
    }

    @Override
    public String getName() {
        return "test_set_execution_context_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        threadPool.getThreadContext().setExecutionContext("should-not-allow-plugin-to-set-execution-context");
        RestResponse response = new BytesRestResponse(RestStatus.OK, "Should not happen");
        return channel -> channel.sendResponse(response);
    }
}
