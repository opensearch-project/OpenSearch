/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.executioncontextplugin;

import org.opensearch.client.Client;
import org.opensearch.client.node.NodeClient;
import org.opensearch.plugins.PluginSubject;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.threadpool.ThreadPool;

import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

public class TestGetExecutionContextRestAction extends BaseRestHandler {

    private final Client client;

    public TestGetExecutionContextRestAction(Client client) {
        this.client = client;
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
        final TestGetExecutionContextRequest getExecutionContextRequest = new TestGetExecutionContextRequest();
        return channel -> client.execute(TestGetExecutionContextAction.INSTANCE, getExecutionContextRequest, new RestToXContentListener<>(channel));
    }
}
