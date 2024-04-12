/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.resource_limit_group.rest;

import org.opensearch.client.node.NodeClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.resource_limit_group.SampleAction;
import org.opensearch.plugin.resource_limit_group.SampleRequest;
import org.opensearch.plugin.resource_limit_group.SampleResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.List;

public class SampleRestAction extends BaseRestHandler {
    /**
     * @return the name of this handler. The name should be human readable and
     * should describe the action that will performed when this API is
     * called. This name is used in the response to the
     */
    @Override
    public String getName() {
        return this.getClass().getName();
    }

    /**
     * The list of {@link Route}s that this RestHandler is responsible for handling.
     */
    @Override
    public List<Route> routes() {
        return List.of(
            new Route(RestRequest.Method.GET, "/_sample_rest")
        );
    }

    /**
     * Prepare the request for execution. Implementations should consume all request params before
     * returning the runnable for actual execution. Unconsumed params will immediately terminate
     * execution of the request. However, some params are only used in processing the response;
     * implementations can override {@link BaseRestHandler#responseParams()} to indicate such
     * params.
     *
     * @param request the request to execute
     * @param client  client for executing actions on the local node
     * @return the action to execute
     * @throws IOException if an I/O exception occurred parsing the request and preparing for
     *                     execution
     */
    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        return (channel) -> {
            client.execute(SampleAction.INSTANCE, new SampleRequest(), new RestResponseListener<SampleResponse>(channel) {
                @Override
                public RestResponse buildResponse(SampleResponse sampleResponse) throws Exception {
                    return new BytesRestResponse(RestStatus.OK, sampleResponse.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
                }
            });
        };
    }
}
