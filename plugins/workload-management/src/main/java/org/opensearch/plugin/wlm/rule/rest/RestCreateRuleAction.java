/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.rest;

import org.opensearch.client.node.NodeClient;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugin.wlm.rule.action.CreateRuleAction;
import org.opensearch.plugin.wlm.rule.action.CreateRuleRequest;
import org.opensearch.plugin.wlm.rule.action.CreateRuleResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.POST;
import static org.opensearch.rest.RestRequest.Method.PUT;

/**
 * Rest action to create a Rule
 * @opensearch.experimental
 */
public class RestCreateRuleAction extends BaseRestHandler {

    public RestCreateRuleAction() {}

    @Override
    public String getName() {
        return "create_rule";
    }

    /**
     * The list of {@link Route}s that this RestHandler is responsible for handling.
     */
    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "_wlm/rule/"), new Route(PUT, "_wlm/rule/"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            CreateRuleRequest createRuleRequest = CreateRuleRequest.fromXContent(parser);
            return channel -> client.execute(CreateRuleAction.INSTANCE, createRuleRequest, createRuleResponse(channel));
        }
    }

    private RestResponseListener<CreateRuleResponse> createRuleResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final CreateRuleResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
