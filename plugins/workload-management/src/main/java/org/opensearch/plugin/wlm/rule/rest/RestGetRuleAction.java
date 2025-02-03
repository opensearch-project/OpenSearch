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
import org.opensearch.plugin.wlm.querygroup.action.GetQueryGroupAction;
import org.opensearch.plugin.wlm.querygroup.action.GetQueryGroupRequest;
import org.opensearch.plugin.wlm.rule.action.*;
import org.opensearch.rest.*;
import org.opensearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.*;

/**
 * Rest action to get a Rule
 * @opensearch.experimental
 */
public class RestGetRuleAction extends BaseRestHandler {

    /**
     * Constructor for RestGetRuleAction
     */
    public RestGetRuleAction() {}

    @Override
    public String getName() {
        return "get_rule";
    }

    /**
     * The list of {@link Route}s that this RestHandler is responsible for handling.
     */
    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "_wlm/rule/"), new Route(GET, "_wlm/rule/{_id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final GetRuleRequest getRuleRequest = new GetRuleRequest(request.param("_id"));
        return channel -> client.execute(GetRuleAction.INSTANCE, getRuleRequest, getRuleResponse(channel));
    }

    private RestResponseListener<GetRuleResponse> getRuleResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final GetRuleResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
