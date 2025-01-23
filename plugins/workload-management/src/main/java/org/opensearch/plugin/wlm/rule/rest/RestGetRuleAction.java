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
import org.opensearch.plugin.wlm.rule.action.GetRuleAction;
import org.opensearch.plugin.wlm.rule.action.GetRuleRequest;
import org.opensearch.plugin.wlm.rule.action.GetRuleResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.wlm.Rule.RuleAttribute;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.wlm.Rule._ID_STRING;

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
        final Map<RuleAttribute, Set<String>> attributeFilters = new HashMap<>();
        for (String attributeName : request.params().keySet()) {
            if (attributeName.equals(_ID_STRING)) {
                continue;
            }
            String[] valuesArray = request.param(attributeName).split(",");
            attributeFilters.put(RuleAttribute.fromName(attributeName), new HashSet<>(Arrays.asList(valuesArray)));
        }
        final GetRuleRequest getRuleRequest = new GetRuleRequest(request.param(_ID_STRING), attributeFilters);
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
