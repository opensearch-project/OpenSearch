/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.rest;

import org.opensearch.action.ActionType;
import org.opensearch.autotagging.Attribute;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.rule.action.GetRuleRequest;
import org.opensearch.rule.action.GetRuleResponse;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.autotagging.Rule._ID_STRING;

/**
 * Rest action to get a Rule
 * @opensearch.experimental
 */
public abstract class RestGetRuleAction extends BaseRestHandler {
    public static final String SEARCH_AFTER_STRING = "search_after";

    public RestGetRuleAction() {}

    @Override
    public abstract String getName();

    @Override
    public abstract List<Route> routes();

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final Map<Attribute, Set<String>> attributeFilters = new HashMap<>();
        for (String attributeName : request.params().keySet()) {
            if (attributeName.equals(_ID_STRING) || attributeName.equals(SEARCH_AFTER_STRING)) {
                continue;
            }
            String[] valuesArray = request.param(attributeName).split(",");
            attributeFilters.put(getAttributeFromName(attributeName), new HashSet<>(Arrays.asList(valuesArray)));
        }
        final GetRuleRequest getRuleRequest = buildGetRuleRequest(
            request.param(_ID_STRING),
            attributeFilters,
            request.param(SEARCH_AFTER_STRING)
        );
        return channel -> client.execute(retrieveGetRuleActionInstance(), getRuleRequest, getRuleResponse(channel));
    }

    private RestResponseListener<GetRuleResponse> getRuleResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final GetRuleResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }

    protected abstract Attribute getAttributeFromName(String name);

    /**
     * Abstract method for subclasses to provide specific ActionType Instance
     */
    protected abstract <T extends ActionType<? extends GetRuleResponse>> T retrieveGetRuleActionInstance();

    protected abstract GetRuleRequest buildGetRuleRequest(String id, Map<Attribute, Set<String>> attributeFilters, String searchAfter);
}
