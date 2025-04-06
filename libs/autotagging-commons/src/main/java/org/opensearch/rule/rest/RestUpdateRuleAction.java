/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.rest;

import org.opensearch.action.ActionType;
import org.opensearch.autotagging.FeatureType;
import org.opensearch.autotagging.Rule.Builder;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.rule.action.UpdateRuleRequest;
import org.opensearch.rule.action.UpdateRuleResponse;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.opensearch.autotagging.Rule._ID_STRING;

/**
 * Rest action to update a Rule
 * @opensearch.experimental
 */
@ExperimentalApi
public class RestUpdateRuleAction extends BaseRestHandler {
    private final String name;
    private final List<Route> routes;
    private final FeatureType featureType;
    private final ActionType<UpdateRuleResponse> updateRuleAction;

    /**
     * constructor for RestUpdateRuleAction
     * @param name - RestUpdateRuleAction name
     * @param routes the list of REST routes this action handles
     * @param featureType the feature type associated with the rule
     * @param updateRuleAction the action to execute for updating a rule
     */
    public RestUpdateRuleAction(String name, List<Route> routes, FeatureType featureType, ActionType<UpdateRuleResponse> updateRuleAction) {
        this.name = name;
        this.routes = routes;
        this.featureType = featureType;
        this.updateRuleAction = updateRuleAction;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public List<Route> routes() {
        return routes;
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            Builder builder = Builder.fromXContent(parser, featureType);
            UpdateRuleRequest updateRuleRequest = new UpdateRuleRequest(
                request.param(_ID_STRING),
                builder.getDescription(),
                builder.getAttributeMap(),
                builder.getFeatureValue(),
                featureType
            );
            return channel -> client.execute(updateRuleAction, updateRuleRequest, updateRuleResponse(channel));
        }
    }

    private RestResponseListener<UpdateRuleResponse> updateRuleResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final UpdateRuleResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
