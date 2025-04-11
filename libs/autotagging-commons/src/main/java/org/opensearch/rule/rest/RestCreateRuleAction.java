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
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.rule.action.CreateRuleRequest;
import org.opensearch.rule.action.CreateRuleResponse;
import org.opensearch.transport.client.node.NodeClient;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.List;

/**
 * Rest action to create a Rule
 * @opensearch.experimental
 */
public class RestCreateRuleAction extends BaseRestHandler {
    private final String name;
    private final List<Route> routes;
    private final FeatureType featureType;
    private final ActionType<CreateRuleResponse> createRuleAction;

    /**
     * constructor for RestUpdateRuleAction
     * @param name - RestUpdateRuleAction name
     * @param routes the list of REST routes this action handles
     * @param featureType the feature type associated with the rule
     * @param createRuleAction the action to execute for updating a rule
     */
    public RestCreateRuleAction(String name, List<Route> routes, FeatureType featureType, ActionType<CreateRuleResponse> createRuleAction) {
        this.name = name;
        this.routes = routes;
        this.featureType = featureType;
        this.createRuleAction = createRuleAction;
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
            CreateRuleRequest createRuleRequest = new CreateRuleRequest(builder.updatedAt(Instant.now().toString()).build());
            return channel -> client.execute(createRuleAction, createRuleRequest, createRuleResponse(channel));
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
