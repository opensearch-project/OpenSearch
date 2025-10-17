/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.rule.action.CreateRuleAction;
import org.opensearch.rule.action.CreateRuleRequest;
import org.opensearch.rule.action.CreateRuleResponse;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule.Builder;
import org.opensearch.transport.client.node.NodeClient;
import org.joda.time.Instant;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.PUT;
import static org.opensearch.rule.rest.RestGetRuleAction.FEATURE_TYPE;

/**
 * Rest action to create a Rule
 * @opensearch.experimental
 */
public class RestCreateRuleAction extends BaseRestHandler {
    /**
     * constructor for RestCreateRuleAction
     */
    public RestCreateRuleAction() {}

    @Override
    public String getName() {
        return "create_rule";
    }

    @Override
    public List<Route> routes() {
        return List.of(new RestHandler.Route(PUT, "_rules/{featureType}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final FeatureType featureType = FeatureType.from(request.param(FEATURE_TYPE));
        try (XContentParser parser = request.contentParser()) {
            Builder builder = Builder.fromXContent(parser, featureType);
            CreateRuleRequest createRuleRequest = new CreateRuleRequest(builder.updatedAt(Instant.now().toString()).id().build());
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
