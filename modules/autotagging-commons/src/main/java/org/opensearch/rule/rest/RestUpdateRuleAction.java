/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.rest;

import org.opensearch.common.annotation.ExperimentalApi;
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
import org.opensearch.rule.action.UpdateRuleAction;
import org.opensearch.rule.action.UpdateRuleRequest;
import org.opensearch.rule.action.UpdateRuleResponse;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.rule.autotagging.Rule.Builder;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.PUT;
import static org.opensearch.rule.autotagging.Rule.ID_STRING;
import static org.opensearch.rule.rest.RestGetRuleAction.FEATURE_TYPE;

/**
 * Rest action to update a Rule
 * @opensearch.experimental
 */
@ExperimentalApi
public class RestUpdateRuleAction extends BaseRestHandler {
    /**
     * constructor for RestUpdateRuleAction
     */
    public RestUpdateRuleAction() {}

    @Override
    public String getName() {
        return "update_rule";
    }

    @Override
    public List<Route> routes() {
        return List.of(new RestHandler.Route(PUT, "_rules/{featureType}/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final FeatureType featureType = FeatureType.from(request.param(FEATURE_TYPE));
        try (XContentParser parser = request.contentParser()) {
            Builder builder = Builder.fromXContent(parser, featureType);
            UpdateRuleRequest updateRuleRequest = new UpdateRuleRequest(
                request.param(ID_STRING),
                builder.getDescription(),
                builder.getAttributeMap(),
                builder.getFeatureValue(),
                featureType
            );
            return channel -> client.execute(UpdateRuleAction.INSTANCE, updateRuleRequest, updateRuleResponse(channel));
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
