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
import org.opensearch.rule.action.UpdateRuleRequest;
import org.opensearch.rule.action.UpdateRuleResponse;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.opensearch.autotagging.Rule._ID_STRING;

/**
 * Rest action to update a Rule
 * @opensearch.experimental
 */
public abstract class RestUpdateRuleAction extends BaseRestHandler {
    /**
     * constructor for RestUpdateRuleAction
     */
    public RestUpdateRuleAction() {}

    @Override
    public abstract String getName();

    @Override
    public abstract List<Route> routes();

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            Builder builder = Builder.fromXContent(parser, retrieveFeatureTypeInstance());
            UpdateRuleRequest updateRuleRequest = buildUpdateRuleRequest(
                request.param(_ID_STRING),
                builder.getDescription(),
                builder.getAttributeMap(),
                builder.getFeatureValue()
            );
            return channel -> client.execute(retrieveUpdateRuleActionInstance(), updateRuleRequest, updateRuleResponse(channel));
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

    /**
     * Abstract method for subclasses to provide specific ActionType Instance
     */
    protected abstract <T extends ActionType<UpdateRuleResponse>> T retrieveUpdateRuleActionInstance();

    /**
     * Abstract method for subclasses to provide specific FeatureType Instance
     */
    protected abstract FeatureType retrieveFeatureTypeInstance();

    /**
     * Abstract method for subclasses to provide implementation to updateRuleRequest
     * @param id - rule id to update
     * @param description - rule description to update
     * @param attributeMap - rule attributes to update
     * @param featureValue - rule feature value to update
     */
    protected abstract UpdateRuleRequest buildUpdateRuleRequest(
        String id,
        String description,
        Map<Attribute, Set<String>> attributeMap,
        String featureValue
    );
}
