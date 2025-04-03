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
import org.opensearch.autotagging.Rule;
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
public abstract class RestCreateRuleAction extends BaseRestHandler {
    /**
     * constructor for RestCreateRuleAction
     */
    public RestCreateRuleAction() {}

    @Override
    public abstract String getName();

    @Override
    public abstract List<Route> routes();

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        try (XContentParser parser = request.contentParser()) {
            Builder builder = Builder.fromXContent(parser, retrieveFeatureTypeInstance());
            CreateRuleRequest createRuleRequest = buildCreateRuleRequest(builder.updatedAt(Instant.now().toString()).build());
            return channel -> client.execute(retrieveCreateRuleActionInstance(), createRuleRequest, createRuleResponse(channel));
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

    /**
     * Abstract method for subclasses to provide specific ActionType Instance
     */
    protected abstract <T extends ActionType<? extends CreateRuleResponse>> T retrieveCreateRuleActionInstance();

    /**
     * Abstract method for subclasses to provide specific FeatureType Instance
     */
    protected abstract FeatureType retrieveFeatureTypeInstance();

    /**
     * Abstract method for subclasses to construct a {@link CreateRuleRequest}. This method allows subclasses
     * to define their own request-building logic depending on their specific needs.
     * @param rule - the rule to create
     */
    protected abstract CreateRuleRequest buildCreateRuleRequest(Rule rule);
}
