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
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.rule.DeleteRuleRequest;
import org.opensearch.rule.DeleteRuleResponse;
import org.opensearch.rule.action.DeleteRuleAction;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

import static org.opensearch.rest.RestRequest.Method.DELETE;

/**
 * Rest action to delete a Rule
 * @opensearch.experimental
 */
public class RestDeleteRuleAction extends BaseRestHandler {
    /**
     * Constructor for RestDeleteRuleAction
     */
    public RestDeleteRuleAction() {}

    @Override
    public String getName() {
        return "delete_rule";
    }

    @Override
    public List<Route> routes() {
        return List.of(new RestHandler.Route(DELETE, "_rules/{featureType}/{ruleId}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final String ruleId = request.param("ruleId");
        FeatureType featureType = FeatureType.from(request.param("featureType"));
        DeleteRuleRequest deleteRuleRequest = new DeleteRuleRequest(ruleId, featureType);
        return channel -> client.execute(DeleteRuleAction.INSTANCE, deleteRuleRequest, deleteRuleResponse(channel));
    }

    private RestResponseListener<DeleteRuleResponse> deleteRuleResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final DeleteRuleResponse response) throws Exception {
                return new BytesRestResponse(RestStatus.OK, response.toXContent(channel.newBuilder(), ToXContent.EMPTY_PARAMS));
            }
        };
    }
}
