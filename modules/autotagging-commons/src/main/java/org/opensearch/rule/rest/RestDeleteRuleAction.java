/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rule.rest;

import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.rule.action.DeleteRuleAction;
import org.opensearch.rule.action.DeleteRuleRequest;
import org.opensearch.rule.autotagging.FeatureType;
import org.opensearch.transport.client.node.NodeClient;

import java.util.List;

import static org.opensearch.rest.RestRequest.Method.DELETE;
import static org.opensearch.rule.autotagging.Rule.ID_STRING;
import static org.opensearch.rule.rest.RestGetRuleAction.FEATURE_TYPE;

/**
 * Rest action to delete a Rule
 * @opensearch.experimental
 */
@ExperimentalApi
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
        return List.of(new RestHandler.Route(DELETE, "_rules/{featureType}/{id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) {
        final String ruleId = request.param(ID_STRING);
        FeatureType featureType = FeatureType.from(request.param(FEATURE_TYPE));
        DeleteRuleRequest deleteRuleRequest = new DeleteRuleRequest(ruleId, featureType);
        return channel -> client.execute(DeleteRuleAction.INSTANCE, deleteRuleRequest, deleteRuleResponse(channel));
    }

    private RestResponseListener<AcknowledgedResponse> deleteRuleResponse(final RestChannel channel) {
        return new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final AcknowledgedResponse response) throws Exception {
                return new BytesRestResponse(
                    RestStatus.OK,
                    channel.newBuilder().startObject().field("acknowledged", response.isAcknowledged()).endObject()
                );
            }
        };
    }
}
