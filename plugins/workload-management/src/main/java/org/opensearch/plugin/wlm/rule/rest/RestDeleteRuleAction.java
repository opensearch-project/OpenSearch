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
import org.opensearch.plugin.wlm.rule.action.*;
import org.opensearch.rest.*;
import org.opensearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.List;

import static org.opensearch.rest.RestRequest.Method.DELETE;

/**
 * Rest action to delete a Rule
 *
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

    /**
     * The list of {@link Route}s that this RestHandler is responsible for handling.
     */
    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "_wlm/rule/{_id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final DeleteRuleRequest deleteRuleRequest = new DeleteRuleRequest(request.param("_id"));
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
