/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.wlm.rule.rest;

import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.plugin.wlm.rule.action.DeleteRuleAction;
import org.opensearch.plugin.wlm.rule.action.DeleteRuleRequest;
import org.opensearch.plugin.wlm.rule.action.DeleteRuleResponse;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static org.opensearch.autotagging.Rule._ID_STRING;
import static org.opensearch.rest.RestRequest.Method.DELETE;

/**
 * Rest action to delete a Rule
 * @opensearch.experimental
 */
public class RestDeleteRuleAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "delete_rule";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(DELETE, "_wlm/rule/{_id}"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        final String id = request.param(_ID_STRING);
        final DeleteRuleRequest deleteRuleRequest = new DeleteRuleRequest(id);
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
