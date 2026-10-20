/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.datastream.DataStreamAction;
import org.opensearch.action.admin.indices.datastream.ModifyDataStreamsAction;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * REST action to add or remove backing indices of a data stream.
 *
 * @opensearch.api
 */
public class RestModifyDataStreamsAction extends BaseRestHandler {

    @Override
    public String getName() {
        return "modify_data_stream_action";
    }

    @Override
    public List<Route> routes() {
        return Collections.singletonList(new Route(RestRequest.Method.POST, "/_data_stream/_modify"));
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        List<DataStreamAction> actions;
        try (XContentParser parser = request.contentParser()) {
            actions = parseActions(parser);
        }
        ModifyDataStreamsAction.Request modifyRequest = new ModifyDataStreamsAction.Request(actions);
        modifyRequest.clusterManagerNodeTimeout(request.paramAsTime("cluster_manager_timeout", modifyRequest.clusterManagerNodeTimeout()));
        modifyRequest.timeout(request.paramAsTime("timeout", modifyRequest.timeout()));
        return channel -> client.execute(ModifyDataStreamsAction.INSTANCE, modifyRequest, new RestToXContentListener<>(channel));
    }

    private static List<DataStreamAction> parseActions(XContentParser parser) throws IOException {
        List<DataStreamAction> actions = new ArrayList<>();
        XContentParser.Token token = parser.nextToken();
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("expected an object with an [actions] array");
        }
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if ("actions".equals(currentFieldName) && token == XContentParser.Token.START_ARRAY) {
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    actions.add(DataStreamAction.fromXContent(parser));
                }
            } else {
                throw new IllegalArgumentException("unexpected field [" + currentFieldName + "], only [actions] is supported");
            }
        }
        return actions;
    }
}
