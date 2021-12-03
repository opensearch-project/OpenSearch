/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.search;

import org.opensearch.action.search.CreatePITAction;
import org.opensearch.action.search.PITRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.Strings;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestCancellableNodeClient;
import org.opensearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;

public class RestCreatePITAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "create_pit_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        PITRequest pitRequest = new PITRequest(request.paramAsTime("keep_alive", null));
        pitRequest.setIndicesOptions(IndicesOptions.fromRequest(request, pitRequest.indicesOptions()));
        pitRequest.setPreference(request.param("preference"));
        pitRequest.setRouting(request.param("routing"));
        pitRequest.setIndices(Strings.splitStringByCommaToArray(request.param("index")));
        return channel -> {
            RestCancellableNodeClient cancelClient = new RestCancellableNodeClient(client, request.getHttpChannel());
            cancelClient.execute(CreatePITAction.INSTANCE, pitRequest, new RestStatusToXContentListener<>(channel));
        };
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(Collections.singletonList(
            new Route(POST, "/{index}/_pit")));
    }

}


