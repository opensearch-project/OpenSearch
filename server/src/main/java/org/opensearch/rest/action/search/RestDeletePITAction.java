/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.search;

import org.opensearch.action.search.DeletePITRequest;
import org.opensearch.action.search.DeletePITResponse;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestStatusToXContentListener;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.POST;

public class RestDeletePITAction extends BaseRestHandler {
    @Override
    public String getName() {
        return "delete_pit_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        DeletePITRequest deletePitRequest = new DeletePITRequest(request.param("pit_id"));
        return channel -> client.deletePit(deletePitRequest, new RestStatusToXContentListener<DeletePITResponse>(channel));
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(Collections.singletonList(
            new Route(POST, "/_pit/{pit_id}")));
    }

}
