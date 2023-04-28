/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.node.filecache.clear.ClearNodesFileCacheRequest;
import org.opensearch.client.node.NodeClient;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestActions;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * Transport action to clear indices cache
 *
 * @opensearch.api
 */
public class RestClearNodesFileCacheAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return singletonList(new Route(POST, "/_filecache/clear"));
    }

    @Override
    public String getName() {
        return "clear_nodes_filecache_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        return channel -> client.admin()
            .cluster()
            .clearFileCache(new ClearNodesFileCacheRequest(), new RestActions.NodesResponseRestListener<>(channel));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

}
