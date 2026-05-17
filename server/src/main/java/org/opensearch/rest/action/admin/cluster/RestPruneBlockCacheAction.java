/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.blockcache.PruneBlockCacheAction;
import org.opensearch.action.admin.cluster.blockcache.PruneBlockCacheRequest;
import org.opensearch.core.common.Strings;
import org.opensearch.plugins.BlockCacheConstants;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static java.util.Collections.singletonList;

/**
 * REST handler for {@code POST /_blockcache/prune?cache=<name>}.
 *
 * <p>The {@code cache} parameter is required and must match a registered block cache name.
 * Currently the only valid value is {@code "disk"} (Foyer). Omitting the parameter or
 * supplying an unknown value returns a 400.
 *
 * @opensearch.api
 */
public class RestPruneBlockCacheAction extends BaseRestHandler {

    private static final Set<String> VALID_CACHE_NAMES = Set.of(BlockCacheConstants.DISK_CACHE);

    @Override
    public List<Route> routes() {
        return singletonList(new Route(RestRequest.Method.POST, "/_blockcache/prune"));
    }

    @Override
    public String getName() {
        return "prune_blockcache_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String cacheName = request.param("cache");

        if (Strings.isNullOrEmpty(cacheName)) {
            throw new IllegalArgumentException(
                "Parameter [cache] is required. Valid values are: " + VALID_CACHE_NAMES
            );
        }
        if (VALID_CACHE_NAMES.contains(cacheName) == false) {
            throw new IllegalArgumentException(
                "Unknown cache [" + cacheName + "]. Valid values are: " + VALID_CACHE_NAMES
            );
        }

        String[] nodeIds = parseNodeIds(request);
        PruneBlockCacheRequest pruneRequest = nodeIds != null
            ? new PruneBlockCacheRequest(cacheName, nodeIds)
            : new PruneBlockCacheRequest(cacheName);
        pruneRequest.timeout(request.paramAsTime("timeout", pruneRequest.timeout()));

        return channel -> client.execute(PruneBlockCacheAction.INSTANCE, pruneRequest, new RestToXContentListener<>(channel));
    }

    private String[] parseNodeIds(RestRequest request) {
        String nodes = request.param("nodes");
        if (nodes != null && nodes.isBlank() == false) {
            return Strings.splitStringByCommaToArray(nodes);
        }
        return null;
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
