/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.snapshots.status.SnapshotIndexStatusRequest;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * REST handler for the paginated snapshot index status API.
 *
 * <pre>
 * GET /_snapshot/{repository}/{snapshot}/_list/indices
 *   ?from=0          (default 0)
 *   &amp;size=20         (default 20, max 200)
 *   &amp;cluster_manager_timeout=...
 * </pre>
 *
 * @opensearch.api
 */
public class RestSnapshotIndexStatusAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestSnapshotIndexStatusAction.class);

    @Override
    public List<Route> routes() {
        return singletonList(new Route(GET, "/_snapshot/{repository}/{snapshot}/_list/indices"));
    }

    @Override
    public String getName() {
        return "snapshot_index_status_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        SnapshotIndexStatusRequest snapshotIndexStatusRequest = new SnapshotIndexStatusRequest(
            request.param("repository"),
            request.param("snapshot")
        );
        snapshotIndexStatusRequest.from(request.paramAsInt("from", SnapshotIndexStatusRequest.DEFAULT_FROM));
        snapshotIndexStatusRequest.size(request.paramAsInt("size", SnapshotIndexStatusRequest.DEFAULT_SIZE));
        snapshotIndexStatusRequest.clusterManagerNodeTimeout(
            request.paramAsTime("cluster_manager_timeout", snapshotIndexStatusRequest.clusterManagerNodeTimeout())
        );
        parseDeprecatedMasterTimeoutParameter(snapshotIndexStatusRequest, request, deprecationLogger, getName());
        return channel -> client.execute(
            org.opensearch.action.admin.cluster.snapshots.status.SnapshotIndexStatusAction.INSTANCE,
            snapshotIndexStatusRequest,
            new RestToXContentListener<>(channel)
        );
    }
}
