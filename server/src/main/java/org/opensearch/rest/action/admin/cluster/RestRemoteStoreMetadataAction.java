/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.remotestore.metadata.RemoteStoreMetadataRequest;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Rest action for fetching remote store metadata
 *
 * @opensearch.internal
 */
public class RestRemoteStoreMetadataAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return unmodifiableList(
            asList(new Route(GET, "/_remotestore/metadata/{index}"), new Route(GET, "/_remotestore/metadata/{index}/{shard_id}"))
        );
    }

    @Override
    public String getName() {
        return "remote_store_metadata";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String index = request.param("index");
        String shardId = request.param("shard_id");

        RemoteStoreMetadataRequest metadataRequest = new RemoteStoreMetadataRequest();
        if (index != null) {
            metadataRequest.indices(index);
        }
        if (shardId != null) {
            metadataRequest.shards(shardId);
        }

        return channel -> client.admin().cluster().remoteStoreMetadata(metadataRequest, new RestToXContentListener<>(channel));
    }
}
